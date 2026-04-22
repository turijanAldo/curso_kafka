package com.lab.account.consumer;

import com.lab.account.entity.Account;
import com.lab.account.service.DebitService;
import com.lab.account.service.IdempotencyService;
import com.lab.account.producer.AccountEventProducer;
import com.lab.common.constants.KafkaTopics;
import com.lab.common.event.TransferValidatedEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.MDC;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.orm.ObjectOptimisticLockingFailureException;
import org.springframework.stereotype.Component;

/**
 * Consumer del evento TRANSFER_VALIDATED → aplica el débito en la cuenta origen.
 *
 * POSICIÓN EN EL SAGA:
 *
 *   transfer-api ──► transfer.requested ──► [validation-service]
 *                                                   │
 *                                          transfer.validated
 *                                                   │
 *                                               [DebitConsumer] ◄── AQUÍ
 *                                                   │
 *                                          transfer.debited ──► [CreditConsumer]
 *
 * GRUPO DE CONSUMERS: account-debit-group
 * Separado de account-credit-group porque son responsabilidades distintas.
 * Si ambos usaran el mismo grupo y el mismo topic (lo cual no ocurre aquí),
 * Kafka asignaría las particiones entre ambos — cada uno vería solo la mitad
 * de los mensajes. Al usar grupos distintos sobre topics distintos, Kafka
 * los trata de forma independiente.
 *
 * DECISIÓN: MANUAL_IMMEDIATE ACK
 * El offset solo se confirma cuando sabemos con certeza qué pasó con el mensaje:
 *   - Duplicado: confirmar para no volver a recibir el mismo duplicado
 *   - Débito exitoso: confirmar y avanzar el Saga
 *   - Saldo insuficiente: publicar FAILED y confirmar (no tiene sentido reintentar)
 *   - Conflicto optimista: NO confirmar → Kafka reentrega para reintento
 *   - Error inesperado: NO confirmar → Kafka reentrega
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class DebitConsumer {

    private final IdempotencyService idempotencyService;
    private final DebitService debitService;
    private final AccountEventProducer producer;

    /**
     * Procesa un evento TRANSFER_VALIDATED aplicando el débito correspondiente.
     *
     * @param event           evento con los datos de la transferencia validada
     * @param acknowledgment  para confirmar manualmente el offset
     */
    @KafkaListener(
            topics  = KafkaTopics.TRANSFER_VALIDATED,
            groupId = "account-debit-group"
    )
    public void onTransferValidated(
            @Payload TransferValidatedEvent event,
            Acknowledgment acknowledgment) {

        String eventKey = event.getTransactionId() + ":TRANSFER_VALIDATED";

        // ── CORRELACIÓN (MDC) ────────────────────────────────────────────────
        MDC.put("txId", event.getTransactionId());

        log.info("📨 TRANSFER_VALIDATED recibido | key={} from={} to={} amount={}",
                eventKey, event.getFromAccount(), event.getToAccount(), event.getAmount());

        try {
            // ── IDEMPOTENCIA ─────────────────────────────────────────────────
            if (!idempotencyService.tryRegister(eventKey)) {
                log.warn("⚠️ Evento duplicado — ignorando débito | key={}", eventKey);
                acknowledgment.acknowledge();
                return;
            }

            // ── DÉBITO ───────────────────────────────────────────────────────
            // @Transactional en DebitService → READ + MODIFY + SAVE atómicos
            // Si otro thread modificó la cuenta entre el READ y el SAVE →
            // OptimisticLockingFailureException → capturado abajo
            Account updated = debitService.debit(event.getFromAccount(), event.getAmount());

            // ── PUBLICAR RESULTADO ───────────────────────────────────────────
            // El evento TRANSFER_DEBITED lleva el remainingBalance para que
            // CreditConsumer y status-service tengan el estado actualizado
            // sin necesidad de consultar la DB (Event-Carried State Transfer).
            producer.publishDebited(event, updated.getBalance());

            // ── ACK ──────────────────────────────────────────────────────────
            acknowledgment.acknowledge();
            log.debug("✅ Offset confirmado tras débito exitoso | key={}", eventKey);

        } catch (DebitService.InsufficientFundsException e) {
            // Saldo insuficiente en el momento del débito.
            // Puede ocurrir si el saldo cambió entre la validación (en validation-service)
            // y este débito (race condition muy poco frecuente).
            // La idempotencia ya registró el evento → aunque Kafka reentregue, no reintenta.
            log.warn("💔 Saldo insuficiente en momento de débito | key={} | {}", eventKey, e.getMessage());
            producer.publishFailed(
                    event.getTransactionId(),
                    event.getFromAccount(),
                    event.getToAccount(),
                    event.getAmount(),
                    e.getMessage(),
                    "account-service-debit"
            );
            acknowledgment.acknowledge();  // No tiene sentido reintentar con el mismo saldo

        } catch (ObjectOptimisticLockingFailureException e) {
            // Conflicto de versión: otro thread modificó la cuenta entre el findById y el save.
            // IMPORTANTE: la idempotencia YA insertó el registro (REQUIRES_NEW committéd).
            // En el reintento de Kafka, la idempotencia filtrará el mensaje como duplicado.
            // TRADEOFF DEL LAB: en producción se usaría el Outbox Pattern para evitar este caso.
            log.warn("⚡ Conflicto de Optimistic Locking | key={} | el mensaje será filtrado por idempotencia en reintento",
                    eventKey, e);
            // NO ACK → Kafka reentrega. La idempotencia lo filtrará como duplicado.
            // Para este lab, es un comportamiento aceptable dado que demuestra el mecanismo.

        } catch (Exception e) {
            // Error inesperado (bug de código, DB caída, etc.)
            log.error("❌ Error inesperado procesando TRANSFER_VALIDATED | key={} | error={}",
                    eventKey, e.getMessage(), e);
            // NO ACK → Kafka reentrega cuando el servicio se recupere

        } finally {
            MDC.remove("txId");
        }
    }
}
