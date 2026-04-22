package com.lab.account.consumer;

import com.lab.account.entity.Account;
import com.lab.account.service.CompensationService;
import com.lab.account.service.CreditService;
import com.lab.account.service.IdempotencyService;
import com.lab.account.producer.AccountEventProducer;
import com.lab.common.constants.KafkaTopics;
import com.lab.common.event.TransferDebitedEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.MDC;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

/**
 * Consumer del evento TRANSFER_DEBITED → aplica el crédito en la cuenta destino.
 *
 * POSICIÓN EN EL SAGA:
 *
 *   [DebitConsumer] ──► transfer.debited ──► [CreditConsumer] ◄── AQUÍ
 *                                                   │
 *                              ┌────────────────────┴────────────────────┐
 *                         (éxito)                                  (fallo)
 *                              │                                         │
 *                    transfer.credited                     compensar + transfer.failed
 *                    (saga completada)                     (saga rollback)
 *
 * OBSERVACIÓN IMPORTANTE: account-service publica transfer.debited Y también
 * lo consume (en este consumer). ¿No es un loop?
 * NO, porque usa un Consumer Group DIFERENTE (account-credit-group).
 * Kafka entrega el mensaje a TODOS los grupos suscritos al topic.
 * account-debit-group NO está suscrito a transfer.debited → no lo recibe.
 * account-credit-group está suscrito → sí lo recibe.
 *
 * FLUJO DE COMPENSACIÓN (cuando simulate-credit-failure=true):
 *   1. CreditService lanza SimulatedCreditFailureException
 *   2. Este consumer llama compensationService.compensate() directamente (in-process)
 *   3. CompensationService restaura el saldo de fromAccount
 *   4. Este consumer publica transfer.compensated (saldo restaurado)
 *   5. Este consumer publica transfer.failed (razón del fallo)
 *   6. ACK → Kafka no reenvía este mensaje
 *
 * ¿Por qué la compensación es in-process y no via un nuevo topic Kafka?
 * Simplifica el Saga para el lab. En producción con múltiples servicios
 * y pasos complejos, la compensación coordinaría via eventos Kafka con
 * un CompensationConsumer dedicado (o un Saga Orchestrator).
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class CreditConsumer {

    private final IdempotencyService idempotencyService;
    private final CreditService creditService;
    private final CompensationService compensationService;
    private final AccountEventProducer producer;

    /**
     * Procesa un evento TRANSFER_DEBITED aplicando el crédito o iniciando compensación.
     *
     * @param event           evento con los datos del débito completado
     * @param acknowledgment  para confirmar manualmente el offset
     */
    @KafkaListener(
            topics  = KafkaTopics.TRANSFER_DEBITED,
            groupId = "account-credit-group"
    )
    public void onTransferDebited(
            @Payload TransferDebitedEvent event,
            Acknowledgment acknowledgment) {

        String eventKey = event.getTransactionId() + ":TRANSFER_DEBITED";

        // ── CORRELACIÓN (MDC) ────────────────────────────────────────────────
        MDC.put("txId", event.getTransactionId());

        log.info("📨 TRANSFER_DEBITED recibido | key={} from={} to={} amount={}",
                eventKey, event.getFromAccount(), event.getToAccount(), event.getAmount());

        try {
            // ── IDEMPOTENCIA ─────────────────────────────────────────────────
            if (!idempotencyService.tryRegister(eventKey)) {
                log.warn("⚠️ Evento duplicado — ignorando crédito | key={}", eventKey);
                acknowledgment.acknowledge();
                return;
            }

            // ── CRÉDITO ──────────────────────────────────────────────────────
            // Si simulate-credit-failure=true, lanza SimulatedCreditFailureException.
            // Si toAccount no existe (muy improbable, ya validado), lanza AccountNotFoundException.
            Account updated = creditService.credit(event.getToAccount(), event.getAmount());

            // ── SAGA COMPLETADA ───────────────────────────────────────────────
            // El crédito fue exitoso. Publicar el evento final del camino feliz.
            // status-service consumirá TRANSFER_CREDITED y marcará la transacción
            // como COMPLETED en su DB.
            producer.publishCredited(event, updated.getBalance());

            acknowledgment.acknowledge();
            log.info("🎉 Saga completada exitosamente | txId={}", event.getTransactionId());

        } catch (CreditService.SimulatedCreditFailureException e) {
            // ── INICIO DEL ROLLBACK DEL SAGA ─────────────────────────────────
            log.warn("⚠️ Crédito fallido (simulado) — iniciando compensación | txId={} | reason={}",
                    event.getTransactionId(), e.getMessage());

            handleCreditFailure(event, e.getMessage(), acknowledgment);

        } catch (CreditService.AccountNotFoundException e) {
            // La cuenta destino no existe. Esto no debería ocurrir si validation-service
            // verificó correctamente, pero la cuenta pudo cerrarse entre la validación y el crédito.
            log.error("❌ Cuenta destino no encontrada en crédito | txId={} | account={}",
                    event.getTransactionId(), event.getToAccount());

            handleCreditFailure(event, e.getMessage(), acknowledgment);

        } catch (Exception e) {
            // Error inesperado (bug, DB caída).
            // NO ACK → Kafka reentrega para reintento.
            // NOTA: la idempotencia ya insertó el registro → el reintento lo filtrará
            // como duplicado sin reintentar el crédito.
            log.error("❌ Error inesperado procesando TRANSFER_DEBITED | key={} | error={}",
                    eventKey, e.getMessage(), e);
            // SIN acknowledgment.acknowledge() → Kafka reentrega

        } finally {
            MDC.remove("txId");
        }
    }

    /**
     * Maneja el fallo del crédito aplicando compensación y publicando los eventos correspondientes.
     *
     * Flujo:
     *   1. Llamar a compensationService para restaurar el saldo de fromAccount
     *   2. Si la compensación fue exitosa → publicar transfer.compensated
     *   3. Siempre → publicar transfer.failed
     *   4. Hacer ACK (el crédito y la compensación son finales — no reintentables via Kafka)
     */
    private void handleCreditFailure(TransferDebitedEvent event, String failureReason, Acknowledgment acknowledgment) {
        try {
            // Intentar compensar: devolver el dinero a fromAccount
            Account compensated = compensationService.compensate(
                    event.getTransactionId(),
                    event.getFromAccount(),
                    event.getAmount()
            );

            if (compensated != null) {
                // Compensación exitosa: publicar el evento de rollback completado
                producer.publishCompensated(
                        event,
                        "Compensación por fallo en crédito: " + failureReason,
                        compensated.getBalance()
                );
                log.info("🔄 Compensación publicada | txId={} fromAccount={} balanceRestaurado={}",
                        event.getTransactionId(), event.getFromAccount(), compensated.getBalance());
            } else {
                // La compensación ya fue aplicada antes (idempotencia activada)
                log.warn("⚠️ Compensación ya aplicada previamente | txId={}", event.getTransactionId());
            }

        } catch (Exception compensationEx) {
            // La compensación falló. Esto es un estado crítico:
            // - El débito fue aplicado
            // - El crédito falló
            // - La compensación también falló → el dinero está "perdido" temporalmente
            // En producción: alertar inmediatamente al equipo de operaciones.
            // Para el lab: loggear el error crítico y publicar FAILED de todas formas.
            log.error("🚨 CRÍTICO: Compensación fallida | txId={} fromAccount={} | " +
                    "REQUIERE INTERVENCIÓN MANUAL | compensationError={}",
                    event.getTransactionId(), event.getFromAccount(), compensationEx.getMessage(), compensationEx);
        }

        // Publicar el fallo independientemente del resultado de la compensación
        // status-service actualizará el estado de la transacción a FAILED
        producer.publishFailed(
                event.getTransactionId(),
                event.getFromAccount(),
                event.getToAccount(),
                event.getAmount(),
                failureReason,
                "account-service-credit"
        );

        // ACK: el mensaje fue procesado (aunque el resultado sea un fallo controlado)
        // No tiene sentido que Kafka reentregue — el fallo es determinístico mientras
        // simulate-credit-failure=true esté activado.
        acknowledgment.acknowledge();
    }
}
