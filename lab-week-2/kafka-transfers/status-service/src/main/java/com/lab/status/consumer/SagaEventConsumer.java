package com.lab.status.consumer;

import com.lab.common.constants.KafkaTopics;
import com.lab.common.enums.TransferStatus;
import com.lab.common.event.SagaEvent;
import com.lab.common.event.TransferCompensatedEvent;
import com.lab.common.event.TransferCreditedEvent;
import com.lab.common.event.TransferDebitedEvent;
import com.lab.common.event.TransferFailedEvent;
import com.lab.common.event.TransferValidatedEvent;
import com.lab.status.service.StatusService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.MDC;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

/**
 * Consumer Kafka de status-service.
 *
 * Escucha TODOS los eventos del Saga en un único @KafkaListener con múltiples topics.
 * Para cada evento, extrae el transactionId, determina el nuevo estado y delega
 * a StatusService para actualizar la tabla transactions.
 *
 * DISEÑO: ¿Por qué un único @KafkaListener en lugar de uno por topic?
 *
 * Alternativa 1 — Un listener por topic:
 *   @KafkaListener(topics = TRANSFER_VALIDATED) → onValidated()
 *   @KafkaListener(topics = TRANSFER_DEBITED)   → onDebited()
 *   etc.
 *   ✅ Tipo del parámetro garantizado
 *   ❌ 5 métodos con lógica casi idéntica (extraer txId, llamar a updateStatus)
 *   ❌ Si se agrega un topic nuevo, hay que agregar un método nuevo
 *
 * Alternativa 2 — Un único listener con múltiples topics (esta implementación):
 *   @KafkaListener(topics = { ... todos ... })
 *   ✅ Un solo punto de entrada → más fácil de rastrear en logs
 *   ✅ La lógica de "qué topic → qué status" está centralizada
 *   ✅ Agregar un nuevo topic solo requiere un nuevo caso en el instanceof chain
 *   ✅ El mismo grupo de consumers (status-group) maneja todos los topics
 *   ❌ Requiere instanceof para determinar el tipo — menos type-safe que Alternativa 1
 *
 * ¿Por qué instanceof funciona aquí?
 * El JsonSerializer del producer agrega un header __TypeId__ con el nombre de clase
 * en cada mensaje. El JsonDeserializer del consumer lee ese header y deserializa al
 * tipo correcto. Al recibir Object en el método, en runtime el objeto ya es del tipo
 * específico (TransferValidatedEvent, TransferDebitedEvent, etc.).
 *
 * Esto es posible porque common es una dependencia de todos los servicios —
 * las clases de eventos están en el classpath tanto del producer como del consumer.
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class SagaEventConsumer {

    private final StatusService statusService;

    /**
     * Punto de entrada único para todos los eventos del Saga.
     *
     * @param record ConsumerRecord con la clave (fromAccount), el valor (evento)
     *               y el topic de origen. El tipo del valor es Object porque
     *               el mismo método maneja múltiples tipos de eventos.
     * @param acknowledgment para confirmar el offset manualmente
     */
    @KafkaListener(
            topics = {
                KafkaTopics.TRANSFER_VALIDATED,
                KafkaTopics.TRANSFER_FAILED,
                KafkaTopics.TRANSFER_DEBITED,
                KafkaTopics.TRANSFER_CREDITED,
                KafkaTopics.TRANSFER_COMPENSATED
            },
            groupId = "status-group"
    )
    public void onSagaEvent(ConsumerRecord<String, Object> record, Acknowledgment acknowledgment) {

        Object event = record.value();
        String topic = record.topic();

        // ── CORRELACIÓN (MDC) ────────────────────────────────────────────────
        // SagaEvent es la interfaz común de todos los eventos del Saga.
        // Gracias a ella, extraemos el txId de forma polimórfica sin encadenar
        // instanceof para cada tipo de evento solo para obtener el transactionId.
        if (event instanceof SagaEvent se) {
            MDC.put("txId", se.getTransactionId());
        }

        log.debug("📨 Evento recibido | topic={} partition={} offset={}",
                topic, record.partition(), record.offset());

        try {
            // Determinar la transición de estado según el tipo del evento.
            // El instanceof con pattern variable (Java 16+) extrae el objeto tipado
            // directamente, sin necesidad de casting explícito posterior.

            if (event instanceof TransferValidatedEvent e) {
                // validation-service validó todas las reglas → la saga avanza
                statusService.updateStatus(e.getTransactionId(), TransferStatus.VALIDATED, null);

            } else if (event instanceof TransferDebitedEvent e) {
                // account-service descontó el saldo de fromAccount → esperando crédito
                statusService.updateStatus(e.getTransactionId(), TransferStatus.DEBITED, null);

            } else if (event instanceof TransferCreditedEvent e) {
                // account-service acreditó toAccount → saga completada exitosamente
                // Saltamos el estado CREDITED y vamos directo a COMPLETED:
                // CREDITED es un estado interno del Saga; el estado visible al cliente es COMPLETED.
                statusService.updateStatus(e.getTransactionId(), TransferStatus.COMPLETED, null);

            } else if (event instanceof TransferCompensatedEvent e) {
                // El débito fue revertido porque el crédito falló → saga en rollback
                statusService.updateStatus(
                        e.getTransactionId(),
                        TransferStatus.ROLLED_BACK,
                        e.getCompensationReason()
                );

            } else if (event instanceof TransferFailedEvent e) {
                // La transferencia falló (en validación, débito o crédito) → estado final de error
                statusService.updateStatus(
                        e.getTransactionId(),
                        TransferStatus.FAILED,
                        e.getReason()
                );

            } else {
                // Tipo de evento desconocido. Puede ocurrir si se publica un tipo nuevo
                // sin actualizar este consumer. Loggeamos con el tipo real para facilitar
                // el debugging.
                log.warn("⚠️ Tipo de evento desconocido | topic={} type={} | ignorando",
                        topic, event != null ? event.getClass().getSimpleName() : "null");
            }

            // ACK siempre después de procesar: tanto en éxito como en tipo desconocido.
            // Si hay un error real (DB caída) lo capturamos en el catch.
            acknowledgment.acknowledge();

        } catch (Exception ex) {
            // Error inesperado: DB caída, bug de código, etc.
            // NO hacemos ACK → Kafka reentrega cuando el servicio se recupere.
            // La actualización de status es idempotente (setear el mismo estado dos veces = OK),
            // por lo que el reintento es seguro.
            log.error("❌ Error procesando evento del Saga | topic={} | error={}",
                    topic, ex.getMessage(), ex);

        } finally {
            MDC.remove("txId");
        }
    }
}
