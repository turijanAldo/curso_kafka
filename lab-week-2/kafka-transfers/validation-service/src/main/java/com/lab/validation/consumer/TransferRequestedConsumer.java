package com.lab.validation.consumer;

import com.lab.common.constants.KafkaTopics;
import com.lab.common.event.TransferRequestedEvent;
import com.lab.validation.service.IdempotencyService;
import com.lab.validation.service.ValidationService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.MDC;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

/**
 * Consumer Kafka de validation-service.
 *
 * Escucha el topic transfer.requested y orquesta el flujo de validación:
 *   1. Verificar idempotencia (¿ya procesamos este evento?)
 *   2. Si es nuevo: delegar a ValidationService
 *   3. Confirmar el offset (ACK) siempre al final — tanto en duplicados como en procesados
 *
 * ¿Por qué ACK siempre al final y no solo en éxito?
 * El ACK le dice a Kafka "ya terminé con este mensaje, no me lo vuelvas a enviar".
 * Si NO hacemos ACK en caso de duplicado, Kafka volvería a entregarlo en el próximo
 * poll — y entraríamos en un loop infinito de mensajes duplicados rechazados.
 * El ACK en caso de error (excepción inesperada) NO se hace — así Kafka reentrega
 * el mensaje y el servicio puede intentarlo de nuevo tras reiniciar.
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class TransferRequestedConsumer {

    private final IdempotencyService idempotencyService;
    private final ValidationService validationService;

    /**
     * Procesa un evento TRANSFER_REQUESTED.
     *
     * @KafkaListener:
     *   topics   → el topic que escucha. Constante de KafkaTopics para evitar typos.
     *   groupId  → el Consumer Group. Todos los mensajes del topic se distribuyen
     *              entre las instancias del mismo groupId. Si arrancas 2 instancias
     *              de validation-service, Kafka reparte las particiones entre ellas
     *              (load balancing automático).
     *
     * @Payload → extrae el valor del ConsumerRecord y lo deserializa a TransferRequestedEvent.
     *   Sin @Payload, Spring Kafka también funciona pero @Payload hace explícita
     *   la intención y permite combinar con otros parámetros (@Header, Acknowledgment).
     *
     * Acknowledgment → inyectado automáticamente por Spring Kafka cuando
     *   ack-mode = MANUAL_IMMEDIATE. Llamar a acknowledge() confirma el offset.
     *
     * @param event           el evento deserializado del mensaje Kafka
     * @param acknowledgment  para confirmar manualmente el offset
     */
    @KafkaListener(
            topics  = KafkaTopics.TRANSFER_REQUESTED,
            groupId = "validation-group"
    )
    public void onTransferRequested(
            @Payload TransferRequestedEvent event,
            Acknowledgment acknowledgment) {

        // La event_key identifica este evento específico en este servicio específico.
        // Formato: transactionId + ":" + tipoEvento
        // Ejemplo: "f47a-...:TRANSFER_REQUESTED"
        String eventKey = event.getTransactionId() + ":TRANSFER_REQUESTED";

        // ── CORRELACIÓN (MDC) ────────────────────────────────────────────
        // Inyectamos el txId en el MDC para que aparezca en TODOS los logs
        // producidos durante el procesamiento de este evento.
        // logback-spring.xml expone el txId con el patrón: [txId=%X{txId:-}]
        MDC.put("txId", event.getTransactionId());

        log.info("📨 Evento recibido | key={} from={} to={} amount={}",
                eventKey, event.getFromAccount(),
                event.getToAccount(), event.getAmount());

        try {
            // ── IDEMPOTENCIA: ¿ya procesamos este evento? ────────────────
            if (!idempotencyService.tryRegister(eventKey)) {
                // Duplicado detectado. ACK para que Kafka no lo reenvíe.
                log.warn("⚠️ Evento duplicado ignorado | key={}", eventKey);
                acknowledgment.acknowledge();
                return;
            }

            // ── VALIDACIÓN: aplicar reglas de negocio ────────────────────
            // Internamente publica TRANSFER_VALIDATED o TRANSFER_FAILED en Kafka.
            validationService.validate(event);

            // ── CONFIRMACIÓN: offset confirmado solo tras procesar ────────
            // Si llegamos aquí sin excepción, todo fue exitoso.
            acknowledgment.acknowledge();
            log.debug("✅ Offset confirmado | key={}", eventKey);

        } catch (Exception e) {
            // Excepción inesperada (error de red, DB caída, bug en código).
            // NO hacemos ACK → Kafka reentregará el mensaje.
            // El registro de idempotencia puede o no haberse insertado:
            //   - Si se insertó (REQUIRES_NEW en IdempotencyService): el reintento
            //     lo detectará como duplicado y lo ignorará → pérdida del mensaje.
            //     En producción se usaría un Dead Letter Topic para estos casos.
            //   - Si no se insertó: el reintento lo procesará normalmente.
            log.error("❌ Error inesperado procesando evento | key={} | error={}",
                    eventKey, e.getMessage(), e);
            // Sin acknowledgment.acknowledge() → Kafka reentrega el mensaje

        } finally {
            // Limpiar el MDC al terminar el procesamiento del mensaje.
            // CRÍTICO: el hilo del KafkaListenerContainer es reutilizado para
            // el siguiente mensaje. Sin remove(), el txId del mensaje anterior
            // aparecería en los logs del siguiente mensaje.
            MDC.remove("txId");
        }
    }
}
