package com.lab.api.producer;

import com.lab.common.constants.KafkaTopics;
import com.lab.common.event.TransferRequestedEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

import java.util.concurrent.CompletableFuture;

/**
 * Componente responsable de publicar eventos en Kafka.
 *
 * ¿Por qué un componente separado y no publicar desde el Service directamente?
 * Separación de responsabilidades (Single Responsibility Principle):
 *   - TransferService → orquesta la lógica de negocio
 *   - TransferEventProducer → sabe cómo hablarle a Kafka
 *
 * Si mañana cambiamos de Kafka a RabbitMQ o a un HTTP webhook,
 * solo cambia TransferEventProducer. TransferService no se toca.
 *
 * @Slf4j → genera automáticamente un logger:
 *   private static final Logger log = LoggerFactory.getLogger(TransferEventProducer.class);
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class TransferEventProducer {

    /**
     * KafkaTemplate<K, V>:
     *   K = tipo de la clave del mensaje (String → fromAccount)
     *   V = tipo del valor del mensaje (Object → cualquier evento)
     *
     * ¿Por qué Object y no TransferRequestedEvent?
     * Un producer puede necesitar publicar distintos tipos de eventos.
     * Con Object y JsonSerializer, Kafka serializa cualquier clase Java a JSON.
     * Si en el futuro transfer-api publica otro tipo de evento, este mismo
     * template lo maneja sin cambios.
     */
    private final KafkaTemplate<String, Object> kafkaTemplate;

    /**
     * Publica un TransferRequestedEvent en el topic transfer.requested.
     *
     * Parámetros de send(topic, key, value):
     *   topic → "transfer.requested" (de la constante KafkaTopics)
     *   key   → fromAccount (determina la partición)
     *   value → el evento serializado como JSON
     *
     * ¿Por qué fromAccount como clave de partición?
     * Kafka garantiza que todos los mensajes con la misma clave van a la
     * misma partición y se procesan en orden. Si Ana hace dos transferencias
     * seguidas, ambas van a la misma partición → validation-service las
     * procesa en orden → no hay condiciones de carrera sobre el saldo de Ana.
     *
     * send() es ASÍNCRONO: no espera a que el broker confirme la recepción.
     * El CompletableFuture permite reaccionar cuando llega la confirmación
     * o cuando ocurre un error.
     *
     * ¿Por qué no usar sendResult().get() para esperar la confirmación?
     * Porque bloquearía el hilo HTTP del controller durante 10-100ms.
     * Con el callback asíncrono, el controller responde al cliente de
     * inmediato y Kafka maneja la entrega en segundo plano.
     */
    public void publishTransferRequested(TransferRequestedEvent event) {
        log.info("Publicando evento TRANSFER_REQUESTED | transactionId={} fromAccount={} amount={}",
                event.getTransactionId(), event.getFromAccount(), event.getAmount());

        CompletableFuture<SendResult<String, Object>> future =
                kafkaTemplate.send(
                        KafkaTopics.TRANSFER_REQUESTED,
                        event.getFromAccount(),   // clave de partición
                        event                     // valor serializado a JSON
                );

        // Callback asíncrono: se ejecuta cuando el broker confirma o rechaza
        future.whenComplete((result, ex) -> {
            if (ex == null) {
                // Confirmación del broker: el mensaje fue escrito en la partición
                log.info("✅ Evento publicado | transactionId={} | partition={} offset={}",
                        event.getTransactionId(),
                        result.getRecordMetadata().partition(),
                        result.getRecordMetadata().offset());
            } else {
                // El broker rechazó el mensaje después de todos los reintentos
                // En producción: enviar a DLQ o activar una alarma
                log.error("❌ Error al publicar evento | transactionId={} | error={}",
                        event.getTransactionId(), ex.getMessage(), ex);
            }
        });
    }
}
