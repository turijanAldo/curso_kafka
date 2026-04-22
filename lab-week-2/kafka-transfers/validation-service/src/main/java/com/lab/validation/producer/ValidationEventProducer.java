package com.lab.validation.producer;

import com.lab.common.constants.KafkaTopics;
import com.lab.common.event.TransferFailedEvent;
import com.lab.common.event.TransferRequestedEvent;
import com.lab.common.event.TransferValidatedEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.time.Instant;

/**
 * Producer Kafka de validation-service.
 *
 * Publica exactamente dos tipos de eventos:
 *   ✅ TransferValidatedEvent → transfer.validated  (si pasan todas las reglas)
 *   ❌ TransferFailedEvent    → transfer.failed     (si falla alguna regla)
 *
 * En ambos casos, la clave de partición es fromAccount para mantener
 * el orden de eventos por cuenta origen a lo largo de todo el Saga.
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class ValidationEventProducer {

    private final KafkaTemplate<String, Object> kafkaTemplate;

    /**
     * Publica que la transferencia pasó todas las validaciones.
     * account-service escucha este topic y procederá con el débito.
     *
     * Propagamos fromAccount, toAccount y amount del evento original
     * (Event-Carried State Transfer) para que account-service no necesite
     * consultar la DB para obtener estos datos.
     */
    public void publishValidated(TransferRequestedEvent original) {
        TransferValidatedEvent event = TransferValidatedEvent.builder()
                .transactionId(original.getTransactionId())
                .fromAccount(original.getFromAccount())
                .toAccount(original.getToAccount())
                .amount(original.getAmount())
                .timestamp(Instant.now())
                .build();

        kafkaTemplate.send(
                KafkaTopics.TRANSFER_VALIDATED,
                original.getFromAccount(),  // clave de partición
                event
        ).whenComplete((result, ex) -> {
            if (ex == null) {
                log.info("✅ TRANSFER_VALIDATED publicado | txId={} partition={} offset={}",
                        original.getTransactionId(),
                        result.getRecordMetadata().partition(),
                        result.getRecordMetadata().offset());
            } else {
                log.error("❌ Error publicando TRANSFER_VALIDATED | txId={} | error={}",
                        original.getTransactionId(), ex.getMessage(), ex);
            }
        });
    }

    /**
     * Publica que la transferencia fue rechazada por validación.
     * status-service escucha este topic y actualizará el estado a FAILED.
     *
     * @param original  el evento original recibido de transfer.requested
     * @param reason    descripción legible del motivo del rechazo
     * @param failedBy  nombre del servicio que detectó el fallo (siempre "validation-service")
     */
    public void publishFailed(TransferRequestedEvent original,
                               String reason,
                               String failedBy) {
        TransferFailedEvent event = TransferFailedEvent.builder()
                .transactionId(original.getTransactionId())
                .fromAccount(original.getFromAccount())
                .toAccount(original.getToAccount())
                .reason(reason)
                .failedBy(failedBy)
                .timestamp(Instant.now())
                .build();

        kafkaTemplate.send(
                KafkaTopics.TRANSFER_FAILED,
                original.getFromAccount(),  // clave de partición
                event
        ).whenComplete((result, ex) -> {
            if (ex == null) {
                log.info("⚠️ TRANSFER_FAILED publicado | txId={} reason={}",
                        original.getTransactionId(), reason);
            } else {
                log.error("❌ Error publicando TRANSFER_FAILED | txId={} | error={}",
                        original.getTransactionId(), ex.getMessage(), ex);
            }
        });
    }
}
