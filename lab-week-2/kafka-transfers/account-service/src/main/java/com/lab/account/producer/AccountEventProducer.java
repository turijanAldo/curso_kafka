package com.lab.account.producer;

import com.lab.common.constants.KafkaTopics;
import com.lab.common.event.TransferCompensatedEvent;
import com.lab.common.event.TransferCreditedEvent;
import com.lab.common.event.TransferDebitedEvent;
import com.lab.common.event.TransferFailedEvent;
import com.lab.common.event.TransferValidatedEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.time.Instant;

/**
 * Producer Kafka de account-service.
 *
 * Publica cuatro tipos de eventos según el resultado de cada operación:
 *
 *   ✅ TRANSFER_DEBITED    → débito aplicado exitosamente (→ account-credit-group lo consume)
 *   ✅ TRANSFER_CREDITED   → crédito aplicado exitosamente (→ saga completada)
 *   🔄 TRANSFER_COMPENSATED → débito revertido por fallo en crédito (→ saga rollback)
 *   ❌ TRANSFER_FAILED     → operación fallida, sin posibilidad de continuar
 *
 * Todos los eventos usan fromAccount como clave de partición para
 * garantizar orden por cuenta origen a lo largo de todo el Saga.
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class AccountEventProducer {

    private final KafkaTemplate<String, Object> kafkaTemplate;

    /**
     * Publica que el débito fue aplicado exitosamente.
     * account-service (CreditConsumer con account-credit-group) escucha este topic.
     *
     * @param original         evento TRANSFER_VALIDATED que originó el débito
     * @param remainingBalance saldo que queda en fromAccount después del débito
     */
    public void publishDebited(TransferValidatedEvent original, BigDecimal remainingBalance) {
        TransferDebitedEvent event = TransferDebitedEvent.builder()
                .transactionId(original.getTransactionId())
                .fromAccount(original.getFromAccount())
                .toAccount(original.getToAccount())
                .amount(original.getAmount())
                .remainingBalance(remainingBalance)
                .timestamp(Instant.now())
                .build();

        kafkaTemplate.send(
                KafkaTopics.TRANSFER_DEBITED,
                original.getFromAccount(),
                event
        ).whenComplete((result, ex) -> {
            if (ex == null) {
                log.info("✅ TRANSFER_DEBITED publicado | txId={} remainingBalance={} partition={} offset={}",
                        original.getTransactionId(),
                        remainingBalance,
                        result.getRecordMetadata().partition(),
                        result.getRecordMetadata().offset());
            } else {
                log.error("❌ Error publicando TRANSFER_DEBITED | txId={} | error={}",
                        original.getTransactionId(), ex.getMessage(), ex);
            }
        });
    }

    /**
     * Publica que el crédito fue aplicado exitosamente.
     * status-service lo consumirá para marcar la transferencia como COMPLETED.
     *
     * @param original   evento TRANSFER_DEBITED que originó el crédito
     * @param newBalance saldo actualizado en la cuenta destino (toAccount)
     */
    public void publishCredited(TransferDebitedEvent original, BigDecimal newBalance) {
        TransferCreditedEvent event = TransferCreditedEvent.builder()
                .transactionId(original.getTransactionId())
                .fromAccount(original.getFromAccount())
                .toAccount(original.getToAccount())
                .amount(original.getAmount())
                .newBalance(newBalance)
                .timestamp(Instant.now())
                .build();

        kafkaTemplate.send(
                KafkaTopics.TRANSFER_CREDITED,
                original.getFromAccount(),
                event
        ).whenComplete((result, ex) -> {
            if (ex == null) {
                log.info("✅ TRANSFER_CREDITED publicado | txId={} newBalance={} partition={} offset={}",
                        original.getTransactionId(),
                        newBalance,
                        result.getRecordMetadata().partition(),
                        result.getRecordMetadata().offset());
            } else {
                log.error("❌ Error publicando TRANSFER_CREDITED | txId={} | error={}",
                        original.getTransactionId(), ex.getMessage(), ex);
            }
        });
    }

    /**
     * Publica que la compensación (rollback del débito) fue aplicada.
     * status-service lo consumirá para marcar la transferencia como ROLLED_BACK.
     *
     * @param original          evento TRANSFER_DEBITED que originó la compensación
     * @param compensationReason descripción del motivo del rollback
     * @param restoredBalance   saldo de fromAccount después de restaurar el monto
     */
    public void publishCompensated(TransferDebitedEvent original,
                                    String compensationReason,
                                    BigDecimal restoredBalance) {
        TransferCompensatedEvent event = TransferCompensatedEvent.builder()
                .transactionId(original.getTransactionId())
                .fromAccount(original.getFromAccount())
                .toAccount(original.getToAccount())
                .amount(original.getAmount())
                .compensationReason(compensationReason)
                .restoredBalance(restoredBalance)
                .timestamp(Instant.now())
                .build();

        kafkaTemplate.send(
                KafkaTopics.TRANSFER_COMPENSATED,
                original.getFromAccount(),
                event
        ).whenComplete((result, ex) -> {
            if (ex == null) {
                log.info("🔄 TRANSFER_COMPENSATED publicado | txId={} reason='{}' restoredBalance={}",
                        original.getTransactionId(), compensationReason, restoredBalance);
            } else {
                log.error("❌ Error publicando TRANSFER_COMPENSATED | txId={} | error={}",
                        original.getTransactionId(), ex.getMessage(), ex);
            }
        });
    }

    /**
     * Publica que una operación de account-service falló.
     * Usado en dos situaciones:
     *   1. Saldo insuficiente al momento del débito (raro — ya validado por validation-service)
     *   2. Crédito fallido (real o simulado con simulate-credit-failure=true)
     *
     * @param transactionId  ID de la transacción fallida
     * @param fromAccount    cuenta origen (clave de partición)
     * @param toAccount      cuenta destino
     * @param amount         monto que estaba en juego
     * @param reason         descripción legible del motivo del fallo
     * @param failedBy       nombre del servicio que detectó el fallo
     */
    public void publishFailed(String transactionId,
                               String fromAccount,
                               String toAccount,
                               BigDecimal amount,
                               String reason,
                               String failedBy) {
        TransferFailedEvent event = TransferFailedEvent.builder()
                .transactionId(transactionId)
                .fromAccount(fromAccount)
                .toAccount(toAccount)
                .reason(reason)
                .failedBy(failedBy)
                .timestamp(Instant.now())
                .build();

        kafkaTemplate.send(
                KafkaTopics.TRANSFER_FAILED,
                fromAccount,
                event
        ).whenComplete((result, ex) -> {
            if (ex == null) {
                log.info("⚠️ TRANSFER_FAILED publicado | txId={} reason='{}' failedBy={}",
                        transactionId, reason, failedBy);
            } else {
                log.error("❌ Error publicando TRANSFER_FAILED | txId={} | error={}",
                        transactionId, ex.getMessage(), ex);
            }
        });
    }
}
