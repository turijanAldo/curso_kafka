package com.lab.common.event;

import com.fasterxml.jackson.annotation.JsonFormat;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.time.Instant;

/**
 * Evento: TRANSFER_COMPENSATED
 *
 * Publicado por: account-service (después de revertir el débito)
 * Consumido por: status-service
 * Topic:         transfer.compensated
 * Partition key: fromAccount
 *
 * Transacción de compensación del Saga.
 * Se publica cuando el crédito en la cuenta destino falló y el
 * débito previo en la cuenta origen necesita ser revertido para
 * mantener la consistencia eventual del sistema.
 *
 * Flujo de compensación:
 *   account-service detecta error en crédito
 *     → revierte débito en cuenta origen (saldo += amount)
 *     → publica TransferCompensatedEvent
 *   status-service recibe el evento
 *     → actualiza estado a ROLLED_BACK
 *
 * IDEMPOTENCIA:
 *   event_key = transactionId + ":TRANSFER_COMPENSATED"
 *   La compensación solo puede ocurrir una vez por transacción.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class TransferCompensatedEvent implements SagaEvent {

    private String transactionId;
    private String fromAccount;
    private String toAccount;
    private BigDecimal amount;

    /**
     * Motivo de la compensación — qué salió mal antes.
     * Ejemplo: "Error al acreditar en cuenta destino: cuenta no encontrada"
     */
    private String compensationReason;

    /**
     * Saldo de la cuenta origen después de revertir el débito.
     * Debe ser igual al saldo que tenía antes de TRANSFER_DEBITED.
     */
    private BigDecimal restoredBalance;

    @JsonFormat(shape = JsonFormat.Shape.STRING)
    private Instant timestamp;
}
