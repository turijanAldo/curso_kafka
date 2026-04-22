package com.lab.common.event;

import com.fasterxml.jackson.annotation.JsonFormat;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.time.Instant;

/**
 * Evento: TRANSFER_DEBITED
 *
 * Publicado por: account-service (débito exitoso en cuenta origen)
 * Consumido por: account-service mismo (paso 2: aplicar crédito)
 *                status-service (para actualizar estado a DEBITED)
 * Topic:         transfer.debited
 * Partition key: fromAccount
 *
 * ¡PUNTO CRÍTICO DEL SAGA!
 * Después de publicar este evento, el dinero ya salió de la cuenta origen.
 * Si el crédito posterior falla, DEBE ejecutarse la transacción compensatoria
 * (revertir el débito) para mantener consistencia.
 *
 * IDEMPOTENCIA:
 *   event_key = transactionId + ":TRANSFER_DEBITED"
 *   Si account-service recibe este evento dos veces (at-least-once),
 *   la segunda vez encontrará el event_key en processed_events y lo ignorará.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class TransferDebitedEvent implements SagaEvent {

    private String transactionId;
    private String fromAccount;
    private String toAccount;
    private BigDecimal amount;

    /**
     * Saldo restante en la cuenta origen tras el débito.
     * Informativo — útil para logs y monitoreo.
     */
    private BigDecimal remainingBalance;

    @JsonFormat(shape = JsonFormat.Shape.STRING)
    private Instant timestamp;
}
