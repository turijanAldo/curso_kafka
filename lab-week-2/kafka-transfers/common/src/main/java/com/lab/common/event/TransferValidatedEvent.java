package com.lab.common.event;

import com.fasterxml.jackson.annotation.JsonFormat;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.time.Instant;

/**
 * Evento: TRANSFER_VALIDATED
 *
 * Publicado por: validation-service (cuando la validación es exitosa)
 * Consumido por: account-service
 * Topic:         transfer.validated
 * Partition key: fromAccount
 *
 * Indica que la transferencia pasó todas las validaciones:
 *   ✔ La cuenta origen existe
 *   ✔ La cuenta destino existe
 *   ✔ El monto es positivo
 *   ✔ Las cuentas son distintas
 * account-service puede proceder a debitar con confianza.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class TransferValidatedEvent implements SagaEvent {

    /** Mismo ID del evento original — permite rastrear el Saga completo. */
    private String transactionId;

    private String fromAccount;
    private String toAccount;

    /** Propagamos el monto para que account-service no necesite consultarlo. */
    private BigDecimal amount;

    @JsonFormat(shape = JsonFormat.Shape.STRING)
    private Instant timestamp;
}
