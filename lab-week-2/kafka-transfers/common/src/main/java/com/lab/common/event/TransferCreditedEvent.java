package com.lab.common.event;

import com.fasterxml.jackson.annotation.JsonFormat;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.time.Instant;

/**
 * Evento: TRANSFER_CREDITED
 *
 * Publicado por: account-service (crédito exitoso en cuenta destino)
 * Consumido por: status-service
 * Topic:         transfer.credited
 * Partition key: fromAccount
 *
 * Indica que el dinero llegó a la cuenta destino.
 * Este es el último evento de la ruta feliz del Saga.
 * status-service al recibirlo actualiza el estado a COMPLETED.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class TransferCreditedEvent implements SagaEvent {

    private String transactionId;
    private String fromAccount;
    private String toAccount;
    private BigDecimal amount;

    /**
     * Nuevo saldo de la cuenta destino tras el crédito.
     * Informativo.
     */
    private BigDecimal newBalance;

    @JsonFormat(shape = JsonFormat.Shape.STRING)
    private Instant timestamp;
}
