package com.lab.common.event;

import com.fasterxml.jackson.annotation.JsonFormat;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;

/**
 * Evento: TRANSFER_FAILED
 *
 * Publicado por: validation-service (validación rechazada)
 *                account-service    (sin fondos o error de crédito)
 * Consumido por: status-service
 * Topic:         transfer.failed
 * Partition key: fromAccount
 *
 * Señal de que el Saga tomó el camino de error.
 * status-service actualiza el estado a FAILED en la DB.
 * Si el débito ya fue aplicado, account-service también publica
 * TransferCompensatedEvent para revertirlo.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class TransferFailedEvent implements SagaEvent {

    /** ID de la transferencia que falló. */
    private String transactionId;

    private String fromAccount;
    private String toAccount;

    /**
     * Razón legible del fallo.
     * Ejemplos:
     *   "Cuenta origen no encontrada: ACC-999"
     *   "Saldo insuficiente. Disponible: 50.00, Requerido: 200.00"
     *   "Cuenta origen y destino son la misma"
     */
    private String reason;

    /**
     * Nombre del servicio que originó el fallo.
     * Útil para diagnóstico en status-service y logs.
     * Ejemplos: "validation-service", "account-service"
     */
    private String failedBy;

    @JsonFormat(shape = JsonFormat.Shape.STRING)
    private Instant timestamp;
}
