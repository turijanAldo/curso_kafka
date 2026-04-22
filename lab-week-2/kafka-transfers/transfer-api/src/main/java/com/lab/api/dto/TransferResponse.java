package com.lab.api.dto;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.lab.common.enums.TransferStatus;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.time.Instant;

/**
 * DTO que representa la respuesta de POST /transfers y GET /transfers/{id}.
 *
 * @JsonInclude(NON_NULL) → Jackson omite del JSON los campos que son null.
 * Así, si failureReason es null (transferencia exitosa), no aparece en el JSON
 * en lugar de aparecer como "failureReason": null.
 * Resultado más limpio para el cliente.
 *
 * @NoArgsConstructor + @AllArgsConstructor:
 * Jackson necesita el constructor sin argumentos para deserializar desde JSON
 * (incluyendo desde Redis cuando se recupera el caché). @Builder genera su
 * propio constructor privado — @AllArgsConstructor lo hace accesible para
 * que Lombok construya correctamente el builder.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class TransferResponse {

    /** UUID de la transferencia. El cliente lo usa para hacer GET /transfers/{id}. */
    private String transactionId;

    private String fromAccount;
    private String toAccount;
    private BigDecimal amount;

    /**
     * Estado actual del Saga. En el POST inicial siempre será PROCESSING.
     * En el GET posterior puede ser cualquier valor del enum TransferStatus.
     */
    private TransferStatus status;

    /**
     * Razón del fallo. Solo presente si status es FAILED o ROLLED_BACK.
     * @JsonInclude(NON_NULL) se encarga de omitirlo cuando es null.
     */
    private String failureReason;

    /**
     * @JsonFormat(STRING) serializa el Instant como ISO-8601 legible:
     * "2024-01-15T10:30:00Z" en lugar del timestamp numérico 1705316600.
     */
    @JsonFormat(shape = JsonFormat.Shape.STRING)
    private Instant createdAt;

    @JsonFormat(shape = JsonFormat.Shape.STRING)
    private Instant updatedAt;

    /**
     * Mensaje descriptivo del estado actual.
     * Útil para mostrar al usuario final sin que tenga que interpretar el enum.
     * Ejemplos: "Transferencia en proceso", "Transferencia completada exitosamente"
     */
    private String message;
}
