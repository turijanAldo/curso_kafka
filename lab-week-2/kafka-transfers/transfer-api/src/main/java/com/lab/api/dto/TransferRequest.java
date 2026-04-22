package com.lab.api.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import jakarta.validation.constraints.Size;
import lombok.Data;

import java.math.BigDecimal;

/**
 * DTO que representa el cuerpo del request POST /transfers.
 *
 * DTO = Data Transfer Object. Es un objeto que solo transporta datos
 * entre capas (HTTP → Controller → Service). No tiene lógica de negocio.
 *
 * ¿Por qué un DTO separado y no usar la entidad Transaction directamente?
 * La entidad Transaction tiene campos que el cliente no debe controlar:
 * id (generado internamente), status (siempre empieza en PROCESSING),
 * failureReason (lo asigna el sistema), createdAt, updatedAt.
 * Si usáramos la entidad como request, el cliente podría enviar esos campos
 * y potencialmente corromper el estado del sistema.
 * El DTO expone solo lo que el cliente tiene permitido enviar.
 *
 * Las anotaciones de validación se activan por el @Valid en el controller.
 * Si alguna falla, Spring devuelve automáticamente HTTP 400 con los detalles.
 */
@Data
public class TransferRequest {

    /**
     * ID de la cuenta que envía el dinero.
     * @NotBlank valida que no sea null, no sea vacío ("") y no sea solo espacios.
     * @Size limita la longitud para evitar valores absurdamente largos.
     */
    @NotBlank(message = "La cuenta origen no puede estar vacía")
    @Size(max = 36, message = "El ID de cuenta origen no puede superar 36 caracteres")
    private String fromAccount;

    /**
     * ID de la cuenta que recibe el dinero.
     * La validación de que fromAccount != toAccount se hace en validation-service,
     * no aquí. transfer-api solo verifica que los campos no estén vacíos.
     * ¿Por qué? Porque transfer-api es el gateway, no el validador de negocio.
     */
    @NotBlank(message = "La cuenta destino no puede estar vacía")
    @Size(max = 36, message = "El ID de cuenta destino no puede superar 36 caracteres")
    private String toAccount;

    /**
     * Monto a transferir.
     * @NotNull → el campo debe estar presente en el JSON
     * @Positive → el valor debe ser estrictamente mayor que 0
     *
     * ¿Por qué no @Min(1)? Porque @Min trabaja con enteros.
     * @Positive funciona con BigDecimal y rechaza 0 y negativos.
     */
    @NotNull(message = "El monto es obligatorio")
    @Positive(message = "El monto debe ser mayor que cero")
    private BigDecimal amount;
}
