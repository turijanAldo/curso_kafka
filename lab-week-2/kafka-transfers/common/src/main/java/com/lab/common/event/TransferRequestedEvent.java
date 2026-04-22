package com.lab.common.event;

import com.fasterxml.jackson.annotation.JsonFormat;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.time.Instant;

/**
 * Evento: TRANSFER_REQUESTED
 *
 * Publicado por: transfer-api (endpoint POST /transfers)
 * Consumido por: validation-service
 * Topic:         transfer.requested
 * Partition key: fromAccount  (garantiza orden por cuenta origen)
 *
 * Este es el evento de entrada al Saga. Contiene todos los datos
 * necesarios para que los servicios downstream procesen la transferencia
 * sin necesidad de hacer llamadas HTTP adicionales (event-carried state).
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class TransferRequestedEvent implements SagaEvent {

    /**
     * Identificador único de la transferencia.
     * Generado por transfer-api al recibir la petición HTTP.
     * Formato: UUID v4 (e.g. "f47ac10b-58cc-4372-a567-0e02b2c3d479")
     * Se usa como clave de partición Kafka y en el event_key de idempotencia.
     */
    private String transactionId;

    /**
     * ID de la cuenta que envía el dinero.
     * Ejemplo: "ACC-001"
     * También es la clave de partición Kafka — garantiza que todas las
     * transferencias de la misma cuenta van a la misma partición y se
     * procesan en orden.
     */
    private String fromAccount;

    /**
     * ID de la cuenta que recibe el dinero.
     * Ejemplo: "ACC-002"
     */
    private String toAccount;

    /**
     * Monto a transferir. Siempre positivo.
     * BigDecimal para precisión monetaria (evita errores de punto flotante).
     */
    private BigDecimal amount;

    /**
     * Timestamp de creación del evento (UTC).
     * Permite medir latencia del Saga de extremo a extremo.
     */
    @JsonFormat(shape = JsonFormat.Shape.STRING)
    private Instant timestamp;
}
