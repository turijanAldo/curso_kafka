package com.lab.status.controller;

import com.lab.common.enums.TransferStatus;
import com.lab.status.entity.Transaction;
import com.lab.status.service.StatusService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.time.Instant;
import java.util.Map;

/**
 * REST API de status-service — exposición del estado de las transferencias.
 *
 * Este controller sirve para dos propósitos:
 *   1. VERIFICACIÓN durante el lab: ver en tiempo real cómo avanza el estado del Saga
 *   2. ALTERNATIVA a transfer-api: acceder al estado sin depender del servicio que creó la transacción
 *
 * Puerto: 8083 (distinto de transfer-api en 8080)
 *
 * ¿Por qué status-service tiene su propio endpoint de consulta si transfer-api
 * ya tiene GET /transfers/{id}?
 *
 * En arquitecturas de microservicios reales es común que el servicio responsable
 * del "estado de una entidad" exponga su propio API de lectura. Esto permite:
 *   - Escalar el servicio de lectura independientemente del de escritura
 *   - transfer-api puede ser reemplazado sin afectar la consulta de estados
 *   - Mejor separación de responsabilidades: transfer-api crea, status-service trackea
 *
 * Para el lab, ambos endpoints coexisten. Los clientes pueden usar cualquiera.
 */
@RestController
@RequestMapping("/transfers")
@RequiredArgsConstructor
@Slf4j
public class StatusController {

    private final StatusService statusService;

    /**
     * Consulta el estado actual de una transferencia.
     *
     * GET /transfers/{id}/status → HTTP 200 con el estado y timestamps
     * GET /transfers/{id}/status → HTTP 404 si no existe la transacción
     *
     * Ejemplo de respuesta exitosa:
     * {
     *   "transactionId": "f47ac10b-58cc-4372-a567-0e02b2c3d479",
     *   "fromAccount": "ACC-001",
     *   "toAccount": "ACC-002",
     *   "amount": 100.00,
     *   "status": "COMPLETED",
     *   "failureReason": null,
     *   "createdAt": "2024-01-15T10:30:00Z",
     *   "updatedAt": "2024-01-15T10:30:02Z"
     * }
     *
     * @param id transactionId (UUID generado por transfer-api al crear la transferencia)
     */
    @GetMapping("/{id}/status")
    public ResponseEntity<Map<String, Object>> getStatus(@PathVariable String id) {
        log.debug("GET /transfers/{}/status", id);

        return statusService.findById(id)
                .map(tx -> ResponseEntity.ok(buildResponse(tx)))
                .orElseGet(() -> {
                    log.warn("Transacción no encontrada | txId={}", id);
                    return ResponseEntity
                            .status(HttpStatus.NOT_FOUND)
                            .body(Map.of(
                                "error", "Transferencia no encontrada",
                                "transactionId", id
                            ));
                });
    }

    /**
     * Construye el mapa de respuesta a partir de la entidad Transaction.
     *
     * ¿Por qué Map<String, Object> en lugar de un DTO dedicado?
     * Para el lab, un mapa es suficiente y evita crear una clase extra.
     * En producción se usaría un DTO con @JsonInclude(NON_NULL) para no
     * incluir el campo failureReason cuando es null.
     */
    private Map<String, Object> buildResponse(Transaction tx) {
        return Map.of(
            "transactionId",  tx.getId(),
            "fromAccount",    tx.getFromAccount(),
            "toAccount",      tx.getToAccount(),
            "amount",         tx.getAmount(),
            "status",         tx.getStatus().name(),
            "statusMessage",  buildStatusMessage(tx.getStatus()),
            "failureReason",  tx.getFailureReason() != null ? tx.getFailureReason() : "",
            "createdAt",      tx.getCreatedAt().toString(),
            "updatedAt",      tx.getUpdatedAt().toString()
        );
    }

    /**
     * Mensaje legible para cada estado del Saga.
     * Facilita la comprensión del estado sin tener que memorizar el significado de cada valor del enum.
     */
    private String buildStatusMessage(TransferStatus status) {
        return switch (status) {
            case PROCESSING  -> "⏳ En proceso — esperando validación";
            case VALIDATED   -> "✔️ Validada — esperando débito";
            case DEBITED     -> "💸 Débito aplicado — esperando crédito";
            case CREDITED    -> "💰 Crédito aplicado";
            case COMPLETED   -> "✅ Transferencia completada exitosamente";
            case FAILED      -> "❌ Transferencia fallida";
            case ROLLED_BACK -> "🔄 Débito revertido — dinero devuelto al origen";
        };
    }
}
