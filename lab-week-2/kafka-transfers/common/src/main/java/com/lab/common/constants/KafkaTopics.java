package com.lab.common.constants;

/**
 * Constantes con los nombres de todos los topics Kafka del lab.
 *
 * USO:
 *   @KafkaListener(topics = KafkaTopics.TRANSFER_REQUESTED)
 *   kafkaTemplate.send(KafkaTopics.TRANSFER_VALIDATED, event);
 *
 * CONVENCIÓN DE NOMBRES:
 *   transfer.<acción>       → evento de dominio principal
 *   transfer.<recurso>.dlq  → Dead Letter Queue (reintentos agotados)
 *
 * PARTICIONES: todas con 3 particiones (excepto DLQ con 1).
 * CLAVE de partición: fromAccount (garantiza orden por cuenta origen).
 *
 * FLUJO:
 *   transfer-api       → TRANSFER_REQUESTED
 *   validation-service → TRANSFER_VALIDATED  | TRANSFER_FAILED
 *   account-service    → TRANSFER_DEBITED    | TRANSFER_CREDITED | TRANSFER_COMPENSATED
 *   status-service     → (consume CREDITED, no publica a otro topic)
 *   DLQ                ← cualquier servicio que agote reintentos
 */
public final class KafkaTopics {

    private KafkaTopics() { /* Clase de constantes — no instanciar */ }

    // ── Eventos del Saga ──────────────────────────────────────────────────

    /**
     * Publicado por: transfer-api
     * Consumido por: validation-service
     * Payload: TransferRequestedEvent
     */
    public static final String TRANSFER_REQUESTED = "transfer.requested";

    /**
     * Publicado por: validation-service (éxito)
     * Consumido por: account-service
     * Payload: TransferValidatedEvent
     */
    public static final String TRANSFER_VALIDATED = "transfer.validated";

    /**
     * Publicado por: validation-service (rechazo)
     * Consumido por: status-service
     * Payload: TransferFailedEvent
     */
    public static final String TRANSFER_FAILED = "transfer.failed";

    /**
     * Publicado por: account-service (débito exitoso)
     * Consumido por: account-service mismo (paso crédito) + status-service
     * Payload: TransferDebitedEvent
     */
    public static final String TRANSFER_DEBITED = "transfer.debited";

    /**
     * Publicado por: account-service (crédito exitoso)
     * Consumido por: status-service
     * Payload: TransferCreditedEvent
     */
    public static final String TRANSFER_CREDITED = "transfer.credited";

    /**
     * Publicado por: account-service (compensación — revierte débito)
     * Consumido por: status-service
     * Payload: TransferCompensatedEvent
     */
    public static final String TRANSFER_COMPENSATED = "transfer.compensated";

    // ── Dead Letter Queue ─────────────────────────────────────────────────

    /**
     * Publicado por: cualquier servicio al agotar reintentos
     * Consumido por: (monitoreo / alertas — no implementado en el lab)
     * 1 sola partición — el orden no importa aquí
     */
    public static final String TRANSFER_DLQ = "transfer.dlq";
}
