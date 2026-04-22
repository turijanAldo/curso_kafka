package com.lab.validation.service;

import com.lab.validation.entity.ProcessedEvent;
import com.lab.validation.repository.ProcessedEventRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

/**
 * Servicio de idempotencia — implementa las Capas 2 y 3.
 *
 * CAPA 1 (Kafka producer): enable.idempotence=true en transfer-api.
 *   → el broker Kafka descarta duplicados a nivel de red/protocolo
 *
 * CAPA 2 (aquí): SELECT EXISTS antes de procesar.
 *   → descarta la mayoría de los duplicados sin llegar a la DB de negocio
 *   → puede fallar en condiciones de carrera (dos threads leen "no existe" al mismo tiempo)
 *
 * CAPA 3 (aquí + DB): UNIQUE constraint en la tabla processed_events.
 *   → si dos threads pasan la Capa 2 simultáneamente, solo uno puede hacer el INSERT
 *   → el segundo recibe DataIntegrityViolationException → lo tratamos como "ya procesado"
 *   → garantía absoluta incluso bajo concurrencia alta
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class IdempotencyService {

    private static final String SERVICE_NAME = "validation-service";

    private final ProcessedEventRepository processedEventRepository;

    /**
     * Intenta registrar un evento como "procesado por este servicio".
     *
     * FLUJO INTERNO:
     *
     *   [CAPA 2] ¿Ya existe en processed_events?
     *       SÍ → return false  (evento duplicado, ignorar)
     *       NO → continuar...
     *
     *   [CAPA 3] INSERT en processed_events
     *       ÉXITO → return true  (evento nuevo, procesar)
     *       FALLA (DataIntegrityViolationException por UNIQUE) → return false
     *              (otro thread ganó la carrera, ignorar)
     *
     * @param eventKey formato: transactionId + ":" + tipoEvento
     *                 ejemplo: "f47a-...:TRANSFER_REQUESTED"
     * @return true si el evento es nuevo y debe procesarse,
     *         false si ya fue procesado (duplicado)
     *
     * PROPAGATION.REQUIRES_NEW: este método corre en su PROPIA transacción,
     * separada de la del caller. ¿Por qué?
     * Si el caller tiene una transacción abierta y algo falla después del
     * tryRegister(), queremos que el registro de idempotencia NO se revierta.
     * De lo contrario, en el siguiente reintento la Capa 2 no vería el registro
     * y volvería a intentar procesar el evento.
     */
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public boolean tryRegister(String eventKey) {

        // CAPA 2: verificación rápida antes del INSERT
        if (processedEventRepository.existsByEventKeyAndServiceName(eventKey, SERVICE_NAME)) {
            log.warn("[IDEMPOTENCIA-C2] Evento ya registrado — ignorando | key={}", eventKey);
            return false;
        }

        try {
            // CAPA 3: INSERT atómico — la UNIQUE constraint en la DB
            // garantiza que solo un thread puede insertar el mismo (eventKey, serviceName)
            ProcessedEvent record = ProcessedEvent.builder()
                    .eventKey(eventKey)
                    .serviceName(SERVICE_NAME)
                    .build();

            processedEventRepository.save(record);
            log.debug("[IDEMPOTENCIA-C3] Evento registrado | key={}", eventKey);
            return true;

        } catch (DataIntegrityViolationException e) {
            // Dos threads pasaron la Capa 2 al mismo tiempo.
            // La DB eligió un ganador — este es el perdedor.
            // No es un error del sistema — es el mecanismo funcionando correctamente.
            log.warn("[IDEMPOTENCIA-C3] Conflicto de inserción — evento procesado por otro thread | key={}", eventKey);
            return false;
        }
    }
}
