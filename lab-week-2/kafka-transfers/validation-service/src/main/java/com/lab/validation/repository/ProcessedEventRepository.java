package com.lab.validation.repository;

import com.lab.validation.entity.ProcessedEvent;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

/**
 * Repositorio JPA para la tabla de idempotencia.
 *
 * Dos operaciones críticas:
 *
 * 1. existsByEventKeyAndServiceName(eventKey, serviceName)
 *    → CAPA 2 de idempotencia: verifica antes de procesar si el evento
 *      ya fue registrado. Si existe → ignorar el evento.
 *    → SQL generado: SELECT EXISTS(SELECT 1 FROM processed_events
 *                    WHERE event_key = ? AND service_name = ?)
 *
 * 2. save(ProcessedEvent)
 *    → CAPA 3 de idempotencia: intenta insertar el registro.
 *      Si dos threads pasan la Capa 2 simultáneamente (ambos leen "no existe"),
 *      solo uno logra el INSERT — el otro recibe DataIntegrityViolationException
 *      por la restricción UNIQUE(event_key, service_name) en la DB.
 *      IdempotencyService captura esa excepción y retorna false.
 */
@Repository
public interface ProcessedEventRepository extends JpaRepository<ProcessedEvent, Long> {

    /**
     * Verifica si un evento ya fue procesado por un servicio específico.
     *
     * Spring Data JPA genera el SQL a partir del nombre del método:
     *   existsBy → SELECT EXISTS(...)
     *   EventKey → WHERE event_key = ?
     *   And → AND
     *   ServiceName → service_name = ?
     *
     * @param eventKey    formato: transactionId + ":" + tipoEvento
     * @param serviceName nombre del microservicio (ej: "validation-service")
     * @return true si ya fue procesado, false si es nuevo
     */
    boolean existsByEventKeyAndServiceName(String eventKey, String serviceName);
}
