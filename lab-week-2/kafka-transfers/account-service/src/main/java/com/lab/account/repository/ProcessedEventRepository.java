package com.lab.account.repository;

import com.lab.account.entity.ProcessedEvent;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

/**
 * Repositorio JPA para la tabla processed_events (en account-service).
 *
 * El método existsByEventKeyAndServiceName() es el corazón de la Capa 2
 * de idempotencia: antes de ejecutar cualquier operación de negocio,
 * el servicio pregunta "¿ya procesé este evento?" con este método.
 *
 * Spring Data JPA genera automáticamente la implementación basándose
 * en el nombre del método:
 *   existsBy[EventKey]And[ServiceName]
 *   → SELECT EXISTS(SELECT 1 FROM processed_events
 *                   WHERE event_key=? AND service_name=?)
 */
@Repository
public interface ProcessedEventRepository extends JpaRepository<ProcessedEvent, Long> {

    /**
     * Verifica si un evento ya fue procesado por este servicio.
     *
     * @param eventKey    formato: transactionId + ":" + tipoEvento
     * @param serviceName siempre "account-service" en este contexto
     * @return true si ya existe el registro → evento duplicado, ignorar
     *         false si no existe → evento nuevo, procesar
     */
    boolean existsByEventKeyAndServiceName(String eventKey, String serviceName);
}
