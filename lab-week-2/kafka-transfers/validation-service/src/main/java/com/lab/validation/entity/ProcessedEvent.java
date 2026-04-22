package com.lab.validation.entity;

import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.hibernate.annotations.CreationTimestamp;

import java.time.Instant;

/**
 * Entidad JPA que mapea la tabla `processed_events`.
 *
 * Esta tabla es el mecanismo de idempotencia del sistema.
 * validation-service inserta un registro aquí ANTES de procesar
 * un evento. Si el INSERT falla por clave duplicada, significa que
 * el evento ya fue procesado — se ignora sin repetir la operación.
 *
 * event_key = transactionId + ":" + tipoEvento
 * Ejemplo: "f47a-...:TRANSFER_REQUESTED"
 *
 * La columna UNIQUE (event_key, service_name) en la DB garantiza
 * que aunque dos instancias del servicio intenten registrar el mismo
 * evento simultáneamente, solo una lo logra (Capa 3 de idempotencia).
 */
@Entity
@Table(name = "processed_events")
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ProcessedEvent {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    /**
     * Clave única del evento. Formato: transactionId + ":" + tipoEvento
     * La restricción UNIQUE en la DB opera sobre esta columna combinada
     * con service_name.
     */
    @Column(name = "event_key", nullable = false, length = 200)
    private String eventKey;

    /**
     * Nombre del servicio que procesó el evento.
     * Valor fijo en este servicio: "validation-service"
     * Permite que el mismo event_key exista en múltiples servicios
     * sin conflicto (cada uno registra su propio procesamiento).
     */
    @Column(name = "service_name", nullable = false, length = 50)
    private String serviceName;

    @CreationTimestamp
    @Column(name = "processed_at", nullable = false, updatable = false)
    private Instant processedAt;
}
