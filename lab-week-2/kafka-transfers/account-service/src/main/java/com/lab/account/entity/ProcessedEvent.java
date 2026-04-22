package com.lab.account.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.hibernate.annotations.CreationTimestamp;

import java.time.Instant;

/**
 * Registro de idempotencia para account-service.
 *
 * Cada vez que este servicio procesa un evento Kafka por primera vez,
 * inserta una fila aquí. Si el mismo evento llega de nuevo (reinicio,
 * redelivery de Kafka, red inestable), la Capa 2 detecta el registro
 * existente y descarta el duplicado antes de tocar el saldo.
 *
 * ¿Por qué es crítica la idempotencia en account-service en particular?
 * Porque este servicio modifica dinero real. Un débito duplicado significa
 * que Ana pierde el doble de lo esperado. Un crédito duplicado significa
 * que Bob recibe dinero que no le corresponde. Sin idempotencia, cualquier
 * reintento de Kafka se convierte en un error financiero.
 *
 * Esta clase es IDÉNTICA a la de validation-service.
 * ¿Por qué no está en common? Porque cada servicio gestiona su propio
 * ciclo de vida de JPA. Si common tuviera @Entity, Spring Boot de cada
 * servicio intentaría escanear ese paquete y encontraría entidades que
 * apuntan a tablas quizás en bases de datos diferentes. Más complejidad
 * que beneficio.
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
     * Clave única del evento procesado.
     * Formato: transactionId + ":" + tipoEvento
     * Ejemplos:
     *   "f47ac10b:TRANSFER_VALIDATED"  → DebitConsumer procesó este evento
     *   "f47ac10b:TRANSFER_DEBITED"    → CreditConsumer procesó este evento
     *   "f47ac10b:COMPENSATION"        → CompensationService ejecutó la compensación
     */
    @Column(name = "event_key", nullable = false, length = 200)
    private String eventKey;

    /**
     * Servicio que procesó el evento. Siempre "account-service" en esta clase.
     * Junto con event_key forma la clave UNIQUE → permite que validation-service
     * y account-service compartan la misma tabla sin colisiones.
     */
    @Column(name = "service_name", nullable = false, length = 50)
    private String serviceName;

    /** Cuándo se procesó. Para auditoría y debugging. */
    @CreationTimestamp
    @Column(name = "processed_at", nullable = false, updatable = false)
    private Instant processedAt;
}
