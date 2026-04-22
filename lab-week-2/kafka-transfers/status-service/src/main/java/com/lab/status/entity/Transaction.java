package com.lab.status.entity;

import com.lab.common.enums.TransferStatus;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.hibernate.annotations.CreationTimestamp;
import org.hibernate.annotations.UpdateTimestamp;

import java.math.BigDecimal;
import java.time.Instant;

/**
 * Entidad JPA que mapea la tabla transactions en status-service.
 *
 * IMPORTANTE: Esta tabla es creada y escrita inicialmente por transfer-api.
 * status-service la comparte en la misma DB del lab. Su rol aquí es:
 *   → LEER para exponer el estado al cliente (GET /transfers/{id}/status)
 *   → ESCRIBIR para actualizar el status cuando llegan eventos Kafka
 *
 * ¿Por qué status-service tiene su propia copia de esta entidad y no la
 * toma de transfer-api?
 * En una arquitectura de microservicios real, cada servicio es un
 * jar independiente desplegado por separado. No puede "importar" clases
 * del jar de otro servicio en tiempo de compilación. La única forma
 * de compartir tipos es via el módulo common (eventos) o via API HTTP.
 * La entidad de BD no va al common — eso acoplaría el schema de DB al
 * contrato de common, rompiendo la independencia de los servicios.
 *
 * Para el lab compartimos la misma MySQL, pero cada servicio tiene su
 * propia representación Java de la tabla que necesita.
 *
 * Diferencia con la entidad en transfer-api:
 *   → Sin @Builder (status-service nunca crea transacciones)
 *   → Sin @Builder.Default (no construimos el objeto, Hibernate lo hace)
 *   → Los campos @Column(updatable=false) aplican desde la perspectiva
 *     de este servicio (solo status y failureReason se modifican aquí)
 */
@Entity
@Table(name = "transactions")
@Data
@NoArgsConstructor
public class Transaction {

    @Id
    @Column(name = "id", updatable = false, length = 36)
    private String id;

    @Column(name = "from_account", updatable = false, nullable = false, length = 36)
    private String fromAccount;

    @Column(name = "to_account", updatable = false, nullable = false, length = 36)
    private String toAccount;

    @Column(name = "amount", nullable = false, precision = 15, scale = 2)
    private BigDecimal amount;

    /**
     * Estado actual de la saga.
     * Este es el campo que status-service ESCRIBE en cada evento Kafka.
     * @Enumerated(STRING) → almacena "VALIDATED", "DEBITED", etc. (legible en DB)
     * Sin STRING almacenaría el ordinal (0, 1, 2...) — frágil si el enum cambia de orden.
     */
    @Enumerated(EnumType.STRING)
    @Column(name = "status", nullable = false, length = 30)
    private TransferStatus status;

    /**
     * Razón del fallo, si la transferencia terminó en FAILED o ROLLED_BACK.
     * Null en el camino feliz (COMPLETED).
     * Escrito por status-service cuando recibe TRANSFER_FAILED o TRANSFER_COMPENSATED.
     */
    @Column(name = "failure_reason", length = 255)
    private String failureReason;

    @CreationTimestamp
    @Column(name = "created_at", updatable = false, nullable = false)
    private Instant createdAt;

    /**
     * Actualizado automáticamente por Hibernate en cada save().
     * Permite saber cuándo fue el último cambio de estado.
     */
    @UpdateTimestamp
    @Column(name = "updated_at", nullable = false)
    private Instant updatedAt;
}
