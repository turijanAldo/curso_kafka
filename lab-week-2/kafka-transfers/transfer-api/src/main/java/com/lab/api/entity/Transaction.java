package com.lab.api.entity;

import com.lab.common.enums.TransferStatus;
import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.hibernate.annotations.CreationTimestamp;
import org.hibernate.annotations.UpdateTimestamp;

import java.math.BigDecimal;
import java.time.Instant;

/**
 * Entidad JPA que mapea la tabla `transactions`.
 *
 * Rol: registro persistente de cada transferencia iniciada.
 * Creada por: transfer-api al recibir POST /transfers
 * Actualizada por: status-service cuando procesa eventos Kafka del Saga
 *
 * La tabla ya existe (creada por Flyway en V1__create_transactions_table.sql).
 * Hibernate con ddl-auto=validate verifica que los campos de esta entidad
 * coinciden con las columnas de la tabla al arrancar el servicio.
 */
@Entity
@Table(name = "transactions")
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Transaction {

    /**
     * UUID de la transferencia. Generado por TransferService, no por la DB.
     * Por eso usamos @Id sin @GeneratedValue — el valor llega desde Java.
     *
     * ¿Por qué UUID generado en Java y no BIGINT AUTO_INCREMENT en DB?
     * Con AUTO_INCREMENT necesitas insertar primero y luego consultar el ID
     * generado. Con UUID generado en Java, conoces el ID ANTES de insertar,
     * lo que te permite incluirlo en el evento Kafka que publicas al mismo tiempo.
     */
    @Id
    @Column(name = "id", nullable = false, updatable = false, length = 36)
    private String id;

    @Column(name = "from_account", nullable = false, updatable = false, length = 36)
    private String fromAccount;

    @Column(name = "to_account", nullable = false, updatable = false, length = 36)
    private String toAccount;

    @Column(name = "amount", nullable = false, precision = 15, scale = 2)
    private BigDecimal amount;

    /**
     * Estado actual del Saga para esta transferencia.
     *
     * @Enumerated(EnumType.STRING) le dice a Hibernate que almacene
     * el nombre del enum ("PROCESSING", "COMPLETED") en lugar de su
     * índice ordinal (0, 1, 2...).
     *
     * ¿Por qué STRING y no ORDINAL?
     * Con ORDINAL, si alguna vez reordenas los valores del enum o insertas
     * uno nuevo en medio, todos los registros existentes apuntan al valor
     * incorrecto. Con STRING, el valor almacenado es legible y estable.
     */
    @Enumerated(EnumType.STRING)
    @Column(name = "status", nullable = false, length = 30)
    @Builder.Default
    private TransferStatus status = TransferStatus.PROCESSING;

    /**
     * Motivo del fallo. Null en transferencias exitosas.
     * Lo escribe status-service cuando recibe un TransferFailedEvent.
     */
    @Column(name = "failure_reason", length = 255)
    private String failureReason;

    /**
     * @CreationTimestamp → Hibernate asigna el valor automáticamente
     * al hacer el primer INSERT. No se modifica en UPDATEs posteriores.
     * updatable = false refuerza que nunca cambie después de la creación.
     */
    @CreationTimestamp
    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt;

    /**
     * @UpdateTimestamp → Hibernate actualiza este valor en cada UPDATE.
     * Equivalente al ON UPDATE CURRENT_TIMESTAMP del SQL.
     */
    @UpdateTimestamp
    @Column(name = "updated_at", nullable = false)
    private Instant updatedAt;
}
