package com.lab.account.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import jakarta.persistence.Version;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.hibernate.annotations.CreationTimestamp;
import org.hibernate.annotations.UpdateTimestamp;

import java.math.BigDecimal;
import java.time.Instant;

/**
 * Entidad JPA que representa una cuenta bancaria.
 *
 * A diferencia de validation-service (donde Account es solo-lectura),
 * aquí es la entidad CENTRAL del sistema. account-service es el único
 * servicio que modifica balances. Esta es la "fuente de verdad" del dinero.
 *
 * El campo @Version activa el Optimistic Locking de JPA.
 * Optimistic Locking supone que los conflictos son raros y no bloquea
 * la fila en la DB durante la operación. En cambio:
 *   1. Lee la fila y guarda el valor de version
 *   2. Realiza la operación en memoria
 *   3. Al guardar: UPDATE accounts SET balance=?, version=version+1
 *                  WHERE id=? AND version=<valor-leído>
 *   Si entre el paso 1 y el 3 otro thread actualizó la fila (version cambió),
 *   el WHERE no encuentra la fila (0 rows updated) → JPA lanza
 *   OptimisticLockingFailureException → la transacción se reintenta.
 *
 * ¿Por qué @NoArgsConstructor pero no @Builder?
 * A diferencia de los eventos (que los construimos nosotros), las entidades
 * Account las crea Hibernate al hacer SELECT. Hibernate necesita el constructor
 * sin argumentos. @Builder no es necesario porque nunca creamos cuentas en
 * código — las lee Flyway/MySQL desde el init-db.sql.
 */
@Entity
@Table(name = "accounts")
@Data
@NoArgsConstructor
public class Account {

    /**
     * ID de la cuenta. En el sistema del lab: "ACC-001", "ACC-002", "ACC-003".
     * VARCHAR(36) — el tamaño máximo de un UUID si en el futuro se usa UUID.
     */
    @Id
    @Column(name = "id", updatable = false, length = 36)
    private String id;

    /**
     * Nombre del titular de la cuenta.
     * updatable = false → una vez creada la cuenta, el nombre no puede
     * cambiarse desde la aplicación (solo desde la DB directamente).
     */
    @Column(name = "owner_name", updatable = false, nullable = false, length = 100)
    private String ownerName;

    /**
     * Saldo actual. BigDecimal porque:
     *   - NUNCA usar double/float para dinero (errores de punto flotante)
     *   - BigDecimal es exacto para operaciones monetarias
     *   - precision=15, scale=2 → coincide con DECIMAL(15,2) de MySQL
     */
    @Column(name = "balance", nullable = false, precision = 15, scale = 2)
    private BigDecimal balance;

    /**
     * CAMPO CRÍTICO — Activador del Optimistic Locking.
     *
     * @Version le dice a JPA: "este campo es el número de versión".
     * JPA gestiona este campo automáticamente:
     *   - En cada UPDATE incrementa el valor en 1
     *   - Agrega AND version=<valor-leído> a todos los UPDATE
     *   - Si el WHERE no encuentra la fila (versión cambió) → OptimisticLockingFailureException
     *
     * El desarrollador NUNCA debe modificar este campo manualmente.
     * Si lo haces, rompes el mecanismo de concurrencia.
     */
    @Version
    @Column(name = "version", nullable = false)
    private Long version;

    /** Timestamp de creación. Solo para auditoría — nunca lo modificamos. */
    @CreationTimestamp
    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt;

    /** Timestamp de última modificación. Útil para debugging y auditoría. */
    @UpdateTimestamp
    @Column(name = "updated_at", nullable = false)
    private Instant updatedAt;
}
