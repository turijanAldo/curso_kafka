package com.lab.validation.entity;

import jakarta.persistence.*;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.time.Instant;

/**
 * Entidad JPA que mapea la tabla `accounts`.
 *
 * IMPORTANTE: validation-service solo LEER esta tabla.
 * Nunca hace INSERT ni UPDATE sobre cuentas — eso es responsabilidad
 * exclusiva de account-service.
 *
 * ¿Por qué definir la entidad completa si solo leemos?
 * Hibernate con ddl-auto=validate verifica al arrancar que TODOS los campos
 * de la entidad existen en la tabla. Si declaráramos solo los campos que
 * necesitamos (id, balance), Hibernate pasaría la validación, pero si
 * alguna vez alguien agrega un campo a la tabla sin agregarlo aquí,
 * no habría aviso. La entidad completa sirve como documentación del schema.
 *
 * No tiene @Builder ni @AllArgsConstructor porque este servicio
 * nunca construye Account manualmente — solo los lee de la DB.
 */
@Entity
@Table(name = "accounts")
@Data
@NoArgsConstructor
public class Account {

    @Id
    @Column(name = "id", nullable = false, length = 36)
    private String id;

    @Column(name = "owner_name", nullable = false, length = 100)
    private String ownerName;

    /**
     * Saldo actual de la cuenta.
     * validation-service lo lee para verificar que la cuenta tiene
     * saldo suficiente (validación básica de monto, no cálculo exacto).
     */
    @Column(name = "balance", nullable = false, precision = 15, scale = 2)
    private BigDecimal balance;

    /**
     * Campo de optimistic locking. validation-service nunca lo usa
     * para bloquear (no hace UPDATEs), pero debe estar en la entidad
     * para que Hibernate validate no falle al comparar con el schema.
     */
    @Column(name = "version", nullable = false)
    private Long version;

    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt;

    @Column(name = "updated_at", nullable = false)
    private Instant updatedAt;
}
