package com.lab.validation.repository;

import com.lab.validation.entity.Account;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

/**
 * Repositorio JPA para consultar cuentas bancarias.
 *
 * validation-service SOLO LEE de esta tabla.
 * account-service es quien escribe (debita/acredita).
 *
 * Métodos heredados de JpaRepository que usa este servicio:
 *
 *   existsById(String id)
 *     → SELECT EXISTS(SELECT 1 FROM accounts WHERE id = ?)
 *     → Más eficiente que findById() cuando solo necesitas saber si existe
 *       porque no transfiere el contenido de la fila, solo un booleano.
 *
 *   findById(String id)
 *     → SELECT * FROM accounts WHERE id = ?
 *     → Retorna Optional<Account>. Usado cuando necesitamos el saldo
 *       para la validación de fondos suficientes.
 */
@Repository
public interface AccountRepository extends JpaRepository<Account, String> {
    // JpaRepository ya provee existsById() y findById() — no necesitamos
    // declarar ningún método adicional para las validaciones de este servicio.
}
