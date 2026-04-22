package com.lab.account.repository;

import com.lab.account.entity.Account;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

/**
 * Repositorio JPA para la tabla accounts.
 *
 * A diferencia de validation-service donde solo necesitamos existsById(),
 * aquí usamos findById() para obtener la fila completa:
 *   1. Leer el saldo actual
 *   2. Leer el valor de version (para Optimistic Locking)
 *   3. Modificar el balance en memoria
 *   4. Llamar save() → JPA genera UPDATE ... WHERE id=? AND version=<leído>
 *
 * El ciclo READ → MODIFY → SAVE es el patrón estándar con @Version.
 * Si entre el READ y el SAVE otro thread modificó la fila, JPA detecta
 * el conflicto de versión y lanza OptimisticLockingFailureException.
 *
 * ¿Por qué no agregar @Lock(PESSIMISTIC_WRITE)?
 * El Pessimistic Locking bloquea la fila en la DB con SELECT FOR UPDATE.
 * Funciona, pero reduce el throughput porque:
 *   - Solo un thread puede leer la fila a la vez (no hay lecturas concurrentes)
 *   - Si el servicio muere entre el lock y el unlock, la DB tiene que esperar
 *     el timeout del lock para liberar la fila
 * Optimistic Locking permite lecturas concurrentes; solo el UPDATE detecta
 * el conflicto. Para cuentas bancarias con baja contención (pocas transferencias
 * simultáneas del mismo usuario), Optimistic es más eficiente.
 */
@Repository
public interface AccountRepository extends JpaRepository<Account, String> {
    // findById(String id) → heredado de JpaRepository
    // save(Account entity) → heredado, genera el UPDATE con AND version=?
    // existsById(String id) → heredado, usado por CompensationService
}
