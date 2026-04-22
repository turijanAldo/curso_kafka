package com.lab.api.repository;

import com.lab.api.entity.Transaction;
import com.lab.common.enums.TransferStatus;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;

/**
 * Repositorio JPA para la entidad Transaction.
 *
 * JpaRepository<Transaction, String> provee automáticamente:
 *   save(entity)        → INSERT o UPDATE según si la entidad es nueva
 *   findById(id)        → SELECT WHERE id = ?
 *   findAll()           → SELECT * FROM transactions
 *   delete(entity)      → DELETE WHERE id = ?
 *   count()             → SELECT COUNT(*)
 *   existsById(id)      → SELECT EXISTS(SELECT 1 WHERE id = ?)
 *
 * Spring Data JPA genera la implementación en tiempo de arranque.
 * No hay ningún SQL ni ningún método que implementar manualmente.
 *
 * El tipo genérico String es el tipo del campo @Id en Transaction.
 *
 * ¿Por qué @Repository si Spring Data JPA no lo necesita técnicamente?
 * La anotación es opcional aquí porque JpaRepository ya está marcado
 * internamente. La ponemos por convención y legibilidad: cualquier
 * developer sabe de inmediato que esta interfaz accede a la DB.
 */
@Repository
public interface TransactionRepository extends JpaRepository<Transaction, String> {

    /**
     * Busca todas las transferencias con un estado específico.
     *
     * Spring Data JPA genera automáticamente el SQL:
     *   SELECT * FROM transactions WHERE status = ?
     *
     * Esto funciona porque el nombre del método sigue la convención:
     *   findBy + NombreDelCampo (con la primera letra en mayúscula)
     *   findByStatus → WHERE status = ?
     *
     * No hay ninguna anotación ni SQL que escribir.
     * Útil para monitoreo: ¿cuántas transferencias están en PROCESSING?
     */
    List<Transaction> findByStatus(TransferStatus status);
}
