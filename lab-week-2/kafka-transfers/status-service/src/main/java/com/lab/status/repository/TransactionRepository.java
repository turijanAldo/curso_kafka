package com.lab.status.repository;

import com.lab.common.enums.TransferStatus;
import com.lab.status.entity.Transaction;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;

/**
 * Repositorio JPA para la tabla transactions en status-service.
 *
 * status-service usa este repositorio de dos formas:
 *   1. findById() → para obtener la transacción antes de actualizar su estado
 *   2. save()     → para persistir el nuevo estado tras recibir un evento Kafka
 *   3. findByStatus() → para el endpoint de consulta por estado (útil para monitoring)
 *
 * ¿Por qué status-service puede actualizar una tabla que "pertenece" a transfer-api?
 * Porque en el lab ambos servicios comparten la misma base de datos.
 * En producción se usaría una de estas alternativas:
 *   a) status-service tiene su propia tabla de estado (duplicado del dato)
 *   b) transfer-api expone un endpoint interno PATCH /transfers/{id}/status
 *      que status-service llama (introduce HTTP síncrono → acoplamiento)
 *   c) transfer-api también es consumer Kafka y actualiza su propia tabla
 *      (elimina la necesidad de status-service)
 *
 * Para el lab, la opción de DB compartida simplifica sin perder los
 * conceptos importantes.
 */
@Repository
public interface TransactionRepository extends JpaRepository<Transaction, String> {

    /**
     * Consulta todas las transacciones en un estado específico.
     * Útil para monitoring: "¿cuántas transferencias llevan más de 5 minutos en PROCESSING?"
     * Spring Data genera: SELECT * FROM transactions WHERE status = ?
     */
    List<Transaction> findByStatus(TransferStatus status);
}
