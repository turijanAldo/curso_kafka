package com.lab.status.service;

import com.lab.common.enums.TransferStatus;
import com.lab.status.entity.Transaction;
import com.lab.status.repository.TransactionRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.Optional;

/**
 * Servicio de actualización de estado del Saga.
 *
 * Responsabilidad única: dado un transactionId y un nuevo estado,
 * actualizar la fila correspondiente en la tabla transactions.
 *
 * Este servicio es deliberadamente simple. No tiene reglas de negocio
 * complejas. Su única lógica es:
 *   1. ¿Existe la transacción? → si no, advertir y salir
 *   2. ¿El nuevo estado es diferente al actual? → si es igual, no hacer nada
 *   3. Actualizar status (y failureReason si aplica)
 *
 * ¿Por qué no tiene una máquina de estados explícita que valide transiciones?
 * En el lab la secuencia de eventos está garantizada por el diseño del Saga.
 * Si validation-service publica VALIDATED, es porque la transacción estaba
 * en PROCESSING. Añadir validación de transiciones duplicaría la lógica
 * que ya existe en los otros servicios.
 *
 * En producción, sería razonable añadir:
 *   if (INVALID_TRANSITION) log.error("Estado inesperado") y enviar al DLQ.
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class StatusService {

    private final TransactionRepository transactionRepository;

    /**
     * Actualiza el estado de una transferencia en la tabla transactions.
     *
     * @param transactionId  ID de la transferencia a actualizar
     * @param newStatus      el nuevo estado según el evento Kafka recibido
     * @param failureReason  razón del fallo (solo para FAILED y ROLLED_BACK; null en éxito)
     */
    @Transactional
    public void updateStatus(String transactionId, TransferStatus newStatus, String failureReason) {

        Optional<Transaction> opt = transactionRepository.findById(transactionId);

        if (opt.isEmpty()) {
            // Puede ocurrir si status-service arranca antes que transfer-api haya
            // commitado la transacción a la DB, o si hay un problema de consistencia.
            // Con auto.offset.reset: earliest, al reiniciar status-service relee
            // todos los eventos — en ese punto la transacción ya existe en DB.
            log.warn("⚠️ Transacción no encontrada para actualizar | txId={} newStatus={}",
                    transactionId, newStatus);
            return;
        }

        Transaction tx = opt.get();
        TransferStatus previousStatus = tx.getStatus();

        // Evitar escrituras innecesarias si el estado ya es el esperado
        // (idempotencia simple: el consumer puede recibir el mismo evento dos veces)
        if (previousStatus == newStatus) {
            log.debug("Estado ya en {} — no se actualiza | txId={}", newStatus, transactionId);
            return;
        }

        // Aplicar la transición
        tx.setStatus(newStatus);

        if (failureReason != null && !failureReason.isBlank()) {
            tx.setFailureReason(failureReason);
        }

        transactionRepository.save(tx);

        log.info("📊 Estado actualizado | txId={} {} → {}{}",
                transactionId,
                previousStatus,
                newStatus,
                failureReason != null ? " | reason=" + failureReason : "");
    }

    /**
     * Consulta el estado actual de una transferencia.
     * Usado por StatusController para el endpoint GET /transfers/{id}/status.
     *
     * @param transactionId ID de la transferencia
     * @return Optional con la Transaction, vacío si no existe
     */
    @Transactional(readOnly = true)
    public Optional<Transaction> findById(String transactionId) {
        return transactionRepository.findById(transactionId);
    }
}
