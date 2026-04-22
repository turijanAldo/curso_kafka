package com.lab.account.service;

import com.lab.account.entity.Account;
import com.lab.account.repository.AccountRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;

/**
 * Servicio de débito — descuenta el saldo de la cuenta origen.
 *
 * Esta es la operación financiera más sensible del sistema.
 * Aplica el dinero real de la cuenta del cliente. Si falla a mitad
 * del proceso, el Saga Choreography activa la compensación.
 *
 * El Optimistic Locking (@Version en Account) protege esta operación
 * contra modificaciones concurrentes: si dos transferencias del mismo
 * cliente se procesan simultáneamente, solo una puede commitear.
 * La otra recibe OptimisticLockingFailureException y Kafka reentrega
 * su mensaje para reintentarlo.
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class DebitService {

    private final AccountRepository accountRepository;

    /**
     * Aplica el débito en la cuenta origen.
     *
     * SECUENCIA INTERNA con @Transactional:
     *   1. BEGIN TRANSACTION
     *   2. SELECT * FROM accounts WHERE id=? → lee balance y version
     *   3. Valida que balance >= amount
     *   4. balance = balance - amount (en memoria)
     *   5. UPDATE accounts SET balance=?, version=version+1
     *      WHERE id=? AND version=<valor-leído>
     *      → Si 0 rows updated: OptimisticLockingFailureException
     *      → Si 1 row updated: COMMIT
     *   6. Retorna el objeto Account actualizado con el nuevo balance
     *
     * ¿Por qué @Transactional aquí y no solo en el consumer?
     * El consumer hace ACK después de llamar a este método. Si el consumer
     * fuera @Transactional, el ACK de Kafka y el commit de DB ocurrirían
     * en distintos momentos — podría commitear la DB y luego fallar antes
     * del ACK, causando un doble procesamiento. Encapsular la transacción
     * aquí hace claro qué está dentro de la transacción y qué no.
     *
     * @param fromAccount  ID de la cuenta que paga
     * @param amount       monto a descontar (siempre positivo, validado por validation-service)
     * @return             objeto Account con el balance ya actualizado
     * @throws InsufficientFundsException          si el saldo es insuficiente
     * @throws AccountNotFoundException            si la cuenta no existe
     * @throws org.springframework.orm.ObjectOptimisticLockingFailureException
     *                                             si otra transacción modificó la cuenta simultáneamente
     */
    @Transactional
    public Account debit(String fromAccount, BigDecimal amount) {
        log.info("💸 Iniciando débito | account={} amount={}", fromAccount, amount);

        // Paso 1: obtener la cuenta con su versión actual
        Account account = accountRepository.findById(fromAccount)
                .orElseThrow(() -> new AccountNotFoundException(
                        "Cuenta origen no encontrada para débito: " + fromAccount));

        log.debug("Cuenta encontrada | account={} balance={} version={}",
                fromAccount, account.getBalance(), account.getVersion());

        // Paso 2: verificar saldo suficiente
        // En teoría validation-service ya lo garantizó, pero:
        //   a) Entre validación y débito el saldo pudo cambiar (otra transferencia)
        //   b) Defensa en profundidad: nunca confiar en que otro servicio lo hizo bien
        if (account.getBalance().compareTo(amount) < 0) {
            log.warn("Saldo insuficiente | account={} balance={} requested={}",
                    fromAccount, account.getBalance(), amount);
            throw new InsufficientFundsException(String.format(
                    "Saldo insuficiente en cuenta %s: disponible=%s, solicitado=%s",
                    fromAccount, account.getBalance(), amount));
        }

        // Paso 3: aplicar el débito en memoria
        BigDecimal newBalance = account.getBalance().subtract(amount);
        account.setBalance(newBalance);

        // Paso 4: persistir → JPA genera:
        //   UPDATE accounts SET balance=?, version=version+1 WHERE id=? AND version=?
        //   Si otro thread modificó la fila entre el findById y el save,
        //   la version no coincide → OptimisticLockingFailureException
        Account saved = accountRepository.save(account);

        log.info("✅ Débito aplicado | account={} oldBalance={} amount={} newBalance={}",
                fromAccount,
                saved.getBalance().add(amount),  // reconstruimos el balance anterior para el log
                amount,
                saved.getBalance());

        return saved;
    }

    // ── Excepciones de dominio ────────────────────────────────────────────────

    /** Lanzada cuando el saldo es insuficiente para cubrir el monto solicitado. */
    public static class InsufficientFundsException extends RuntimeException {
        public InsufficientFundsException(String message) {
            super(message);
        }
    }

    /** Lanzada cuando la cuenta especificada no existe en la DB. */
    public static class AccountNotFoundException extends RuntimeException {
        public AccountNotFoundException(String message) {
            super(message);
        }
    }
}
