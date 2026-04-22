package com.lab.account.service;

import com.lab.account.entity.Account;
import com.lab.account.repository.AccountRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;

/**
 * Servicio de crédito — acredita el monto en la cuenta destino.
 *
 * El crédito ocurre DESPUÉS del débito (en respuesta al evento TRANSFER_DEBITED).
 * En la Saga choreografiada:
 *
 *   transfer.validated → [débito] → transfer.debited → [crédito] → transfer.credited
 *                                                                 ↘ (si falla) transfer.failed
 *                                                                             + transfer.compensated
 *
 * El flag simulate-credit-failure permite probar el flujo de compensación
 * sin necesidad de introducir un bug real. Al activarlo, el servicio
 * actúa como si el crédito hubiera fallado, disparando el rollback del Saga.
 *
 * En producción, un fallo real podría ser:
 *   - La cuenta destino fue bloqueada mientras la transferencia estaba en vuelo
 *   - La cuenta destino fue cerrada
 *   - Un error externo de un sistema de pagos
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class CreditService {

    /**
     * Flag de simulación de fallo. Se inyecta desde application.yml:
     *   app.simulate-credit-failure: false
     *
     * Para activar el rollback del Saga en PASO 8:
     *   mvn spring-boot:run -Dspring-boot.run.arguments="--app.simulate-credit-failure=true"
     * O modificar application.yml antes de arrancar.
     *
     * El valor por defecto (false) asegura que el flujo normal funciona
     * sin configuración adicional.
     */
    @Value("${app.simulate-credit-failure:false}")
    private boolean simulateCreditFailure;

    private final AccountRepository accountRepository;

    /**
     * Acredita el monto en la cuenta destino.
     *
     * SECUENCIA INTERNA con @Transactional:
     *   1. Si simulateCreditFailure=true → lanzar SimulatedCreditFailureException
     *      (ANTES de tocar la DB — el crédito no ocurre)
     *   2. SELECT * FROM accounts WHERE id=? → obtener cuenta destino con version
     *   3. balance = balance + amount (en memoria)
     *   4. UPDATE accounts SET balance=?, version=version+1 WHERE id=? AND version=?
     *   5. Retornar Account actualizado
     *
     * El crédito también tiene Optimistic Locking pero es mucho menos probable
     * que genere conflictos: la cuenta destino no suele tener transferencias
     * concurrentes de distintos remitentes (aunque es posible).
     *
     * @param toAccount cuenta que recibe el dinero
     * @param amount    monto a acreditar
     * @return          Account con el nuevo balance (balance original + amount)
     * @throws SimulatedCreditFailureException si el flag de simulación está activo
     * @throws AccountNotFoundException si la cuenta destino no existe
     */
    @Transactional
    public Account credit(String toAccount, BigDecimal amount) {
        log.info("💰 Iniciando crédito | account={} amount={}", toAccount, amount);

        // Punto de inyección de fallo para el PASO 8 (demo de compensación)
        if (simulateCreditFailure) {
            log.warn("⚠️ FALLO SIMULADO activado — el crédito no se aplicará | account={}", toAccount);
            throw new SimulatedCreditFailureException(
                    "Fallo simulado en el crédito (app.simulate-credit-failure=true). " +
                    "La Saga iniciará compensación para revertir el débito.");
        }

        // Obtener la cuenta destino
        Account account = accountRepository.findById(toAccount)
                .orElseThrow(() -> new AccountNotFoundException(
                        "Cuenta destino no encontrada para crédito: " + toAccount));

        log.debug("Cuenta destino encontrada | account={} currentBalance={} version={}",
                toAccount, account.getBalance(), account.getVersion());

        // Acreditar el monto
        BigDecimal newBalance = account.getBalance().add(amount);
        account.setBalance(newBalance);

        Account saved = accountRepository.save(account);

        log.info("✅ Crédito aplicado | account={} amount={} newBalance={}",
                toAccount, amount, saved.getBalance());

        return saved;
    }

    // ── Excepción de simulación ───────────────────────────────────────────────

    /**
     * Excepción lanzada intencionalmente cuando simulate-credit-failure=true.
     * No es un bug — es una herramienta de testing del flujo de compensación.
     * CreditConsumer la captura y activa CompensationService.
     */
    public static class SimulatedCreditFailureException extends RuntimeException {
        public SimulatedCreditFailureException(String message) {
            super(message);
        }
    }

    /**
     * La cuenta destino no existe en el momento del crédito.
     * Puede ocurrir si la cuenta fue cerrada entre la validación y el crédito.
     * CreditConsumer la captura e inicia la compensación (igual que SimulatedCreditFailureException).
     */
    public static class AccountNotFoundException extends RuntimeException {
        public AccountNotFoundException(String message) {
            super(message);
        }
    }
}
