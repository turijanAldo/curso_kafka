package com.lab.account.service;

import com.lab.account.entity.Account;
import com.lab.account.repository.AccountRepository;
import com.lab.account.repository.ProcessedEventRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;

/**
 * Servicio de compensación — revierte un débito cuando el crédito falla.
 *
 * En la Saga choreografiada, si el crédito falla después de que el débito
 * ya fue aplicado, hay que devolver el dinero al cuenta origen. Esto se
 * llama "compensación" (rollback del Saga).
 *
 * FLUJO DE COMPENSACIÓN:
 *
 *   1. CreditConsumer procesa transfer.debited
 *   2. CreditService.credit() lanza SimulatedCreditFailureException (o real)
 *   3. CreditConsumer llama compensationService.compensate() [in-process]
 *   4. CompensationService restaura el balance de fromAccount
 *   5. CompensationService publica transfer.compensated
 *   6. CreditConsumer publica transfer.failed
 *   7. CreditConsumer hace ACK
 *
 * ¿Por qué la compensación es in-process (llamada directa) y no via un
 * nuevo topic de Kafka?
 * Porque simplifica el Saga para el propósito del lab. En producción con
 * múltiples servicios y pasos más complejos, la compensación se coordinaría
 * via eventos Kafka con un CompensationConsumer dedicado.
 *
 * La compensación también es IDEMPOTENTE: si el mensaje de compensación
 * llega dos veces (Kafka redelivery), solo la primera vez se aplica.
 * La clave de idempotencia es: transactionId + ":COMPENSATION"
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class CompensationService {

    private static final String SERVICE_NAME = "account-service";

    private final AccountRepository accountRepository;
    private final ProcessedEventRepository processedEventRepository;

    /**
     * Revierte el débito restaurando el saldo de la cuenta origen.
     *
     * @param transactionId  ID de la transacción que se está compensando
     * @param fromAccount    cuenta a la que hay que devolver el dinero
     * @param amount         monto que se descontó en el débito (y hay que devolver)
     * @return               Account con el saldo restaurado, o null si ya fue compensado
     */
    @Transactional
    public Account compensate(String transactionId, String fromAccount, BigDecimal amount) {
        String compensationKey = transactionId + ":COMPENSATION";

        log.warn("🔄 Iniciando compensación | txId={} fromAccount={} amount={}",
                transactionId, fromAccount, amount);

        // Idempotencia de compensación: si ya fue compensada (reintento), no volver a hacerlo
        // Aquí usamos la misma tabla processed_events con una clave distinta
        if (processedEventRepository.existsByEventKeyAndServiceName(compensationKey, SERVICE_NAME)) {
            log.warn("⚠️ Compensación ya aplicada anteriormente | txId={}", transactionId);
            return null;
        }

        // Obtener la cuenta origen
        Account account = accountRepository.findById(fromAccount)
                .orElseThrow(() -> new DebitService.AccountNotFoundException(
                        "Cuenta no encontrada para compensación: " + fromAccount));

        log.debug("Compensando cuenta | account={} balanceActual={} amount={}",
                fromAccount, account.getBalance(), amount);

        // Restaurar el saldo: devolver el monto que se descontó en el débito
        account.setBalance(account.getBalance().add(amount));
        Account saved = accountRepository.save(account);

        // Registrar la compensación en processed_events para idempotencia
        try {
            processedEventRepository.save(
                com.lab.account.entity.ProcessedEvent.builder()
                    .eventKey(compensationKey)
                    .serviceName(SERVICE_NAME)
                    .build()
            );
        } catch (DataIntegrityViolationException e) {
            // Race condition: otro thread también compensó. El resultado es el mismo.
            log.warn("Conflicto al registrar compensación (race condition manejada) | txId={}", transactionId);
        }

        log.info("✅ Compensación aplicada | account={} balanceRestaurado={}",
                fromAccount, saved.getBalance());

        return saved;
    }
}
