package com.lab.validation.service;

import com.lab.common.event.TransferRequestedEvent;
import com.lab.validation.producer.ValidationEventProducer;
import com.lab.validation.repository.AccountRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;

/**
 * Servicio de validación de reglas de negocio.
 *
 * Responsabilidad única: dado un TransferRequestedEvent, determinar si
 * la transferencia puede continuar o debe rechazarse, y publicar el evento
 * correspondiente (VALIDATED o FAILED).
 *
 * NO hace ningún débito ni crédito — solo valida que los datos son correctos.
 * El principio es "fail fast": al primer error detectado, publicar FAILED
 * inmediatamente sin seguir evaluando más reglas. Esto evita acumular
 * múltiples eventos de fallo para la misma transferencia.
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class ValidationService {

    private final AccountRepository accountRepository;
    private final ValidationEventProducer producer;

    /**
     * Valida una solicitud de transferencia aplicando las 4 reglas de negocio
     * en orden de menor a mayor costo de operación.
     *
     * ORDEN DE LAS REGLAS — ¿por qué este orden y no otro?
     *
     *   Regla 1: fromAccount != toAccount (comparación en memoria, sin DB)
     *   Regla 2: monto > 0              (comparación en memoria, sin DB)
     *   Regla 3: cuenta origen existe    (1 query a DB)
     *   Regla 4: cuenta destino existe   (1 query a DB)
     *
     * Las reglas sin costo de DB van primero. Si la transferencia es
     * claramente inválida (misma cuenta, monto cero), ahorramos queries.
     * La regla de saldo suficiente NO se valida aquí — account-service
     * la verifica con Optimistic Locking en el momento exacto del débito,
     * lo que garantiza que el saldo no cambió entre la validación y el débito.
     *
     * @param event el evento recibido desde transfer.requested
     */
    public void validate(TransferRequestedEvent event) {
        log.info("Validando transferencia | transactionId={} from={} to={} amount={}",
                event.getTransactionId(), event.getFromAccount(),
                event.getToAccount(), event.getAmount());

        // ── Regla 1: cuentas distintas ───────────────────────────────────
        // Verificación en memoria: sin costo de DB. Va primero.
        if (event.getFromAccount().equals(event.getToAccount())) {
            log.warn("Validación FALLIDA — misma cuenta origen/destino | txId={}",
                    event.getTransactionId());
            producer.publishFailed(event,
                    "La cuenta origen y destino no pueden ser la misma",
                    "validation-service");
            return;  // fail fast: no seguir validando
        }

        // ── Regla 2: monto positivo ──────────────────────────────────────
        // Defensa en profundidad: transfer-api ya validó esto con @Positive.
        // Lo repetimos aquí porque un evento podría llegar de otra fuente
        // que no tenga esa validación (otro producer mal configurado).
        if (event.getAmount() == null || event.getAmount().compareTo(BigDecimal.ZERO) <= 0) {
            log.warn("Validación FALLIDA — monto inválido {} | txId={}",
                    event.getAmount(), event.getTransactionId());
            producer.publishFailed(event,
                    "El monto debe ser mayor que cero. Recibido: " + event.getAmount(),
                    "validation-service");
            return;
        }

        // ── Regla 3: cuenta origen existe en la DB ───────────────────────
        // existsById() genera: SELECT EXISTS(SELECT 1 FROM accounts WHERE id = ?)
        // Más eficiente que findById() porque no transfiere toda la fila.
        if (!accountRepository.existsById(event.getFromAccount())) {
            log.warn("Validación FALLIDA — cuenta origen no encontrada: {} | txId={}",
                    event.getFromAccount(), event.getTransactionId());
            producer.publishFailed(event,
                    "Cuenta origen no encontrada: " + event.getFromAccount(),
                    "validation-service");
            return;
        }

        // ── Regla 4: cuenta destino existe en la DB ──────────────────────
        if (!accountRepository.existsById(event.getToAccount())) {
            log.warn("Validación FALLIDA — cuenta destino no encontrada: {} | txId={}",
                    event.getToAccount(), event.getTransactionId());
            producer.publishFailed(event,
                    "Cuenta destino no encontrada: " + event.getToAccount(),
                    "validation-service");
            return;
        }

        // ── Todas las reglas pasaron: publicar VALIDATED ─────────────────
        log.info("✅ Validación EXITOSA | transactionId={}", event.getTransactionId());
        producer.publishValidated(event);
    }
}
