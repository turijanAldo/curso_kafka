package com.lab.account.service;

import com.lab.account.entity.ProcessedEvent;
import com.lab.account.repository.ProcessedEventRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

/**
 * Guardián contra procesamiento duplicado en account-service.
 *
 * Implementa las Capas 2 y 3 de idempotencia (la Capa 1 es el producer
 * en transfer-api con enable.idempotence=true).
 *
 * En account-service la idempotencia es ESPECIALMENTE crítica porque
 * cada operación mueve dinero real. Un débito duplicado = pérdida de
 * dinero para el cliente. Un crédito duplicado = ganancia no autorizada.
 *
 * Esta clase es structuralmente idéntica a la de validation-service,
 * con la diferencia en SERVICE_NAME.
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class IdempotencyService {

    private static final String SERVICE_NAME = "account-service";

    private final ProcessedEventRepository processedEventRepository;

    /**
     * Intenta registrar un evento como "procesado por account-service".
     *
     * Flujo:
     *   [Capa 2] SELECT EXISTS → si ya existe return false (duplicado)
     *   [Capa 3] INSERT con UNIQUE constraint
     *             → si éxito: return true (evento nuevo)
     *             → si DataIntegrityViolationException: return false (race condition, otro thread ganó)
     *
     * @param eventKey  identificador del evento: transactionId + ":" + tipoEvento
     *                  Ejemplos usados en este servicio:
     *                    "tx-uuid:TRANSFER_VALIDATED" → DebitConsumer
     *                    "tx-uuid:TRANSFER_DEBITED"   → CreditConsumer
     *                    "tx-uuid:COMPENSATION"       → CompensationService
     * @return true si el evento es nuevo y debe procesarse
     *         false si ya fue procesado (duplicado detectado)
     *
     * PROPAGATION.REQUIRES_NEW → transacción independiente.
     * Si el caller (DebitConsumer, CreditConsumer) falla DESPUÉS de que
     * tryRegister() retorna true, el registro de idempotencia NO se revierte.
     * Así, en el siguiente reintento de Kafka, el evento se detecta como
     * duplicado y se descarta sin reprocesar la operación financiera.
     */
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public boolean tryRegister(String eventKey) {

        // Capa 2: verificación rápida (mayoría de los duplicados se descartan aquí)
        if (processedEventRepository.existsByEventKeyAndServiceName(eventKey, SERVICE_NAME)) {
            log.warn("[IDEMPOTENCIA-C2] Evento ya registrado | key={}", eventKey);
            return false;
        }

        try {
            // Capa 3: INSERT atómico respaldado por UNIQUE constraint en la DB
            processedEventRepository.save(
                ProcessedEvent.builder()
                    .eventKey(eventKey)
                    .serviceName(SERVICE_NAME)
                    .build()
            );
            log.debug("[IDEMPOTENCIA-C3] Evento registrado exitosamente | key={}", eventKey);
            return true;

        } catch (DataIntegrityViolationException e) {
            // Condición de carrera: dos threads pasaron la Capa 2 al mismo tiempo.
            // La DB solo permite uno. Este thread fue el perdedor → ignorar el evento.
            log.warn("[IDEMPOTENCIA-C3] Conflicto de inserción (race condition manejada) | key={}", eventKey);
            return false;
        }
    }
}
