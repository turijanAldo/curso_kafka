package com.lab.api.service;

import com.lab.api.cache.TransferCacheService;
import com.lab.api.dto.TransferRequest;
import com.lab.api.dto.TransferResponse;
import com.lab.api.entity.Transaction;
import com.lab.api.producer.TransferEventProducer;
import com.lab.api.repository.TransactionRepository;
import com.lab.common.enums.TransferStatus;
import com.lab.common.event.TransferRequestedEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.UUID;

/**
 * Lógica de negocio del gateway de transferencias.
 *
 * ¿Por qué @Transactional en initiateTransfer?
 * Necesitamos garantizar que la inserción en DB y la publicación en Kafka
 * sean atómicas desde el punto de vista del negocio. El problema:
 * las transacciones de DB y Kafka son sistemas distintos — no existe
 * una transacción distribuida real que abarque ambos.
 *
 * Lo que @Transactional garantiza aquí:
 *   - Si la inserción en DB falla → rollback → no se publica nada en Kafka
 *   - Si Kafka falla DESPUÉS del commit de DB → el registro existe en DB
 *     con status PROCESSING pero el evento nunca llega a validation-service
 *
 * ¿Cómo se resuelve el segundo caso?
 * En producción: Outbox Pattern (guardar el evento en DB junto con la
 * transacción y publicarlo desde un proceso separado). En este lab
 * lo simplificamos: el fallo de Kafka se loguea como error y el
 * registro queda en PROCESSING indefinidamente (monitoreable).
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class TransferService {

    private final TransactionRepository transactionRepository;
    private final TransferEventProducer eventProducer;
    private final TransferCacheService cacheService;

    /**
     * Inicia una transferencia asíncrona.
     *
     * Flujo paso a paso:
     *   1. Generar UUID único para esta transferencia
     *   2. Crear la entidad Transaction con status PROCESSING
     *   3. Persistir en DB (dentro de la transacción JPA)
     *   4. Construir el evento Kafka con los mismos datos
     *   5. Publicar el evento en Kafka (asíncrono, no bloquea)
     *   6. Construir y devolver el DTO de respuesta al controller
     *
     * ¿Por qué el orden DB primero, Kafka segundo?
     * Si publicáramos en Kafka primero y la DB fallara:
     *   - validation-service recibe el evento
     *   - intenta actualizar el status de la transferencia en DB
     *   - no encuentra el registro → error en cascada
     * Con DB primero, si Kafka falla el registro existe y puede reintentarse.
     *
     * @param request DTO con fromAccount, toAccount y amount (ya validado por @Valid)
     * @return DTO con transactionId y status=PROCESSING
     */
    @Transactional
    public TransferResponse initiateTransfer(TransferRequest request) {
        // PASO 1: Generar el identificador único del Saga completo
        String transactionId = UUID.randomUUID().toString();

        log.info("Iniciando transferencia | id={} from={} to={} amount={}",
                transactionId, request.getFromAccount(),
                request.getToAccount(), request.getAmount());

        // PASO 2 y 3: Crear y persistir la transacción en DB
        // Builder pattern (generado por @Builder de Lombok): legible y seguro
        Transaction transaction = Transaction.builder()
                .id(transactionId)
                .fromAccount(request.getFromAccount())
                .toAccount(request.getToAccount())
                .amount(request.getAmount())
                .status(TransferStatus.PROCESSING)  // siempre empieza aquí
                .build();

        transactionRepository.save(transaction);
        log.debug("Transacción persistida en DB | id={}", transactionId);

        // PASO 4: Construir el evento Kafka con los mismos datos
        // Nota: usamos Instant.now() para el timestamp del evento,
        // no transaction.getCreatedAt(), porque @CreationTimestamp
        // de Hibernate puede no estar populado hasta después del flush.
        TransferRequestedEvent event = TransferRequestedEvent.builder()
                .transactionId(transactionId)
                .fromAccount(request.getFromAccount())
                .toAccount(request.getToAccount())
                .amount(request.getAmount())
                .timestamp(Instant.now())
                .build();

        // PASO 5: Publicar en Kafka (asíncrono, no bloquea el hilo HTTP)
        // Si falla, el callback en TransferEventProducer logueará el error
        eventProducer.publishTransferRequested(event);

        // PASO 6: Construir la respuesta para el cliente
        // Status siempre es PROCESSING en este punto — el Saga apenas arrancó
        return TransferResponse.builder()
                .transactionId(transactionId)
                .fromAccount(request.getFromAccount())
                .toAccount(request.getToAccount())
                .amount(request.getAmount())
                .status(TransferStatus.PROCESSING)
                .message("Transferencia iniciada. Consulta el estado con GET /transfers/" + transactionId)
                .createdAt(transaction.getCreatedAt() != null
                        ? transaction.getCreatedAt()
                        : Instant.now())
                .build();
    }

    /**
     * Consulta el estado actual de una transferencia.
     *
     * PATRÓN: Read-Through Cache
     *
     *   1. Buscar en Redis (cacheService.get)
     *      → HIT:  devolver respuesta cacheada inmediatamente (sin tocar MySQL)
     *      → MISS: continuar al paso 2
     *
     *   2. Buscar en MySQL (transactionRepository.findById)
     *
     *   3. Si existe, construir la respuesta y guardar en Redis (cacheService.put)
     *      con TTL según el estado (5s para intermedios, 10min para finales)
     *
     *   4. Devolver la respuesta
     *
     * BENEFICIO:
     *   - GET /transfers/{id} en un estado final (COMPLETED): ~0.5ms desde Redis
     *     vs ~5ms con query MySQL. Con alta concurrencia, la diferencia escala.
     *
     * STALE DATA:
     *   - Estados intermedios se cachean solo 5 segundos → el cliente puede ver
     *     estado desactualizado durante máximo 5 segundos. Aceptable para polling.
     *
     * @Transactional(readOnly = true): solo aplica cuando se llega a MySQL
     * (cache miss). Hibernate omite el flush del contexto de persistencia.
     *
     * @param transactionId UUID de la transferencia
     * @return DTO con el estado actual
     * @throws TransactionNotFoundException si el ID no existe en MySQL ni en caché
     */
    @Transactional(readOnly = true)
    public TransferResponse getStatus(String transactionId) {

        // ── PASO 1: Consultar caché Redis ────────────────────────────────────
        // Si hay hit, retornamos sin tocar MySQL. Si Redis está caído,
        // cacheService.get() retorna Optional.empty() y continuamos normalmente.
        var cached = cacheService.get(transactionId);
        if (cached.isPresent()) {
            return cached.get();
        }

        // ── PASO 2: Cache miss → consultar MySQL ─────────────────────────────
        Transaction transaction = transactionRepository
                .findById(transactionId)
                .orElseThrow(() -> new TransactionNotFoundException(
                        "Transferencia no encontrada: " + transactionId));

        TransferResponse response = TransferResponse.builder()
                .transactionId(transaction.getId())
                .fromAccount(transaction.getFromAccount())
                .toAccount(transaction.getToAccount())
                .amount(transaction.getAmount())
                .status(transaction.getStatus())
                .failureReason(transaction.getFailureReason())
                .createdAt(transaction.getCreatedAt())
                .updatedAt(transaction.getUpdatedAt())
                .message(buildStatusMessage(transaction.getStatus()))
                .build();

        // ── PASO 3: Guardar en Redis para las próximas consultas ─────────────
        // TTL corto para estados intermedios, largo para estados finales.
        // Si Redis está caído, cacheService.put() no hace nada (silencioso).
        cacheService.put(transactionId, response);

        return response;
    }

    /**
     * Convierte el enum TransferStatus en un mensaje legible para el usuario.
     * Centralizado aquí para no repetirlo en múltiples lugares.
     */
    private String buildStatusMessage(TransferStatus status) {
        return switch (status) {
            case PROCESSING  -> "En proceso: esperando validación";
            case VALIDATED   -> "Validado: procesando débito en cuenta origen";
            case DEBITED     -> "Debitado: procesando crédito en cuenta destino";
            case CREDITED    -> "Acreditado: confirmando transferencia";
            case COMPLETED   -> "✅ Transferencia completada exitosamente";
            case FAILED      -> "❌ Transferencia fallida";
            case ROLLED_BACK -> "↩️ Transferencia revertida";
        };
    }

    /**
     * Excepción interna para cuando no se encuentra una transferencia.
     * Clase interna estática: vive dentro de TransferService porque
     * solo tiene sentido en el contexto de este servicio.
     * El controller la captura y la convierte en HTTP 404.
     */
    public static class TransactionNotFoundException extends RuntimeException {
        public TransactionNotFoundException(String message) {
            super(message);
        }
    }
}
