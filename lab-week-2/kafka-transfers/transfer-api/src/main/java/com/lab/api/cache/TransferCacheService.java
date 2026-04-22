package com.lab.api.cache;

import com.lab.api.dto.TransferResponse;
import com.lab.common.enums.TransferStatus;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.Optional;

/**
 * Caché de estado de transferencias sobre Redis (Bonus A).
 *
 * ────────────────────────────────────────────────────────────────────────────
 * PROBLEMA QUE RESUELVE
 * ────────────────────────────────────────────────────────────────────────────
 *
 * Cuando un cliente consulta GET /transfers/{id} repetidamente para conocer
 * el estado de su transferencia ("polling"), cada llamada va a MySQL. Con alta
 * concurrencia (miles de transferencias activas), esto se convierte en un
 * cuello de botella de base de datos.
 *
 * La solución es un cache de lectura (read-through cache): transfer-api primero
 * consulta Redis. Si hay hit → respuesta inmediata sin tocar MySQL. Si hay miss
 * → consulta MySQL, guarda en Redis, devuelve la respuesta.
 *
 * ────────────────────────────────────────────────────────────────────────────
 * TTL DIFERENCIADO POR ESTADO
 * ────────────────────────────────────────────────────────────────────────────
 *
 * No todos los estados se cachean igual:
 *
 * Estados FINALES (COMPLETED, FAILED, ROLLED_BACK):
 *   → TTL largo: 10 minutos
 *   → El estado nunca más cambia. Podemos cachearlo mucho tiempo.
 *   → Si el cliente consulta 100 veces "¿está completada mi transferencia?"
 *     después de que completó, las 100 respuestas vienen de Redis.
 *
 * Estados INTERMEDIOS (PROCESSING, VALIDATED, DEBITED, CREDITED):
 *   → TTL corto: 5 segundos
 *   → El estado CAMBIARÁ pronto (el Saga sigue avanzando).
 *   → Un TTL largo causaría stale reads: el cliente vería PROCESSING
 *     durante minutos aunque el Saga ya completó hace 2 minutos.
 *   → Con 5 segundos, la información "tiene hasta 5 segundos de retraso"
 *     — aceptable para un sistema de polling.
 *
 * ────────────────────────────────────────────────────────────────────────────
 * INVALIDACIÓN DE CACHÉ (cache eviction)
 * ────────────────────────────────────────────────────────────────────────────
 *
 * En este diseño, la caché expira por TTL. No hay invalidación activa.
 *
 * Alternativa: status-service publica un evento "estado cambiado" que
 * transfer-api consume via Kafka para invalidar la entrada en Redis.
 * Eso daría consistencia inmediata pero añade complejidad (un consumer
 * adicional en transfer-api). Para el lab, el TTL es suficiente.
 *
 * ────────────────────────────────────────────────────────────────────────────
 * TOLERANCIA A FALLOS
 * ────────────────────────────────────────────────────────────────────────────
 *
 * Si Redis no está disponible, get() retorna Optional.empty() y put() no hace
 * nada → TransferService cae sobre MySQL automáticamente. El sistema degrada
 * con elegancia: más lento pero funcional.
 *
 * Usamos try/catch en cada operación Redis para garantizar este comportamiento.
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class TransferCacheService {

    private final RedisTemplate<String, TransferResponse> redisTemplate;

    /** Prefijo de las keys Redis. Ejemplo: "transfer:f47ac10b-58cc-..." */
    private static final String KEY_PREFIX = "transfer:";

    /** TTL para estados finales (el estado ya no cambiará) */
    private static final Duration TTL_FINAL = Duration.ofMinutes(10);

    /** TTL para estados intermedios (el Saga sigue avanzando) */
    private static final Duration TTL_INTERMEDIATE = Duration.ofSeconds(5);

    /**
     * Intenta obtener el estado de una transferencia desde Redis.
     *
     * @param transactionId ID de la transferencia
     * @return Optional con el TransferResponse si está en caché, vacío si no
     */
    public Optional<TransferResponse> get(String transactionId) {
        String key = KEY_PREFIX + transactionId;
        try {
            TransferResponse cached = redisTemplate.opsForValue().get(key);
            if (cached != null) {
                log.debug("✅ Cache HIT | txId={}", transactionId);
                return Optional.of(cached);
            }
            log.debug("❌ Cache MISS | txId={}", transactionId);
            return Optional.empty();
        } catch (Exception e) {
            // Redis no disponible → degradación elegante, continuamos sin caché
            log.warn("⚠️ Redis no disponible para GET | txId={} | error={}", transactionId, e.getMessage());
            return Optional.empty();
        }
    }

    /**
     * Guarda el estado de una transferencia en Redis con TTL según el estado.
     *
     * @param transactionId ID de la transferencia
     * @param response      respuesta a cachear
     */
    public void put(String transactionId, TransferResponse response) {
        String key = KEY_PREFIX + transactionId;
        Duration ttl = isFinalState(response.getStatus()) ? TTL_FINAL : TTL_INTERMEDIATE;
        try {
            redisTemplate.opsForValue().set(key, response, ttl);
            log.debug("💾 Cache WRITE | txId={} status={} ttl={}s",
                    transactionId, response.getStatus(), ttl.getSeconds());
        } catch (Exception e) {
            // Redis no disponible → el sistema funciona igual, sin caché
            log.warn("⚠️ Redis no disponible para WRITE | txId={} | error={}", transactionId, e.getMessage());
        }
    }

    /**
     * Elimina la entrada de caché de una transferencia.
     * Útil para invalidación explícita (no se usa en el flujo normal del lab,
     * pero disponible para pruebas o para un futuro consumer de invalidación).
     *
     * @param transactionId ID de la transferencia
     */
    public void evict(String transactionId) {
        String key = KEY_PREFIX + transactionId;
        try {
            redisTemplate.delete(key);
            log.debug("🗑️ Cache EVICT | txId={}", transactionId);
        } catch (Exception e) {
            log.warn("⚠️ Redis no disponible para DELETE | txId={} | error={}", transactionId, e.getMessage());
        }
    }

    /**
     * Determina si un estado es final (no cambiará más).
     *
     * Estados finales: COMPLETED, FAILED, ROLLED_BACK
     * Estados intermedios: PROCESSING, VALIDATED, DEBITED, CREDITED
     *
     * @param status estado actual de la transferencia
     * @return true si el estado es terminal y puede cachearse mucho tiempo
     */
    private boolean isFinalState(TransferStatus status) {
        return switch (status) {
            case COMPLETED, FAILED, ROLLED_BACK -> true;
            case PROCESSING, VALIDATED, DEBITED, CREDITED -> false;
        };
    }
}
