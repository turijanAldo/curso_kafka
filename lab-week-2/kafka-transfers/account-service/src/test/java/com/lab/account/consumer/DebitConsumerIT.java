package com.lab.account.consumer;

import com.lab.account.AbstractContainerBaseTest;
import com.lab.account.entity.Account;
import com.lab.account.repository.AccountRepository;
import com.lab.account.repository.ProcessedEventRepository;
import com.lab.common.constants.KafkaTopics;
import com.lab.common.event.TransferValidatedEvent;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration Tests de DebitConsumer con Testcontainers.
 *
 * CAPA: Integración completa (Capa 3 de la pirámide — la más fiel a producción)
 *
 * Infraestructura:
 *   - MySQL 8 real (vía Testcontainers)
 *   - Kafka real (vía Testcontainers)
 *   - Spring Boot context completo
 *   - Flyway ejecuta las migraciones reales
 *
 * Casos de prueba cubiertos:
 *   1. Débito exitoso en el camino feliz
 *   2. Idempotencia: el mismo evento enviado N veces solo aplica el débito 1 vez
 *   3. Concurrencia: múltiples threads enviando el mismo evento simultáneamente
 *      solo resulta en 1 débito (Capa 3 de idempotencia con UNIQUE constraint)
 *
 * ¿Por qué estos tests son lentos?
 *   Cada test de esta clase requiere:
 *   - MySQL container: ~5-10s de arranque (solo la primera vez)
 *   - Kafka container: ~3-5s de arranque (solo la primera vez)
 *   - Spring context: ~3-5s
 *   - Tiempo de processing Kafka: variable
 *
 *   Total primera ejecución: ~15-30 segundos.
 *   Ejecuciones subsiguientes del mismo test run: mucho más rápido (containers reutilizados).
 *   ESTO ES NORMAL — es el costo de la fidelidad a producción.
 *
 * Estrategia de limpieza:
 *   @BeforeEach limpia y recrea las cuentas de prueba para que cada test
 *   empiece en un estado conocido. Esto hace los tests independientes entre sí.
 */
@DisplayName("DebitConsumer — Integration Tests con Testcontainers (MySQL + Kafka)")
class DebitConsumerIT extends AbstractContainerBaseTest {

    // ── Beans de Spring ───────────────────────────────────────────────────────

    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplate;

    @Autowired
    private AccountRepository accountRepository;

    @Autowired
    private ProcessedEventRepository processedEventRepository;

    // ── Datos de prueba ───────────────────────────────────────────────────────

    /** Balance inicial de la cuenta de prueba en cada test */
    private static final BigDecimal BALANCE_INICIAL = new BigDecimal("1000.00");
    /** ID de la cuenta que usaremos para los débitos */
    private static final String CUENTA_PRUEBA = "TEST-DEBIT-001";

    @BeforeEach
    void configurarDatosDePrueba() {
        // Limpiar estado anterior: eventos de idempotencia y cuenta de test
        processedEventRepository.deleteAll();

        // Eliminar la cuenta si existe (puede quedar de tests anteriores)
        accountRepository.deleteById(CUENTA_PRUEBA);

        // Crear cuenta de prueba con balance conocido
        Account cuenta = new Account();
        cuenta.setId(CUENTA_PRUEBA);
        cuenta.setOwnerName("Test User Débito");
        cuenta.setBalance(BALANCE_INICIAL);
        cuenta.setVersion(0L);
        accountRepository.save(cuenta);
    }

    // ════════════════════════════════════════════════════════════════════════
    // TEST 1: Camino feliz — débito exitoso
    // ════════════════════════════════════════════════════════════════════════

    @Nested
    @DisplayName("Camino feliz — débito exitoso")
    class CaminoFeliz {

        @Test
        @DisplayName("El balance se reduce correctamente al recibir transfer.validated")
        void cuandoEventoValidado_reduceSaldo() throws Exception {
            // Arrange
            BigDecimal monto = new BigDecimal("250.00");
            BigDecimal balanceEsperado = BALANCE_INICIAL.subtract(monto); // 750.00

            String txId = UUID.randomUUID().toString();
            TransferValidatedEvent evento = crearEventoValidado(txId, CUENTA_PRUEBA, "ACC-002", monto);

            // Act: publicar el evento al topic de Kafka
            kafkaTemplate.send(KafkaTopics.TRANSFER_VALIDATED, evento.getFromAccount(), evento);
            kafkaTemplate.flush();

            // Assert: esperar que el DebitConsumer procese el evento y modifique el balance
            // Awaitility sondea cada 300ms y falla si no se cumple en 20s
            Awaitility.await()
                    .atMost(20, TimeUnit.SECONDS)
                    .pollInterval(Duration.ofMillis(300))
                    .untilAsserted(() -> {
                        Account cuentaActualizada = accountRepository.findById(CUENTA_PRUEBA)
                                .orElseThrow(() -> new AssertionError("Cuenta no encontrada"));

                        assertThat(cuentaActualizada.getBalance())
                                .as("El balance debe reducirse en %s", monto)
                                .isEqualByComparingTo(balanceEsperado);
                    });

            // Assert adicional: el evento fue registrado en processed_events
            Awaitility.await()
                    .atMost(5, TimeUnit.SECONDS)
                    .untilAsserted(() -> {
                        boolean registrado = processedEventRepository.findAll().stream()
                                .anyMatch(e -> e.getEventKey().startsWith(txId));
                        assertThat(registrado)
                                .as("El evento debe registrarse en processed_events para idempotencia")
                                .isTrue();
                    });
        }
    }

    // ════════════════════════════════════════════════════════════════════════
    // TEST 2: Idempotencia — el mismo evento N veces
    // ════════════════════════════════════════════════════════════════════════

    @Nested
    @DisplayName("Idempotencia — mismo evento enviado múltiples veces")
    class Idempotencia {

        @Test
        @DisplayName("El mismo evento enviado 3 veces solo aplica el débito 1 vez")
        void cuandoMismoEventoTresVeces_soloDebitaUnaVez() throws Exception {
            // Arrange: un evento que se enviará 3 veces
            BigDecimal monto = new BigDecimal("100.00");
            BigDecimal balanceEsperadoTrasUnDebito = BALANCE_INICIAL.subtract(monto); // 900.00

            String txId = UUID.randomUUID().toString();
            TransferValidatedEvent evento = crearEventoValidado(txId, CUENTA_PRUEBA, "ACC-002", monto);

            // Act: enviar el mismo evento 3 veces (simula reentregas de Kafka)
            for (int i = 0; i < 3; i++) {
                kafkaTemplate.send(KafkaTopics.TRANSFER_VALIDATED, evento.getFromAccount(), evento);
            }
            kafkaTemplate.flush();

            // Esperar tiempo suficiente para que los 3 mensajes sean procesados
            Thread.sleep(5000);

            // Assert: el balance solo se redujo UNA VEZ (idempotencia funcionó)
            Account cuentaFinal = accountRepository.findById(CUENTA_PRUEBA)
                    .orElseThrow(() -> new AssertionError("Cuenta no encontrada"));

            assertThat(cuentaFinal.getBalance())
                    .as("El balance debe ser %s (solo 1 débito de 100), no %s (3 débitos)",
                            balanceEsperadoTrasUnDebito, BALANCE_INICIAL.subtract(monto.multiply(BigDecimal.valueOf(3))))
                    .isEqualByComparingTo(balanceEsperadoTrasUnDebito);

            // Assert: solo 1 registro en processed_events para este txId
            long countEventos = processedEventRepository.findAll().stream()
                    .filter(e -> e.getEventKey().startsWith(txId))
                    .count();
            assertThat(countEventos)
                    .as("Solo debe existir 1 registro de idempotencia para txId=%s", txId)
                    .isEqualTo(1);
        }
    }

    // ════════════════════════════════════════════════════════════════════════
    // TEST 3: Concurrencia — Optimistic Locking + Idempotencia
    // ════════════════════════════════════════════════════════════════════════

    @Nested
    @DisplayName("Concurrencia — Optimistic Locking e idempotencia bajo carga")
    class Concurrencia {

        @Test
        @DisplayName("5 threads enviando el mismo evento simultáneamente → solo 1 débito")
        void cuandoCincoThreadsEnvianMismoEvento_soloDebitaUnaVez() throws Exception {
            // Arrange: definir el evento que todos los threads intentarán enviar
            BigDecimal monto = new BigDecimal("200.00");
            BigDecimal balanceEsperado = BALANCE_INICIAL.subtract(monto); // 800.00

            String txId = UUID.randomUUID().toString();
            TransferValidatedEvent evento = crearEventoValidado(txId, CUENTA_PRUEBA, "ACC-002", monto);

            int NUM_THREADS = 5;
            ExecutorService executor = Executors.newFixedThreadPool(NUM_THREADS);
            CountDownLatch startLatch = new CountDownLatch(1);
            CountDownLatch doneLatch = new CountDownLatch(NUM_THREADS);
            List<Exception> errores = new ArrayList<>();

            // Act: todos los threads esperan el startLatch y luego
            // envían el mismo evento simultáneamente
            for (int i = 0; i < NUM_THREADS; i++) {
                executor.submit(() -> {
                    try {
                        startLatch.await(); // Esperar la señal de inicio
                        kafkaTemplate.send(KafkaTopics.TRANSFER_VALIDATED,
                                evento.getFromAccount(), evento);
                        kafkaTemplate.flush();
                    } catch (Exception e) {
                        errores.add(e);
                    } finally {
                        doneLatch.countDown();
                    }
                });
            }

            // Señal de inicio: todos los threads arrancaron
            startLatch.countDown();
            // Esperar que todos los threads terminen de enviar
            doneLatch.await(10, TimeUnit.SECONDS);
            executor.shutdown();

            // Verificar que no hubo errores al enviar
            assertThat(errores).as("No deben haber errores al publicar los eventos").isEmpty();

            // Esperar que Kafka y los consumers procesen todos los mensajes
            // Tiempo extra porque hay 5 mensajes que el consumer debe procesar
            Thread.sleep(8000);

            // Assert: el balance solo se redujo UNA VEZ
            // Aunque 5 mensajes llegaron a Kafka, la idempotencia (UNIQUE constraint)
            // garantiza que solo el primero en "ganar la carrera" aplica el débito.
            Account cuentaFinal = accountRepository.findById(CUENTA_PRUEBA)
                    .orElseThrow(() -> new AssertionError("Cuenta no encontrada"));

            assertThat(cuentaFinal.getBalance())
                    .as("Con 5 threads enviando el mismo evento, el balance debe reducirse solo 1 vez: %s",
                            balanceEsperado)
                    .isEqualByComparingTo(balanceEsperado);

            // Assert: exactamente 1 registro en processed_events
            long countRegistros = processedEventRepository.findAll().stream()
                    .filter(e -> e.getEventKey().startsWith(txId))
                    .count();
            assertThat(countRegistros)
                    .as("La UNIQUE constraint debe garantizar exactamente 1 registro de idempotencia")
                    .isEqualTo(1);
        }

        @Test
        @DisplayName("Dos transferencias diferentes sobre la misma cuenta — ambas se aplican")
        void dosTransferenciasDistintas_ambasDebitanCorrectamente() throws Exception {
            // Este test verifica que la idempotencia solo bloquea DUPLICADOS,
            // no transacciones diferentes sobre la misma cuenta.

            BigDecimal monto1 = new BigDecimal("100.00");
            BigDecimal monto2 = new BigDecimal("150.00");
            // Balance esperado: 1000 - 100 - 150 = 750
            BigDecimal balanceEsperado = BALANCE_INICIAL.subtract(monto1).subtract(monto2);

            // IDs únicos para cada transferencia
            String txId1 = UUID.randomUUID().toString();
            String txId2 = UUID.randomUUID().toString();

            TransferValidatedEvent evento1 = crearEventoValidado(txId1, CUENTA_PRUEBA, "ACC-002", monto1);
            TransferValidatedEvent evento2 = crearEventoValidado(txId2, CUENTA_PRUEBA, "ACC-003", monto2);

            // Act: enviar ambas transferencias
            kafkaTemplate.send(KafkaTopics.TRANSFER_VALIDATED, evento1.getFromAccount(), evento1);
            kafkaTemplate.send(KafkaTopics.TRANSFER_VALIDATED, evento2.getFromAccount(), evento2);
            kafkaTemplate.flush();

            // Assert: esperar que ambas se procesen
            Awaitility.await()
                    .atMost(25, TimeUnit.SECONDS)
                    .pollInterval(Duration.ofMillis(500))
                    .untilAsserted(() -> {
                        // Verificar que hay 2 registros en processed_events
                        long count = processedEventRepository.findAll().stream()
                                .filter(e -> e.getEventKey().startsWith(txId1)
                                        || e.getEventKey().startsWith(txId2))
                                .count();
                        assertThat(count)
                                .as("Deben procesarse 2 transacciones distintas")
                                .isEqualTo(2);
                    });

            // Verificar el balance final
            Account cuentaFinal = accountRepository.findById(CUENTA_PRUEBA)
                    .orElseThrow(() -> new AssertionError("Cuenta no encontrada"));

            assertThat(cuentaFinal.getBalance())
                    .as("Balance final: %s - %s - %s = %s",
                            BALANCE_INICIAL, monto1, monto2, balanceEsperado)
                    .isEqualByComparingTo(balanceEsperado);
        }
    }

    // ── Helpers ──────────────────────────────────────────────────────────────

    /**
     * Crea un TransferValidatedEvent para usar en los tests.
     * En producción, este evento lo genera validation-service.
     * Aquí lo generamos directamente para simular que ya fue validado.
     */
    private TransferValidatedEvent crearEventoValidado(String txId, String from,
                                                        String to, BigDecimal amount) {
        return TransferValidatedEvent.builder()
                .transactionId(txId)
                .fromAccount(from)
                .toAccount(to)
                .amount(amount)
                .timestamp(Instant.now())
                .build();
    }
}
