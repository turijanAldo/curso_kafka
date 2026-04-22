package com.lab.validation.consumer;

import com.lab.common.constants.KafkaTopics;
import com.lab.common.event.TransferRequestedEvent;
import com.lab.validation.repository.ProcessedEventRepository;
import com.lab.validation.service.ValidationService;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * Integration Test de TransferRequestedConsumer.
 *
 * CAPA: Integración con @EmbeddedKafka (Capa 2 de la pirámide)
 *
 * ¿Qué prueba este test?
 *   1. El consumer recibe un mensaje de transfer.requested y lo procesa
 *   2. La idempotencia funciona: el mismo evento procesado dos veces
 *      solo resulta en 1 registro en processed_events
 *   3. Un evento válido publica en transfer.validated
 *   4. Un evento inválido publica en transfer.failed
 *
 * Infraestructura utilizada:
 *   - @EmbeddedKafka: broker Kafka en memoria (sin Docker, sin red)
 *   - H2: base de datos en memoria (activa por @ActiveProfiles("test"))
 *   - @SpringBootTest: levanta el contexto completo de Spring Boot
 *
 * @EmbeddedKafka vs Testcontainers:
 *   @EmbeddedKafka es más rápido (no necesita Docker) pero menos fiel
 *   a producción. Ideal para verificar la lógica de integración básica.
 *   Testcontainers se usa cuando necesitas el comportamiento exacto de
 *   Kafka (compresión, TLS, seguridad, etc.).
 *
 * @DirtiesContext: indica a Spring que este test contamina el contexto
 * (el broker embebido es stateful). El contexto se recrea para el siguiente test.
 */
@SpringBootTest
@EmbeddedKafka(
        partitions = 1,
        topics = {
                KafkaTopics.TRANSFER_REQUESTED,   // "transfer.requested"
                KafkaTopics.TRANSFER_VALIDATED,   // "transfer.validated"
                KafkaTopics.TRANSFER_FAILED       // "transfer.failed"
        },
        // Inyecta automáticamente los bootstrap-servers del broker embebido
        // en spring.kafka.bootstrap-servers. El consumer y producer de la
        // aplicación se conectarán al broker de test en lugar del de producción.
        brokerProperties = {
                "listeners=PLAINTEXT://localhost:29092",
                "port=29092"
        }
)
@ActiveProfiles("test")  // Activa application-test.yml (H2, Flyway deshabilitado)
@DirtiesContext           // Recrea el contexto de Spring después de este test
@DisplayName("TransferRequestedConsumer — Integration Tests con @EmbeddedKafka")
class TransferRequestedConsumerIT {

    // ── Infraestructura de test ──────────────────────────────────────────────

    @Autowired
    private EmbeddedKafkaBroker embeddedKafkaBroker;

    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplate;

    // ── Repositorios para verificar el estado de la DB ───────────────────────

    @Autowired
    private ProcessedEventRepository processedEventRepository;

    // ── SpyBean: envuelve el bean real para verificar llamadas ───────────────
    // @SpyBean ≠ @MockBean:
    //   @MockBean  → reemplaza el bean con un stub que no hace nada
    //   @SpyBean   → envuelve el bean real; el código real SÍ se ejecuta,
    //               pero podemos verificar que fue llamado y con qué argumentos.
    // Usamos @SpyBean para verificar que ValidationService.validate() fue
    // invocado, sin dejar de ejecutar la lógica real.
    @SpyBean
    private ValidationService validationService;

    @BeforeEach
    void limpiarDB() {
        // Limpiar la tabla de idempotencia entre tests para independencia
        processedEventRepository.deleteAll();
    }

    // ════════════════════════════════════════════════════════════════════════
    // TEST 1: El consumer procesa un evento válido
    // ════════════════════════════════════════════════════════════════════════

    @Test
    @DisplayName("Procesa evento válido: registra idempotencia y llama a ValidationService")
    void cuandoEventoValido_procesaYRegistraIdempotencia() throws Exception {
        // Arrange: crear evento válido
        String txId = UUID.randomUUID().toString();
        TransferRequestedEvent evento = crearEventoValido(txId, "ACC-001", "ACC-002", "100.00");

        // Act: publicar el evento al topic de Kafka
        kafkaTemplate.send(KafkaTopics.TRANSFER_REQUESTED, evento.getFromAccount(), evento);
        kafkaTemplate.flush();

        // Assert: esperar (con Awaitility) que ValidationService sea invocado
        // Awaitility.await() es la alternativa correcta a Thread.sleep():
        //   - Prueba la condición cada 100ms
        //   - Falla con AssertionError si no se cumple en 10 segundos
        //   - Hace el test determinista: pasa exactamente cuando está listo
        Awaitility.await()
                .atMost(10, TimeUnit.SECONDS)
                .pollInterval(Duration.ofMillis(200))
                .untilAsserted(() ->
                        verify(validationService, times(1)).validate(any())
                );

        // Assert: verificar que se registró en processed_events (Capa 2 de idempotencia)
        Awaitility.await()
                .atMost(5, TimeUnit.SECONDS)
                .untilAsserted(() -> {
                    long count = processedEventRepository
                            .findAll()
                            .stream()
                            .filter(e -> e.getEventKey().startsWith(txId))
                            .count();
                    assertThat(count)
                            .as("Debe haber exactamente 1 registro de idempotencia")
                            .isEqualTo(1);
                });
    }

    // ════════════════════════════════════════════════════════════════════════
    // TEST 2: Idempotencia — el mismo evento dos veces
    // ════════════════════════════════════════════════════════════════════════

    @Test
    @DisplayName("Idempotencia: el mismo evento enviado 2 veces solo se procesa 1 vez")
    void cuandoMismoEventoDosVeces_soloProcessaUnaVez() throws Exception {
        // Arrange: un solo evento que se enviará dos veces
        String txId = UUID.randomUUID().toString();
        TransferRequestedEvent evento = crearEventoValido(txId, "ACC-001", "ACC-002", "200.00");

        // Act: enviar el MISMO evento dos veces (simula reentrega de Kafka)
        kafkaTemplate.send(KafkaTopics.TRANSFER_REQUESTED, evento.getFromAccount(), evento);
        kafkaTemplate.flush();

        // Esperar que el primer evento sea procesado antes de enviar el segundo
        Awaitility.await()
                .atMost(10, TimeUnit.SECONDS)
                .until(() -> processedEventRepository.findAll().stream()
                        .anyMatch(e -> e.getEventKey().startsWith(txId)));

        // Enviar el duplicado
        kafkaTemplate.send(KafkaTopics.TRANSFER_REQUESTED, evento.getFromAccount(), evento);
        kafkaTemplate.flush();

        // Esperar un poco para dar tiempo al consumer de procesar el duplicado
        Thread.sleep(2000);

        // Assert: ValidationService solo fue llamado 1 vez (el duplicado fue filtrado)
        verify(validationService, times(1)).validate(any());

        // Assert: Solo 1 registro en processed_events (no 2)
        long count = processedEventRepository.findAll().stream()
                .filter(e -> e.getEventKey().startsWith(txId))
                .count();
        assertThat(count)
                .as("La idempotencia debe producir exactamente 1 registro aunque el evento llegue 2 veces")
                .isEqualTo(1);
    }

    // ════════════════════════════════════════════════════════════════════════
    // TEST 3: Evento inválido → transfer.failed
    // ════════════════════════════════════════════════════════════════════════

    @Test
    @DisplayName("Evento con cuenta inválida publica en transfer.failed")
    void cuandoEventoInvalido_publicaEnTransferFailed() throws Exception {
        // Arrange: crear un consumer de test para leer el topic transfer.failed
        Consumer<String, String> testConsumer = crearConsumerDeTest("transfer-failed-test-group");
        embeddedKafkaBroker.consumeFromAnEmbeddedTopic(testConsumer, KafkaTopics.TRANSFER_FAILED);

        // Evento con misma cuenta origen y destino → fallará la Regla 1
        String txId = UUID.randomUUID().toString();
        TransferRequestedEvent eventoInvalido = TransferRequestedEvent.builder()
                .transactionId(txId)
                .fromAccount("ACC-001")
                .toAccount("ACC-001")      // ← misma cuenta: regla 1 falla
                .amount(new BigDecimal("50.00"))
                .timestamp(Instant.now())
                .build();

        // Act
        kafkaTemplate.send(KafkaTopics.TRANSFER_REQUESTED,
                eventoInvalido.getFromAccount(), eventoInvalido);
        kafkaTemplate.flush();

        // Assert: debe aparecer un mensaje en transfer.failed
        Awaitility.await()
                .atMost(10, TimeUnit.SECONDS)
                .untilAsserted(() -> {
                    ConsumerRecord<String, String> record =
                            KafkaTestUtils.getSingleRecord(testConsumer, KafkaTopics.TRANSFER_FAILED, Duration.ofSeconds(3));
                    assertThat(record).isNotNull();
                    assertThat(record.value()).contains(txId);
                });

        testConsumer.close();
    }

    // ── Helpers ──────────────────────────────────────────────────────────────

    /**
     * Crea un TransferRequestedEvent con los datos indicados.
     */
    private TransferRequestedEvent crearEventoValido(String txId, String from,
                                                      String to, String amount) {
        return TransferRequestedEvent.builder()
                .transactionId(txId)
                .fromAccount(from)
                .toAccount(to)
                .amount(new BigDecimal(amount))
                .timestamp(Instant.now())
                .build();
    }

    /**
     * Crea un Consumer Kafka de test que lee mensajes como String raw (JSON).
     * Útil para verificar que un mensaje llegó a un topic sin necesidad de
     * deserializar a un tipo específico.
     */
    private Consumer<String, String> crearConsumerDeTest(String groupId) {
        Map<String, Object> consumerProps = KafkaTestUtils.consumerProps(
                groupId,
                "true",  // autoCommit
                embeddedKafkaBroker
        );
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class);
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        return new DefaultKafkaConsumerFactory<String, String>(consumerProps)
                .createConsumer();
    }
}
