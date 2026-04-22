package com.lab.account;

import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

/**
 * Clase base para Integration Tests de account-service con Testcontainers.
 *
 * CAPA: Integración con contenedores Docker reales (Capa 3 de la pirámide)
 *
 * ¿Qué aporta esta clase base?
 *   - Los contenedores MySQL y Kafka son ESTÁTICOS: se crean UNA SOLA VEZ
 *     para todos los tests de la clase hija. Si los contenedores fueran
 *     de instancia (@Container sin static), se crearían/destruirían por cada
 *     test — multiplicando el tiempo de ejecución por N (cada container
 *     tarda ~5-10s en arrancar).
 *
 *   - @DynamicPropertySource sobreescribe las propiedades de Spring Boot
 *     DESPUÉS de que los contenedores arrancan y se conocen los puertos.
 *     Esto es necesario porque Testcontainers asigna puertos aleatorios
 *     (para evitar conflictos con otros servicios locales).
 *
 * Patrón de herencia:
 *   Los tests concretos extienden esta clase y heredan los contenedores.
 *   Basta con anotar el test concreto con @Testcontainers — la clase
 *   base ya tiene los @Container declarados como static.
 *
 *   class DebitConsumerIT extends AbstractContainerBaseTest { ... }
 *
 * ¿Por qué MySQL de Testcontainers y no H2?
 *   - El schema de Flyway usa ENGINE=InnoDB (MySQL-only)
 *   - Las migraciones Flyway se ejecutan contra MySQL real
 *   - El comportamiento del Optimistic Locking es específico de MySQL
 *   - H2 no puede reproducir el CHECK CONSTRAINT (balance >= 0) de MySQL 8
 *   → Testcontainers garantiza fidelidad al comportamiento de producción.
 *
 * ¿Por qué KafkaContainer y no @EmbeddedKafka?
 *   - account-service tiene lógica de consumer groups distintos (debit/credit)
 *   - Testcontainers Kafka soporta el protocolo completo
 *   - Mayor fidelidad para probar la idempotencia con particiones reales
 *   → Se paga el costo de Docker a cambio de certeza en el comportamiento.
 */
@SpringBootTest
@ActiveProfiles("test")
@Testcontainers
public abstract class AbstractContainerBaseTest {

    // ── MySQL Testcontainer ──────────────────────────────────────────────────
    // static: el contenedor es compartido entre todos los tests de la clase hija
    // @SuppressWarnings: el campo está "unused" pero Testcontainers lo gestiona
    @SuppressWarnings("resource")
    static final MySQLContainer<?> mysql =
            new MySQLContainer<>(DockerImageName.parse("mysql:8.0"))
                    .withDatabaseName("transfers_db")
                    .withUsername("lab_user")
                    .withPassword("lab_pass")
                    // Optimizar el arranque del contenedor para tests:
                    // innodb_buffer_pool_size reducido (no necesitamos máximo rendimiento)
                    .withCommand("--character-set-server=utf8mb4",
                                 "--collation-server=utf8mb4_unicode_ci",
                                 "--innodb-buffer-pool-size=64M");

    // ── Kafka Testcontainer ──────────────────────────────────────────────────
    // KafkaContainer usa la imagen de Confluent Platform (cp-kafka) por defecto.
    // Es compatible con el protocolo Kafka estándar.
    @SuppressWarnings("resource")
    static final KafkaContainer kafka =
            new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.1"));

    // ── Arranque de contenedores ─────────────────────────────────────────────
    // static {}: se ejecuta cuando la clase se carga (antes de @BeforeAll).
    // Arranca AMBOS contenedores en paralelo (Testcontainers lo hace internamente).
    static {
        mysql.start();
        kafka.start();
    }

    // ── Inyección dinámica de propiedades ────────────────────────────────────
    // @DynamicPropertySource se ejecuta después de que los contenedores arrancan
    // y sus puertos son conocidos. Sobreescribe las propiedades del
    // application-test.yml con los valores reales de los contenedores.
    //
    // Sin esto, Spring intentaría conectarse al localhost:3306 del sistema
    // operativo (que puede no tener MySQL) en lugar del contenedor.
    @DynamicPropertySource
    static void configurarPropiedades(DynamicPropertyRegistry registry) {
        // MySQL — URL incluye el puerto aleatorio asignado por Testcontainers
        // Ejemplo: jdbc:mysql://localhost:49152/transfers_db
        registry.add("spring.datasource.url", mysql::getJdbcUrl);
        registry.add("spring.datasource.username", mysql::getUsername);
        registry.add("spring.datasource.password", mysql::getPassword);

        // Kafka — dirección del broker con puerto aleatorio
        // Ejemplo: localhost:49153
        registry.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers);
    }
}
