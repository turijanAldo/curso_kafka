# Lab Week 3 — Testing de Microservicios Kafka con Spring Boot

Sistema bajo prueba: **kafka-transfers** (Lab Week 2)  
Objetivo: aplicar la pirámide de tests al sistema de transferencias asíncronas.

> 📖 **Lectura previa recomendada:** [`../kafka-testing-intro.html`](../kafka-testing-intro.html)

---

## ¿Qué vamos a testear?

Los tests del Lab Week 3 se añaden directamente al proyecto `lab-week-2/kafka-transfers/`.  
La estructura completa de test es:

```
kafka-transfers/
├── validation-service/
│   └── src/test/
│       ├── resources/application-test.yml          # H2 + EmbeddedKafka
│       └── java/com/lab/validation/
│           ├── service/ValidationServiceTest.java   # ← PASO 1 (Mockito)
│           └── consumer/TransferRequestedConsumerIT.java  # ← PASO 2 (@EmbeddedKafka)
└── account-service/
    └── src/test/
        ├── resources/application-test.yml          # Testcontainers placeholders
        └── java/com/lab/account/
            ├── service/
            │   ├── DebitServiceTest.java             # ← PASO 3 (Mockito)
            │   └── CreditServiceTest.java            # ← PASO 3 (Mockito)
            ├── AbstractContainerBaseTest.java        # ← PASO 4 (Testcontainers base)
            └── consumer/
                └── DebitConsumerIT.java              # ← PASO 5 (Testcontainers full)
```

---

## Prerrequisitos

- Lab Week 2 completado (`kafka-transfers/` funcionando)
- Java 17, Maven 3.9+
- **Docker Desktop corriendo** (Testcontainers lo necesita para DebitConsumerIT)
- No necesitas levantar Kafka/MySQL manualmente — los tests de integración usan Testcontainers

Verificar prerrequisitos:
```powershell
java -version              # 17+
mvn -version               # 3.9+
docker info | head -5      # Docker corriendo
```

---

## PASO 1 — Tests unitarios: ValidationServiceTest

**Archivo:** `validation-service/src/test/java/com/lab/validation/service/ValidationServiceTest.java`

### Conceptos clave

- **`@ExtendWith(MockitoExtension.class)`**: activa Mockito sin cargar Spring Context.
  Sin este, los `@Mock` no se inyectan.
- **`@Mock`**: crea un doble de prueba de AccountRepository y ValidationEventProducer.
  Las implementaciones no hacen nada por defecto (retornan null/false/0).
- **`@InjectMocks`**: crea una instancia REAL de ValidationService e inyecta los mocks.
- **`verify(mock).método(args)`**: verifica que el mock fue llamado con esos argumentos.
- **`verifyNoInteractions(mock)`**: verifica que el mock no fue llamado en absoluto.
- **`never()`**: dentro de verify, asegura que el método nunca fue llamado.

### Ejecutar

```powershell
cd lab-week-2/kafka-transfers
mvn test -pl validation-service -Dtest=ValidationServiceTest
```

### Resultado esperado

```
[INFO] Tests run: 7, Failures: 0, Errors: 0, Skipped: 0
[INFO] BUILD SUCCESS
[INFO] Total time: 1.5 s  ← Velocidad de tests unitarios
```

### Qué observar

1. **No hay logs de Spring**: sin contexto, sin Kafka, sin DB — puro Mockito
2. **Cobertura de las 4 reglas**: cada `@Nested` prueba una regla de negocio
3. **Fail-fast verificado**: regla 1 falla → no se consulta la DB (verifyNoInteractions)

### Ejercicio: añadir un test

Agrega un test en `Regla2MontoPositivo` que verifique que un monto de exactamente `0` falla,
pero un monto de `0.001` (BigDecimal con más decimales) pasa la regla.

---

## PASO 2 — Integration Test: TransferRequestedConsumerIT

**Archivo:** `validation-service/src/test/java/com/lab/validation/consumer/TransferRequestedConsumerIT.java`

### Conceptos clave

- **`@SpringBootTest`**: carga el contexto completo de Spring Boot (consumers activos).
- **`@EmbeddedKafka`**: lanza un broker Kafka en memoria dentro de la JVM.
  No necesita Docker. Los topics declarados se crean automáticamente.
- **`@ActiveProfiles("test")`**: activa `application-test.yml` que usa H2 (sin MySQL).
- **`@DirtiesContext`**: Spring destruye y recrea el contexto tras el test.
  Necesario porque el broker embebido tiene estado.
- **`@SpyBean ValidationService`**: envuelve el bean REAL. El código de producción
  se ejecuta, pero podemos verificar las llamadas con `verify()`.
- **`Awaitility.await()`**: polling con timeout — NO usar `Thread.sleep()` fijo.
  El test pasa exactamente cuando la condición se cumple, no antes, no después.

### Configuración de `application-test.yml`

El perfil `test` usa H2 en lugar de MySQL y deshabilita Flyway:
```yaml
spring:
  datasource:
    url: jdbc:h2:mem:testdb;DB_CLOSE_DELAY=-1
    driver-class-name: org.h2.Driver
  jpa.hibernate.ddl-auto: create-drop   # Hibernate genera el schema desde entidades
  flyway.enabled: false                  # Flyway usa SQL MySQL — no compatible con H2
```

### Ejecutar

```powershell
mvn test -pl validation-service -Dtest=TransferRequestedConsumerIT
```

> ⚠️ **Sin Docker** — H2 + @EmbeddedKafka no requieren Docker.

### Resultado esperado

```
[INFO] Tests run: 3, Failures: 0, Errors: 0, Skipped: 0
[INFO] Total time: ~15 s  ← Spring context + EmbeddedKafka arrancan una vez
```

### Qué observar en los logs

```
[validation-group-0-C-1] 📨 Evento recibido | key=<txId>:TRANSFER_REQUESTED
[validation-group-0-C-1] [IDEMPOTENCIA-C2] Evento ya registrado — ignorando
```

La segunda línea aparece en el test de idempotencia: el segundo envío del mismo evento
es filtrado por la Capa 2 sin llegar a ValidationService.

### Ejercicio: añadir un test

Agrega un test que verifique que un evento válido publica en `transfer.validated`.  
Pista: usa `KafkaTestUtils.getSingleRecord(testConsumer, KafkaTopics.TRANSFER_VALIDATED, ...)`.

---

## PASO 3 — Tests unitarios: DebitServiceTest y CreditServiceTest

**Archivos:**
- `account-service/src/test/java/com/lab/account/service/DebitServiceTest.java`
- `account-service/src/test/java/com/lab/account/service/CreditServiceTest.java`

### Conceptos clave — DebitServiceTest

- **`assertThatThrownBy(() -> ...)`**: verifica que un lambda lanza una excepción.
  Equivalente a `assertThrows(ExceptionType.class, () -> ...)` pero con fluent API.
- **`isInstanceOf(Class)`**: verifica el tipo de la excepción.
- **`hasMessageContaining(String)`**: verifica el mensaje de error (crítico para diagnóstico en producción).
- **`verify(repo, never()).save(any())`**: el repositorio NO guardó nada cuando la validación falla.

### Conceptos clave — CreditServiceTest

- **`ReflectionTestUtils.setField(objeto, "campo", valor)`**:
  Inyecta un valor en un campo privado `@Value` sin Spring Context.
  Es la herramienta correcta cuando no queremos levantar el contexto completo
  solo para cambiar un flag de configuración.

```java
// Activar el flag de simulación de fallo:
ReflectionTestUtils.setField(creditService, "simulateCreditFailure", true);
```

### Ejecutar

```powershell
mvn test -pl account-service -Dtest=DebitServiceTest
mvn test -pl account-service -Dtest=CreditServiceTest

# O ambos a la vez:
mvn test -pl account-service -Dtest="DebitServiceTest,CreditServiceTest"
```

### Resultado esperado

```
[INFO] Tests run: 8, Failures: 0, Errors: 0, Skipped: 0
[INFO] Tests run: 6, Failures: 0, Errors: 0, Skipped: 0
[INFO] Total time: ~2 s
```

### Ejercicio: añadir un test en DebitServiceTest

Verifica que cuando `accountRepository.save()` lanza una excepción inesperada
(configura el mock para `thenThrow(new RuntimeException("DB caída"))`),
la excepción se propaga sin ser capturada silenciosamente.

---

## PASO 4 — Testcontainers base: AbstractContainerBaseTest

**Archivo:** `account-service/src/test/java/com/lab/account/AbstractContainerBaseTest.java`

### Conceptos clave

- **`static final MySQLContainer<?>`**: el contenedor es **estático** → se comparte
  entre todos los tests que extiendan esta clase. Se crea una sola vez.
  Si fuera de instancia, se crearía/destruiría por cada test (lentísimo).

- **`static { mysql.start(); kafka.start(); }`**: el bloque estático inicia los
  contenedores antes de que Spring cargue el contexto.

- **`@DynamicPropertySource`**: sobreescribe propiedades DESPUÉS de que los
  contenedores están corriendo y sus puertos son conocidos.
  ```java
  registry.add("spring.datasource.url", mysql::getJdbcUrl);
  // mysql.getJdbcUrl() retorna algo como:
  // jdbc:mysql://localhost:49152/transfers_db
  ```

- **`@Testcontainers`**: habilita la integración JUnit 5 de Testcontainers.
  Gestiona el ciclo de vida de los contenedores marcados con `@Container`.

### Verificar que Docker está disponible

```powershell
docker info | Select-String "Server Version"
# Debe mostrar la versión de Docker Engine
```

Si Docker no está corriendo, los tests de Testcontainers fallan con:
```
org.testcontainers.containers.ContainerLaunchException: Timed out waiting for log output
```

---

## PASO 5 — Integration Test completo: DebitConsumerIT

**Archivo:** `account-service/src/test/java/com/lab/account/consumer/DebitConsumerIT.java`

Este es el test más completo y más lento. Usa MySQL real + Kafka real.

### Ejecutar

> ⚠️ **Requiere Docker Desktop corriendo**

```powershell
mvn test -pl account-service -Dtest=DebitConsumerIT
```

La primera ejecución descarga las imágenes Docker si no las tienes:
- `mysql:8.0` (~500MB)
- `confluentinc/cp-kafka:7.6.1` (~800MB)

Ejecuciones posteriores usan el caché de Docker.

### Resultado esperado (primera vez: ~60-120s, siguientes: ~30s)

```
[INFO] Tests run: 4, Failures: 0, Errors: 0, Skipped: 0
[INFO] BUILD SUCCESS
```

### Qué observar en los logs durante la ejecución

```
🐳 Creating container for image: mysql:8.0
🐳 Creating container for image: confluentinc/cp-kafka:7.6.1
🐳 Container mysql:8.0 started in 8.2s
🐳 Container confluentinc/cp-kafka:7.6.1 started in 5.1s
[Flyway] Successfully applied 1 migration to schema 'transfers_db'
📨 TRANSFER_VALIDATED recibido | key=...:TRANSFER_VALIDATED
💸 Iniciando débito | account=TEST-DEBIT-001 amount=250.00
✅ Débito aplicado | account=TEST-DEBIT-001 newBalance=750.00
```

### El test de concurrencia — ¿qué está probando realmente?

El test `cuandoCincoThreadsEnvianMismoEvento_soloDebitaUnaVez` simula el escenario
de **reentrega de Kafka bajo concurrencia**:

```
Thread 1 → tranfer.validated [txId=abc] → DebitConsumer → tryRegister("abc:TRANSFER_VALIDATED")
Thread 2 → tranfer.validated [txId=abc] → DebitConsumer → tryRegister("abc:TRANSFER_VALIDATED")
Thread 3 → tranfer.validated [txId=abc] → DebitConsumer → tryRegister("abc:TRANSFER_VALIDATED")
Thread 4 → tranfer.validated [txId=abc] → DebitConsumer → tryRegister("abc:TRANSFER_VALIDATED")
Thread 5 → tranfer.validated [txId=abc] → DebitConsumer → tryRegister("abc:TRANSFER_VALIDATED")
                                                                   ↓
                                               Solo 1 INSERT tiene éxito (UNIQUE constraint)
                                               Los otros 4 reciben DataIntegrityViolationException
                                               → return false → ignorar → no debitar
                                               RESULTADO: 1 solo débito aplicado
```

---

## PASO 6 — Ejecutar todos los tests

```powershell
cd lab-week-2/kafka-transfers

# Solo tests unitarios (rápido, sin Docker):
mvn test -Dtest="ValidationServiceTest,DebitServiceTest,CreditServiceTest"

# Solo integration tests de @EmbeddedKafka (sin Docker):
mvn test -Dtest="TransferRequestedConsumerIT"

# Solo integration tests con Testcontainers (requiere Docker):
mvn test -Dtest="DebitConsumerIT"

# TODOS los tests:
mvn test
```

### Resumen de tiempos esperados

| Test | Tipo | Docker | Tiempo aproximado |
|------|------|--------|-------------------|
| ValidationServiceTest | Unitario | ❌ | < 2s |
| DebitServiceTest | Unitario | ❌ | < 2s |
| CreditServiceTest | Unitario | ❌ | < 2s |
| TransferRequestedConsumerIT | @EmbeddedKafka | ❌ | ~15-20s |
| DebitConsumerIT | Testcontainers | ✅ | ~30-120s |
| **Total** | | | **~60-150s** |

---

## Verificación de la cobertura

### ¿Qué estamos probando y qué no?

| Componente | Unitario | IT EmbeddedKafka | IT Testcontainers |
|-----------|----------|-----------------|-------------------|
| ValidationService (4 reglas) | ✅ ValidationServiceTest | ✅ (vía SpyBean) | — |
| TransferRequestedConsumer (idempotencia) | — | ✅ | — |
| DebitService (happy path, funds, not found) | ✅ DebitServiceTest | — | ✅ (real DB) |
| CreditService (happy path, simulated fail) | ✅ CreditServiceTest | — | — |
| DebitConsumer (idempotencia + concurrencia) | — | — | ✅ DebitConsumerIT |
| Flyway migrations | — | — | ✅ (corre en MySQL TC) |
| Optimistic Locking real | — | — | ✅ (MySQL TC) |

### Cobertura intencional no cubierta en este lab

- **CreditConsumer + compensación** → Exercise: añadir `CreditConsumerIT`
- **SagaEventConsumer** → Exercise: añadir `SagaEventConsumerIT`
- **TransferService (transfer-api)** → Exercise: añadir `TransferServiceTest`
- **E2E Saga completo** → requeriría levantar los 4 servicios en Testcontainers

---

## Ejercicios opcionales

### Ejercicio A: CreditConsumerIT con simulación de compensación

1. Crea `account-service/src/test/java/com/lab/account/consumer/CreditConsumerIT.java`
2. Extiende `AbstractContainerBaseTest`
3. Configura `app.simulate-credit-failure=true` con `@TestPropertySource`
4. Envía un `TransferDebitedEvent`
5. Verifica que el saldo de `fromAccount` se restaura (compensación funcionó)
6. Verifica que se publicó en `transfer.failed`

### Ejercicio B: TransferServiceTest (transfer-api)

1. Crea `transfer-api/src/test/java/com/lab/api/service/TransferServiceTest.java`
2. Mockea `TransactionRepository`, `KafkaTemplate`, y `TransferCacheService`
3. Prueba `initiateTransfer()`: verifica que guarda en DB y publica en Kafka
4. Prueba `getStatus()`: verifica cache hit vs cache miss

### Ejercicio C: Test de Saga E2E

1. Crea `src/test/java/com/lab/e2e/SagaEndToEndIT.java`
2. Arranca los 4 servicios como contextos de Spring separados
3. Posta una transferencia via `TestRestTemplate` al puerto 8080
4. Espera con Awaitility que el estado sea `COMPLETED`
5. Verifica los saldos en la DB

---

## Troubleshooting

### "Could not connect to Docker"

```
com.github.dockerjava.api.exception.DockerException
```

**Solución**: Iniciar Docker Desktop. Los tests de Testcontainers requieren Docker.
Los tests unitarios y @EmbeddedKafka NO requieren Docker.

### "H2 dialect not found" o error de tabla

```
org.hibernate.HibernateException: Schema-validation: missing table [accounts]
```

**Causa**: El perfil `test` no está activo — Hibernate usa `validate` y no encuentra tablas.  
**Solución**: Verificar que el test tiene `@ActiveProfiles("test")` y que
`application-test.yml` existe en `src/test/resources/`.

### "Timeout waiting for Kafka consumer"

El consumer tardó más de lo esperado. Aumentar el timeout de Awaitility:
```java
Awaitility.await()
    .atMost(30, TimeUnit.SECONDS)  // Aumentar si la máquina es lenta
    .pollInterval(Duration.ofMillis(500))
    .untilAsserted(() -> ...);
```

### Tests de Testcontainers lentos en primera ejecución

Normal — las imágenes Docker se descargan una sola vez. Ejecuciones posteriores
usan el caché. Para preicargar las imágenes:
```powershell
docker pull mysql:8.0
docker pull confluentinc/cp-kafka:7.6.1
```

### "DataIntegrityViolationException inesperado en test"

Ocurre si el `@BeforeEach` no limpió el estado anterior.  
Verificar que `processedEventRepository.deleteAll()` se ejecuta en `@BeforeEach`.
