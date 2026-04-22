# DECISIONES TÉCNICAS — Lab Week 3 (Testing)

Registro de las decisiones de diseño tomadas al escribir los tests.
Cada decisión tiene un contexto, las alternativas consideradas y el razonamiento.

---

## DEC-01: Por qué @ExtendWith(MockitoExtension.class) en lugar de @SpringBootTest para unit tests

**Contexto:** ValidationServiceTest, DebitServiceTest, CreditServiceTest son "unit tests" puros.

**Decisión:** usar `@ExtendWith(MockitoExtension.class)` sin Spring.

**Alternativas consideradas:**
1. `@SpringBootTest` con `@MockBean` → carga el contexto completo (slow)
2. `@ExtendWith(MockitoExtension.class)` → crea solo los mocks (fast)
3. `MockitoAnnotations.openMocks(this)` en `@BeforeEach` → sin extension (verboso)

**Razonamiento:**
- Los servicios no tienen dependencias de Spring framework en su lógica de negocio.
  Solo usan POJOs, interfaces de repositorio, y excepciones simples.
- Sin Spring context: arranque en ~50ms en lugar de ~3-5s.
- El feedback durante el desarrollo es 60x más rápido.
- La regla: si el código puede testearse sin Spring, testéalo sin Spring.

---

## DEC-02: @SpyBean en lugar de @MockBean para ValidationService en el IT test

**Contexto:** TransferRequestedConsumerIT verifica que ValidationService fue llamado.

**Decisión:** usar `@SpyBean ValidationService` en lugar de `@MockBean`.

**Diferencia crítica:**
```java
@MockBean ValidationService validationService;
// → Spring reemplaza el bean con un stub vacío.
//   validate() no hace nada. La lógica real nunca se ejecuta.
//   Solo podemos verificar si fue llamado, no si funcionó.

@SpyBean ValidationService validationService;
// → Spring envuelve el bean real con un spy.
//   validate() ejecuta el código REAL de producción.
//   Además podemos verificar si fue llamado y con qué argumentos.
```

**¿Cuándo usar cada uno?**
- `@MockBean`: cuando NO quieres ejecutar el código real (demasiado lento, tiene efectos secundarios no deseados).
- `@SpyBean`: cuando quieres ejecutar el código real Y también verificar que fue llamado.

**En este caso:** queremos que ValidationService ejecute su lógica real (publica en Kafka),
y también queremos verificar que fue invocado con el evento correcto → `@SpyBean`.

---

## DEC-03: Awaitility vs Thread.sleep()

**Contexto:** todos los IT tests necesitan esperar que el consumer de Kafka procese el mensaje.

**Decisión:** usar `Awaitility.await().atMost(...).untilAsserted(...)`.

**Problema con Thread.sleep(3000):**
```java
kafkaTemplate.send(topic, evento);
Thread.sleep(3000);  // ← ¿qué pasa si el consumer tarda 3.1s?
assertThat(...)      // ← falla intermitentemente (flaky test)
```

**Problema con Thread.sleep() corto:**
```java
Thread.sleep(500);   // ← rápido en CI, pero ¿y en una máquina lenta?
// El consumer puede no haber procesado el mensaje en 500ms → test falla
```

**Solución con Awaitility:**
```java
Awaitility.await()
    .atMost(20, TimeUnit.SECONDS)     // Tiempo máximo: falla si tarda más
    .pollInterval(Duration.ofMillis(300))  // Sondea cada 300ms
    .untilAsserted(() -> {
        // Condición que se evalúa cada 300ms hasta cumplirse o timeout
        assertThat(repo.findById("ACC-001").get().getBalance())
            .isEqualByComparingTo("750.00");
    });
```

El test pasa exactamente cuando el consumer termina, sin tiempo fijo de espera.

**Cuándo es válido `Thread.sleep()`:**
Solo en el test de concurrencia (`cuandoCincoThreadsEnvianMismoEvento_soloDebitaUnaVez`)
para esperar que todos los 5 mensajes sean consumidos antes de verificar.
En ese caso, la condición de "todos procesados" es difícil de expresar con Awaitility
sin introducir más complejidad que la que resuelve.

---

## DEC-04: ReflectionTestUtils.setField() para @Value en unit tests

**Contexto:** CreditService tiene `@Value("${app.simulate-credit-failure:false}")`.
En tests unitarios sin Spring, este campo no se inyecta.

**Decisión:** usar `ReflectionTestUtils.setField(objeto, "nombreCampo", valor)`.

**Alternativas consideradas:**
1. `@SpringBootTest` con `@TestPropertySource(properties = "app.simulate-credit-failure=true")`
   → carga el contexto completo → ~5s adicionales por cada test que cambia el flag.
2. `ReflectionTestUtils.setField(...)` → inyecta el valor sin Spring → instantáneo.
3. Anotar el campo como `package-private` para acceso directo → modifica el código de producción.
4. Agregar un constructor o setter en CreditService → expone la implementación interna.

**Razonamiento:** `ReflectionTestUtils` es parte de `spring-test` y está diseñado exactamente
para este propósito. No requiere modificar el código de producción ni cargar Spring.
Es la solución estándar para este patrón en el ecosistema Spring.

---

## DEC-05: Contenedores estáticos en AbstractContainerBaseTest

**Contexto:** DebitConsumerIT extiende AbstractContainerBaseTest que tiene MySQL y Kafka.

**Decisión:** los contenedores son `static final`, iniciados en bloque `static {}`.

**¿Por qué estáticos y no de instancia?**

Con contenedores de instancia (no estáticos):
```
Test 1 → crear MySQL(~8s) + Kafka(~5s) → test(~5s) → destruir
Test 2 → crear MySQL(~8s) + Kafka(~5s) → test(~5s) → destruir
Test 3 → crear MySQL(~8s) + Kafka(~5s) → test(~5s) → destruir
TOTAL: 3 × (13s + 5s) = 54s
```

Con contenedores estáticos:
```
Primera vez → crear MySQL(~8s) + Kafka(~5s) → test1(~5s) → test2(~5s) → test3(~5s)
TOTAL: 13s + 3 × 5s = 28s  ← menos de la mitad
```

El ahorro se multiplica con cada test adicional.

**Cuidado con el estado compartido:**
Los contenedores se reusan, pero el estado de las tablas persiste entre tests.
Por eso cada test hace `@BeforeEach` que limpia y recrea los datos necesarios.

---

## DEC-06: H2 para validation-service vs Testcontainers para account-service

**Contexto:** los IT tests necesitan una base de datos. ¿H2 o Testcontainers MySQL?

**Decisión:**
- `TransferRequestedConsumerIT` (validation-service) → **H2 in-memory**
- `DebitConsumerIT` (account-service) → **Testcontainers MySQL**

**Razonamiento para H2 en validation-service:**
- validation-service solo lee de `accounts` (existsById) y escribe en `processed_events`
- La lógica de procesamiento no depende de características MySQL-específicas
- H2 + `@EmbeddedKafka` permite el test sin Docker → correr en CI sin restricciones
- El objetivo del test es verificar la integración consumer→service→producer, no el SQL

**Razonamiento para Testcontainers en account-service:**
- El schema usa `CHECK (balance >= 0)` que en H2 requiere configuración especial
- Flyway migrations usan `ENGINE=InnoDB` → H2 no puede ejecutarlas
- Optimistic Locking (`@Version`) tiene comportamiento ligeramente diferente en H2
- El test de concurrencia necesita el comportamiento exacto de MySQL para la
  `UNIQUE CONSTRAINT` y el `DataIntegrityViolationException`

**Regla general:**
- Tests que prueban lógica de aplicación → H2 es suficiente
- Tests que prueban comportamiento de persistencia real → Testcontainers MySQL

---

## DEC-07: @DirtiesContext en TransferRequestedConsumerIT

**Contexto:** `@EmbeddedKafka` crea un broker con estado interno.

**Decisión:** anotar el test con `@DirtiesContext`.

**Problema sin @DirtiesContext:**
Si el contexto Spring se comparte entre múltiples tests con `@EmbeddedKafka`,
los offsets del broker persisten entre tests. El test B podría recibir mensajes
que el test A dejó sin consumir → resultados no determinísticos.

**Con @DirtiesContext:**
Spring destruye y recrea el contexto (incluyendo el broker embebido) después del test.
Cada test empieza con un broker limpio.

**Costo:** el contexto se crea de nuevo (~3-5s extra).
Para la suite de tests actual, el costo es aceptable.

**Alternativa (más eficiente pero más compleja):**
`@TestInstance(Lifecycle.PER_CLASS)` + limpieza manual de topics entre tests.
No implementado en este lab por simplicidad.

---

## DEC-08: Por qué no testear el Saga completo en este lab

**Contexto:** el Saga involucra 4 servicios y 6 topics de Kafka.

**Decisión:** NO incluir un test E2E del Saga completo en Lab Week 3.

**Razonamiento:**
1. **Complejidad de setup**: arrancar 4 Spring contexts separados con Testcontainers
   requiere una infraestructura de test significativa.
2. **Valor pedagógico decreciente**: los conceptos clave (Mockito, EmbeddedKafka,
   Testcontainers) ya se demuestran con los tests actuales.
3. **Cobertura suficiente**: cada pieza del Saga está probada individualmente.
   Un test de integración con los 4 servicios juntos no añadiría nuevos escenarios.
4. **Tiempo de ejecución**: un E2E completo podría tomar 2-5 minutos, inaceptable
   para feedback rápido en desarrollo.

**Alternativa real**: usar los `curl.exe` del `EJECUCION.md` como suite de smoke tests
manuales para verificar el Saga E2E. En un CI/CD real se automatizarían con
un script de shell o con Postman/Newman.

---

## DEC-09: Estructura @Nested en los unit tests

**Contexto:** los unit tests tienen múltiples escenarios.

**Decisión:** agrupar los tests con `@Nested` por regla de negocio o comportamiento.

**Beneficios:**
```
ValidationServiceTest
├── Regla1CuentasDistintas
│   └── debePublicarFailed_cuandoCuentasIguales
├── Regla2MontoPositivo
│   ├── debePublicarFailed_cuandoMontoCero
│   ├── debePublicarFailed_cuandoMontoNegativo
│   └── debePublicarFailed_cuandoMontoNull
├── Regla3CuentaOrigenExiste
│   └── debePublicarFailed_cuandoCuentaOrigenNoExiste
├── Regla4CuentaDestinoExiste
│   └── debePublicarFailed_cuandoCuentaDestinoNoExiste
├── HappyPath
│   ├── debePublicarValidated_cuandoTodasLasReglasApasan
│   └── debePublicarValidated_conMontoMinimo
└── OrdenFailFast
    └── regla1Falla_noConsultaDB
```

En el reporte de Maven/Surefire, cada `@Nested` aparece como subsección.
Cuando un test falla, inmediatamente sabes QUÉ REGLA falló sin leer el nombre completo.

---

## DEC-10: Por qué AssertJ y no JUnit Assertions

**Contexto:** los tests usan `assertThat(...)` de AssertJ en lugar de `assertEquals` de JUnit.

**Decisión:** AssertJ para todas las verificaciones de estado.

**Comparación:**

JUnit 5:
```java
assertEquals(new BigDecimal("700.00"), resultado.getBalance());
// Mensaje de error: "expected: <700.00> but was: <750.00>"
```

AssertJ:
```java
assertThat(resultado.getBalance())
    .as("El balance debe ser 1000 - 300 = 700")
    .isEqualByComparingTo(new BigDecimal("700.00"));
// Mensaje de error: "[El balance debe ser 1000 - 300 = 700]
//   expected: 700.00 but was: 750.00"
```

**Ventajas de AssertJ:**
1. **Fluent API**: las assertions se leen como oraciones
2. **`.as("descripción")`**: el contexto del fallo es inmediatamente claro
3. **`isEqualByComparingTo()`** para BigDecimal: ignora la escala
   (`700.0` == `700.00` == `700.000`) — JUnit's `assertEquals` fallaría aquí
4. **Mensajes de error más ricos** con contexto del objeto completo
5. **Encadenamiento**: `assertThat(lista).hasSize(3).extracting("campo").contains("valor")`

**Spring Boot Test** ya incluye AssertJ en el classpath — no requiere dependencia adicional.
