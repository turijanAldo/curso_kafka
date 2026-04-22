# 🏦 Lab Week 2 — Sistema de Transferencias Asíncronas con Kafka + Idempotencia

> **Nivel:** Intermedio-Avanzado  
> **Stack:** Java 17 · Spring Boot 3.3 · Apache Kafka 4.x · MySQL 8 · Docker  
> **Patrón principal:** Choreography-based Saga + Idempotencia en 3 capas  
> **Tiempo estimado:** 12–13 horas  
> **Shell del host:** PowerShell (Windows) — los comandos fuera de Docker usan sintaxis PS1

---

## ⚠️ Nota sobre PowerShell (leer antes de empezar)

Todos los comandos que se ejecutan **en el host (tu máquina)** están escritos en **PowerShell**. Los comandos que se ejecutan **dentro de los contenedores Docker** (via `docker exec -it kafka ...`) siguen siendo bash de Linux — esos no cambian.

Diferencias clave que aplican en todo el documento:

| Bash (Linux/macOS) | PowerShell (Windows) |
|--------------------|----------------------|
| Continuación de línea: `\` | Continuación de línea: `` ` `` (backtick) |
| `curl http://...` | `curl.exe http://...` (usar `.exe` para evitar el alias de PS) |
| `watch -n 1 'cmd'` | `while($true){ cmd; Start-Sleep -Seconds 1 }` |
| `export VAR=valor` | `$env:VAR = "valor"` |
| `./script.sh` | `.\script.ps1` |
| Scripts: `.sh` | Scripts: `.ps1` |

---

## 📋 Índice de pasos

| Paso | Nombre | Tipo | Tiempo |
|------|--------|------|--------|
| [PASO 0](#paso-0--infraestructura-y-estructura-de-proyecto) | Infraestructura y estructura de proyecto | Bloqueante | ~1.5h |
| [PASO 1](#paso-1--módulo-common-contratos-compartidos) | Módulo Common (contratos compartidos) | Bloqueante | ~1h |
| [PASO 2](#paso-2--esquema-de-base-de-datos) | Esquema de base de datos | Bloqueante | ~30min |
| [PASO 3](#paso-3--transfer-api-gateway) | Transfer API — Gateway REST | Desarrollo | ~1.5h |
| [PASO 4](#paso-4--validation-service) | Validation Service | Desarrollo | ~1h |
| [PASO 5](#paso-5--account-service-el-corazón-del-sistema) | Account Service — DEBIT / CREDIT / ROLLBACK | Desarrollo | ~4h |
| [PASO 6](#paso-6--status-service) | Status Service — Tracker de saga | Desarrollo | ~1.5h |
| [PASO 7](#paso-7--simulación-de-duplicados) | Simulación de duplicados Kafka | Demo | ~45min |
| [PASO 8](#paso-8--escenario-de-rollback-saga-compensation) | Escenario de rollback Saga | Demo | ~45min |
| [PASO 9](#paso-9--pruebas-y-documentación) | Pruebas e integración final | Cierre | ~1h |
| [PASO 10](#paso-10--bonus) | Bonus: Redis, correlationId, DLQ | Opcional | ~2h |

---

## Árbol de carpetas del proyecto completo

Antes de empezar, esta es la visión completa de lo que vamos a construir:

```
lab-week-2/
├── LAB-GUIDE.md                        ← este archivo
├── Instrucciones.md                    ← análisis previo (referencia)
│
└── kafka-transfers/                    ← raíz del proyecto Maven Multi-Module
    │
    ├── pom.xml                         ← POM padre: declara todos los módulos, versiones
    │
    ├── docker/                         ← toda la infraestructura de contenedores
    │   ├── docker-compose.yml          ← levanta Kafka + MySQL (+ Redis en Bonus)
    │   ├── init-db.sql                 ← datos iniciales de cuentas A y B
    │   └── wait-for-kafka.ps1           ← script de espera antes de arrancar servicios
    │
    ├── common/                         ← módulo compartido: DTOs, eventos, constantes
    │   ├── pom.xml
    │   └── src/main/java/com/lab/common/
    │       ├── event/
    │       │   └── TransferEvent.java  ← objeto que viaja en todos los topics Kafka
    │       ├── enums/
    │       │   ├── TransferStatus.java ← PROCESSING, VALIDATED, DEBITED, COMPLETED...
    │       │   └── EventType.java      ← transfer.created, transfer.debited...
    │       └── constants/
    │           └── KafkaTopics.java    ← nombres de topics como constantes String
    │
    ├── transfer-api/                   ← microservicio 1: Gateway REST (puerto 8080)
    │   ├── pom.xml
    │   └── src/main/
    │       ├── java/com/lab/api/
    │       │   ├── TransferApiApplication.java
    │       │   ├── controller/
    │       │   │   └── TransferController.java     ← POST /transfer, GET /status/{id}
    │       │   ├── service/
    │       │   │   └── TransferService.java        ← lógica de negocio del gateway
    │       │   ├── producer/
    │       │   │   └── TransferEventProducer.java  ← publica en transfer.created
    │       │   ├── repository/
    │       │   │   └── TransactionRepository.java  ← acceso a tabla transactions
    │       │   ├── entity/
    │       │   │   └── Transaction.java            ← entidad JPA de la tabla transactions
    │       │   └── dto/
    │       │       ├── TransferRequest.java        ← body del POST /transfer
    │       │       └── TransferResponse.java       ← respuesta con transactionId + status
    │       └── resources/
    │           └── application.yml                 ← puerto 8080, datasource, kafka
    │
    ├── validation-service/             ← microservicio 2: Validaciones (puerto 8081)
    │   ├── pom.xml
    │   └── src/main/
    │       ├── java/com/lab/validation/
    │       │   ├── ValidationServiceApplication.java
    │       │   ├── consumer/
    │       │   │   └── TransferCreatedConsumer.java  ← escucha transfer.created
    │       │   ├── producer/
    │       │   │   └── ValidationEventProducer.java  ← publica validated o failed
    │       │   ├── service/
    │       │   │   └── ValidationService.java        ← reglas de negocio de validación
    │       │   ├── repository/
    │       │   │   ├── AccountRepository.java        ← verifica que la cuenta existe
    │       │   │   └── ProcessedEventRepository.java ← idempotencia: tabla processed_events
    │       │   └── entity/
    │       │       ├── Account.java                  ← entidad JPA de accounts
    │       │       └── ProcessedEvent.java           ← entidad JPA de processed_events
    │       └── resources/
    │           └── application.yml                   ← puerto 8081, group.id propio
    │
    ├── account-service/                ← microservicio 3: Débito, Crédito, Rollback (8082)
    │   ├── pom.xml
    │   └── src/main/
    │       ├── java/com/lab/account/
    │       │   ├── AccountServiceApplication.java
    │       │   ├── consumer/
    │       │   │   ├── DebitConsumer.java         ← escucha transfer.validated → DEBITA
    │       │   │   ├── CreditConsumer.java        ← escucha transfer.debited  → ACREDITA
    │       │   │   └── RollbackConsumer.java      ← escucha transfer.rollback.debit
    │       │   ├── producer/
    │       │   │   └── AccountEventProducer.java  ← publica debited, credited, failed...
    │       │   ├── service/
    │       │   │   ├── DebitService.java          ← lógica de débito con idempotencia
    │       │   │   ├── CreditService.java         ← lógica de crédito con fallo simulable
    │       │   │   └── RollbackService.java       ← lógica de compensación
    │       │   ├── repository/
    │       │   │   ├── AccountRepository.java
    │       │   │   └── ProcessedEventRepository.java
    │       │   └── entity/
    │       │       ├── Account.java               ← tiene campo @Version para optimistic lock
    │       │       └── ProcessedEvent.java
    │       └── resources/
    │           └── application.yml                ← puerto 8082, SIMULATE_CREDIT_FAILURE flag
    │
    └── status-service/                 ← microservicio 4: Tracker de Saga (puerto 8083)
        ├── pom.xml
        └── src/main/
            ├── java/com/lab/status/
            │   ├── StatusServiceApplication.java
            │   ├── consumer/
            │   │   └── AllEventsConsumer.java     ← escucha transfer.* y actualiza estado
            │   ├── service/
            │   │   └── SagaStateMachine.java      ← decide qué estado corresponde a qué evento
            │   ├── repository/
            │   │   └── TransactionRepository.java
            │   └── entity/
            │       └── Transaction.java
            └── resources/
                └── application.yml               ← puerto 8083, group.id propio
```

---

## PASO 0 — Infraestructura y estructura de proyecto

**Objetivo:** Levantar Kafka + MySQL con Docker y crear el esqueleto del proyecto Maven. Nadie escribe lógica de negocio hasta que este paso esté verde.

---

### 0.1 — Crear la carpeta raíz del proyecto

Dentro de `lab-week-2/` crear la carpeta `kafka-transfers/`. Esta será la raíz del proyecto Maven Multi-Module. Todo lo que construyamos vivirá dentro de ella.

```
lab-week-2/
└── kafka-transfers/        ← crear esta carpeta
```

---

### 0.2 — Crear el POM padre

**Archivo:** `kafka-transfers/pom.xml`

**Qué es:** El POM padre es el coordinador de todo el proyecto. No contiene código Java. Su trabajo es:
- Declarar qué módulos forman el proyecto (`transfer-api`, `validation-service`, etc.)
- Fijar las versiones de todas las dependencias en un solo lugar (Spring Boot, Kafka, MySQL)
- Evitar que cada módulo declare su propia versión de Spring Boot de forma inconsistente

**Información que debe contener:**
- `groupId`: `com.lab`
- `artifactId`: `kafka-transfers`
- `version`: `1.0.0`
- `packaging`: `pom` (indica que es un padre, no produce JAR)
- Lista de módulos hijos: `common`, `transfer-api`, `validation-service`, `account-service`, `status-service`
- Sección `<properties>` con versiones: Java 17, Spring Boot 3.3.x, kafka-clients, MySQL connector
- Sección `<dependencyManagement>` para que los hijos hereden versiones sin declararlas

---

### 0.3 — Crear la carpeta de infraestructura Docker

```
kafka-transfers/
└── docker/
    ├── docker-compose.yml      ← crear primero
    ├── init-db.sql             ← datos iniciales
    └── wait-for-kafka.ps1       ← script auxiliar
```

**Archivo: `docker/docker-compose.yml`**

Servicios que debe levantar:
1. **kafka** — imagen `apache/kafka:4.0.0`, modo KRaft (sin ZooKeeper), puerto `9092` mapeado al host. Variables de entorno necesarias para KRaft: `KAFKA_NODE_ID`, `KAFKA_PROCESS_ROLES`, `KAFKA_LISTENERS`, `KAFKA_ADVERTISED_LISTENERS`, `KAFKA_CONTROLLER_QUORUM_VOTERS`, `KAFKA_LOG_DIRS`. Healthcheck: ejecutar `kafka-topics.sh --list` y verificar que responde.

2. **mysql** — imagen `mysql:8.0`, puerto `3306`, variables: `MYSQL_ROOT_PASSWORD`, `MYSQL_DATABASE: transfers_db`, `MYSQL_USER`, `MYSQL_PASSWORD`. Montar `init-db.sql` en el directorio de inicialización de MySQL (`/docker-entrypoint-initdb.d/`). Healthcheck: `mysqladmin ping`.

> **Por qué no incluir los microservicios en docker-compose todavía:** Los servicios Spring Boot se arrancarán localmente durante el desarrollo (paso a paso). Solo al final, en el PASO 9, se dockerizan y se agregan al compose.

**Archivo: `docker/init-db.sql`**

Este script SQL corre automáticamente cuando MySQL arranca por primera vez. Debe:
- Crear las tablas (ver PASO 2 para el schema completo)
- Insertar los datos de prueba iniciales:
  - Cuenta `ACC-001` (propietario: Ana) con saldo `1000.00`
  - Cuenta `ACC-002` (propietario: Bob) con saldo `500.00`

**Archivo: `docker/wait-for-kafka.ps1`**

Script PowerShell que hace ping al broker cada 2 segundos hasta que Kafka responde. Se ejecuta desde el host antes de arrancar los microservicios con `mvn spring-boot:run`. Solo necesita el host y puerto como parámetros (`-Host localhost -Port 9092`).

> En los Dockerfiles (PASO 9) se usará la variante bash equivalente dentro del contenedor, pero durante el desarrollo local se ejecuta este `.ps1` desde PowerShell.

---

### 0.4 — Verificación del PASO 0

Desde PowerShell, posicionado en la carpeta `docker/`:

```powershell
# 1. Levantar los contenedores en background
docker compose up -d

# 2. Ver el estado de los servicios (esperar que aparezcan como "healthy")
docker compose ps

# 3. Verificar Kafka — debe listar topics sin error
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh `
    --list --bootstrap-server localhost:9092

# 4. Verificar MySQL — te pedirá la contraseña definida en docker-compose.yml
docker exec -it mysql mysql -u lab_user -p transfers_db

# 5. Dentro del prompt de MySQL, ejecutar:
#    SHOW TABLES;
#    SELECT * FROM accounts;
#    EXIT;
```

### 0.5 — Crear los topics de Kafka manualmente

Desde PowerShell, crear los 7 topics del sistema:

```powershell
$topics = @(
    "transfer.requested",
    "transfer.validated",
    "transfer.failed",
    "transfer.debited",
    "transfer.credited",
    "transfer.compensated"
)

foreach ($topic in $topics) {
    docker exec kafka /opt/kafka/bin/kafka-topics.sh `
        --create --topic $topic `
        --partitions 3 --replication-factor 1 `
        --bootstrap-server localhost:9092
    Write-Host "✅ Topic creado: $topic"
}

# DLQ con 1 partición (el orden no importa aquí)
docker exec kafka /opt/kafka/bin/kafka-topics.sh `
    --create --topic transfer.dlq `
    --partitions 1 --replication-factor 1 `
    --bootstrap-server localhost:9092
Write-Host "✅ Topic creado: transfer.dlq"
```

Verificar:
```powershell
docker exec kafka /opt/kafka/bin/kafka-topics.sh `
    --list --bootstrap-server localhost:9092
```

---

## ✅ GATE DE SALIDA — PASO 0
> Ejecuta estos comandos manualmente. Si todos pasan, avanza al PASO 1.

```powershell
# GATE 0.1 — Docker: ambos contenedores deben decir "Up (healthy)"
docker compose ps

# GATE 0.2 — MySQL: deben aparecer las 3 tablas
docker exec mysql mysql -u lab_user -plab_pass transfers_db `
    -e "SHOW TABLES;"
# Esperado: accounts | processed_events | transactions

# GATE 0.3 — Datos semilla: 3 cuentas con saldo
docker exec mysql mysql -u lab_user -plab_pass transfers_db `
    -e "SELECT id, owner_name, balance FROM accounts;"
# Esperado: ACC-001/Ana 1000.00 | ACC-002/Bob 500.00 | ACC-003/Carlos 2500.00

# GATE 0.4 — Kafka: los 7 topics del sistema
docker exec kafka /opt/kafka/bin/kafka-topics.sh `
    --list --bootstrap-server localhost:9092
# Esperado: transfer.compensated transfer.credited transfer.debited
#           transfer.dlq transfer.failed transfer.requested transfer.validated
```

| Gate | Señal de éxito | Error común |
|------|----------------|-------------|
| 0.1 Docker | `Up (healthy)` en ambas filas | Esperar 30s más y reintentar |
| 0.2 Tablas | 3 tablas listadas | `init-db.sql` no corrió — `docker compose down -v && up -d` |
| 0.3 Semilla | 3 cuentas con balance correcto | Ver log: `docker logs mysql --tail 20` |
| 0.4 Topics | 7 topics listados | Kafka no listo — ejecutar `wait-for-kafka.ps1` primero |

---

## PASO 1 — Módulo Common (Contratos compartidos)

**Objetivo:** Crear las clases Java que son compartidas por TODOS los microservicios. Este módulo no tiene lógica de negocio, solo estructuras de datos y constantes. Compila a un JAR que los otros módulos declaran como dependencia.

> **⚠️ Nota sobre el diseño implementado:** El plan original preveía un único `TransferEvent.java` con un campo `eventType` para diferenciar mensajes. Se decidió usar **6 clases de evento separadas** (una por tipo). Esta decisión está explicada en detalle en `DECISIONES.md`. El resultado es más explícito, más fácil de validar y más fácil de evolucionar independientemente.

---

### 1.1 — Estructura real del módulo

```
kafka-transfers/
└── common/
    ├── pom.xml
    └── src/main/java/com/lab/common/
        ├── event/
        │   ├── TransferRequestedEvent.java    ← API publica, validation-service consume
        │   ├── TransferValidatedEvent.java    ← validation publica, account-service consume
        │   ├── TransferFailedEvent.java       ← validation o account publican, status consume
        │   ├── TransferDebitedEvent.java      ← account publica (débito listo)
        │   ├── TransferCreditedEvent.java     ← account publica (crédito listo)
        │   └── TransferCompensatedEvent.java  ← account publica (rollback ejecutado)
        ├── enums/
        │   └── TransferStatus.java            ← 7 estados del Saga
        └── constants/
            └── KafkaTopics.java               ← nombres de los 7 topics como constantes
```

---

### 1.2 — Archivo: `common/pom.xml`

**Qué tiene:**
- Hereda del POM padre (`kafka-transfers`)
- `artifactId`: `common`, `packaging`: `jar`
- Dependencias: `jackson-databind`, `jackson-datatype-jsr310` (para `Instant`), `jakarta.validation-api`
- **NO tiene** `spring-boot-starter` ni `spring-boot-maven-plugin` — es una librería, no una aplicación

---

### 1.3 — Los 6 eventos del Saga

Cada evento es una clase Java con anotaciones `@Data @Builder @NoArgsConstructor @AllArgsConstructor` de Lombok.
Todos los eventos comparten estos campos base: `transactionId`, `fromAccount`, `toAccount`, `amount`, `timestamp (Instant)`.

| Archivo | Publicado por | Consumido por | Campos extra |
|---------|--------------|---------------|--------------|
| `TransferRequestedEvent` | transfer-api | validation-service | — |
| `TransferValidatedEvent` | validation-service | account-service | — |
| `TransferFailedEvent` | validation o account | status-service | `reason`, `failedBy` |
| `TransferDebitedEvent` | account-service | account-service (crédito) + status | `remainingBalance` |
| `TransferCreditedEvent` | account-service | status-service | `newBalance` |
| `TransferCompensatedEvent` | account-service | status-service | `compensationReason`, `restoredBalance` |

---

### 1.4 — Archivo: `TransferStatus.java`

**Ruta:** `common/src/main/java/com/lab/common/enums/TransferStatus.java`

**Qué es:** Enum que representa todos los estados posibles de una transferencia a lo largo de la saga.

**Valores:**

```
PROCESSING      → recién creada por el API, aún no validada
VALIDATED       → pasó las validaciones
DEBITED         → saldo debitado de la cuenta origen
CREDITED        → saldo acreditado en la cuenta destino
COMPLETED       → saga terminada correctamente
FAILED          → algo falló, ver errorReason
ROLLED_BACK     → el débito fue revertido después de un fallo en crédito
```

**Flujo feliz:**       `PROCESSING → VALIDATED → DEBITED → CREDITED → COMPLETED`

**Flujo de fallo:**    `PROCESSING → VALIDATED → DEBITED → FAILED → ROLLED_BACK`

**Flujo de rechazo:**  `PROCESSING → FAILED` (si la validación falla)

---

### 1.5 — Archivo: `EventType.java`

**Ruta:** `common/src/main/java/com/lab/common/enums/EventType.java`

**Qué es:** Enum con los nombres de los tipos de eventos. Cada valor corresponde exactamente a un topic de Kafka.

**Valores:**
```
TRANSFER_CREATED        → topic: transfer.created
TRANSFER_VALIDATED      → topic: transfer.validated
TRANSFER_DEBITED        → topic: transfer.debited
TRANSFER_CREDITED       → topic: transfer.credited
TRANSFER_COMPLETED      → topic: transfer.completed
TRANSFER_FAILED         → topic: transfer.failed
TRANSFER_ROLLBACK_DEBIT → topic: transfer.rollback.debit
```

---

### 1.6 — Archivo: `KafkaTopics.java`

**Ruta:** `common/src/main/java/com/lab/common/constants/KafkaTopics.java`

**Qué es:** Clase con constantes `public static final String` que contienen los nombres exactos de los topics. Todos los microservicios referencian estas constantes en lugar de escribir strings literales.

**Por qué:** Evita errores de tipeo en nombres de topics. Si cambia un nombre, se cambia en un solo lugar.

**Constantes a definir:**
```
TRANSFER_CREATED         = "transfer.created"
TRANSFER_VALIDATED       = "transfer.validated"
TRANSFER_DEBITED         = "transfer.debited"
TRANSFER_CREDITED        = "transfer.credited"
TRANSFER_COMPLETED       = "transfer.completed"
TRANSFER_FAILED          = "transfer.failed"
TRANSFER_ROLLBACK_DEBIT  = "transfer.rollback.debit"
TRANSFER_DLQ             = "transfer.dlq"
```

---

### 1.7 — Verificación del PASO 1

```powershell
# Desde PowerShell, posicionarse en el módulo common
cd kafka-transfers\common
mvn clean install
```

Resultado esperado: `BUILD SUCCESS`. El JAR `common-1.0.0.jar` queda en el repositorio Maven local (`~\.m2\repository\com\lab\common\`) y los otros módulos pueden depender de él.

---

## ✅ GATE DE SALIDA — PASO 1
> Ejecuta estos comandos manualmente. Si todos pasan, avanza al PASO 2.

```powershell
# Desde la raíz del proyecto
cd C:\Users\aldo_\Documents\SIIE\kafka_documentacion\kafka_curso\lab-week-2\kafka-transfers

# GATE 1.1 — Compilar e instalar common en el repositorio Maven local
mvn clean install -pl common

# GATE 1.2 — Verificar que el JAR quedó en ~/.m2
Test-Path "$env:USERPROFILE\.m2\repository\com\lab\common\1.0.0\common-1.0.0.jar"
# Esperado: True

# GATE 1.3 — Verificar las clases dentro del JAR
jar tf "$env:USERPROFILE\.m2\repository\com\lab\common\1.0.0\common-1.0.0.jar" | Select-String "com/lab"
# Esperado: listado con event/, enums/, constants/
```

| Gate | Señal de éxito | Error común |
|------|----------------|-------------|
| 1.1 Build | `BUILD SUCCESS` al final del log | Revisar errores de compilación Java |
| 1.2 JAR existe | `True` | El install no completó — ver output de Maven |
| 1.3 Clases | Aparecen `TransferRequestedEvent`, `TransferStatus`, `KafkaTopics` | JAR vacío — limpiar y repetir |

---

## PASO 2 — Esquema de base de datos

**Objetivo:** Definir las tablas exactas antes de escribir código de servicios. El schema va en scripts SQL que Flyway aplica automáticamente al arrancar cada microservicio.

---

### 2.1 — Qué se crea en este paso

El PASO 2 establece la capa de datos **antes** de escribir lógica de negocio. Tres tipos de archivos por cada microservicio:

```
transfer-api/
├── pom.xml                                          ← dependencias del servicio
├── src/main/resources/
│   ├── application.yml                              ← config DB, Kafka, Flyway
│   └── db/migration/
│       └── V1__create_transactions_table.sql        ← schema de la tabla transactions

validation-service/
├── pom.xml
├── src/main/resources/
│   ├── application.yml
│   └── db/migration/
│       └── V1__create_validation_schema.sql         ← crea accounts + processed_events (IF NOT EXISTS)

account-service/
├── pom.xml
├── src/main/resources/
│   ├── application.yml
│   └── db/migration/
│       └── V1__create_account_schema.sql            ← propietario de accounts + processed_events

status-service/
├── pom.xml
├── src/main/resources/
│   ├── application.yml
│   └── db/migration/
│       └── V1__create_status_schema.sql             ← crea transactions (IF NOT EXISTS)
```

> **Convención Flyway obligatoria:** `V{número}__{descripción}.sql`
> El doble guion bajo `__` es obligatorio. `V1__` primera migración, `V2__` segunda, etc.
> Flyway rechaza cualquier nombre que no cumpla el patrón.

---

### 2.2 — Propietario de cada tabla

En este lab todos los servicios comparten la misma base de datos (`transfers_db`).
Cada tabla tiene un **propietario lógico** (quien la crea y tiene autoridad sobre su schema),
pero otros servicios pueden leer/escribir en ella:

| Tabla | Propietario | Lectores / Escritores adicionales |
|-------|-------------|-----------------------------------|
| `transactions` | transfer-api | status-service (actualiza status) |
| `accounts` | account-service | validation-service (verifica existencia y saldo) |
| `processed_events` | account-service | validation-service (escribe su propia idempotencia) |

Las migraciones de `validation-service` y `status-service` usan `CREATE TABLE IF NOT EXISTS`
para las tablas que no son suyas — así arrancan correctamente sin importar el orden de inicio.

---

### 2.3 — Las 3 tablas del sistema

**`accounts`** — el recurso más sensible del sistema

| Columna | Tipo | Descripción |
|---------|------|-------------|
| `id` | VARCHAR(36) PK | Identificador de cuenta (`ACC-001`) |
| `owner_name` | VARCHAR(100) | Nombre del titular |
| `balance` | DECIMAL(15,2) | Saldo. CHECK constraint: `balance >= 0` |
| `version` | BIGINT DEFAULT 0 | Optimistic Locking — JPA `@Version` |
| `created_at` / `updated_at` | DATETIME | Auditoría |

**`transactions`** — el registro central del Saga

| Columna | Tipo | Descripción |
|---------|------|-------------|
| `id` | VARCHAR(36) PK | UUID de la transferencia |
| `from_account` / `to_account` | VARCHAR(36) | Cuentas involucradas |
| `amount` | DECIMAL(15,2) | Monto |
| `status` | VARCHAR(30) | Estado actual (CHECK valida valores del enum) |
| `failure_reason` | VARCHAR(255) NULL | Motivo del fallo si aplica |
| `created_at` / `updated_at` | DATETIME | Auditoría |

**`processed_events`** — la tabla de idempotencia ⚠️

| Columna | Tipo | Descripción |
|---------|------|-------------|
| `id` | BIGINT AUTO_INCREMENT | PK técnica |
| `event_key` | VARCHAR(200) | `transactionId:tipoEvento` (ej: `"tx-abc:TRANSFER_DEBITED"`) |
| `service_name` | VARCHAR(50) | Qué servicio lo procesó |
| `processed_at` | DATETIME | Cuándo fue procesado |
| **UNIQUE** | `(event_key, service_name)` | **La Capa 3 de idempotencia** |

---

### 2.4 — Configuración clave en `application.yml`

Todos los servicios comparten la misma estructura de config. Puntos críticos:

**`spring.jpa.hibernate.ddl-auto: validate`**
Hibernate verifica que las entidades Java coinciden con el schema real de la DB,
pero **no modifica las tablas**. Flyway es el único que gestiona el schema.
Si usaras `create` o `update`, Flyway y Hibernate entrarían en conflicto.

**`spring.flyway.table: flyway_history_<servicio>`**
Flyway guarda en esta tabla qué migraciones ya aplicó. Al dar a cada servicio
un nombre distinto, cada uno lleva su propio historial en la misma DB compartida.
Sin esto, el segundo servicio en arrancar vería las migraciones del primero
como "ya aplicadas" y no ejecutaría las suyas propias.

**`spring.kafka.consumer.enable.auto.commit: false`**
El offset (posición de lectura en Kafka) se confirma **manualmente**, solo después
de que el mensaje fue procesado y persistido en DB. Si el servicio falla en medio
del procesamiento, Kafka reentregará el mensaje al reiniciar — garantizando
at-least-once delivery. Con `true`, el offset se confirmaría automáticamente cada
pocos segundos aunque el procesamiento no haya terminado → pérdida de mensajes.

**`spring.kafka.listener.ack-mode: MANUAL_IMMEDIATE`**
Indica a Spring Kafka que el código llamará a `acknowledgment.acknowledge()`
manualmente para confirmar el offset. Trabaja en conjunto con `enable.auto.commit: false`.

**`app.simulate-credit-failure: false`** (solo en account-service)
Flag para el PASO 8 del lab. Cuando se cambia a `true`, el servicio simula
un fallo en el crédito para demostrar el mecanismo de compensación del Saga.

---

### 2.5 — Verificación del PASO 2

Desde PowerShell en la carpeta `docker/`:

```powershell
# Bajar y volver a levantar (fuerza que init-db.sql corra de nuevo)
docker compose down -v
docker compose up -d

# Esperar ~20 segundos y verificar tablas
docker exec -it mysql mysql -u lab_user -plab_pass transfers_db `
    -e "SHOW TABLES;"

# Ver datos semilla
docker exec -it mysql mysql -u lab_user -plab_pass transfers_db `
    -e "SELECT id, owner_name, balance FROM accounts;"
```

Resultado esperado:
```
+------------------+
| Tables_in_transfers_db |
+------------------+
| accounts           |
| processed_events   |
| transactions       |
+------------------+

+--------+-----------------+---------+
| id     | owner_name      | balance |
+--------+-----------------+---------+
| ACC-001| Ana García      | 1000.00 |
| ACC-002| Bob Martínez    |  500.00 |
| ACC-003| Carlos López    | 2500.00 |
+--------+-----------------+---------+
```

> **Nota:** Las tablas de historial de Flyway (`flyway_history_*`) se crearán automáticamente
> cuando arranques cada microservicio en el PASO 3, no antes.

---

## ✅ GATE DE SALIDA — PASO 2
> Ejecuta estos comandos manualmente en 4 terminales distintas. Si todos pasan, avanza al PASO 3.

**Terminal 1 — transfer-api:**
```powershell
cd kafka-transfers\transfer-api
mvn spring-boot:run
# Busca en el log:
#   "Migrating schema to version 1 - create transactions table"
#   "Successfully applied 1 migration"
#   "Started TransferApiApplication"
```

**Terminal 2 — account-service:**
```powershell
cd kafka-transfers\account-service
mvn spring-boot:run
# Busca: "Migrating schema to version 1 - create account schema"
#        "Started AccountServiceApplication"
```

**Terminal 3 — validation-service:**
```powershell
cd kafka-transfers\validation-service
mvn spring-boot:run
# Busca: "Successfully applied 1 migration"
#        "Started ValidationServiceApplication"
```

**Terminal 4 — status-service:**
```powershell
cd kafka-transfers\status-service
mvn spring-boot:run
# Busca: "Started StatusServiceApplication"
```

**Verificar Flyway history en DB** (nueva terminal):
```powershell
# GATE 2.1 — 4 tablas de historial de Flyway creadas
docker exec mysql mysql -u lab_user -plab_pass transfers_db `
    -e "SHOW TABLES LIKE 'flyway%';"
# Esperado: flyway_history_account | flyway_history_status
#           flyway_history_transfer_api | flyway_history_validation

# GATE 2.2 — La migración de transfer-api fue exitosa
docker exec mysql mysql -u lab_user -plab_pass transfers_db `
    -e "SELECT version, description, success FROM flyway_history_transfer_api;"
# Esperado: version=1, success=1

# GATE 2.3 — La migración de account-service fue exitosa
docker exec mysql mysql -u lab_user -plab_pass transfers_db `
    -e "SELECT version, description, success FROM flyway_history_account;"
# Esperado: version=1, success=1
```

| Gate | Señal de éxito | Error común |
|------|----------------|-------------|
| 2.1 Flyway tables | 4 tablas `flyway_history_*` | Algún servicio no arrancó — ver su log |
| 2.2 transfer-api migration | `success=1` | Tablas no existen — `docker compose down -v && up -d` |
| 2.3 account-service migration | `success=1` | common no instalado — `mvn install -pl common` |
| Todos los servicios inician | `Started XApplication` en cada terminal | Error de conexión a Kafka/MySQL — verificar `docker compose ps` |

> Puedes detener los 4 servicios con `Ctrl+C` en cada terminal. En PASO 3 los volverás a arrancar con código real.

---

## PASO 3 — Transfer API (Gateway)

**Objetivo:** El primer microservicio completo. Recibe requests HTTP, los persiste en DB y dispara el primer evento del Saga en Kafka. No valida saldos ni toca cuentas — eso lo hace validation-service.

---

### 3.1 — Estructura real implementada

```
transfer-api/src/main/java/com/lab/api/
├── TransferApiApplication.java     ← @SpringBootApplication (paquete raíz)
├── entity/
│   └── Transaction.java            ← entidad JPA: tabla transactions
├── dto/
│   ├── TransferRequest.java        ← body de POST /transfers (con validaciones @Valid)
│   └── TransferResponse.java       ← respuesta HTTP con transactionId y status
├── repository/
│   └── TransactionRepository.java  ← JpaRepository<Transaction, String>
├── producer/
│   └── TransferEventProducer.java  ← publica TransferRequestedEvent en Kafka
├── service/
│   └── TransferService.java        ← orquesta: DB primero, Kafka segundo
└── controller/
    └── TransferController.java     ← POST /transfers y GET /transfers/{id}
```

---

### 3.2 — Flujo completo de una petición POST /transfers

```
Cliente HTTP
  │  POST /transfers  { fromAccount, toAccount, amount }
  ▼
TransferController.createTransfer()
  │  @Valid valida el body. Si falla → HTTP 400 { campo: "mensaje de error" }
  ▼
TransferService.initiateTransfer()        ← @Transactional
  │  1. UUID.randomUUID() → transactionId
  │  2. Transaction.builder()...save()   → INSERT en tabla transactions (PROCESSING)
  │  3. TransferRequestedEvent.builder() → construye el evento con los mismos datos
  │  4. eventProducer.publishTransferRequested(event) → Kafka (asíncrono)
  │  5. return TransferResponse(status=PROCESSING)
  ▼
TransferEventProducer.publishTransferRequested()
  │  kafkaTemplate.send("transfer.requested", fromAccount, event)
  │  whenComplete → log partition+offset (éxito) o log error (fallo)
  ▼
HTTP 202 ACCEPTED
  { transactionId, fromAccount, toAccount, amount, status: "PROCESSING", message }
```

---

### 3.3 — Puntos clave del código generado

**`Transaction.java` — `@Enumerated(EnumType.STRING)`**
Hibernate guarda `"PROCESSING"` en la DB en lugar del índice `0`. Si reordenas
el enum, los datos existentes no se corrompen.

**`TransferRequest.java` — validaciones activas con `@Valid`**
- `@NotBlank` → rechaza null, `""` y `"   "` (espacios)
- `@Positive` → rechaza 0 y negativos en `amount`
- Sin `@Valid` en el controller, estas anotaciones no hacen nada

**`TransferService.java` — `@Transactional`**
Si `save()` falla (DB caída), el evento Kafka no se publica. Al revés no aplica:
si Kafka falla después del `save()`, el registro queda en PROCESSING en DB.

**`TransferEventProducer.java` — callback asíncrono**
`send()` retorna inmediatamente (no bloquea el hilo HTTP). El callback
`whenComplete` logea el resultado cuando el broker responde.

**`TransferController.java` — dos `@ExceptionHandler`**
- `TransactionNotFoundException` → HTTP 404
- `MethodArgumentNotValidException` → HTTP 400 con mapa `{ campo: "error" }`

---

### 3.4 — Arrancar el servicio

```powershell
# Desde la raíz del proyecto
cd C:\Users\aldo_\Documents\SIIE\kafka_documentacion\kafka_curso\lab-week-2\kafka-transfers
mvn clean install -pl common    # instala common si no lo has hecho
cd transfer-api
mvn spring-boot:run
```

Señales de arranque correcto en el log:
```
Migrating schema to version 1 - create transactions table
Successfully applied 1 migration to schema
Tomcat started on port(s): 8080
Started TransferApiApplication in X.XXX seconds
```

---

## ✅ GATE DE SALIDA — PASO 3
> Con transfer-api corriendo (`mvn spring-boot:run`), ejecuta estos comandos manualmente.

**GATE 3.1 — El servicio arrancó y Flyway corrió correctamente:**
```
Busca en el log de arranque:
  ✅ "Successfully applied 1 migration"
  ✅ "Started TransferApiApplication"
  ✅ "Tomcat started on port(s): 8080"
```

**GATE 3.2 — POST /transfers crea una transferencia:**
```powershell
curl.exe -X POST http://localhost:8080/transfers `
  -H "Content-Type: application/json" `
  -d '{\"fromAccount\":\"ACC-001\",\"toAccount\":\"ACC-002\",\"amount\":150.00}'
```
Respuesta esperada (HTTP 202):
```json
{
  "transactionId": "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx",
  "fromAccount": "ACC-001",
  "toAccount": "ACC-002",
  "amount": 150.00,
  "status": "PROCESSING",
  "message": "Transferencia iniciada. Consulta el estado con GET /transfers/..."
}
```

**GATE 3.3 — GET /transfers/{id} devuelve el estado:**
```powershell
# Copia el transactionId de la respuesta anterior
$txId = "pega-aqui-el-uuid"
curl.exe http://localhost:8080/transfers/$txId
```
Respuesta esperada (HTTP 200) con `"status": "PROCESSING"`.

**GATE 3.4 — La transacción existe en la base de datos:**
```powershell
docker exec mysql mysql -u lab_user -plab_pass transfers_db `
    -e "SELECT id, from_account, to_account, amount, status FROM transactions;"
```
Esperado: 1 fila con `status = PROCESSING`.

**GATE 3.5 — El evento llegó a Kafka (nueva terminal, Ctrl+C para salir):**
```powershell
docker exec kafka /opt/kafka/bin/kafka-console-consumer.sh `
    --topic transfer.requested --from-beginning `
    --bootstrap-server localhost:9092
```
Esperado: JSON del evento `TransferRequestedEvent` con los datos de la transferencia.

**GATE 3.6 — Validaciones funcionan (HTTP 400 con campos inválidos):**
```powershell
# Monto negativo
curl.exe -X POST http://localhost:8080/transfers `
  -H "Content-Type: application/json" `
  -d '{\"fromAccount\":\"ACC-001\",\"toAccount\":\"ACC-002\",\"amount\":-50}'
# Esperado: HTTP 400 { "amount": "El monto debe ser mayor que cero" }

# Campo vacío
curl.exe -X POST http://localhost:8080/transfers `
  -H "Content-Type: application/json" `
  -d '{\"fromAccount\":\"\",\"toAccount\":\"ACC-002\",\"amount\":100}'
# Esperado: HTTP 400 { "fromAccount": "La cuenta origen no puede estar vacía" }
```

**GATE 3.7 — ID inexistente devuelve 404:**
```powershell
curl.exe http://localhost:8080/transfers/id-que-no-existe
# Esperado: HTTP 404 { "error": "Transferencia no encontrada: id-que-no-existe" }
```

| Gate | Señal de éxito | Error común |
|------|----------------|-------------|
| 3.1 Arranque | Log muestra los 3 mensajes esperados | `common` no instalado — `mvn install -pl common` |
| 3.2 POST | HTTP 202 con `transactionId` UUID | `Connection refused` — Docker no corre |
| 3.3 GET | HTTP 200 con `status: PROCESSING` | Copiar bien el UUID del paso anterior |
| 3.4 DB | Fila en `transactions` con `PROCESSING` | `mvn install -pl common` y reiniciar |
| 3.5 Kafka | JSON del evento visible en la consola | Topic no creado — ejecutar el script de creación del GATE 0.4 |
| 3.6 Validaciones | HTTP 400 con mensaje del campo | `spring-boot-starter-validation` falta en pom.xml |
| 3.7 404 | HTTP 404 con mensaje legible | Revisar `@ExceptionHandler` en controller |

---

## PASO 4 — Validation Service

**Objetivo:** El primer consumer de la Saga. Lee de `transfer.requested`, aplica 4 reglas de negocio con patrón *fail fast*, y publica `transfer.validated` o `transfer.failed`. Implementa las Capas 2 y 3 de idempotencia.

---

### 4.1 — Estructura del módulo (archivos creados en este paso)

```
kafka-transfers/validation-service/
├── pom.xml                              ← ya existía desde PASO 2
└── src/main/
    ├── java/com/lab/validation/
    │   ├── ValidationServiceApplication.java   ← ya existía desde PASO 2
    │   ├── entity/
    │   │   ├── Account.java             ← vista de solo-lectura de la tabla accounts
    │   │   └── ProcessedEvent.java      ← registro de idempotencia
    │   ├── repository/
    │   │   ├── AccountRepository.java   ← existsById() para validar cuentas
    │   │   └── ProcessedEventRepository.java   ← existsByEventKeyAndServiceName()
    │   ├── service/
    │   │   ├── IdempotencyService.java  ← Capas 2 y 3 de idempotencia
    │   │   └── ValidationService.java   ← 4 reglas de negocio, fail-fast
    │   ├── producer/
    │   │   └── ValidationEventProducer.java    ← publica validated / failed
    │   └── consumer/
    │       └── TransferRequestedConsumer.java  ← @KafkaListener en transfer.requested
    └── resources/
        ├── application.yml              ← actualizado: ddl-auto: validate
        └── db/migration/
            └── V1__create_validation_schema.sql  ← ya existía desde PASO 2
```

**Orden de creación dentro del PASO 4:**
1. `entity/` primero — Hibernate necesita las clases para validar el schema al arrancar
2. `repository/` segundo — los services los inyectan; no pueden compilar sin ellos
3. `service/` tercero — IdempotencyService antes que ValidationService (ValidationService no depende de Idempotency, pero el consumer sí; orden por dependencia del consumer)
4. `producer/` cuarto — ValidationService publica eventos; producer debe existir antes que el service lo use
5. `consumer/` último — orquesta todo lo anterior; si va antes, falla por beans no definidos
6. `application.yml` — actualizar `ddl-auto: none → validate` cuando ya existen las entidades

---

### 4.2 — Archivo: `application.yml` (cambio clave respecto a PASO 2)

El único cambio respecto al PASO 2 es una línea:

```yaml
jpa:
  hibernate:
    ddl-auto: validate   # ← era "none" en PASO 2; ahora Hibernate verifica el schema
```

**Configuraciones críticas para el consumer:**

```yaml
spring:
  kafka:
    consumer:
      group-id: validation-group        # ← ÚNICO por servicio (clave para load balancing)
      value-deserializer: org.springframework.kafka.support.serializer.JsonDeserializer
      properties:
        spring.json.trusted.packages: com.lab.*   # ← permite deserializar eventos de common
        auto.offset.reset: earliest               # ← si hay mensajes no leídos, leerlos todos
        enable.auto.commit: false                 # ← el código confirma manualmente el offset
    listener:
      ack-mode: MANUAL_IMMEDIATE        # ← Acknowledgment se inyecta en @KafkaListener
```

---

### 4.3 — Entidades: `Account.java` y `ProcessedEvent.java`

**`Account.java` — ¿por qué no tiene `@Builder`?**

Esta entidad es de **solo lectura** en validation-service. Nunca creamos cuentas aquí — solo consultamos si existen. Por eso:
- Tiene `@Data` y `@NoArgsConstructor` (Hibernate los necesita)
- **No tiene `@Builder`** — jamás construiremos un objeto Account en este servicio; si alguien intenta hacerlo, el compilador lo impedirá
- Tiene el campo `version Long` porque Flyway creó la columna `version` en accounts y Hibernate en modo `validate` verificará que el campo exista

**`ProcessedEvent.java` — el registro de idempotencia**

```
event_key   VARCHAR(255) ─────┐
service_name VARCHAR(50) ─────┘ ← UNIQUE constraint (Capa 3)
processed_at TIMESTAMP
```

- `@GeneratedValue(IDENTITY)` — MySQL auto-incrementa el ID; no lo generamos nosotros
- La columna `(event_key, service_name)` tiene `UNIQUE` en la DB — eso hace posible la Capa 3

---

### 4.4 — Repositorios: `AccountRepository` y `ProcessedEventRepository`

```java
// AccountRepository — solo usa métodos heredados
existsById(String id)
// → genera: SELECT EXISTS(SELECT 1 FROM accounts WHERE id = ?)
// → más eficiente que findById() porque no transfiere la fila completa

// ProcessedEventRepository — método declarativo Spring Data
boolean existsByEventKeyAndServiceName(String eventKey, String serviceName)
// → Spring Data lo implementa automáticamente por el nombre del método
// → sin SQL escrito a mano
```

---

### 4.5 — Servicio: `IdempotencyService.java`

**Las 3 capas de idempotencia:**

| Capa | Dónde | Protege contra |
|------|-------|---------------|
| 1 | Kafka producer (`enable.idempotence=true` en transfer-api) | Duplicados a nivel de red/protocolo |
| 2 | `existsByEventKeyAndServiceName()` antes del INSERT | La mayoría de duplicados; rápido |
| 3 | `UNIQUE constraint` en DB + catch `DataIntegrityViolationException` | Condición de carrera entre threads |

**`@Transactional(propagation = REQUIRES_NEW)` — la decisión más importante:**

```
Sin REQUIRES_NEW:           Con REQUIRES_NEW:
────────────────            ─────────────────────────
Tx del caller               Tx del caller
  └─ tryRegister() ─────── Tx NUEVA independiente
       ↓ INSERT OK            └─ INSERT OK → commit inmediato
  ↓ falla algo después      ↓ falla algo después
  ↓ rollback total          ↓ rollback solo del caller
  ↓ el INSERT se revierte   ↓ el INSERT PERMANECE
  ↓ reintento ve "no existe"↓ reintento ve "ya existe"
  ↓ PROCESA DOBLE ❌         ↓ lo filtra como duplicado ✅
```

**Flujo de `tryRegister()`:**

```
¿Ya existe en processed_events?
        │
       SÍ ──→ return false  (Capa 2: duplicado detectado)
        │
       NO
        │
    INSERT en processed_events
        │
     ¿Éxito? ──→ return true  (Capa 3: evento nuevo, procesar)
        │
     ¿DataIntegrityViolationException?
        │
    return false  (Capa 3: otro thread ganó la carrera)
```

---

### 4.6 — Servicio: `ValidationService.java`

**4 reglas en orden de menor a mayor costo:**

```
Regla 1: fromAccount ≠ toAccount   → comparación en memoria, 0 queries DB
Regla 2: amount > 0                → comparación en memoria, 0 queries DB
Regla 3: fromAccount existe en DB  → 1 query SELECT EXISTS
Regla 4: toAccount existe en DB    → 1 query SELECT EXISTS
```

**¿Por qué este orden?** Ordenar de menor a mayor costo garantiza que si la transferencia es
claramente inválida (misma cuenta, monto cero), no se desperdician queries a la DB.

**¿Por qué NO valida el saldo aquí?** El saldo lo verifica `account-service` en el momento
exacto del débito, con `@Version` (Optimistic Locking). Si se validara aquí, entre la
validación y el débito el saldo podría cambiar — habría una condición de carrera sin solución.
`account-service` es quien cierra esa ventana.

**Patrón fail-fast:**
```java
if (regla1Falla) { producer.publishFailed(...); return; }  // ← return inmediato
if (regla2Falla) { producer.publishFailed(...); return; }  // ← no acumula errores
// ...
producer.publishValidated(event);  // ← solo si todas pasan
```

---

### 4.7 — Producer: `ValidationEventProducer.java`

Publica exactamente dos tipos de eventos:

| Método | Topic | Evento | Clave de partición |
|--------|-------|--------|--------------------|
| `publishValidated()` | `transfer.validated` | `TransferValidatedEvent` | `fromAccount` |
| `publishFailed()` | `transfer.failed` | `TransferFailedEvent` | `fromAccount` |

**¿Por qué `fromAccount` como clave de partición?**
Kafka garantiza orden dentro de una partición. Usar `fromAccount` como clave asegura que
todos los eventos de la misma cuenta origen van a la misma partición, en orden. Si dos
transferencias de la misma cuenta se procesan en paralelo, Kafka las serializa.

---

### 4.8 — Consumer: `TransferRequestedConsumer.java`

**Flujo completo del método `onTransferRequested()`:**

```
Llega mensaje de transfer.requested
         │
   Construir eventKey = transactionId + ":TRANSFER_REQUESTED"
         │
   ┌─────▼──────────────────────────────────┐
   │  idempotencyService.tryRegister(key)   │
   └─────┬──────────────────────────────────┘
         │
    false (duplicado)        true (nuevo)
         │                       │
   acknowledgment.acknowledge()  validationService.validate(event)
         │                       │
       return              acknowledgment.acknowledge()
                                 │
                           log "✅ Offset confirmado"
         │
   catch(Exception e)
         │
   log error
   (SIN acknowledge → Kafka reentrega el mensaje)
```

**¿Por qué ACK en duplicados pero NO en excepciones?**

- **Duplicado → ACK:** Si no hacemos ACK, Kafka reentregará el mensaje en el próximo poll → loop infinito de duplicados rechazados.
- **Excepción → sin ACK:** Queremos que Kafka reentregue el mensaje para que el servicio lo intente de nuevo tras reiniciar. Si hay un bug, el mensaje queda "pendiente" hasta que el bug se corrija (en producción: Dead Letter Topic).

---

### 4.9 — GATE DE SALIDA: verificar PASO 4

Antes de continuar al PASO 5, confirma que el validation-service funciona correctamente.

```powershell
# Terminal 1 — asegúrate que transfer-api sigue corriendo (del PASO 3)
# Si no está corriendo:
cd kafka-transfers\transfer-api
mvn spring-boot:run
```

```powershell
# Terminal 2 — arrancar validation-service
cd kafka-transfers\validation-service
mvn spring-boot:run
```

Espera ver en los logs de validation-service:
```
Started ValidationServiceApplication in X.XXX seconds
```

```powershell
# Terminal 3 — abrir consumer de transfer.validated para ver resultados
docker exec -it kafka /opt/kafka/bin/kafka-console-consumer.sh `
    --topic transfer.validated `
    --from-beginning `
    --bootstrap-server localhost:9092
```

```powershell
# Terminal 4 — abrir consumer de transfer.failed para ver rechazos
docker exec -it kafka /opt/kafka/bin/kafka-console-consumer.sh `
    --topic transfer.failed `
    --from-beginning `
    --bootstrap-server localhost:9092
```

```powershell
# Terminal 5 — casos de prueba

# ── CASO 1: transferencia VÁLIDA ──────────────────────────────────────────
curl.exe -X POST http://localhost:8080/transfers `
  -H "Content-Type: application/json" `
  -d '{\"fromAccount\":\"ACC-001\",\"toAccount\":\"ACC-002\",\"amount\":100}'
# Respuesta esperada: HTTP 202, status=PROCESSING
# En Terminal 3 (transfer.validated) debe aparecer el evento JSON con:
#   transactionId, fromAccount=ACC-001, toAccount=ACC-002, amount=100

# ── CASO 2: misma cuenta origen y destino ─────────────────────────────────
curl.exe -X POST http://localhost:8080/transfers `
  -H "Content-Type: application/json" `
  -d '{\"fromAccount\":\"ACC-001\",\"toAccount\":\"ACC-001\",\"amount\":50}'
# En Terminal 4 (transfer.failed) debe aparecer:
#   reason: "La cuenta origen y destino no pueden ser la misma"
#   failedBy: "validation-service"

# ── CASO 3: cuenta origen inexistente ─────────────────────────────────────
curl.exe -X POST http://localhost:8080/transfers `
  -H "Content-Type: application/json" `
  -d '{\"fromAccount\":\"ACC-999\",\"toAccount\":\"ACC-002\",\"amount\":100}'
# En Terminal 4 (transfer.failed):
#   reason: "Cuenta origen no encontrada: ACC-999"

# ── CASO 4: cuenta destino inexistente ────────────────────────────────────
curl.exe -X POST http://localhost:8080/transfers `
  -H "Content-Type: application/json" `
  -d '{\"fromAccount\":\"ACC-001\",\"toAccount\":\"ACC-999\",\"amount\":100}'
# En Terminal 4 (transfer.failed):
#   reason: "Cuenta destino no encontrada: ACC-999"

# ── CASO 5: monto inválido (cero) ─────────────────────────────────────────
curl.exe -X POST http://localhost:8080/transfers `
  -H "Content-Type: application/json" `
  -d '{\"fromAccount\":\"ACC-001\",\"toAccount\":\"ACC-002\",\"amount\":0}'
# Nota: transfer-api rechaza esto con HTTP 400 (@Positive en el DTO).
# No llega a Kafka. Esperado: { "amount": "El monto debe ser mayor que cero" }

# ── CASO 6: verificar idempotencia (duplicado manual) ─────────────────────
# 1. Toma el transactionId de la respuesta del CASO 1
# 2. El validation-service ya lo procesó y lo registró en processed_events
# 3. Publica el mismo evento manualmente en Kafka:
docker exec -it kafka /opt/kafka/bin/kafka-console-producer.sh `
    --topic transfer.requested `
    --bootstrap-server localhost:9092
# Pega exactamente el mismo JSON que viste en transfer.validated (el original)
# En los logs de validation-service verás:
#   ⚠️ Evento duplicado ignorado | key=<txId>:TRANSFER_REQUESTED
# En transfer.validated NO debe aparecer un segundo evento
```

**Verificar que `processed_events` tiene registros:**

```powershell
docker exec -it mysql mysql -u lab_user -plab_pass transfers_db `
    -e "SELECT * FROM processed_events WHERE service_name='validation-service';"
# Debe mostrar una fila por cada transferencia procesada (no duplicados)
```

**Tabla de errores del PASO 4:**

| Síntoma | Causa probable | Solución |
|---------|---------------|----------|
| `Caused by: com.mysql.cj.exceptions.DataTruncation` | `event_key` supera 255 chars | Verificar longitud del transactionId |
| `JsonParseException` en consumer | `trusted.packages` no incluye `com.lab.*` | Verificar application.yml |
| `No offset stored` en los logs | `auto.offset.reset` no está en `earliest` | Verificar application.yml |
| `Hibernate SchemaValidationException` | La entidad no coincide con la tabla | Verificar campos de `Account.java` vs tabla accounts |
| `validation-service` no recibe mensajes | group-id diferente al esperado | Verificar que `group-id: validation-group` en application.yml |
| `DataIntegrityViolationException` en logs (info) | Capa 3 de idempotencia funcionando | Normal — es el mecanismo funcionando |

✅ **Listo para PASO 5** cuando: los 4 casos de prueba producen los eventos esperados en los topics correctos y `processed_events` tiene exactamente un registro por transferencia única.

---

## PASO 5 — Account Service (El corazón del sistema)

**Objetivo:** El servicio más complejo. Maneja el débito, el crédito y la compensación (rollback) usando Optimistic Locking (`@Version`) e idempotencia en 3 capas. Aquí el dinero realmente se mueve.

---

### 5.1 — Estructura del módulo (archivos creados en este paso)

```
kafka-transfers/account-service/
├── pom.xml                              ← ya existía desde PASO 2
└── src/main/
    ├── java/com/lab/account/
    │   ├── AccountServiceApplication.java   ← ya existía desde PASO 2
    │   ├── entity/
    │   │   ├── Account.java             ← @Version activa Optimistic Locking
    │   │   └── ProcessedEvent.java      ← registro de idempotencia
    │   ├── repository/
    │   │   ├── AccountRepository.java   ← findById + save (para el ciclo read-modify-save)
    │   │   └── ProcessedEventRepository.java
    │   ├── service/
    │   │   ├── IdempotencyService.java  ← mismas 3 capas que validation-service
    │   │   ├── DebitService.java        ← descuenta saldo con Optimistic Locking
    │   │   ├── CreditService.java       ← acredita saldo, puede simular fallo
    │   │   └── CompensationService.java ← restaura el saldo cuando el crédito falla
    │   ├── producer/
    │   │   └── AccountEventProducer.java  ← publica debited / credited / failed / compensated
    │   └── consumer/
    │       ├── DebitConsumer.java       ← @KafkaListener en transfer.validated (account-debit-group)
    │       └── CreditConsumer.java      ← @KafkaListener en transfer.debited (account-credit-group)
    └── resources/
        ├── application.yml              ← actualizado: ddl-auto: validate
        └── db/migration/
            └── V1__create_account_schema.sql   ← ya existía desde PASO 2
```

**Dos consumer groups en el mismo servicio — ¿por qué?**

`account-debit-group` (DebitConsumer) escucha `transfer.validated`.
`account-credit-group` (CreditConsumer) escucha `transfer.debited`.

account-service mismo publica `transfer.debited` y también lo consume (para el crédito). Si ambos consumers usaran el mismo grupo, solo uno podría estar activo a la vez por topic. Con grupos distintos, ambos son completamente independientes y Kafka los trata como suscripciones separadas.

---

### 5.2 — La pieza más importante: `@Version` y Optimistic Locking

```java
@Version
@Column(name = "version", nullable = false)
private Long version;
```

**SQL que genera JPA automáticamente por la presencia de `@Version`:**

```sql
-- Sin @Version (lo que escribirías tú a mano, inseguro):
UPDATE accounts SET balance = 900.00 WHERE id = 'ACC-001'

-- Con @Version (lo que JPA genera, seguro):
UPDATE accounts SET balance = 900.00, version = 6
WHERE id = 'ACC-001' AND version = 5
```

**¿Qué pasa si dos threads debitan al mismo tiempo?**

```
Thread A                          Thread B
──────────────────────────────    ──────────────────────────────
SELECT → balance=1000, version=5  SELECT → balance=1000, version=5
balance = 1000 - 300 = 700        balance = 1000 - 200 = 800
UPDATE ... WHERE version=5 ✅     UPDATE ... WHERE version=5 ❌
  → rows affected: 1 → COMMIT       → rows affected: 0
  → version ahora es 6              → JPA lanza OptimisticLockingFailureException
                                    → sin ACK → Kafka reentrega
                                    → reintento: SELECT → balance=700, version=6
                                    → UPDATE ... WHERE version=6 ✅
```

Sin `@Version`, ambos threads leerían balance=1000 y lo dejarían en 800 o 700 (perdiendo uno de los débitos). Con `@Version`, el segundo thread detecta el conflicto y reintenta con el balance correcto.

---

### 5.3 — `DebitService.java` — el ciclo read-modify-save

```java
@Transactional
public Account debit(String fromAccount, BigDecimal amount) {
    // 1. READ con version actual
    Account account = accountRepository.findById(fromAccount).orElseThrow(...);
    // 2. Validar saldo suficiente
    if (account.getBalance().compareTo(amount) < 0) throw new InsufficientFundsException(...);
    // 3. MODIFY en memoria
    account.setBalance(account.getBalance().subtract(amount));
    // 4. SAVE → genera UPDATE ... WHERE id=? AND version=<leído>
    return accountRepository.save(account);
    // Si version cambió entre READ y SAVE → OptimisticLockingFailureException
}
```

**¿Por qué `@Transactional` en el servicio y no en el consumer?**
El consumer hace ACK de Kafka. Si el consumer fuera @Transactional, el commit de DB y el ACK de Kafka estarían en scopes distintos — podría commitear la DB y fallar antes del ACK. Al encapsular la transacción DB en DebitService, queda claro qué está dentro del contexto transaccional y qué no.

---

### 5.4 — `CreditService.java` — el punto de fallo controlado

```yaml
# application.yml — para demostrar el rollback en PASO 8
app:
  simulate-credit-failure: false  ← cambiar a true para activar el rollback
```

Cuando `simulate-credit-failure=true`, `CreditService.credit()` lanza `SimulatedCreditFailureException` **antes de tocar la DB**. El dinero de `toAccount` nunca se acredita. El `CreditConsumer` captura esta excepción y activa la compensación.

---

### 5.5 — `CompensationService.java` — cómo se revierte un débito

La compensación devuelve el dinero a `fromAccount` cuando el crédito falló.

**Flujo dentro de `CreditConsumer` cuando el crédito falla:**

```
CreditService.credit() lanza SimulatedCreditFailureException
         │
         ▼
CompensationService.compensate(txId, fromAccount, amount)
    ├─ ¿Ya fue compensado? (idempotencia con key txId:COMPENSATION)
    │     SI → return null (ya está hecho)
    │     NO → account.balance += amount → save()
    │           INSERT en processed_events (key txId:COMPENSATION)
    └─ return Account con saldo restaurado
         │
         ▼
producer.publishCompensated(...)    → transfer.compensated
producer.publishFailed(...)         → transfer.failed
acknowledgment.acknowledge()        → ACK (la saga terminó con rollback)
```

**¿Por qué la compensación es in-process (llamada directa) y no via Kafka?**
Para el lab, simplifica la arquitectura: no hay que crear un CompensationConsumer separado ni un topic adicional. En producción con sagas más complejas (múltiples servicios a compensar), se usaría un `transfer.rollback` topic o un Saga Orchestrator.

---

### 5.6 — `AccountEventProducer.java` — los 4 eventos

| Método | Topic publicado | Cuándo |
|--------|----------------|--------|
| `publishDebited()` | `transfer.debited` | Débito exitoso |
| `publishCredited()` | `transfer.credited` | Crédito exitoso (saga completa) |
| `publishCompensated()` | `transfer.compensated` | Débito revertido (saga rollback) |
| `publishFailed()` | `transfer.failed` | Cualquier fallo no recuperable |

---

### 5.7 — GATE DE SALIDA: verificar PASO 5

```powershell
# Terminal 1 — transfer-api (debe seguir corriendo del PASO 3)
cd kafka-transfers\transfer-api
mvn spring-boot:run
```

```powershell
# Terminal 2 — validation-service (debe seguir corriendo del PASO 4)
cd kafka-transfers\validation-service
mvn spring-boot:run
```

```powershell
# Terminal 3 — arrancar account-service
cd kafka-transfers\account-service
mvn spring-boot:run
```

Espera en los logs de account-service:
```
Started AccountServiceApplication in X.XXX seconds
```

```powershell
# Terminal 4 — consumers Kafka para observar el flujo completo
docker exec -it kafka /opt/kafka/bin/kafka-console-consumer.sh `
    --topic transfer.debited `
    --from-beginning `
    --bootstrap-server localhost:9092
```

```powershell
# Terminal 5 — consumer de transfer.credited (flujo feliz)
docker exec -it kafka /opt/kafka/bin/kafka-console-consumer.sh `
    --topic transfer.credited `
    --from-beginning `
    --bootstrap-server localhost:9092
```

```powershell
# Terminal 6 — casos de prueba

# ── CASO 1: flujo completo feliz ──────────────────────────────────────────
curl.exe -X POST http://localhost:8080/transfers `
  -H "Content-Type: application/json" `
  -d '{\"fromAccount\":\"ACC-001\",\"toAccount\":\"ACC-002\",\"amount\":100}'
# Secuencia esperada en Kafka:
#   transfer.requested → (validation) → transfer.validated
#   → (debit)   → transfer.debited    ← ver en Terminal 4
#   → (credit)  → transfer.credited   ← ver en Terminal 5
# Saldos esperados en MySQL:
#   ACC-001: 900.00 (era 1000 - 100)
#   ACC-002: 600.00 (era 500 + 100)

# Verificar saldos:
docker exec -it mysql mysql -u lab_user -plab_pass transfers_db `
    -e "SELECT id, owner_name, balance, version FROM accounts;"
```

```powershell
# ── CASO 2: verificar Optimistic Locking (el campo version cambia) ────────
# Después del CASO 1, ejecutar otra transferencia de ACC-001
curl.exe -X POST http://localhost:8080/transfers `
  -H "Content-Type: application/json" `
  -d '{\"fromAccount\":\"ACC-001\",\"toAccount\":\"ACC-003\",\"amount\":50}'
# Verificar que version de ACC-001 incrementó nuevamente:
docker exec -it mysql mysql -u lab_user -plab_pass transfers_db `
    -e "SELECT id, balance, version FROM accounts WHERE id='ACC-001';"
# version debe ser 2 ahora (o mayor si hubo más transferencias)
```

```powershell
# ── CASO 3: verificar idempotencia en débito ──────────────────────────────
# Verificar en processed_events que hay registros de account-service
docker exec -it mysql mysql -u lab_user -plab_pass transfers_db `
    -e "SELECT event_key, service_name, processed_at FROM processed_events WHERE service_name='account-service';"
# Debe mostrar filas con:
#   event_key: <txId>:TRANSFER_VALIDATED  (DebitConsumer)
#   event_key: <txId>:TRANSFER_DEBITED    (CreditConsumer)
```

```powershell
# ── CASO 4: flujo de compensación (rollback del Saga) ─────────────────────
# 1. Detener account-service (Ctrl+C en Terminal 3)
# 2. Editar application.yml y cambiar:
#      simulate-credit-failure: true
# 3. Reiniciar:
cd kafka-transfers\account-service
mvn spring-boot:run

# 4. Abrir consumers adicionales:
docker exec -it kafka /opt/kafka/bin/kafka-console-consumer.sh `
    --topic transfer.compensated `
    --from-beginning `
    --bootstrap-server localhost:9092
docker exec -it kafka /opt/kafka/bin/kafka-console-consumer.sh `
    --topic transfer.failed `
    --from-beginning `
    --bootstrap-server localhost:9092

# 5. Enviar transferencia:
curl.exe -X POST http://localhost:8080/transfers `
  -H "Content-Type: application/json" `
  -d '{\"fromAccount\":\"ACC-002\",\"toAccount\":\"ACC-003\",\"amount\":50}'

# Secuencia esperada:
#   transfer.requested → transfer.validated → transfer.debited
#   → (crédito FALLA) → transfer.compensated (saldo restaurado)
#                     → transfer.failed

# Verificar que el saldo de ACC-002 NO cambió (fue restaurado):
docker exec -it mysql mysql -u lab_user -plab_pass transfers_db `
    -e "SELECT id, balance, version FROM accounts;"
# ACC-002: debe mantener el mismo balance que tenía antes (compensación exitosa)
# Verificar en processed_events:
docker exec -it mysql mysql -u lab_user -plab_pass transfers_db `
    -e "SELECT event_key FROM processed_events WHERE event_key LIKE '%COMPENSATION%';"
# Debe aparecer el registro <txId>:COMPENSATION

# 6. Restaurar: volver simulate-credit-failure: false y reiniciar
```

**Tabla de errores del PASO 5:**

| Síntoma | Causa probable | Solución |
|---------|---------------|----------|
| `SchemaValidationException: missing column: version` | `Account.java` no tiene el campo `version` | Verificar que `@Version private Long version` existe |
| `OptimisticLockingFailureException` en logs frecuentes | Muchas transferencias concurrentes de la misma cuenta | Normal en carga alta; el reintento lo resuelve |
| `transfer.debited` no aparece | `DebitConsumer` no arrancó o group-id incorrecto | Verificar logs de startup, `group-id: account-debit-group` |
| `transfer.credited` no aparece | `CreditConsumer` no arrancó o simulate-credit-failure=true | Verificar application.yml |
| `transfer.compensated` no aparece | `CompensationService` falló o no se activó | Revisar logs de CreditConsumer por errores de compensación |
| Saldo de la cuenta bajó pero no subió la destino | `simulate-credit-failure=true` activo | Cambiar a `false` y reiniciar |
| `NullPointerException` en `publishCompensated` | La compensación retornó null (ya compensado) | Normal si el mensaje llegó dos veces |

✅ **Listo para PASO 6** cuando: el CASO 1 muestra débito y crédito en Kafka, los saldos cambian correctamente en MySQL, y el CASO 4 demuestra que el saldo se restaura cuando simulate-credit-failure=true.

---

## PASO 6 — Status Service

**Objetivo:** El "observador" de la saga. Escucha todos los eventos y mantiene el campo `status` de la tabla `transactions` actualizado en tiempo real. También expone `GET /transfers/{id}/status` para que cualquier cliente pueda consultar el estado sin esperar en la respuesta inicial.

---

### 6.1 — Estructura del módulo (archivos creados en este paso)

```
kafka-transfers/status-service/
├── pom.xml                              ← ya existía desde PASO 2
└── src/main/
    ├── java/com/lab/status/
    │   ├── StatusServiceApplication.java   ← ya existía desde PASO 2
    │   ├── entity/
    │   │   └── Transaction.java         ← mapea la tabla transactions (solo lectura + update de status)
    │   ├── repository/
    │   │   └── TransactionRepository.java  ← findById + save + findByStatus
    │   ├── service/
    │   │   └── StatusService.java       ← updateStatus() + findById() con idempotencia simple
    │   ├── consumer/
    │   │   └── SagaEventConsumer.java   ← @KafkaListener en 5 topics simultáneos
    │   └── controller/
    │       └── StatusController.java    ← GET /transfers/{id}/status (puerto 8083)
    └── resources/
        ├── application.yml              ← actualizado: ddl-auto: validate
        └── db/migration/
            └── V1__create_status_schema.sql   ← ya existía desde PASO 2
```

**¿Por qué status-service tiene un controller si transfer-api ya tiene GET /transfers/{id}?**
transfer-api devuelve el estado que guardó al crear la transacción — siempre `PROCESSING` hasta que status-service actualice la fila. Al tener su propio endpoint, status-service puede ser escalado y desplegado independientemente. En el lab es útil para observar las transiciones en tiempo real desde el servicio responsable de actualizarlas.

---

### 6.2 — Diagrama de transiciones del Saga

```
transfer-api crea la transacción
         │
         ▼
     PROCESSING   ← estado inicial en DB
         │
   transfer.validated
         │
         ▼
     VALIDATED
         │
   transfer.debited
         │
         ▼
      DEBITED
         │
   transfer.credited         transfer.failed (en cualquier punto)
         │                            │
         ▼                            ▼
    COMPLETED ✅                   FAILED ❌
                          (o si hubo compensación)
                                       │
                              transfer.compensated
                                       │
                                       ▼
                                 ROLLED_BACK 🔄
```

**Tabla evento → estado:**

| Evento Kafka recibido | Nuevo `status` en DB |
|-----------------------|----------------------|
| `transfer.validated`  | `VALIDATED`          |
| `transfer.debited`    | `DEBITED`            |
| `transfer.credited`   | `COMPLETED`          |
| `transfer.failed`     | `FAILED`             |
| `transfer.compensated`| `ROLLED_BACK`        |

> **Nota:** `transfer.credited` → `COMPLETED` (no `CREDITED`). El estado `CREDITED` existe en el enum para uso interno del Saga, pero el estado visible al cliente salta directo a `COMPLETED` porque en ese punto la saga terminó exitosamente.

---

### 6.3 — `SagaEventConsumer.java` — un listener para todos los topics

```java
@KafkaListener(
    topics = {
        KafkaTopics.TRANSFER_VALIDATED,   // transfer.validated
        KafkaTopics.TRANSFER_FAILED,      // transfer.failed
        KafkaTopics.TRANSFER_DEBITED,     // transfer.debited
        KafkaTopics.TRANSFER_CREDITED,    // transfer.credited
        KafkaTopics.TRANSFER_COMPENSATED  // transfer.compensated
    },
    groupId = "status-group"
)
public void onSagaEvent(ConsumerRecord<String, Object> record, Acknowledgment ack) {
    Object event = record.value();  // tipo concreto en runtime gracias al header __TypeId__
    
    if      (event instanceof TransferValidatedEvent  e) → VALIDATED
    else if (event instanceof TransferDebitedEvent    e) → DEBITED
    else if (event instanceof TransferCreditedEvent   e) → COMPLETED
    else if (event instanceof TransferCompensatedEvent e) → ROLLED_BACK
    else if (event instanceof TransferFailedEvent      e) → FAILED
    
    ack.acknowledge();
}
```

**¿Cómo funciona `instanceof` con tipos Kafka?**
El `JsonSerializer` (en los producers) agrega automáticamente un header HTTP-style `__TypeId__` en cada mensaje con el nombre completo de la clase (`com.lab.common.event.TransferValidatedEvent`). El `JsonDeserializer` (en status-service) lee ese header y deserializa al tipo correcto. Cuando declaramos `Object` como tipo del parámetro, el objeto en runtime ya es del tipo específico — el `instanceof` simplemente verifica cuál.

**¿Por qué `ConsumerRecord<String, Object>` en lugar de solo `@Payload Object event`?**
`ConsumerRecord` da acceso al topic, partition y offset — información útil para debugging y logging. Con `@Payload` solo tenemos el valor deserializado sin metadatos del mensaje.

---

### 6.4 — `StatusService.java` — idempotencia simple

```java
@Transactional
public void updateStatus(String txId, TransferStatus newStatus, String failureReason) {
    Optional<Transaction> opt = transactionRepository.findById(txId);
    if (opt.isEmpty()) { log.warn(...); return; }  // transacción no existe aún → skip
    
    Transaction tx = opt.get();
    if (tx.getStatus() == newStatus) { return; }   // ya en ese estado → idempotente, no escribir
    
    tx.setStatus(newStatus);
    if (failureReason != null) tx.setFailureReason(failureReason);
    transactionRepository.save(tx);
}
```

**¿Por qué status-service no necesita processed_events como account-service?**
Las actualizaciones de estado son **naturalmente idempotentes**: setear `status = VALIDATED` dos veces produce el mismo resultado que hacerlo una vez. No hay operación destructiva (como un débito) que se pueda duplicar. La única complejidad sería recibir eventos fuera de orden (ej: `COMPLETED` antes que `VALIDATED`), pero Kafka garantiza orden dentro de una partición, y todos los eventos de una misma transferencia tienen la misma clave (`fromAccount`) → misma partición → orden garantizado.

---

### 6.5 — GATE DE SALIDA: verificar PASO 6

```powershell
# Asegúrate que todos los servicios anteriores están corriendo:
# - Docker: kafka + mysql
# - transfer-api (port 8080)
# - validation-service (port 8081)
# - account-service (port 8082)

# Terminal — arrancar status-service
cd kafka-transfers\status-service
mvn spring-boot:run
```

Espera en los logs:
```
Started StatusServiceApplication in X.XXX seconds
```
Si usas `auto.offset.reset: earliest`, en el arranque verás que status-service lee TODOS los eventos pasados y actualiza los estados. En los logs verás mensajes como:
```
📊 Estado actualizado | txId=... PROCESSING → VALIDATED
📊 Estado actualizado | txId=... VALIDATED → DEBITED
📊 Estado actualizado | txId=... DEBITED → COMPLETED
```

```powershell
# ── CASO 1: flujo completo con todos los servicios ─────────────────────────
# (asegúrate que simulate-credit-failure=false en account-service)

curl.exe -X POST http://localhost:8080/transfers `
  -H "Content-Type: application/json" `
  -d '{\"fromAccount\":\"ACC-001\",\"toAccount\":\"ACC-002\",\"amount\":75}'

# Guarda el transactionId de la respuesta
$txId = "<pega el transactionId aquí>"

# Polling del estado (verás las transiciones en tiempo real):
while ($true) {
    $r = curl.exe -s "http://localhost:8083/transfers/$txId/status" | ConvertFrom-Json
    Write-Host "$(Get-Date -Format 'HH:mm:ss') | $($r.status) | $($r.statusMessage)"
    if ($r.status -in @("COMPLETED","FAILED","ROLLED_BACK")) { break }
    Start-Sleep -Seconds 1
}
# Salida esperada:
# 10:30:00 | PROCESSING   | ⏳ En proceso — esperando validación
# 10:30:00 | VALIDATED    | ✔️ Validada — esperando débito
# 10:30:01 | DEBITED      | 💸 Débito aplicado — esperando crédito
# 10:30:01 | COMPLETED    | ✅ Transferencia completada exitosamente
```

```powershell
# También funciona desde transfer-api (port 8080) — comparten la misma tabla:
curl.exe "http://localhost:8080/transfers/$txId"
```

```powershell
# ── CASO 2: verificar estado FAILED ───────────────────────────────────────
curl.exe -X POST http://localhost:8080/transfers `
  -H "Content-Type: application/json" `
  -d '{\"fromAccount\":\"ACC-001\",\"toAccount\":\"ACC-999\",\"amount\":50}'

$txIdFailed = "<pega el transactionId>"
Start-Sleep -Seconds 2
curl.exe "http://localhost:8083/transfers/$txIdFailed/status"
# Esperado: status=FAILED, failureReason="Cuenta destino no encontrada: ACC-999"
```

```powershell
# ── CASO 3: verificar estado ROLLED_BACK ──────────────────────────────────
# Activar fallo de crédito en account-service:
# 1. Editar account-service/application.yml: simulate-credit-failure: true
# 2. Reiniciar account-service

curl.exe -X POST http://localhost:8080/transfers `
  -H "Content-Type: application/json" `
  -d '{\"fromAccount\":\"ACC-002\",\"toAccount\":\"ACC-003\",\"amount\":25}'

$txIdRollback = "<pega el transactionId>"
Start-Sleep -Seconds 3
curl.exe "http://localhost:8083/transfers/$txIdRollback/status"
# Esperado: status=ROLLED_BACK
# failureReason=compensación por fallo en crédito
```

```powershell
# ── CASO 4: verificar en DB directamente ──────────────────────────────────
docker exec -it mysql mysql -u lab_user -plab_pass transfers_db `
    -e "SELECT id, status, failure_reason, updated_at FROM transactions ORDER BY created_at DESC LIMIT 5;"
# Debes ver varias filas con distintos estados
```

**Tabla de errores del PASO 6:**

| Síntoma | Causa probable | Solución |
|---------|---------------|----------|
| `SchemaValidationException` al arrancar | `Transaction.java` tiene campos no presentes en la tabla | Verificar que los campos de la entidad coinciden con el schema SQL |
| El estado siempre queda en `PROCESSING` | status-service no recibe eventos | Verificar `group-id: status-group` y que los topics existen |
| `⚠️ Transacción no encontrada` en los logs | status-service procesó el evento antes que transfer-api insertara la fila | Normal en el primer arranque; en siguientes reinicios ya existe la fila |
| `GET /transfers/{id}/status` → 404 | La transacción aún no fue creada por transfer-api | Esperar o verificar que transfer-api esté corriendo |
| Estado salta de PROCESSING a COMPLETED (sin VALIDATED/DEBITED) | auto.offset.reset: earliest procesa eventos viejos en orden | Normal — status-service reconstruye el estado desde el inicio |
| `JsonParseException: unknown type` | `trusted.packages` no incluye `com.lab.*` | Verificar application.yml |

✅ **Listo para PASO 7** cuando: El polling del estado muestra la secuencia `PROCESSING → VALIDATED → DEBITED → COMPLETED`, el CASO 2 muestra `FAILED` y el CASO 3 muestra `ROLLED_BACK`.

---

## PASO 7 — Simulación de Duplicados

**Objetivo:** Demostrar con evidencia concreta que el sistema es idempotente. La técnica: resetear el offset de un consumer group para que re-lea mensajes que ya procesó, y observar que los servicios los descartan sin ejecutar operaciones duplicadas.

> **¿Por qué offset reset y no el console-producer?**
> El `kafka-console-producer.sh` no puede agregar el header `__TypeId__` que necesita el `JsonDeserializer`. Sin ese header, los consumers lanzan `JsonParseException`. Resetear el offset es más realista: simula exactamente lo que pasa cuando un servicio se reinicia después de un fallo sin haber confirmado los offsets.

---

### 7.1 — Estado inicial: registrar saldos antes del experimento

Arranca todos los servicios si no están corriendo. Luego:

```powershell
# Saldos de partida (ajusta si ya hiciste transferencias anteriores)
docker exec -it mysql mysql -u lab_user -plab_pass transfers_db `
    -e "SELECT id, balance, version FROM accounts;"
# Guarda los valores — los compararás al final para verificar que no cambiaron

# Registros de idempotencia existentes
docker exec -it mysql mysql -u lab_user -plab_pass transfers_db `
    -e "SELECT event_key, service_name FROM processed_events ORDER BY processed_at DESC LIMIT 10;"
```

---

### 7.2 — Ejecutar una transferencia y esperar COMPLETED

```powershell
# Enviar la transferencia
curl.exe -X POST http://localhost:8080/transfers `
  -H "Content-Type: application/json" `
  -d '{\"fromAccount\":\"ACC-001\",\"toAccount\":\"ACC-002\",\"amount\":100}'

# Guarda el transactionId de la respuesta
$txId = "<pega el transactionId aquí>"

# Esperar COMPLETED (polling)
Start-Sleep -Seconds 3
curl.exe "http://localhost:8083/transfers/$txId/status"
# Esperado: "status": "COMPLETED"
```

**Verificar estado en DB antes del experimento:**
```powershell
# Saldos después de la transferencia correcta
docker exec -it mysql mysql -u lab_user -plab_pass transfers_db `
    -e "SELECT id, balance, version FROM accounts;"
# ACC-001: reducido en 100 (ej: 900.00), version incrementó
# ACC-002: aumentado en 100 (ej: 600.00), version incrementó

# Registros de idempotencia generados
docker exec -it mysql mysql -u lab_user -plab_pass transfers_db `
    -e "SELECT event_key, service_name, processed_at FROM processed_events WHERE event_key LIKE '%$txId%';"
# Debe mostrar 2 filas:
#   <txId>:TRANSFER_VALIDATED | account-service
#   <txId>:TRANSFER_DEBITED   | account-service
# Y 1 fila de validation-service:
#   <txId>:TRANSFER_REQUESTED | validation-service   (si la columna tiene ese nombre)
```

---

### 7.3 — Inyectar el duplicado: resetear el offset del consumer de débito

Este es el momento central del experimento. Vamos a forzar que `account-service` vuelva a leer el evento `transfer.validated` que ya procesó.

```powershell
# Paso 1: DETENER account-service (Ctrl+C en la terminal donde corre)
# Es necesario detenerlo porque Kafka no permite resetear offsets
# de un grupo con consumers activos.

# Paso 2: Resetear el offset de account-debit-group al inicio del topic
docker exec -it kafka /opt/kafka/bin/kafka-consumer-groups.sh `
    --bootstrap-server localhost:9092 `
    --group account-debit-group `
    --topic transfer.validated `
    --reset-offsets --to-earliest --execute
# Salida esperada:
# GROUP              TOPIC              PARTITION  NEW-OFFSET
# account-debit-group transfer.validated 0         0

# Paso 3: Reiniciar account-service
cd kafka-transfers\account-service
mvn spring-boot:run
```

---

### 7.4 — Observar la idempotencia en acción

Al reiniciar, `account-service` re-lee **todos** los mensajes de `transfer.validated` desde el offset 0.
Observa los logs inmediatamente:

```
# Lo que VERÁS en los logs de account-service (idempotencia funcionando):
⚠️ Evento duplicado — ignorando débito | key=<txId>:TRANSFER_VALIDATED

# Lo que NO verás (no ocurrirá porque la idempotencia lo bloqueó):
💸 Iniciando débito | account=ACC-001 amount=100
```

```powershell
# Verificar en DB que los saldos NO cambiaron:
docker exec -it mysql mysql -u lab_user -plab_pass transfers_db `
    -e "SELECT id, balance, version FROM accounts;"
# ACC-001: MISMO balance que tenía antes del reset ✅
# ACC-002: MISMO balance que tenía antes del reset ✅
# version: SIN cambio (no hubo UPDATE) ✅

# Verificar que processed_events sigue teniendo solo 1 registro para este txId
docker exec -it mysql mysql -u lab_user -plab_pass transfers_db `
    -e "SELECT COUNT(*) as total FROM processed_events WHERE event_key LIKE '%$txId%';"
# total: 2 (no 4, no 6 — exactamente los mismos que había antes del reset)
```

**Secuencia completa de evidencia:**

| Verificación | Antes del reset | Después del reset | Conclusión |
|---|---|---|---|
| `accounts.balance ACC-001` | X.XX | X.XX (igual) | ✅ No se debitó dos veces |
| `accounts.version ACC-001` | N | N (igual) | ✅ No hubo UPDATE |
| `processed_events` count | 2 | 2 (igual) | ✅ No se insertó registro duplicado |
| Logs account-service | `💸 Iniciando débito` | `⚠️ Evento duplicado` | ✅ Capa 2 de idempotencia actuó |

---

### 7.5 — Segunda prueba: duplicado en validation-service

Para ver la Capa 2 y 3 desde el lado de validation-service:

```powershell
# Detener validation-service
# Resetear su offset:
docker exec -it kafka /opt/kafka/bin/kafka-consumer-groups.sh `
    --bootstrap-server localhost:9092 `
    --group validation-group `
    --topic transfer.requested `
    --reset-offsets --to-earliest --execute

# Reiniciar validation-service
cd kafka-transfers\validation-service
mvn spring-boot:run

# En los logs de validation-service verás:
# ⚠️ Evento duplicado ignorado | key=<txId>:TRANSFER_REQUESTED
# (para cada transferencia ya procesada)

# Verificar que NO se publicaron nuevos eventos en transfer.validated:
docker exec -it kafka /opt/kafka/bin/kafka-consumer-groups.sh `
    --bootstrap-server localhost:9092 `
    --group status-group --describe
# El lag de status-group en transfer.validated debe ser 0 (nada nuevo)
```

✅ **PASO 7 completado** cuando: los saldos no cambian tras el offset reset y los logs muestran `⚠️ Evento duplicado` para los mensajes reentregados.

---

## PASO 8 — Escenario de Rollback (Saga Compensation)

**Objetivo:** Demostrar el mecanismo de compensación del Saga Choreography. El crédito falla (simulado con un flag), y el sistema revierte automáticamente el débito devolviendo el dinero a la cuenta origen. No hay intervención manual — la compensación ocurre via el flujo de eventos.

---

### 8.1 — Entender el flujo de compensación antes de ejecutarlo

```
                 transfer.requested
                        │
              [validation-service]
                        │ transfer.validated
                        │
              [DebitConsumer] ── account-debit-group
                        │
              Débito aplicado en fromAccount
                        │ transfer.debited
                        │
              [CreditConsumer] ── account-credit-group
                        │
              simulate-credit-failure=true
              SimulatedCreditFailureException lanzada
                        │
              ┌─────────▼──────────────────────────┐
              │  compensationService.compensate()  │ ← IN-PROCESS (no via Kafka)
              │  fromAccount.balance += amount     │
              └─────────┬──────────────────────────┘
                        │
              producer.publishCompensated() → transfer.compensated
              producer.publishFailed()      → transfer.failed
              acknowledgment.acknowledge()
                        │
              [status-service] ← escucha ambos topics
                        │
              UPDATE transactions SET status='ROLLED_BACK'
```

---

### 8.2 — Preparar: registrar saldos iniciales

```powershell
docker exec -it mysql mysql -u lab_user -plab_pass transfers_db `
    -e "SELECT id, balance, version FROM accounts;"
# Anota los saldos — los compararás al final para verificar que no cambiaron
```

---

### 8.3 — Activar el fallo simulado y abrir los consumers de Kafka

```powershell
# Paso 1: Detener account-service (Ctrl+C)

# Paso 2: Editar kafka-transfers\account-service\src\main\resources\application.yml
#   Cambiar: simulate-credit-failure: false
#         A: simulate-credit-failure: true

# Paso 3: Abrir consumers Kafka para observar el flujo en tiempo real
# Terminal A — ver el débito ocurrir:
docker exec -it kafka /opt/kafka/bin/kafka-console-consumer.sh `
    --topic transfer.debited --from-beginning `
    --bootstrap-server localhost:9092

# Terminal B — ver la compensación (rollback del débito):
docker exec -it kafka /opt/kafka/bin/kafka-console-consumer.sh `
    --topic transfer.compensated --from-beginning `
    --bootstrap-server localhost:9092

# Terminal C — ver el fallo final:
docker exec -it kafka /opt/kafka/bin/kafka-console-consumer.sh `
    --topic transfer.failed --from-beginning `
    --bootstrap-server localhost:9092

# Paso 4: Reiniciar account-service con el fallo activado
cd kafka-transfers\account-service
mvn spring-boot:run
```

---

### 8.4 — Ejecutar la transferencia que fallará

```powershell
curl.exe -X POST http://localhost:8080/transfers `
  -H "Content-Type: application/json" `
  -d '{\"fromAccount\":\"ACC-001\",\"toAccount\":\"ACC-002\",\"amount\":200}'

$txId = "<pega el transactionId aquí>"
```

**Secuencia esperada en los logs de account-service (en orden cronológico):**
```
📨 TRANSFER_VALIDATED recibido | key=<txId>:TRANSFER_VALIDATED from=ACC-001 to=ACC-002 amount=200
💸 Iniciando débito | account=ACC-001 amount=200
✅ Débito aplicado | account=ACC-001 oldBalance=900 amount=200 newBalance=700
✅ TRANSFER_DEBITED publicado | txId=<txId> remainingBalance=700

📨 TRANSFER_DEBITED recibido | key=<txId>:TRANSFER_DEBITED
⚠️ FALLO SIMULADO activado — el crédito no se aplicará | account=ACC-002
⚠️ Crédito fallido (simulado) — iniciando compensación | txId=<txId>
🔄 Iniciando compensación | txId=<txId> fromAccount=ACC-001 amount=200
✅ Compensación aplicada | account=ACC-001 balanceRestaurado=900
🔄 TRANSFER_COMPENSATED publicado | txId=<txId>
⚠️ TRANSFER_FAILED publicado | txId=<txId>
```

**Eventos visibles en los consumers Kafka abiertos:**
- **Terminal A** (`transfer.debited`): evento con `fromAccount=ACC-001`, `remainingBalance=700`
- **Terminal B** (`transfer.compensated`): evento con `fromAccount=ACC-001`, `restoredBalance=900`
- **Terminal C** (`transfer.failed`): evento con `failedBy=account-service-credit`, `reason=Fallo simulado`

---

### 8.5 — Verificar el resultado final

```powershell
# Esperar 2-3 segundos para que status-service procese los eventos
Start-Sleep -Seconds 3

# Estado final via REST (status-service):
curl.exe "http://localhost:8083/transfers/$txId/status"
# Esperado:
# {
#   "status": "ROLLED_BACK",
#   "statusMessage": "🔄 Débito revertido — dinero devuelto al origen",
#   "failureReason": "Compensación por fallo en crédito: Fallo simulado en el crédito..."
# }

# También disponible en transfer-api (misma DB):
curl.exe "http://localhost:8080/transfers/$txId"
```

```powershell
# Saldos finales — la clave del experimento:
docker exec -it mysql mysql -u lab_user -plab_pass transfers_db `
    -e "SELECT id, balance, version FROM accounts;"
# ACC-001: MISMO balance que antes de la transferencia ✅
#   → El débito fue aplicado (900 → 700) y luego revertido (700 → 900)
# ACC-002: SIN cambio ✅
#   → El crédito nunca ocurrió (la excepción se lanzó antes de tocar la DB)
```

```powershell
# Idempotencia de la compensación:
docker exec -it mysql mysql -u lab_user -plab_pass transfers_db `
    -e "SELECT event_key, service_name FROM processed_events WHERE event_key LIKE '%$txId%';"
# Debe mostrar 3 filas:
#   <txId>:TRANSFER_VALIDATED  | account-service  ← DebitConsumer
#   <txId>:TRANSFER_DEBITED    | account-service  ← CreditConsumer
#   <txId>:COMPENSATION        | account-service  ← CompensationService
```

**Tabla de evidencia del PASO 8:**

| Verificación | Antes | Después | Conclusión |
|---|---|---|---|
| `accounts.balance ACC-001` | 900.00 | 900.00 | ✅ Compensación devolvió el dinero |
| `accounts.balance ACC-002` | X.XX | X.XX | ✅ Crédito nunca se aplicó |
| `transactions.status` | PROCESSING | ROLLED_BACK | ✅ Estado final correcto |
| `processed_events` | N filas | N+3 filas | ✅ 3 operaciones registradas |
| `transfer.compensated` | vacío | 1 evento | ✅ Compensación publicada |
| `transfer.failed` | N eventos | N+1 eventos | ✅ Fallo notificado |

---

### 8.6 — Restaurar el sistema al estado normal

```powershell
# Paso 1: Detener account-service (Ctrl+C)

# Paso 2: Editar application.yml de account-service:
#   Cambiar: simulate-credit-failure: true
#         A: simulate-credit-failure: false

# Paso 3: Reiniciar account-service
cd kafka-transfers\account-service
mvn spring-boot:run

# Paso 4: Verificar que el flujo normal vuelve a funcionar
curl.exe -X POST http://localhost:8080/transfers `
  -H "Content-Type: application/json" `
  -d '{\"fromAccount\":\"ACC-003\",\"toAccount\":\"ACC-001\",\"amount\":50}'
# Debe llegar a COMPLETED en ~3 segundos
```

✅ **PASO 8 completado** cuando: el saldo de `fromAccount` queda igual al inicial (compensación exitosa), el status es `ROLLED_BACK`, y al restaurar `simulate-credit-failure=false` el flujo normal funciona.

---

## PASO 9 — Integración Final y Evidencia

**Objetivo:** Ejecutar el sistema completo de extremo a extremo, capturar evidencia de los 4 escenarios clave y verificar que todo está en orden antes de cerrar el lab.

---

### 9.1 — Lista de arranque del sistema completo

Abre 4 terminales PowerShell. En cada una, ejecuta el servicio correspondiente **en este orden**:

```powershell
# Terminal 1 — transfer-api (Gateway REST)
cd kafka-transfers\transfer-api
mvn spring-boot:run
# Esperar: Started TransferApiApplication — port 8080
```

```powershell
# Terminal 2 — validation-service
cd kafka-transfers\validation-service
mvn spring-boot:run
# Esperar: Started ValidationServiceApplication — port 8081
```

```powershell
# Terminal 3 — account-service (simulate-credit-failure: false)
cd kafka-transfers\account-service
mvn spring-boot:run
# Esperar: Started AccountServiceApplication — port 8082
```

```powershell
# Terminal 4 — status-service
cd kafka-transfers\status-service
mvn spring-boot:run
# Esperar: Started StatusServiceApplication — port 8083
```

> **Nota:** Si es la primera vez que arrancas status-service con `auto.offset.reset: earliest`,
> leerá todos los eventos históricos de Kafka y actualizará los estados en DB. Verás varios
> mensajes `📊 Estado actualizado` en los logs — esto es correcto.

---

### 9.2 — Script de prueba integral (PowerShell)

Copia y pega este bloque completo en una terminal PowerShell. Ejecuta todo de una vez:

```powershell
Write-Host "`n═══════════════════════════════════════════════"
Write-Host "   PRUEBA INTEGRAL — Sistema de Transferencias"
Write-Host "═══════════════════════════════════════════════`n"

# ── ESCENARIO 1: Flujo feliz ──────────────────────────────────────────────
Write-Host "▶ ESCENARIO 1: Transferencia exitosa ACC-001 → ACC-002"
$resp1 = curl.exe -s -X POST http://localhost:8080/transfers `
  -H "Content-Type: application/json" `
  -d '{\"fromAccount\":\"ACC-001\",\"toAccount\":\"ACC-002\",\"amount\":100}' | ConvertFrom-Json
$txId1 = $resp1.transactionId
Write-Host "  transactionId: $txId1"
Write-Host "  status inicial: $($resp1.status)"

Start-Sleep -Seconds 4

$status1 = curl.exe -s "http://localhost:8083/transfers/$txId1/status" | ConvertFrom-Json
Write-Host "  status final: $($status1.status) — $($status1.statusMessage)"
Write-Host ""

# ── ESCENARIO 2: Validación fallida (cuenta inexistente) ──────────────────
Write-Host "▶ ESCENARIO 2: Cuenta destino inexistente (ACC-999)"
$resp2 = curl.exe -s -X POST http://localhost:8080/transfers `
  -H "Content-Type: application/json" `
  -d '{\"fromAccount\":\"ACC-001\",\"toAccount\":\"ACC-999\",\"amount\":50}' | ConvertFrom-Json
$txId2 = $resp2.transactionId

Start-Sleep -Seconds 3
$status2 = curl.exe -s "http://localhost:8083/transfers/$txId2/status" | ConvertFrom-Json
Write-Host "  status: $($status2.status)"
Write-Host "  reason: $($status2.failureReason)"
Write-Host ""

# ── ESCENARIO 3: Misma cuenta origen y destino ────────────────────────────
Write-Host "▶ ESCENARIO 3: Misma cuenta origen y destino"
$resp3 = curl.exe -s -X POST http://localhost:8080/transfers `
  -H "Content-Type: application/json" `
  -d '{\"fromAccount\":\"ACC-002\",\"toAccount\":\"ACC-002\",\"amount\":10}' | ConvertFrom-Json
$txId3 = $resp3.transactionId

Start-Sleep -Seconds 3
$status3 = curl.exe -s "http://localhost:8083/transfers/$txId3/status" | ConvertFrom-Json
Write-Host "  status: $($status3.status)"
Write-Host "  reason: $($status3.failureReason)"
Write-Host ""

# ── RESUMEN DE SALDOS ─────────────────────────────────────────────────────
Write-Host "▶ SALDOS FINALES EN BASE DE DATOS:"
docker exec -it mysql mysql -u lab_user -plab_pass transfers_db `
    -e "SELECT id, owner_name, balance FROM accounts;"
Write-Host ""

# ── RESUMEN DE TRANSACCIONES ──────────────────────────────────────────────
Write-Host "▶ ÚLTIMAS TRANSACCIONES:"
docker exec -it mysql mysql -u lab_user -plab_pass transfers_db `
    -e "SELECT id, status, failure_reason FROM transactions ORDER BY created_at DESC LIMIT 5;"
Write-Host ""

Write-Host "═══════════════════════════════════════════════"
Write-Host "RESULTADOS ESPERADOS:"
Write-Host "  Escenario 1: COMPLETED ✅"
Write-Host "  Escenario 2: FAILED    ❌"
Write-Host "  Escenario 3: FAILED    ❌"
Write-Host "═══════════════════════════════════════════════"
```

---

### 9.3 — Verificar la idempotencia en los registros

```powershell
# Ver todos los registros de idempotencia generados
docker exec -it mysql mysql -u lab_user -plab_pass transfers_db `
    -e "SELECT event_key, service_name, processed_at FROM processed_events ORDER BY processed_at DESC LIMIT 20;"

# Para el Escenario 1 (COMPLETED) deben existir exactamente:
#   <txId1>:TRANSFER_REQUESTED  | validation-service
#   <txId1>:TRANSFER_VALIDATED  | account-service
#   <txId1>:TRANSFER_DEBITED    | account-service
```

---

### 9.4 — Evidencia requerida para completar el lab

Captura pantalla o copia el output de cada comando. Esta es la evidencia del lab:

| # | Evidencia | Comando | Resultado esperado |
|---|-----------|---------|-------------------|
| 1 | Flujo COMPLETED | `GET /transfers/{id}/status` (Escenario 1) | `"status": "COMPLETED"` |
| 2 | Saldos correctos | `SELECT id, balance FROM accounts` | ACC-001 reducido, ACC-002 aumentado |
| 3 | Validación falla | `GET /transfers/{id}/status` (Escenario 2) | `"status": "FAILED"` + reason |
| 4 | Idempotencia actúa | Reset de offset + logs de account-service (PASO 7) | `⚠️ Evento duplicado` sin cambio de saldo |
| 5 | Rollback funciona | `GET /transfers/{id}/status` (PASO 8) | `"status": "ROLLED_BACK"` |
| 6 | Dinero restaurado | `SELECT balance FROM accounts` (PASO 8) | fromAccount sin cambio neto |
| 7 | processed_events | `SELECT * FROM processed_events` | 1 fila por evento, no duplicados |
| 8 | `@Version` funciona | `SELECT version FROM accounts` | Incrementa con cada modificación |

---

### 9.5 — Preguntas de reflexión del lab

Antes de cerrar, responde estas preguntas (puedes consultarlas contra el código que escribiste):

**1. ¿Por qué el consumer usa `enable.auto.commit: false` y `ack-mode: MANUAL_IMMEDIATE`?**
> Para controlar exactamente cuándo se confirma el offset. Si el commit fuera automático, Kafka podría confirmar el mensaje antes de que el servicio lo procese completamente. Si el servicio falla después del commit pero antes de procesar, el mensaje se pierde. Con commit manual, confirmamos solo cuando sabemos que el procesamiento terminó correctamente.

**2. ¿Qué garantía ofrece `@Version` en JPA y en qué caso falla?**
> Garantiza que ningún UPDATE sobreescribe cambios de otro thread: añade `AND version=N` al SQL. Detecta el conflicto, pero **no resuelve la contención** — lanza `OptimisticLockingFailureException` y deja al llamador decidir qué hacer (reintentar, fallar, compensar).

**3. ¿Por qué `REQUIRES_NEW` en `IdempotencyService.tryRegister()`?**
> Para que el INSERT en `processed_events` se commitee en su propia transacción, independientemente de lo que le pase al caller. Si el caller falla después del registro, el registro persiste → el reintento de Kafka detecta duplicado y lo descarta sin reprocesar la operación financiera.

**4. ¿Por qué la compensación es in-process (llamada directa) y no via un topic Kafka?**
> Para simplificar el Saga del lab. En producción con múltiples servicios a compensar, se publicaría un evento `transfer.rollback` para que cada servicio afectado lo consuma y revierta su parte. El tradeoff: la compensación in-process es más simple pero acopla CreditConsumer con CompensationService.

---

## PASO 10 — Bonus

Implementar **solo después** de que todos los pasos 0–9 estén funcionando.

Los tres bonuses implementados en este paso:
- **Bonus A** — Caché Redis de estado en `transfer-api` (read-through cache)
- **Bonus B** — correlationId por MDC en todos los servicios (tracing cross-service)
- **Bonus C** — Verificación de partition key y ordenamiento por cuenta

---

### Bonus A — Redis para caché de estado

#### 10.A.1 — Levantar Redis

Agregar Redis al `docker/docker-compose.yml`:

```yaml
redis:
  image: redis:7-alpine
  container_name: redis
  restart: unless-stopped
  ports:
    - "6379:6379"
  command: redis-server --save "" --appendonly no --maxmemory 64mb --maxmemory-policy allkeys-lru
  healthcheck:
    test: ["CMD", "redis-cli", "ping"]
    interval: 10s
    timeout: 5s
    retries: 5
    start_period: 10s
  networks:
    - transfers-net
```

```powershell
cd docker
docker compose up -d redis
docker compose ps   # redis debe estar "healthy"
```

#### 10.A.2 — Dependencia Maven

En `transfer-api/pom.xml`:

```xml
<dependency>
    <groupId>org.springframework.boot</groupId>
    <artifactId>spring-boot-starter-data-redis</artifactId>
</dependency>
```

#### 10.A.3 — Conexión en application.yml

En `transfer-api/src/main/resources/application.yml`:

```yaml
spring:
  data:
    redis:
      host: localhost
      port: 6379
      timeout: 2000ms
```

#### 10.A.4 — Configuración del RedisTemplate

**Archivo nuevo:** `transfer-api/src/main/java/com/lab/api/cache/RedisConfig.java`

```java
@Configuration
public class RedisConfig {

    @Bean
    public RedisTemplate<String, TransferResponse> redisTemplate(RedisConnectionFactory cf) {
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.registerModule(new JavaTimeModule());
        objectMapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

        Jackson2JsonRedisSerializer<TransferResponse> valueSerializer =
                new Jackson2JsonRedisSerializer<>(objectMapper, TransferResponse.class);

        RedisTemplate<String, TransferResponse> template = new RedisTemplate<>();
        template.setConnectionFactory(cf);
        template.setKeySerializer(new StringRedisSerializer());     // keys legibles
        template.setHashKeySerializer(new StringRedisSerializer());
        template.setValueSerializer(valueSerializer);               // values como JSON
        template.setHashValueSerializer(valueSerializer);
        template.afterPropertiesSet();
        return template;
    }
}
```

> **¿Por qué configurar el RedisTemplate manualmente?**  
> El auto-configurado usa `JdkSerializationRedisSerializer` → keys ilegibles en redis-cli y valores binarios frágiles de versionar. Con esta config, en redis-cli verás `transfer:f47a-...` como key y JSON limpio como value.

#### 10.A.5 — TransferCacheService

**Archivo nuevo:** `transfer-api/src/main/java/com/lab/api/cache/TransferCacheService.java`

```java
@Service
@RequiredArgsConstructor
@Slf4j
public class TransferCacheService {

    private final RedisTemplate<String, TransferResponse> redisTemplate;
    private static final String KEY_PREFIX = "transfer:";

    /** Estados terminales → TTL largo (no cambiarán más) */
    private static final Duration TTL_FINAL = Duration.ofMinutes(10);

    /** Estados intermedios → TTL corto (el Saga sigue avanzando) */
    private static final Duration TTL_INTERMEDIATE = Duration.ofSeconds(5);

    public Optional<TransferResponse> get(String transactionId) {
        try {
            TransferResponse cached = redisTemplate.opsForValue().get(KEY_PREFIX + transactionId);
            if (cached != null) { log.debug("✅ Cache HIT | txId={}", transactionId); return Optional.of(cached); }
            log.debug("❌ Cache MISS | txId={}", transactionId);
            return Optional.empty();
        } catch (Exception e) {
            log.warn("⚠️ Redis no disponible para GET | txId={}", transactionId);
            return Optional.empty();    // degradación elegante → cae a MySQL
        }
    }

    public void put(String transactionId, TransferResponse response) {
        Duration ttl = isFinalState(response.getStatus()) ? TTL_FINAL : TTL_INTERMEDIATE;
        try {
            redisTemplate.opsForValue().set(KEY_PREFIX + transactionId, response, ttl);
            log.debug("💾 Cache WRITE | txId={} status={} ttl={}s", transactionId, response.getStatus(), ttl.getSeconds());
        } catch (Exception e) {
            log.warn("⚠️ Redis no disponible para WRITE | txId={}", transactionId);
        }
    }

    private boolean isFinalState(TransferStatus status) {
        return switch (status) {
            case COMPLETED, FAILED, ROLLED_BACK -> true;
            case PROCESSING, VALIDATED, DEBITED, CREDITED -> false;
        };
    }
}
```

**TTL diferenciado — por qué:**

| Estado | Tipo | TTL | Razón |
|--------|------|-----|-------|
| COMPLETED, FAILED, ROLLED_BACK | Final | 10 min | El estado nunca cambiará. Cacheable mucho tiempo. |
| PROCESSING, VALIDATED, DEBITED | Intermedio | 5 seg | El Saga avanza pronto. TTL largo causaría stale reads. |

#### 10.A.6 — Integrar en TransferService

En `getStatus()`, aplicar el patrón **read-through cache**:

```java
@Transactional(readOnly = true)
public TransferResponse getStatus(String transactionId) {

    // 1. Consultar caché Redis (si Redis está caído → Optional.empty())
    var cached = cacheService.get(transactionId);
    if (cached.isPresent()) return cached.get();

    // 2. Cache miss → consultar MySQL
    Transaction transaction = transactionRepository
            .findById(transactionId)
            .orElseThrow(() -> new TransactionNotFoundException("Transferencia no encontrada: " + transactionId));

    TransferResponse response = TransferResponse.builder()
            .transactionId(transaction.getId())
            /* ... resto de campos ... */
            .build();

    // 3. Guardar en Redis para las próximas consultas
    cacheService.put(transactionId, response);
    return response;
}
```

#### 10.A.7 — Verificar la caché con redis-cli

```powershell
# Hacer una transferencia y esperar a que esté COMPLETED:
$resp = curl.exe -s -X POST http://localhost:8080/transfers `
  -H "Content-Type: application/json" `
  -d '{"fromAccount":"ACC-001","toAccount":"ACC-002","amount":50}' | ConvertFrom-Json

$txId = $resp.transactionId

# Esperar ~3 segundos para que el Saga complete
Start-Sleep -Seconds 4

# Primera consulta → MISS → MySQL → escribe en Redis
curl.exe "http://localhost:8080/transfers/$txId"

# Verificar en redis-cli que la key existe:
docker exec -it redis redis-cli GET "transfer:$txId"
# Debe mostrar el JSON completo con status=COMPLETED

# Segunda consulta → HIT → Redis (sin tocar MySQL)
curl.exe "http://localhost:8080/transfers/$txId"
# En los logs de transfer-api: "✅ Cache HIT | txId=..."

# Ver el TTL restante:
docker exec -it redis redis-cli TTL "transfer:$txId"
# ~599 segundos (10 minutos menos lo que pasó)
```

---

### Bonus B — correlationId via MDC

#### ¿Por qué MDC?

Sin MDC, los logs de múltiples transferencias concurrentes se mezclan:
```
INFO  c.l.v.s.ValidationService - Validando transferencia
INFO  c.l.a.c.DebitConsumer    - Debitando 100.00 de ACC-001
INFO  c.l.v.s.ValidationService - Validando transferencia   ← ¿es la misma? ¿otra?
```

Con MDC y el patrón `[txId=%X{txId:-}]` en logback:
```
INFO  [txId=f47a-...] c.l.v.s.ValidationService - Validando transferencia
INFO  [txId=f47a-...] c.l.a.c.DebitConsumer    - Debitando 100.00 de ACC-001
INFO  [txId=9a3c-...] c.l.v.s.ValidationService - Validando transferencia   ← OTRA transferencia
```

Ahora puedes filtrar por txId para trazar una transferencia completa en todos los servicios.

#### 10.B.1 — Interfaz SagaEvent (common)

**Archivo nuevo:** `common/src/main/java/com/lab/common/event/SagaEvent.java`

```java
public interface SagaEvent {
    String getTransactionId();
    String getFromAccount();
}
```

Todos los eventos del Saga implementan esta interfaz:
```java
// Antes:
public class TransferValidatedEvent {

// Después:
public class TransferValidatedEvent implements SagaEvent {
```

> **¿Para qué sirve la interfaz?**  
> En `SagaEventConsumer` (status-service), que recibe `Object` y maneja 5 tipos de eventos, permite extraer el txId de forma polimórfica sin encadenar instanceof:  
> `if (event instanceof SagaEvent se) { MDC.put("txId", se.getTransactionId()); }`

#### 10.B.2 — logback-spring.xml (×4 servicios)

Crear en `src/main/resources/` de cada servicio:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<configuration>
    <conversionRule conversionWord="clr"
                    converterClass="org.springframework.boot.logging.logback.ColorConverter"/>
    <conversionRule conversionWord="wex"
                    converterClass="org.springframework.boot.logging.logback.WhitespaceThrowableProxyConverter"/>

    <!-- %X{txId:-} → muestra el valor del MDC "txId", o "-" si no está presente -->
    <property name="CONSOLE_LOG_PATTERN"
              value="%clr(%d{HH:mm:ss.SSS}){faint} %clr([%15.15t]){faint} %clr(%-5p){highlight} %clr([txId=%X{txId:-}]){cyan} %clr(%-36.36logger{36}){blue} %clr(-){faint} %m%n%wex"/>

    <appender name="CONSOLE" class="ch.qos.logback.core.ConsoleAppender">
        <encoder>
            <pattern>${CONSOLE_LOG_PATTERN}</pattern>
            <charset>UTF-8</charset>
        </encoder>
    </appender>

    <logger name="com.lab" level="DEBUG"/>
    <logger name="org.springframework.kafka" level="INFO"/>
    <logger name="org.flywaydb" level="INFO"/>

    <root level="INFO">
        <appender-ref ref="CONSOLE"/>
    </root>
</configuration>
```

#### 10.B.3 — MDC en los 4 consumers

**Patrón aplicado en todos los consumers:**

```java
// Al inicio del método @KafkaListener:
MDC.put("txId", event.getTransactionId());

try {
    // ... procesamiento normal ...
} finally {
    // CRÍTICO: limpiar el MDC → el hilo se reutiliza para el siguiente mensaje
    MDC.remove("txId");
}
```

**Consumers actualizados:**
- `validation-service` → `TransferRequestedConsumer.onTransferRequested()`
- `account-service` → `DebitConsumer.onTransferValidated()`
- `account-service` → `CreditConsumer.onTransferDebited()`
- `status-service` → `SagaEventConsumer.onSagaEvent()` (polimórfico via SagaEvent)

> **¿Por qué `finally` y no al final del try?**  
> Si hay una excepción, el código después del `try` no se ejecuta. El `finally` garantiza que el MDC siempre se limpia, incluso en caso de error.

#### 10.B.4 — Verificar correlationId en logs

```powershell
# Hacer una transferencia:
curl.exe -X POST http://localhost:8080/transfers `
  -H "Content-Type: application/json" `
  -d '{"fromAccount":"ACC-001","toAccount":"ACC-002","amount":100}'

# Observar los logs de los 4 servicios.
# En validation-service verás:
#   10:23:41.500 [kafka-consumer-1] INFO  [txId=f47a-...] c.l.v.c.TransferRequestedConsumer - 📨 Evento recibido

# En account-service verás EL MISMO txId:
#   10:23:42.100 [account-debit-1]  INFO  [txId=f47a-...] c.l.a.c.DebitConsumer - 📨 TRANSFER_VALIDATED recibido
#   10:23:42.250 [account-credit-1] INFO  [txId=f47a-...] c.l.a.c.CreditConsumer - 🎉 Saga completada

# En status-service verás EL MISMO txId:
#   10:23:42.300 [kafka-status-1]   INFO  [txId=f47a-...] c.l.s.c.SagaEventConsumer - 📨 Evento recibido
```

---

### Bonus C — Partition key y evidencia de ordenamiento

#### 10.C.1 — Enviar 10 transferencias desde la misma cuenta

```powershell
for ($i = 1; $i -le 10; $i++) {
    curl.exe -s -X POST http://localhost:8080/transfers `
      -H "Content-Type: application/json" `
      -d "{`"fromAccount`":`"ACC-001`",`"toAccount`":`"ACC-002`",`"amount`":$i}" | Out-Null
    Start-Sleep -Milliseconds 300
}
```

#### 10.C.2 — Verificar distribución por particiones

```powershell
# Ver el offset actual de cada partición (cuántos mensajes hay):
docker exec -it kafka /opt/kafka/bin/kafka-run-class.sh `
    kafka.tools.GetOffsetShell `
    --bootstrap-server localhost:9092 `
    --topic transfer.requested

# Resultado esperado (el número de partición con ACC-001 tendrá más mensajes):
# transfer.requested:0:X   ← pocas o ninguna transferencia de ACC-001
# transfer.requested:1:10  ← TODAS las de ACC-001 (si hash(ACC-001) mod 3 = 1)
# transfer.requested:2:Y   ← pocas o ninguna transferencia de ACC-001
```

#### 10.C.3 — Ver el contenido de la partición que recibió ACC-001

```powershell
# Determinar a qué partición va ACC-001:
# Kafka usa: murmur2(fromAccount) mod numPartitions
# Con "ACC-001" y 3 particiones, la partición exacta depende del hash.
# Verificar leyendo cada partición:

docker exec -it kafka /opt/kafka/bin/kafka-console-consumer.sh `
    --bootstrap-server localhost:9092 `
    --topic transfer.requested `
    --partition 0 --from-beginning --max-messages 5

# Repetir para partition 1 y 2 hasta encontrar la que tiene ACC-001.
# TODOS los mensajes de ACC-001 deben estar en LA MISMA partición.
```

> **¿Por qué importa la partition key?**  
> Kafka garantiza ORDEN FIFO **dentro de una partición**. Si las transferencias de `ACC-001` van a particiones distintas, podrían procesarse fuera de orden:  
> - Transfer #1: débito 100 → saldo 900  
> - Transfer #2: débito 50  → saldo 850  
> Si se procesan en orden inverso: débito 50 primero → 950, luego 100 → 850. El resultado es el mismo aquí, pero en un sistema más complejo el orden puede importar (ej: verificación de saldo con límite diario).

---

### GATE DE SALIDA — PASO 10

Antes de marcar PASO 10 como completo, verifica:

| Verificación | Evidencia esperada |
|---|---|
| Redis levantado | `docker compose ps` → redis `healthy` |
| Cache HIT visible | Log `✅ Cache HIT` en segunda consulta de GET /transfers/{id} |
| Key en Redis | `redis-cli GET "transfer:<txId>"` devuelve JSON |
| TTL diferenciado | `redis-cli TTL "transfer:<txId>"` devuelve ~600 para COMPLETED, ~5 para PROCESSING |
| txId en logs | Logs de validación y account-service muestran el mismo `[txId=f47a-...]` |
| MDC limpiado | No aparece txId de mensaje anterior en logs del siguiente mensaje |
| Partition key | Las 10 transferencias de ACC-001 están en la misma partición |

---

## 📌 Referencia rápida — Puertos y responsabilidades

| Servicio | Puerto | Escucha topics | Publica topics |
|----------|--------|----------------|----------------|
| transfer-api | 8080 | — | `transfer.requested` |
| validation-service | 8081 | `transfer.requested` | `transfer.validated`, `transfer.failed` |
| account-service | 8082 | `transfer.validated` (DebitConsumer), `transfer.debited` (CreditConsumer) | `transfer.debited`, `transfer.credited`, `transfer.compensated`, `transfer.failed` |
| status-service | 8083 | `transfer.validated`, `transfer.debited`, `transfer.credited`, `transfer.failed`, `transfer.compensated` | — |

## 📌 Referencia rápida — Consumer Group IDs

| Servicio | Consumer | group.id |
|----------|----------|----------|
| validation-service | TransferRequestedConsumer | `validation-group` |
| account-service | DebitConsumer | `account-debit-group` |
| account-service | CreditConsumer | `account-credit-group` |
| status-service | SagaEventConsumer | `status-group` |

> **¿Por qué account-service tiene dos group IDs distintos?**  
> DebitConsumer escucha `transfer.validated` y CreditConsumer escucha `transfer.debited`. Son topics distintos y responsabilidades distintas, pero el mismo proceso JVM. Cada consumer group permite que cada consumer reciba **todos** los mensajes de su topic de forma independiente.

## 📌 Preguntas de reflexión — Guía de respuestas

Estas preguntas son parte del entregable. Responder en el `README.md`:

1. **¿Qué pasa si el consumer cae después del débito pero antes del commit?**  
   → Al reiniciar, Kafka reentrega el mensaje desde el último offset confirmado → el consumer intenta insertar en `processed_events` → ya existe → ignora → hace commit. Sin pérdida, sin doble cargo.

2. **¿Dónde es mejor validar idempotencia: DB o memoria?**  
   → DB siempre, por dos razones: (a) los microservicios pueden tener múltiples instancias — la memoria no es compartida; (b) un reinicio limpia la memoria — el DB persiste.

3. **¿Cómo evitar condiciones de carrera en cuentas?**  
   → `@Version` + Optimistic Locking en la entidad `Account`. JPA agrega `AND version = N` al UPDATE. Si el número de filas afectadas es 0 → `OptimisticLockException` → retry.

4. **¿Qué pasa si Kafka reintenta después de 5 minutos?**  
   → La idempotencia en `processed_events` lo maneja igual: 5 minutos o 5 días después, si la key ya existe → evento ignorado. No hay ventana de tiempo — la protección es permanente mientras el registro esté en DB.
