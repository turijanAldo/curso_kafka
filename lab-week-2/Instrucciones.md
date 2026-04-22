## 🔍 FASE 1 — ENTENDIMIENTO DEL PROBLEMA

### El problema real detrás del laboratorio

Esto no es solo un CRUD con Kafka. El lab replica **tres problemas que existen en la banca real**:

**Problema 1 — Confiabilidad de red:**
Kafka garantiza `at-least-once`. Un evento `transfer.debited` puede llegar 2 veces al consumer si el broker reinicia justo después de escribir pero antes de confirmar el offset. Sin protección → doble cargo → desastre legal.

**Problema 2 — Transacciones distribuidas sin 2PC:**
No existe una transacción global entre el servicio de débito y el de crédito. Si el crédito falla DESPUÉS del débito exitoso, la cuenta A quedó con saldo reducido pero la cuenta B nunca recibió nada → dinero desaparecido.

**Problema 3 — UX asíncrona:**
El cliente HTTP no puede quedarse esperando indefinidamente. El endpoint responde `PROCESSING` inmediatamente y el cliente hace polling para saber cuándo terminó.

### ¿Qué conceptos de Kafka se ponen a prueba?

| Concepto Kafka | Dónde aparece en el lab |
|----------------|------------------------|
| At-least-once delivery | Origen del problema de duplicados |
| Partition key | `fromAccount` como key → orden garantizado por cuenta |
| Consumer groups | Cada microservicio tiene su propio grupo |
| Manual commit | Controlar cuándo confirmar offset (después de procesar) |
| Dead Letter Queue | Eventos que fallaron N veces van a `transfer.dlq` |
| Event-driven Saga | La coreografía entre servicios vía topics |

---

## 🔍 FASE 2 — ANÁLISIS DE REQUERIMIENTOS

### 2.1 Requerimientos Funcionales (RF)

| ID | Requerimiento | Prioridad |
|----|---------------|-----------|
| RF-01 | `POST /transfer` crea una transferencia y retorna `transactionId + PROCESSING` inmediatamente | MUST |
| RF-02 | `GET /status/{id}` retorna el estado actual de la transacción | MUST |
| RF-03 | El sistema procesa la transferencia de forma asíncrona a través de eventos Kafka | MUST |
| RF-04 | Si el mismo evento llega dos veces, solo se procesa una vez (idempotencia) | MUST |
| RF-05 | Si el crédito falla, el débito es revertido automáticamente (Saga rollback) | MUST |
| RF-06 | Los estados posibles son: `PROCESSING → VALIDATED → DEBITED → CREDITED → COMPLETED / FAILED / ROLLED_BACK` | MUST |
| RF-07 | Simular duplicado de `transfer.debited` y comprobar que el saldo no cambia doble | MUST |
| RF-08 | El polling del cliente muestra la transición de estados en tiempo real | MUST |
| RF-09 | Cache Redis para consultas de estado frecuentes | BONUS |
| RF-10 | Backoff exponencial en reintentos de polling | BONUS |

### 2.2 Requerimientos No Funcionales (RNF)

| ID | Requerimiento | Decisión técnica |
|----|---------------|-----------------|
| RNF-01 | Cada microservicio debe ser independiente y desplegable por separado | Maven Multi-Module |
| RNF-02 | El sistema debe levantarse completo con un solo `docker-compose up` | Docker Compose |
| RNF-03 | La idempotencia debe ser enforced a nivel de base de datos, no solo en memoria | `UNIQUE CONSTRAINT` en DB |
| RNF-04 | Las condiciones de carrera en saldos deben prevenirse | Optimistic Locking (`@Version`) |
| RNF-05 | El `transactionId` debe ser correlacionable en todos los logs | `correlationId` en headers Kafka |
| RNF-06 | No debe haber acoplamiento directo entre microservicios | Solo se comunican vía Kafka |

### 2.3 Requerimientos de Datos

**Entidades principales:**

```
Account:          accountId, balance, version (optimistic lock)
Transaction:      transactionId, fromAccount, toAccount, amount, status, timestamps
ProcessedEvent:   eventKey (transactionId:eventType), processedAt
```

**Reglas de negocio:**
- `amount > 0`
- `fromAccount ≠ toAccount`
- Balance de cuenta origen ≥ amount en el momento del débito
- Un `transactionId` nunca puede procesar el mismo tipo de evento dos veces

### 2.4 Contratos de Eventos Kafka

Todos los eventos comparten esta estructura base:

```
{
  "transactionId": "tx-UUID",
  "fromAccount":   "A",
  "toAccount":     "B",
  "amount":        100.00,
  "eventType":     "transfer.created | transfer.validated | ...",
  "timestamp":     "ISO-8601",
  "correlationId": "UUID para tracing",
  "errorReason":   null | "INSUFFICIENT_FUNDS | ACCOUNT_NOT_FOUND | ..."
}
```

**Key de partición Kafka:** `fromAccount` → todos los eventos de la misma cuenta origen van a la misma partición → orden garantizado por cuenta.

### 2.5 Topics Kafka a crear

| Topic | Productor | Consumidor(es) | Particiones |
|-------|-----------|----------------|-------------|
| `transfer.created` | API Gateway | Validation Service | 3 |
| `transfer.validated` | Validation Service | Account Service | 3 |
| `transfer.debited` | Account Service | Account Service (CREDIT) | 3 |
| `transfer.credited` | Account Service | Status Service | 3 |
| `transfer.completed` | Status Service | Status Service (update DB) | 3 |
| `transfer.failed` | Cualquier servicio | Status Service | 3 |
| `transfer.rollback.debit` | Account Service | Account Service (compensación) | 3 |
| `transfer.dlq` | Error handlers | Monitoreo / alertas | 1 |

---

## 🔍 FASE 3 — DECISIONES DE ARQUITECTURA

### 3.1 Patrón Saga: Coreografía vs. Orquestación

**Decisión: Coreografía (Choreography-based Saga)**

- ✅ No hay punto único de fallo (no hay orquestador central)
- ✅ Cada servicio es 100% autónomo
- ✅ Kafka es el mecanismo de coordinación natural
- ⚠️ Trade-off: más difícil de trazar el flujo completo → compensamos con `correlationId`

```
API GW ──[transfer.created]──► Validation ──[transfer.validated]──► Account(DEBIT)
                                                                          │
Account(ROLLBACK) ◄──[transfer.rollback.debit]── Account(CREDIT) ◄──[transfer.debited]
                                                       │
                                              [transfer.credited]──► Status Service
```

### 3.2 Estrategia de Idempotencia: 3 capas

```
CAPA 1 — Kafka Producer
  └─ enable.idempotence=true (evita duplicados producidos por el producer)

CAPA 2 — Application Layer
  └─ Antes de procesar: check si transactionId:eventType ya existe en processed_events

CAPA 3 — Database Layer
  └─ UNIQUE CONSTRAINT en processed_events.event_key
  └─ Si el INSERT falla por duplicate key → evento ya procesado → ignorar
```

La capa 3 es la defensa crítica: incluso con condición de carrera entre threads, el UNIQUE CONSTRAINT del DB garantiza que solo un proceso gana el INSERT.

### 3.3 Prevención de condición de carrera en saldos

**Problema:** Dos hilos intentan debitar la misma cuenta simultáneamente.

**Decisión: Optimistic Locking con `@Version` de JPA**

```
Account {
  balance: 1000,
  version: 5
}

Hilo 1: UPDATE accounts SET balance=900, version=6 WHERE account_id='A' AND version=5 → OK
Hilo 2: UPDATE accounts SET balance=900, version=6 WHERE account_id='A' AND version=5 → 0 rows → retry
```

Si version no coincide → `OptimisticLockException` → el consumer hace retry → el segundo intento lee el balance ya descontado → puede rechazar por saldo insuficiente → falla correctamente.

### 3.4 Estructura del proyecto (Maven Multi-Module)

```
kafka-transfers/                    ← Parent POM
├── docker/
│   ├── docker-compose.yml
│   └── init-db.sql
├── common/                         ← DTOs, eventos, constantes compartidas
│   └── src/.../TransferEvent.java
│   └── src/.../TransferStatus.java
│   └── src/.../KafkaTopics.java
├── transfer-api/                   ← API Gateway (puerto 8080)
│   └── POST /transfer
│   └── GET /status/{id}
├── validation-service/             ← Validación (puerto 8081)
├── account-service/                ← Débito + Crédito + Rollback (puerto 8082)
└── status-service/                 ← Tracker de estado (puerto 8083)
```

---

## 🔍 FASE 4 — PLAN DE CONSTRUCCIÓN (PASO A PASO)

### PASO 0 — Preparación del entorno (Pre-coding)
**Objetivo:** Todo el equipo puede levantar la infraestructura antes de escribir una línea de código.

Actividades:
- Definir versiones exactas: Java 17, Spring Boot 3.x, Kafka 4.x, MySQL 8.x
- Crear el repositorio con estructura de carpetas vacía
- Escribir el `docker-compose.yml` base con Kafka KRaft + MySQL
- Verificar que `docker-compose up` levanta todo sin errores
- Crear los topics manualmente con `kafka-topics.sh` y verificar con `--list`
- Insertar datos de prueba en MySQL: cuentas A (1000) y B (500)

**Criterio de éxito:** `docker-compose up` verde, topics existentes, cuentas en DB.

---

### PASO 1 — Módulo Common (Contratos compartidos)
**Objetivo:** Definir los contratos de datos que usarán TODOS los servicios.

Actividades:
- Crear `TransferEvent` (el objeto que viaja en Kafka)
- Crear `TransferStatus` enum: `PROCESSING, VALIDATED, DEBITED, CREDITED, COMPLETED, FAILED, ROLLED_BACK`
- Crear `KafkaTopics` con constantes de nombres de topics
- Configurar serialización JSON (Jackson)
- Este módulo NO tiene Spring Boot — es puro Java con dependencias mínimas

**Criterio de éxito:** El módulo compila y otros módulos pueden depender de él vía Maven.

---

### PASO 2 — Esquema de Base de Datos
**Objetivo:** Tener el esquema completo antes de escribir código de servicios.

Actividades:
- Tabla `accounts`: `account_id, balance DECIMAL(15,2), version BIGINT`
- Tabla `transactions`: `transaction_id, from_account, to_account, amount, status, created_at, updated_at`
- Tabla `processed_events`: `event_key VARCHAR(100) PRIMARY KEY, processed_at TIMESTAMP`
  - `event_key = transactionId + ":" + eventType` (ej: `"tx-abc:transfer.debited"`)
  - El `PRIMARY KEY` es el UNIQUE CONSTRAINT que hace la magia de idempotencia
- Script de datos iniciales: cuentas A=1000, B=500
- Integrar Flyway para versionado del esquema

**Criterio de éxito:** `docker-compose up` aplica las migraciones automáticamente.

---

### PASO 3 — Transfer API (Gateway)
**Objetivo:** El primer punto de contacto con el cliente funciona.

Actividades:
- `POST /transfer`: validación de input (amount > 0, cuentas distintas), generar UUID como `transactionId`, guardar en `transactions` con status `PROCESSING`, publicar `transfer.created` en Kafka, retornar `{transactionId, status: "PROCESSING"}`
- `GET /status/{id}`: consultar tabla `transactions` → retornar estado actual
- Configurar Kafka Producer: `acks=all`, `enable.idempotence=true`, `key=fromAccount`
- Manejo de errores: 400 si input inválido, 404 si transactionId no existe

**Criterio de éxito:**
```
POST /transfer → 200 con transactionId
GET /status/{transactionId} → PROCESSING
kafka-console-consumer en transfer.created → ve el evento
```

---

### PASO 4 — Validation Service
**Objetivo:** El primer consumer del flujo procesa y avanza la saga.

Actividades:
- Consumer de `transfer.created`: deserializar evento, verificar que ambas cuentas existen en DB, verificar que `fromAccount.balance >= amount`
- Si válido: publicar `transfer.validated`
- Si inválido: publicar `transfer.failed` con `errorReason`
- `enable.auto.commit=false` + `commitSync()` después de procesar y publicar el siguiente evento
- Primer test de idempotencia básica: registrar en `processed_events` antes de procesar

**Criterio de éxito:**
```
Enviar POST /transfer con cuenta inexistente → transfer.failed en Kafka
Enviar POST /transfer válido → transfer.validated en Kafka
GET /status → sigue en PROCESSING (el status-service no existe aún)
```

---

### PASO 5 — Account Service (la pieza más crítica)
**Objetivo:** El corazón del sistema. Aquí vive toda la lógica de idempotencia y el Saga.

**Sub-paso 5a — DEBIT Handler:**
- Consumer de `transfer.validated`
- **Antes de procesar:** intentar INSERT en `processed_events` con key `tx-123:transfer.validated`
  - Si falla por duplicate key → ya procesado → return sin hacer nada → commitSync()
- Hacer SELECT ... FOR UPDATE en cuenta origen (o usar optimistic locking `@Version`)
- Verificar balance suficiente
- Reducir balance
- Publicar `transfer.debited`

**Sub-paso 5b — CREDIT Handler:**
- Consumer de `transfer.debited`
- Misma lógica de idempotencia con key `tx-123:transfer.debited`
- Aumentar balance de cuenta destino
- Publicar `transfer.credited`
- **Punto de fallo intencional:** Flag de configuración `SIMULATE_CREDIT_FAILURE=true` en `application.yml`
  - Si activado: lanzar excepción → publicar `transfer.failed` + `transfer.rollback.debit`

**Sub-paso 5c — ROLLBACK Handler:**
- Consumer de `transfer.rollback.debit`
- Restaurar el balance de la cuenta origen (amount de vuelta)
- Idempotencia: key `tx-123:transfer.rollback.debit`
- Publicar `transfer.failed` con razón `CREDIT_FAILED_DEBIT_ROLLED_BACK`

**Criterio de éxito:**
```
Flujo normal: account A: 1000 → 900, account B: 500 → 600
Flujo con SIMULATE_CREDIT_FAILURE: account A: 1000 → 900 → 1000 (rollback)
Flujo duplicado: enviar transfer.debited 2 veces → account A: 900 (no 800)
```

---

### PASO 6 — Status Service
**Objetivo:** Trazabilidad completa de la saga en tiempo real.

Actividades:
- Consumer de TODOS los topics de eventos (`transfer.*`)
- Por cada evento: actualizar `transactions.status` en DB según la máquina de estados
- Máquina de estados simple:
  ```
  transfer.created    → PROCESSING
  transfer.validated  → VALIDATED
  transfer.debited    → DEBITED
  transfer.credited   → CREDITED
  transfer.completed  → COMPLETED
  transfer.failed     → FAILED
  transfer.rollback.* → ROLLED_BACK
  ```
- Publicar `transfer.completed` cuando reciba `transfer.credited`
- Ahora `GET /status/{id}` del API Gateway empieza a mostrar transiciones reales

**Criterio de éxito:**
```
Polling cada segundo → ver PROCESSING → VALIDATED → DEBITED → COMPLETED
```

---

### PASO 7 — Simulación de Duplicados (El momento del lab)
**Objetivo:** Demostrar que el sistema es idempotente con evidencia visual.

Actividades:
- Crear un script de prueba (o endpoint de testing) que publique manualmente `transfer.debited` por segunda vez con el mismo `transactionId`
- Ejecutar y verificar:
  - Logs: `[IDEMPOTENCY] Evento ya procesado: tx-123:transfer.debited — ignorando`
  - DB: balance de cuenta A sigue en 900, no en 800
  - `processed_events`: solo una fila con esa key
- Documentar el antes/después con evidencia en consola

**Criterio de éxito:**
```
Antes: balance A = 1000
Primera transfer.debited → balance A = 900 ✅
Segunda transfer.debited (duplicado) → balance A = 900 (sin cambio) ✅
Log muestra "evento ignorado" ✅
```

---

### PASO 8 — Escenario de Rollback (Saga Compensation)
**Objetivo:** Demostrar que el dinero nunca desaparece.

Actividades:
- Activar `SIMULATE_CREDIT_FAILURE=true` en account-service
- Ejecutar una transferencia completa
- Verificar:
  - `transfer.debited` procesado → balance A: 900
  - `transfer.failed` con `errorReason: CREDIT_FAILED`
  - `transfer.rollback.debit` procesado → balance A: 1000 (restaurado)
  - `GET /status` → `ROLLED_BACK`
- Desactivar el flag y verificar que el flujo normal vuelve a funcionar

**Criterio de éxito:**
```
balance A siempre vuelve a 1000 cuando el crédito falla
balance B nunca cambia
status final: ROLLED_BACK
```

---

### PASO 9 — Pruebas de Integración y Documentación
**Objetivo:** El lab es reproducible por cualquier persona nueva.

Actividades:
- Colección Postman/Bruno con los 4 casos de prueba:
  - Caso 1: Flujo exitoso
  - Caso 2: Duplicado Kafka
  - Caso 3: Falla en crédito + rollback
  - Caso 4: Polling con transición de estados
- `README.md` con instrucciones de levantamiento
- Evidencia visual: capturas de consola / logs con los 4 escenarios
- Respuestas a las preguntas de reflexión del laboratorio (en comentarios de código o README)

---

### PASO 10 — Bonus (Si el tiempo lo permite)

| Bonus | Complejidad | Valor educativo |
|-------|-------------|----------------|
| Redis para cache de `GET /status` | Media | Cache-aside pattern |
| Backoff exponencial en reintentos | Baja | `@Backoff` de Spring Retry |
| `correlationId` en headers Kafka | Baja | Distributed tracing pattern |
| Partition key = `fromAccount` con evidencia | Baja | Ver cómo todos los eventos de cuenta A van a la misma partición |
| Dead Letter Queue para errores irrecuperables | Media | DLQ pattern |

---

## 🔍 FASE 5 — RIESGOS Y DECISIONES ANTICIPADAS

| Riesgo | Mitigation |
|--------|-----------|
| Condición de carrera en `processed_events` con 2 hilos | El `PRIMARY KEY` del DB rechaza el segundo INSERT → uno gana, otro ignora |
| Consumer cae después del débito pero antes del commit | `enable.auto.commit=false` → al reiniciar relee el evento → idempotencia lo filtra |
| `OptimisticLockException` por concurrencia en saldo | Retry automático con `@Retryable` en el handler |
| El status-service se reinicia y pierde eventos ya procesados | Lee desde `auto.offset.reset=earliest` → idempotencia filtra los ya procesados |
| Docker Compose levanta los servicios antes que Kafka esté listo | `depends_on` + `healthcheck` en docker-compose |

---

## Resumen del plan

```
PASO 0  →  Infraestructura Docker + Kafka + MySQL        [bloqueante]
PASO 1  →  Módulo Common (contratos compartidos)         [bloqueante]
PASO 2  →  Esquema de Base de Datos (Flyway)             [bloqueante]
PASO 3  →  Transfer API (POST + GET + Kafka producer)    [independiente]
PASO 4  →  Validation Service (consumer + producer)      [depende de 0,1,2,3]
PASO 5  →  Account Service (DEBIT + CREDIT + ROLLBACK)   [el más complejo]
PASO 6  →  Status Service (tracker de saga)              [depende de 5]
PASO 7  →  Simulación de Duplicados                      [demo del lab]
PASO 8  →  Escenario de Rollback                         [demo del lab]
PASO 9  →  Pruebas + Documentación                       [entregable final]
PASO 10 →  Bonus                                         [opcional]
```

**Tiempo estimado por paso:**
- PASOS 0–2: ~2 horas (infraestructura y contratos)
- PASOS 3–4: ~3 horas (servicios simples)
- PASO 5: ~4 horas (el corazón del sistema)
- PASO 6: ~1.5 horas
- PASOS 7–9: ~2 horas (demostración y documentación)
- **Total:** ~12–13 horas de laboratorio

---

¿Empezamos a generar el código por algún paso específico, o prefieres que arranquemos en orden desde el PASO 0 con el `docker-compose.yml` y la estructura del proyecto?