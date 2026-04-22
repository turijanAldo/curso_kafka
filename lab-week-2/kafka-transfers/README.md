# kafka-transfers — Sistema de Transferencias Asíncronas

Sistema de transferencias bancarias basado en **Choreography-based Saga** con Apache Kafka,
Spring Boot 3 y MySQL 8. Demuestra idempotencia en 3 capas y compensación automática.

> 📋 **Para ejecutar y probar el sistema paso a paso:** ver [`../EJECUCION.md`](../EJECUCION.md)

---

## ¿Qué hace este sistema?

Un cliente envía `POST /transfers` al `transfer-api`. La transferencia no se procesa de forma
síncrona — en cambio, se publica un evento Kafka y la respuesta inmediata es `HTTP 202 ACCEPTED`
con estado `PROCESSING`. El procesamiento real ocurre de forma asíncrona a través de una cadena
de microservicios que se coordinan exclusivamente via eventos Kafka (sin llamadas HTTP entre ellos).

Cada servicio hace exactamente una cosa y publica el resultado para que el siguiente lo procese.
Si algo falla en el medio (ej: el crédito falla), el sistema compensa automáticamente deshaciendo
las operaciones anteriores — sin intervención manual.

## Arquitectura

```
Cliente
  │
  │ POST /transfers
  ▼
┌─────────────────┐
│  transfer-api   │  puerto 8080
│  (Gateway REST) │──── INSERT transactions (PROCESSING)
└────────┬────────┘
         │ publish: transfer.requested
         ▼
┌─────────────────────┐
│ validation-service  │  puerto 8081  │ group: validation-group
│                     │──── 4 reglas de negocio (cuentas válidas, monto > 0)
└────────┬────────────┘
         │ publish: transfer.validated  ──OR──  transfer.failed
         ▼
┌─────────────────────┐
│  account-service    │  puerto 8082  │ group: account-debit-group
│  (DebitConsumer)    │──── Débito con @Version (Optimistic Locking)
└────────┬────────────┘
         │ publish: transfer.debited
         ▼
┌─────────────────────┐
│  account-service    │  puerto 8082  │ group: account-credit-group
│  (CreditConsumer)   │──── Crédito en cuenta destino
└────────┬────────────┘    Si falla → CompensationService (in-process)
         │                            └─ publish: transfer.compensated
         │ publish: transfer.credited ──OR──  transfer.failed
         │
         ▼ (todos los eventos)
┌─────────────────────┐
│  status-service     │  puerto 8083  │ group: status-group
│                     │──── UPDATE transactions SET status=...
└─────────────────────┘

Cliente
  │ GET /transfers/{id}        → transfer-api  (8080)
  │ GET /transfers/{id}/status → status-service (8083)
  ▼
{ "status": "COMPLETED" }
```

## Prerrequisitos

- **Java 17** o superior
- **Maven 3.9+**
- **Docker Desktop** con Docker Compose v2

Verificar:
```powershell
java -version    # openjdk 17 o superior
mvn -version     # Apache Maven 3.9+
docker version   # Docker 24+
```

## Levantar el sistema

### Paso 1: Infraestructura (Kafka + MySQL)

```powershell
cd docker
docker compose up -d

# Verificar que ambos contenedores están healthy:
docker compose ps
# kafka    running (healthy)
# mysql    running (healthy)

# Crear los topics de Kafka:
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh `
    --create --bootstrap-server localhost:9092 `
    --topic transfer.requested --partitions 3 --replication-factor 1
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh `
    --create --bootstrap-server localhost:9092 `
    --topic transfer.validated --partitions 3 --replication-factor 1
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh `
    --create --bootstrap-server localhost:9092 `
    --topic transfer.failed --partitions 3 --replication-factor 1
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh `
    --create --bootstrap-server localhost:9092 `
    --topic transfer.debited --partitions 3 --replication-factor 1
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh `
    --create --bootstrap-server localhost:9092 `
    --topic transfer.credited --partitions 3 --replication-factor 1
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh `
    --create --bootstrap-server localhost:9092 `
    --topic transfer.compensated --partitions 3 --replication-factor 1
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh `
    --create --bootstrap-server localhost:9092 `
    --topic transfer.dlq --partitions 1 --replication-factor 1
```

### Paso 2: Compilar el proyecto

```powershell
cd kafka-transfers
mvn clean package -DskipTests
```

### Paso 3: Arrancar los 4 servicios (una terminal por servicio)

```powershell
# Terminal 1
cd transfer-api && mvn spring-boot:run

# Terminal 2
cd validation-service && mvn spring-boot:run

# Terminal 3
cd account-service && mvn spring-boot:run

# Terminal 4
cd status-service && mvn spring-boot:run
```

## Casos de prueba

### Caso 1: Transferencia exitosa

```powershell
curl.exe -X POST http://localhost:8080/transfers `
  -H "Content-Type: application/json" `
  -d '{\"fromAccount\":\"ACC-001\",\"toAccount\":\"ACC-002\",\"amount\":100}'

# Respuesta: HTTP 202 { "transactionId": "...", "status": "PROCESSING" }

# Consultar estado (esperar ~3 segundos):
curl.exe "http://localhost:8083/transfers/<transactionId>/status"
# { "status": "COMPLETED" }
```

Cuentas disponibles: `ACC-001` (Ana, 1000.00), `ACC-002` (Bob, 500.00), `ACC-003` (Carlos, 2500.00)

### Caso 2: Validación fallida

```powershell
# Cuenta inexistente:
curl.exe -X POST http://localhost:8080/transfers `
  -H "Content-Type: application/json" `
  -d '{\"fromAccount\":\"ACC-001\",\"toAccount\":\"ACC-999\",\"amount\":50}'
# Estado final: FAILED | reason: "Cuenta destino no encontrada: ACC-999"

# Misma cuenta origen y destino:
curl.exe -X POST http://localhost:8080/transfers `
  -H "Content-Type: application/json" `
  -d '{\"fromAccount\":\"ACC-001\",\"toAccount\":\"ACC-001\",\"amount\":50}'
# Estado final: FAILED | reason: "La cuenta origen y destino no pueden ser la misma"
```

### Caso 3: Idempotencia (duplicado Kafka)

```powershell
# 1. Hacer una transferencia y esperar COMPLETED
# 2. Detener account-service
# 3. Resetear el offset:
docker exec -it kafka /opt/kafka/bin/kafka-consumer-groups.sh `
    --bootstrap-server localhost:9092 `
    --group account-debit-group --topic transfer.validated `
    --reset-offsets --to-earliest --execute
# 4. Reiniciar account-service
# 5. Observar en logs: "⚠️ Evento duplicado — ignorando débito"
# 6. Verificar saldos sin cambio
```

### Caso 4: Rollback de Saga (compensación)

```powershell
# 1. Editar account-service/src/main/resources/application.yml:
#    simulate-credit-failure: true
# 2. Reiniciar account-service
# 3. Enviar transferencia → estado final: ROLLED_BACK
# 4. Verificar que el saldo de fromAccount no cambió (débito revertido)
# 5. Restaurar: simulate-credit-failure: false
```

## Idempotencia — las 3 capas

| Capa | Dónde | Protege contra |
|------|-------|---------------|
| **Capa 1** | Producer (`enable.idempotence=true`) | Duplicados a nivel de red/protocolo Kafka |
| **Capa 2** | `SELECT EXISTS` antes de procesar | La mayoría de duplicados en condiciones normales |
| **Capa 3** | `UNIQUE(event_key, service_name)` en DB | Condiciones de carrera entre threads concurrentes |

## Estructura del proyecto

```
kafka-transfers/
├── common/                  # Eventos, enums y constantes compartidos
├── transfer-api/            # Gateway REST — puerto 8080
├── validation-service/      # Validación de reglas — puerto 8081
├── account-service/         # Débito, crédito y compensación — puerto 8082
└── status-service/          # Tracker del Saga — puerto 8083

docker/
├── docker-compose.yml       # Kafka (KRaft) + MySQL 8
└── init-db.sql              # Schema y datos iniciales
```

## Puertos y endpoints

| Servicio | Puerto | Endpoints |
|---------|--------|-----------|
| transfer-api | 8080 | `POST /transfers`, `GET /transfers/{id}` |
| validation-service | 8081 | (solo Kafka, sin REST) |
| account-service | 8082 | (solo Kafka, sin REST) |
| status-service | 8083 | `GET /transfers/{id}/status` |
