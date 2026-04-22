# 🚀 EJECUCIÓN Y PRUEBAS — kafka-transfers

Guía completa para levantar el sistema desde cero, ejecutar los 8 casos de prueba y
verificar cada capa del diseño: flujo normal, validación, idempotencia, rollback, Redis y MDC.

> **Shell:** PowerShell (Windows). Comandos `docker exec` usan bash de Linux — no cambian.

---

## 1 — Prerequisitos

```powershell
# Verificar versiones mínimas
java -version    # openjdk 17 o superior
mvn -version     # Apache Maven 3.9+
docker version   # Docker 24+
```

Si algo falta: instala [Java 17 (Temurin)](https://adoptium.net/), [Maven](https://maven.apache.org/download.cgi) y [Docker Desktop](https://www.docker.com/products/docker-desktop/).

---

## 2 — Levantar infraestructura (Kafka + MySQL + Redis)

```powershell
cd kafka-transfers\docker
docker compose up -d

# Esperar a que los 3 servicios sean "healthy" (~45 segundos)
docker compose ps
```

Resultado esperado:

```
NAME    STATUS              PORTS
kafka   running (healthy)   0.0.0.0:9092->9092/tcp
mysql   running (healthy)   0.0.0.0:3306->3306/tcp
redis   running (healthy)   0.0.0.0:6379->6379/tcp
```

> Si alguno no llega a `healthy` en 2 minutos: `docker compose logs <servicio> --tail 30`

---

## 3 — Crear los topics Kafka

Solo necesario la **primera vez** (o si recreaste el contenedor de Kafka):

```powershell
$kafka = "docker exec -it kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092"

Invoke-Expression "$kafka --create --topic transfer.requested   --partitions 3 --replication-factor 1"
Invoke-Expression "$kafka --create --topic transfer.validated   --partitions 3 --replication-factor 1"
Invoke-Expression "$kafka --create --topic transfer.failed      --partitions 3 --replication-factor 1"
Invoke-Expression "$kafka --create --topic transfer.debited     --partitions 3 --replication-factor 1"
Invoke-Expression "$kafka --create --topic transfer.credited    --partitions 3 --replication-factor 1"
Invoke-Expression "$kafka --create --topic transfer.compensated --partitions 3 --replication-factor 1"
Invoke-Expression "$kafka --create --topic transfer.dlq         --partitions 1 --replication-factor 1"

# Verificar que los 7 topics existen:
Invoke-Expression "$kafka --list"
```

---

## 4 — Compilar el proyecto

```powershell
cd kafka-transfers
mvn clean package -DskipTests
# Duración: ~1-2 minutos primera vez (descarga dependencias)
# Resultado esperado: BUILD SUCCESS en los 5 módulos
```

---

## 5 — Arrancar los 4 servicios

Abre **4 terminales PowerShell** separadas. Arrancar en este orden:

```powershell
# Terminal 1 — transfer-api (Gateway REST · puerto 8080)
cd kafka-transfers\transfer-api
mvn spring-boot:run
# Esperar: "Started TransferApiApplication on port 8080"
```

```powershell
# Terminal 2 — validation-service (puerto 8081)
cd kafka-transfers\validation-service
mvn spring-boot:run
# Esperar: "Started ValidationServiceApplication on port 8081"
```

```powershell
# Terminal 3 — account-service (puerto 8082)
cd kafka-transfers\account-service
mvn spring-boot:run
# Esperar: "Started AccountServiceApplication on port 8082"
```

```powershell
# Terminal 4 — status-service (puerto 8083)
cd kafka-transfers\status-service
mvn spring-boot:run
# Esperar: "Started StatusServiceApplication on port 8083"
```

> **Primera vez:** status-service leerá todos los eventos históricos de Kafka con `auto.offset.reset: earliest`.
> Verás mensajes `📨 Evento recibido` — es normal.

---

## 6 — Verificar que el sistema levantó correctamente

```powershell
# Transfer-api responde:
curl.exe http://localhost:8080/transfers/no-existe
# Esperado: HTTP 404 { "error": "Transferencia no encontrada: no-existe" }

# Status-service responde:
curl.exe http://localhost:8083/transfers/no-existe/status
# Esperado: HTTP 404

# Saldos iniciales en DB:
docker exec -it mysql mysql -u lab_user -plab_pass transfers_db `
    -e "SELECT id, owner_name, balance FROM accounts;"
```

Resultado esperado de los saldos:

```
+---------+----------------+---------+
| id      | owner_name     | balance |
+---------+----------------+---------+
| ACC-001 | Ana García     | 1000.00 |
| ACC-002 | Bob Martínez   |  500.00 |
| ACC-003 | Carlos López   | 2500.00 |
+---------+----------------+---------+
```

---

## 7 — Casos de prueba

### CASO 1 — Flujo feliz (transferencia exitosa)

```powershell
# Enviar transferencia
$r = curl.exe -s -X POST http://localhost:8080/transfers `
  -H "Content-Type: application/json" `
  -d '{"fromAccount":"ACC-001","toAccount":"ACC-002","amount":100}' | ConvertFrom-Json

$tx1 = $r.transactionId
Write-Host "txId: $tx1"
Write-Host "status inicial: $($r.status)"   # PROCESSING
```

```powershell
# Esperar ~4 segundos para que el Saga complete
Start-Sleep -Seconds 4

# Consultar estado final
curl.exe -s "http://localhost:8083/transfers/$tx1/status" | ConvertFrom-Json
```

**Resultado esperado:**
```json
{ "status": "COMPLETED" }
```

**Verificar saldos:**
```powershell
docker exec -it mysql mysql -u lab_user -plab_pass transfers_db `
    -e "SELECT id, balance FROM accounts WHERE id IN ('ACC-001','ACC-002');"
# ACC-001: 900.00  (era 1000, restó 100)
# ACC-002: 600.00  (era 500, sumó 100)
```

**Verificar idempotencia registrada:**
```powershell
docker exec -it mysql mysql -u lab_user -plab_pass transfers_db `
    -e "SELECT event_key, service_name FROM processed_events WHERE event_key LIKE '%$tx1%';"
# 3 filas:
#   <tx1>:TRANSFER_REQUESTED  | validation-service
#   <tx1>:TRANSFER_VALIDATED  | account-service
#   <tx1>:TRANSFER_DEBITED    | account-service
```

**Verificar @Version incrementado:**
```powershell
docker exec -it mysql mysql -u lab_user -plab_pass transfers_db `
    -e "SELECT id, balance, version FROM accounts WHERE id IN ('ACC-001','ACC-002');"
# version de ACC-001: 1  (una modificación: el débito)
# version de ACC-002: 1  (una modificación: el crédito)
```

---

### CASO 2 — Validación fallida (cuenta inexistente)

```powershell
$r2 = curl.exe -s -X POST http://localhost:8080/transfers `
  -H "Content-Type: application/json" `
  -d '{"fromAccount":"ACC-001","toAccount":"ACC-999","amount":50}' | ConvertFrom-Json

$tx2 = $r2.transactionId
Start-Sleep -Seconds 3

curl.exe -s "http://localhost:8083/transfers/$tx2/status" | ConvertFrom-Json
```

**Resultado esperado:**
```json
{
  "status": "FAILED",
  "failureReason": "Cuenta destino no encontrada: ACC-999"
}
```

Prueba también los otros casos de validación:

```powershell
# Misma cuenta origen y destino:
curl.exe -s -X POST http://localhost:8080/transfers `
  -H "Content-Type: application/json" `
  -d '{"fromAccount":"ACC-001","toAccount":"ACC-001","amount":50}' | ConvertFrom-Json
# → FAILED: "La cuenta origen y destino no pueden ser la misma"

# Monto negativo (rechazado por Bean Validation antes de Kafka):
curl.exe -s -X POST http://localhost:8080/transfers `
  -H "Content-Type: application/json" `
  -d '{"fromAccount":"ACC-001","toAccount":"ACC-002","amount":-10}' | ConvertFrom-Json
# → HTTP 400 inmediato (no llega a Kafka)

# Monto cero:
curl.exe -s -X POST http://localhost:8080/transfers `
  -H "Content-Type: application/json" `
  -d '{"fromAccount":"ACC-001","toAccount":"ACC-002","amount":0}' | ConvertFrom-Json
# → HTTP 400 inmediato
```

---

### CASO 3 — Saldo insuficiente

```powershell
# ACC-002 tiene 600.00 (tras el CASO 1). Intentar transferir 1000:
$r3 = curl.exe -s -X POST http://localhost:8080/transfers `
  -H "Content-Type: application/json" `
  -d '{"fromAccount":"ACC-002","toAccount":"ACC-003","amount":1000}' | ConvertFrom-Json

$tx3 = $r3.transactionId
Start-Sleep -Seconds 4

curl.exe -s "http://localhost:8083/transfers/$tx3/status" | ConvertFrom-Json
```

**Resultado esperado:**
```json
{
  "status": "FAILED",
  "failureReason": "Saldo insuficiente. Disponible: 600.00, Requerido: 1000.00"
}
```

> La validación de saldo ocurre en **account-service** (momento del débito), no en validation-service.
> La transferencia llega a `VALIDATED` antes de fallar con `FAILED` por saldo.

---

### CASO 4 — Idempotencia (simular duplicado Kafka)

Este caso simula que Kafka reentrega un mensaje ya procesado.

```powershell
# Paso 1: Hacer una transferencia y esperar COMPLETED
$r4 = curl.exe -s -X POST http://localhost:8080/transfers `
  -H "Content-Type: application/json" `
  -d '{"fromAccount":"ACC-003","toAccount":"ACC-001","amount":50}' | ConvertFrom-Json
$tx4 = $r4.transactionId
Start-Sleep -Seconds 4
curl.exe -s "http://localhost:8083/transfers/$tx4/status"
# Debe ser COMPLETED

# Anotar saldos actuales:
docker exec -it mysql mysql -u lab_user -plab_pass transfers_db `
    -e "SELECT id, balance FROM accounts WHERE id IN ('ACC-001','ACC-003');"
```

```powershell
# Paso 2: Detener account-service (Ctrl+C en Terminal 3)

# Paso 3: Resetear el offset de account-debit-group al inicio
docker exec -it kafka /opt/kafka/bin/kafka-consumer-groups.sh `
    --bootstrap-server localhost:9092 `
    --group account-debit-group `
    --topic transfer.validated `
    --reset-offsets --to-earliest --execute
```

```powershell
# Paso 4: Reiniciar account-service
cd kafka-transfers\account-service
mvn spring-boot:run
```

```powershell
# Paso 5: Observar logs de account-service — debe aparecer:
# ⚠️ Evento duplicado — ignorando débito | key=<tx4>:TRANSFER_VALIDATED
# (para TODOS los eventos anteriores, no solo el último)
```

```powershell
# Paso 6: Verificar que los saldos NO cambiaron
docker exec -it mysql mysql -u lab_user -plab_pass transfers_db `
    -e "SELECT id, balance FROM accounts WHERE id IN ('ACC-001','ACC-003');"
# Los saldos deben ser IDÉNTICOS a los del Paso 1
```

---

### CASO 5 — Rollback del Saga (compensación automática)

```powershell
# Paso 1: Activar el flag de fallo simulado en account-service
# Detener account-service (Ctrl+C en Terminal 3)
# Editar: kafka-transfers\account-service\src\main\resources\application.yml
#   simulate-credit-failure: false  →  simulate-credit-failure: true
# Reiniciar account-service:
cd kafka-transfers\account-service
mvn spring-boot:run
```

```powershell
# Paso 2: Anotar saldos ANTES
docker exec -it mysql mysql -u lab_user -plab_pass transfers_db `
    -e "SELECT id, balance FROM accounts;"
```

```powershell
# Paso 3: Enviar una transferencia
$r5 = curl.exe -s -X POST http://localhost:8080/transfers `
  -H "Content-Type: application/json" `
  -d '{"fromAccount":"ACC-001","toAccount":"ACC-002","amount":200}' | ConvertFrom-Json
$tx5 = $r5.transactionId

Start-Sleep -Seconds 4

curl.exe -s "http://localhost:8083/transfers/$tx5/status" | ConvertFrom-Json
```

**Resultado esperado:**
```json
{
  "status": "ROLLED_BACK",
  "failureReason": "Compensación por fallo en crédito: Fallo simulado..."
}
```

```powershell
# Paso 4: Verificar que los saldos quedaron IGUAL que antes
docker exec -it mysql mysql -u lab_user -plab_pass transfers_db `
    -e "SELECT id, balance FROM accounts;"
# ACC-001: mismo saldo (débito aplicado y revertido)
# ACC-002: mismo saldo (crédito nunca ocurrió)
```

```powershell
# Paso 5: Verificar los 3 registros de idempotencia del rollback
docker exec -it mysql mysql -u lab_user -plab_pass transfers_db `
    -e "SELECT event_key, service_name FROM processed_events WHERE event_key LIKE '%$tx5%';"
# <tx5>:TRANSFER_VALIDATED | account-service  ← DebitConsumer
# <tx5>:TRANSFER_DEBITED   | account-service  ← CreditConsumer
# <tx5>:COMPENSATION       | account-service  ← CompensationService
```

```powershell
# Paso 6: Restaurar (simulate-credit-failure: true → false) y reiniciar account-service
```

---

### CASO 6 — Caché Redis (Bonus A)

```powershell
# Hacer una transferencia y esperar COMPLETED
$r6 = curl.exe -s -X POST http://localhost:8080/transfers `
  -H "Content-Type: application/json" `
  -d '{"fromAccount":"ACC-002","toAccount":"ACC-003","amount":25}' | ConvertFrom-Json
$tx6 = $r6.transactionId
Start-Sleep -Seconds 4
```

```powershell
# Primera consulta → MISS → va a MySQL → escribe en Redis
curl.exe -s "http://localhost:8080/transfers/$tx6" | ConvertFrom-Json
# En logs de transfer-api: "❌ Cache MISS | txId=..."
# En logs de transfer-api: "💾 Cache WRITE | txId=... status=COMPLETED ttl=600s"
```

```powershell
# Verificar que la key existe en Redis:
docker exec -it redis redis-cli GET "transfer:$tx6"
# Debe mostrar el JSON completo con status=COMPLETED

# Ver el TTL:
docker exec -it redis redis-cli TTL "transfer:$tx6"
# ~599 segundos (10 minutos para estados finales)
```

```powershell
# Segunda consulta → HIT → desde Redis (sin tocar MySQL)
curl.exe -s "http://localhost:8080/transfers/$tx6" | ConvertFrom-Json
# En logs de transfer-api: "✅ Cache HIT | txId=..."
```

```powershell
# Verificar TTL corto para estados intermedios:
# Enviar transferencia y consultar INMEDIATAMENTE (antes de que complete)
$r6b = curl.exe -s -X POST http://localhost:8080/transfers `
  -H "Content-Type: application/json" `
  -d '{"fromAccount":"ACC-003","toAccount":"ACC-001","amount":10}' | ConvertFrom-Json
$tx6b = $r6b.transactionId

# Consultar enseguida (estado PROCESSING)
curl.exe -s "http://localhost:8080/transfers/$tx6b" | ConvertFrom-Json
# logs: "💾 Cache WRITE | txId=... status=PROCESSING ttl=5s"

docker exec -it redis redis-cli TTL "transfer:$tx6b"
# ~4 segundos (TTL corto para estados intermedios)
```

---

### CASO 7 — correlationId en logs / MDC (Bonus B)

```powershell
# Enviar una transferencia
$r7 = curl.exe -s -X POST http://localhost:8080/transfers `
  -H "Content-Type: application/json" `
  -d '{"fromAccount":"ACC-001","toAccount":"ACC-003","amount":75}' | ConvertFrom-Json

Write-Host "txId a buscar: $($r7.transactionId)"
```

Observar los logs de **cada servicio**. Todos deben mostrar el mismo `txId` en el campo `[txId=...]`:

```
# transfer-api:
HH:mm:ss.SSS [...] INFO  [txId=f47a-...] c.l.a.c.TransferController - 202 ACCEPTED | txId=f47a-...

# validation-service:
HH:mm:ss.SSS [...] INFO  [txId=f47a-...] c.l.v.c.TransferRequestedConsumer - 📨 Evento recibido
HH:mm:ss.SSS [...] INFO  [txId=f47a-...] c.l.v.s.ValidationService - Validando transferencia

# account-service (DebitConsumer):
HH:mm:ss.SSS [...] INFO  [txId=f47a-...] c.l.a.c.DebitConsumer - 📨 TRANSFER_VALIDATED recibido
HH:mm:ss.SSS [...] INFO  [txId=f47a-...] c.l.a.s.DebitService  - Debitando 75.00 de ACC-001

# account-service (CreditConsumer):
HH:mm:ss.SSS [...] INFO  [txId=f47a-...] c.l.a.c.CreditConsumer - 📨 TRANSFER_DEBITED recibido
HH:mm:ss.SSS [...] INFO  [txId=f47a-...] c.l.a.c.CreditConsumer - 🎉 Saga completada

# status-service:
HH:mm:ss.SSS [...] INFO  [txId=f47a-...] c.l.s.c.SagaEventConsumer - 📨 Evento recibido | topic=transfer.credited
```

> El mismo `txId` aparece en los 4 servicios = trazabilidad cross-service sin sistema de tracing externo.

---

### CASO 8 — Partition key (Bonus C)

```powershell
# Enviar 6 transferencias desde ACC-001 y 4 desde ACC-003
for ($i = 1; $i -le 6; $i++) {
    curl.exe -s -X POST http://localhost:8080/transfers `
      -H "Content-Type: application/json" `
      -d "{`"fromAccount`":`"ACC-001`",`"toAccount`":`"ACC-002`",`"amount`":1}" | Out-Null
}
for ($i = 1; $i -le 4; $i++) {
    curl.exe -s -X POST http://localhost:8080/transfers `
      -H "Content-Type: application/json" `
      -d "{`"fromAccount`":`"ACC-003`",`"toAccount`":`"ACC-001`",`"amount`":1}" | Out-Null
}
Start-Sleep -Seconds 2
```

```powershell
# Ver distribución por partición en transfer.requested
docker exec -it kafka /opt/kafka/bin/kafka-run-class.sh `
    kafka.tools.GetOffsetShell `
    --bootstrap-server localhost:9092 `
    --topic transfer.requested
```

**Resultado esperado:**

```
transfer.requested:0:N    ← solo mensajes de ACC-001 (o solo de ACC-003)
transfer.requested:1:M    ← solo mensajes de la otra cuenta
transfer.requested:2:0    ← ninguna de las dos cuentas fue hasheada aquí
```

Las 6 transferencias de `ACC-001` deben estar **todas en la misma partición**.
Las 4 transferencias de `ACC-003` deben estar **todas en otra partición**.

---

## 8 — Verificaciones de DB al final de todas las pruebas

```powershell
# Estado de todas las transferencias (últimas 15)
docker exec -it mysql mysql -u lab_user -plab_pass transfers_db `
    -e "SELECT LEFT(id,8) AS tx, status, failure_reason FROM transactions ORDER BY created_at DESC LIMIT 15;"

# Saldos finales con versión
docker exec -it mysql mysql -u lab_user -plab_pass transfers_db `
    -e "SELECT id, owner_name, balance, version FROM accounts;"

# Registros de idempotencia totales
docker exec -it mysql mysql -u lab_user -plab_pass transfers_db `
    -e "SELECT service_name, COUNT(*) AS total FROM processed_events GROUP BY service_name;"

# Keys en Redis
docker exec -it redis redis-cli KEYS "transfer:*"
docker exec -it redis redis-cli DBSIZE
```

---

## 9 — Tabla resumen de evidencias

| Caso | Verificación | Resultado esperado |
|------|-------------|-------------------|
| 1 — Flujo feliz | `GET /transfers/{id}/status` | `"status": "COMPLETED"` |
| 1 — Flujo feliz | `SELECT balance FROM accounts` | ACC-001 −100, ACC-002 +100 |
| 1 — Flujo feliz | `SELECT version FROM accounts` | version = 1 en ambas cuentas |
| 1 — Flujo feliz | `SELECT * FROM processed_events` | 3 filas con el txId |
| 2 — Validación | `GET .../status` (cuenta inexistente) | `"status": "FAILED"` + reason |
| 3 — Sin fondos | `GET .../status` (monto > saldo) | `"status": "FAILED"` + reason |
| 4 — Idempotencia | Logs de account-service tras reset offset | `⚠️ Evento duplicado — ignorando débito` |
| 4 — Idempotencia | `SELECT balance FROM accounts` | Sin cambio respecto al estado anterior |
| 5 — Rollback | `GET .../status` (credit-failure=true) | `"status": "ROLLED_BACK"` |
| 5 — Rollback | `SELECT balance FROM accounts` | fromAccount sin cambio neto |
| 5 — Rollback | `SELECT * FROM processed_events` | Fila con `:COMPENSATION` |
| 6 — Redis HIT | Logs de transfer-api (2ª consulta) | `✅ Cache HIT` |
| 6 — Redis TTL | `redis-cli TTL "transfer:<txId>"` | ~600 para COMPLETED, ~5 para PROCESSING |
| 7 — MDC | Logs de los 4 servicios | Mismo `[txId=...]` en todos |
| 8 — Partition | `GetOffsetShell` en transfer.requested | ACC-001 concentrada en 1 partición |

---

## 10 — Detener el sistema

```powershell
# Detener los 4 servicios Spring Boot: Ctrl+C en cada terminal

# Detener infraestructura Docker (mantiene los datos):
cd kafka-transfers\docker
docker compose stop

# Detener Y eliminar contenedores + volúmenes (reinicio limpio):
docker compose down -v
# ⚠️ -v elimina el volumen mysql-data → los datos de DB se pierden
```

---

## 11 — Troubleshooting

### El servicio no arranca — `Flyway checksum mismatch`

```powershell
# Ocurre si modificaste un archivo de migración ya aplicado.
# Solución: resetear la tabla de historial de Flyway de ese servicio.
docker exec -it mysql mysql -u lab_user -plab_pass transfers_db `
    -e "DELETE FROM flyway_history_account;"  # o _transfer_api, _validation, _status
```

### `Connection refused` al conectar a Kafka

```powershell
# Verificar que Kafka está healthy:
docker compose ps kafka
# Si no lo está:
docker compose restart kafka
.\wait-for-kafka.ps1
```

### Error `Table 'transfers_db.X' doesn't exist`

```powershell
# init-db.sql no se ejecutó (el contenedor MySQL ya existía de antes).
# Solución: recrear MySQL limpio:
docker compose down -v
docker compose up -d
# Esperar healthy, luego crear topics y reiniciar servicios.
```

### Redis `Connection refused` al arrancar transfer-api

```powershell
docker compose ps redis
# Si no está corriendo:
docker compose up -d redis
# transfer-api se conecta a Redis de forma lazy — si Redis no estaba,
# reiniciar transfer-api después de que Redis esté healthy.
```

### `OptimisticLockingFailureException` en logs de account-service

Comportamiento esperado bajo carga concurrente (dos transferencias de la misma cuenta al mismo tiempo). El mensaje **no tiene ACK** → Kafka lo reentrega → la idempotencia lo filtra como duplicado. No es un error de la aplicación.

### Estado PROCESSING sin cambiar (el Saga se detuvo)

```powershell
# Verificar qué servicios están corriendo:
curl.exe http://localhost:8080/transfers/test  # 404 = transfer-api OK
curl.exe http://localhost:8083/transfers/test/status  # 404 = status-service OK

# Verificar que los topics tienen mensajes pendientes:
docker exec -it kafka /opt/kafka/bin/kafka-consumer-groups.sh `
    --bootstrap-server localhost:9092 `
    --group validation-group --describe
# LAG > 0 significa mensajes sin consumir → validation-service no está corriendo o está caído
```
