# =============================================================
# 13-consumer-group-multiple.ps1
# Inicia 4 instancias de InstrumentedConsumer en ventanas
# PowerShell separadas, todas en el mismo consumer group.
#
# Que observar:
#   - Cada consumer recibe un subconjunto de particiones
#   - Con 4 consumers y 4 particiones: 1 particion cada uno
#   - Si cierras un consumer: sus particiones se reasignan (rebalanceo)
#   - Si agregas un 5to consumer: quedaria idle (sin particion)
# =============================================================

$ErrorActionPreference = "Continue"
$TOPIC      = "transacciones-4p"
$GROUP_ID   = "grupo-nivel-2"
$JAR_PATH   = "$PSScriptRoot\..\..\java\target\kafka-lab-nivel-2-1.0.0.jar"

function Write-OK    { param($msg) Write-Host "[  OK  ] $msg" -ForegroundColor Green }
function Write-ERROR { param($msg) Write-Host "[ ERROR] $msg" -ForegroundColor Red }
function Write-INFO  { param($msg) Write-Host "[ INFO ] $msg" -ForegroundColor Cyan }
function Write-STEP  { param($msg) Write-Host "`n>>> $msg" -ForegroundColor Yellow }

Write-Host ""
Write-Host "============================================================" -ForegroundColor Cyan
Write-Host "  Kafka Nivel 2: Consumer Group con Multiples Instancias  " -ForegroundColor Cyan
Write-Host "============================================================" -ForegroundColor Cyan

# ── Verificar JAVA_HOME ──────────────────────────────────────
if (-not $env:JAVA_HOME) {
    Write-ERROR "JAVA_HOME no configurado. Ejemplo:"
    Write-Host '  $env:JAVA_HOME = "C:\Program Files\Java\jdk-17"' -ForegroundColor Yellow
    exit 1
}
$JAVA_EXE = "$env:JAVA_HOME\bin\java.exe"
if (-not (Test-Path $JAVA_EXE)) {
    Write-ERROR "java.exe no encontrado en: $JAVA_EXE"
    exit 1
}

# ── Verificar JAR ────────────────────────────────────────────
$JAR_REAL = Resolve-Path $JAR_PATH -ErrorAction SilentlyContinue
if (-not $JAR_REAL) {
    Write-ERROR "JAR no encontrado. Compila primero: cd java && mvn clean package"
    exit 1
}

Write-OK "JAVA_HOME: $env:JAVA_HOME"
Write-OK "JAR: $JAR_REAL"
Write-INFO "Topic: $TOPIC | Group: $GROUP_ID | Consumers: 4"

Write-Host ""
Write-Host "  Se abriran 4 ventanas de PowerShell." -ForegroundColor Yellow
Write-Host "  Observa en cada ventana que particiones le fueron asignadas." -ForegroundColor Yellow
Write-Host "  Cuando todas esten listas, ejecuta el BatchProducer para enviar mensajes." -ForegroundColor Yellow
Write-Host ""

$respuesta = Read-Host "Presiona ENTER para abrir las 4 ventanas (o Ctrl+C para cancelar)"

# ── Lanzar 4 consumers en ventanas separadas ─────────────────
for ($i = 1; $i -le 4; $i++) {
    $consumerId = "consumer-$i"
    $titulo     = "Kafka Consumer $i - Particion(es) asignadas por Kafka"

    # Comando que se ejecutara en cada nueva ventana PowerShell
    $cmd = "& '$JAVA_EXE' -cp '$JAR_REAL' com.nexus.kafka.nivel2.InstrumentedConsumer $TOPIC $GROUP_ID $consumerId; Write-Host 'Consumer detenido. Presiona Enter para cerrar.'; Read-Host"

    Start-Process powershell -ArgumentList "-NoExit", "-Command", $cmd `
        -WindowStyle Normal

    Write-OK "Ventana $i iniciada: $consumerId"
    Start-Sleep -Milliseconds 800   # Pequena pausa para que el rebalanceo sea observable
}

Write-Host ""
Write-Host "============================================================" -ForegroundColor Green
Write-Host "  4 CONSUMERS INICIADOS                                   " -ForegroundColor Green
Write-Host "============================================================" -ForegroundColor Green
Write-Host ""
Write-Host "  QUE HACER AHORA:" -ForegroundColor White
Write-Host "  1. Espera ~5 segundos a que termine el rebalanceo inicial" -ForegroundColor Gray
Write-Host "  2. Observa en cada ventana el mensaje '🎯 ASIGNADO A: Partition [X]'" -ForegroundColor Gray
Write-Host "  3. Verifica que cada consumer tiene exactamente 1 particion" -ForegroundColor Gray
Write-Host "  4. Luego ejecuta 12-producer-con-claves.ps1 para enviar mensajes" -ForegroundColor Gray
Write-Host "  5. Observa como cada mensaje llega al consumer que tiene su particion" -ForegroundColor Gray
Write-Host ""
Write-Host "  EXPERIMENTO ADICIONAL:" -ForegroundColor Yellow
Write-Host "  - Cierra una ventana de consumer con Ctrl+C" -ForegroundColor Yellow
Write-Host "  - Observa en las ventanas restantes el mensaje '🎯 ASIGNADO A: Partition [X, Y]'" -ForegroundColor Yellow
Write-Host "  - El consumer muerto libera su particion y otro la toma (rebalanceo)" -ForegroundColor Yellow
Write-Host ""

# ── Verificar asignaciones desde el broker ───────────────────
Write-STEP "Estado del consumer group '$GROUP_ID' en el broker"
Start-Sleep -Seconds 5   # Esperar a que el rebalanceo termine

docker exec kafka-nivel1 /opt/kafka/bin/kafka-consumer-groups.sh `
    --bootstrap-server localhost:9092 `
    --describe `
    --group $GROUP_ID 2>&1

Write-Host ""
Write-Host "  Columnas de la tabla:" -ForegroundColor Gray
Write-Host "    CONSUMER-ID  -> identificador unico de cada instancia del consumer" -ForegroundColor Gray
Write-Host "    HOST         -> maquina donde corre el consumer" -ForegroundColor Gray
Write-Host "    PARTITION    -> que particion le fue asignada" -ForegroundColor Gray
Write-Host "    CURRENT-OFFSET -> ultimo offset leido por este consumer" -ForegroundColor Gray
Write-Host "    LOG-END-OFFSET -> ultimo offset disponible en la particion" -ForegroundColor Gray
Write-Host "    LAG          -> mensajes pendientes de leer (LOG-END - CURRENT)" -ForegroundColor Gray
Write-Host ""
