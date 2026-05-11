# =============================================================================
# 32-simular-fallo-broker.ps1
# Nivel 4 - Simular fallo catastrófico de un broker
# Uso: .\32-simular-fallo-broker.ps1 -Broker <1|2|3>
# =============================================================================

param(
    [Parameter(Mandatory=$true)]
    [ValidateSet("1","2","3")]
    [string]$Broker
)

$ErrorActionPreference = "SilentlyContinue"
$SCRIPT_DIR   = Split-Path -Parent $MyInvocation.MyCommand.Path
$ROOT_DIR     = Split-Path -Parent (Split-Path -Parent $SCRIPT_DIR)
$RESULTADOS   = Join-Path $ROOT_DIR "experimentos\resultados"
$BROKER_NAME  = "kafka-broker-$Broker"
$OTROS_BROKERS = @("kafka-broker-1","kafka-broker-2","kafka-broker-3") | Where-Object { $_ -ne $BROKER_NAME }
$BOOTSTRAP_OK  = "localhost:909" + (@(1,2,3) | Where-Object { $_ -ne [int]$Broker } | Select-Object -First 1)

function Write-Header { param($m) Write-Host "`n$m" -ForegroundColor Cyan }
function Write-OK     { param($m) Write-Host "  [OK] $m" -ForegroundColor Green }
function Write-Info   { param($m) Write-Host "  [>>] $m" -ForegroundColor White }
function Write-Warn   { param($m) Write-Host "  [!]  $m" -ForegroundColor Yellow }
function Write-Err    { param($m) Write-Host "  [X]  $m" -ForegroundColor Red }

Write-Host ""
Write-Host "╔══════════════════════════════════════════════════════════════╗" -ForegroundColor Red
Write-Host "║       NIVEL 4 - SIMULAR FALLO DE BROKER $Broker                     ║" -ForegroundColor Red
Write-Host "╚══════════════════════════════════════════════════════════════╝" -ForegroundColor Red

# ─────────────────────────────────────────────────────────────────────────────
# Validaciones previas
# ─────────────────────────────────────────────────────────────────────────────
Write-Header "Validaciones previas al fallo simulado..."

# Verificar que el broker objetivo está corriendo
$estadoBroker = docker inspect --format='{{.State.Status}}' $BROKER_NAME 2>$null
if ($estadoBroker -ne "running") {
    Write-Err "$BROKER_NAME no esta corriendo. No hay nada que detener."
    exit 1
}
Write-OK "$BROKER_NAME esta corriendo - puede simularse el fallo"

# Verificar que los otros brokers están corriendo
foreach ($ob in $OTROS_BROKERS) {
    $est = docker inspect --format='{{.State.Status}}' $ob 2>$null
    if ($est -ne "running") {
        Write-Err "$ob NO esta corriendo. El cluster no tendria suficientes brokers para continuar."
        Write-Err "No es seguro detener $BROKER_NAME en este estado."
        exit 1
    }
    Write-OK "$ob esta corriendo - continuara operando despues del fallo"
}

# Verificar que hay topics replicados
Write-Info "Verificando existencia de topics replicados..."
$topicsRep = docker exec kafka-broker-1 /opt/kafka/bin/kafka-topics.sh `
    --bootstrap-server localhost:9092 --list 2>$null | Where-Object { $_ -match "rf[23]|critical" }

if (-not $topicsRep) {
    Write-Warn "No se encontraron topics con replicacion (rf2, rf3, critical-data)."
    Write-Warn "Ejecuta primero: .\scripts\powershell\30-crear-topics-replicados.ps1"
    Write-Warn "Sin replicacion, el fallo de un broker causa perdida de datos."
    $continuar = Read-Host "  ¿Deseas continuar de todas formas? (s/N)"
    if ($continuar -ne "s" -and $continuar -ne "S") { exit 0 }
} else {
    foreach ($t in $topicsRep) { Write-OK "Topic replicado encontrado: $t" }
}

# ─────────────────────────────────────────────────────────────────────────────
# Capturar estado ANTES del fallo
# ─────────────────────────────────────────────────────────────────────────────
Write-Header "Capturando estado del cluster ANTES del fallo..."
$timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"

$estadoAntes = docker exec kafka-broker-1 /opt/kafka/bin/kafka-topics.sh `
    --bootstrap-server localhost:9092 --describe 2>$null | Where-Object { $_ -notmatch "^__" }

$archivoAntes = "$RESULTADOS\estado-replicas-antes-fallo.txt"
"Estado del cluster ANTES del fallo - $timestamp" | Out-File -FilePath $archivoAntes -Encoding UTF8
"Broker a detener: $BROKER_NAME" | Out-File -FilePath $archivoAntes -Append -Encoding UTF8
"=" * 60 | Out-File -FilePath $archivoAntes -Append -Encoding UTF8
$estadoAntes | Out-File -FilePath $archivoAntes -Append -Encoding UTF8
Write-OK "Estado guardado en: $archivoAntes"

# Identificar particiones cuyo leader es el broker a detener
Write-Info "Identificando particiones con leader en Broker $Broker..."
$particionesAfectadas = @()
foreach ($linea in $estadoAntes) {
    if ($linea -match "Leader:\s*$Broker\b" -and $linea -match "Partition:") {
        $topicMatch = if ($linea -match "Topic:\s*(\S+)") { $Matches[1] } else { "?" }
        $partMatch  = if ($linea -match "Partition:\s*(\d+)") { $Matches[1] } else { "?" }
        $particionesAfectadas += "  Topic=$topicMatch, Partition=$partMatch"
        Write-Warn "Particion con leader en Broker $Broker : Topic=$topicMatch, Partition=$partMatch"
    }
}

if ($particionesAfectadas.Count -eq 0) {
    Write-Info "El broker $Broker no es leader de ninguna particion actualmente."
    Write-Info "(Kafka puede haber rebalanceado los leaders previamente)"
}

# ─────────────────────────────────────────────────────────────────────────────
# Advertencia y confirmación
# ─────────────────────────────────────────────────────────────────────────────
Write-Host ""
Write-Host "  ┌──────────────────────────────────────────────────────────┐" -ForegroundColor Yellow
Write-Host "  │                   ADVERTENCIA                            │" -ForegroundColor Yellow
Write-Host "  │                                                          │" -ForegroundColor Yellow
Write-Host "  │  Este script detendrá $BROKER_NAME          " -ForegroundColor Yellow
Write-Host "  │  simulando un fallo catastrófico (sin shutdown graceful).│" -ForegroundColor Yellow
Write-Host "  │                                                          │" -ForegroundColor Yellow
Write-Host "  │  - Topics RF>1: Kafka hará failover automatico en ~5s   │" -ForegroundColor Yellow
Write-Host "  │  - Topics RF=1: Esas particiones quedarán inaccesibles  │" -ForegroundColor Yellow
Write-Host "  │  - El cluster seguirá con 2 brokers operativos          │" -ForegroundColor Yellow
Write-Host "  │                                                          │" -ForegroundColor Yellow
Write-Host "  │  Para recuperar: ejecuta 33-recuperar-broker.ps1        │" -ForegroundColor Yellow
Write-Host "  └──────────────────────────────────────────────────────────┘" -ForegroundColor Yellow
Write-Host ""

$confirmacion = Read-Host "  ¿Confirmas detener $BROKER_NAME? (escribe 'SI' para confirmar)"
if ($confirmacion -ne "SI") {
    Write-Info "Operacion cancelada por el usuario."
    exit 0
}

# ─────────────────────────────────────────────────────────────────────────────
# Detener el broker (sin -t para simular fallo abrupto)
# ─────────────────────────────────────────────────────────────────────────────
Write-Header "Deteniendo $BROKER_NAME (fallo abrupto simulado)..."
$tiempoInicio = Get-Date

docker stop $BROKER_NAME 2>&1 | Out-Null
Write-OK "$BROKER_NAME detenido a las $(Get-Date -Format 'HH:mm:ss.fff')"

# ─────────────────────────────────────────────────────────────────────────────
# Monitoreo de failover (30 segundos, cada 2 segundos)
# ─────────────────────────────────────────────────────────────────────────────
Write-Header "Monitoreando failover automatico (30 segundos)..."
Write-Info "Observa como Kafka detecta el fallo y elige nuevos leaders..."
Write-Host ""

$brokerReferencia = $OTROS_BROKERS[0] -replace "kafka-broker-", ""
$puertoRef = "909$brokerReferencia"
$estadosPrevios = @{}
$cambiosDetectados = @()

for ($i = 1; $i -le 15; $i++) {
    Start-Sleep -Seconds 2
    $ahora = Get-Date -Format "HH:mm:ss.fff"

    $estadoActual = docker exec $OTROS_BROKERS[0] /opt/kafka/bin/kafka-topics.sh `
        --bootstrap-server "localhost:$puertoRef" --describe 2>$null | Where-Object { $_ -notmatch "^__" -and $_ -match "Leader:" }

    foreach ($linea in $estadoActual) {
        $topicM = if ($linea -match "Topic:\s*(\S+)")     { $Matches[1] } else { continue }
        $partM  = if ($linea -match "Partition:\s*(\d+)") { $Matches[1] } else { continue }
        $leaderM = if ($linea -match "Leader:\s*(\d+)")   { $Matches[1] } else { continue }

        $clave = "$topicM-$partM"
        if ($estadosPrevios.ContainsKey($clave)) {
            if ($estadosPrevios[$clave] -ne $leaderM) {
                $elapsed = [math]::Round(((Get-Date) - $tiempoInicio).TotalMilliseconds)
                Write-Host "  [$ahora] FAILOVER DETECTADO!" -ForegroundColor Green
                Write-Host "    Topic=$topicM Partition=$partM" -ForegroundColor White
                Write-Host "    Leader: Broker $($estadosPrevios[$clave]) → Broker $leaderM" -ForegroundColor Cyan
                Write-Host "    Tiempo desde fallo: ${elapsed}ms" -ForegroundColor Cyan
                $cambiosDetectados += "[$ahora] $topicM P$partM : Broker$($estadosPrevios[$clave]) -> Broker$leaderM ($elapsed ms)"
            }
        }
        $estadosPrevios[$clave] = $leaderM
    }

    $seg = $i * 2
    Write-Host "  [$ahora] $seg seg - brokers restantes OK, monitoreando..." -ForegroundColor DarkGray
}

# ─────────────────────────────────────────────────────────────────────────────
# Capturar estado DURANTE el fallo
# ─────────────────────────────────────────────────────────────────────────────
Write-Header "Capturando estado del cluster DURANTE el fallo..."
$timestamp2 = Get-Date -Format "yyyy-MM-dd HH:mm:ss"

$estadoDurante = docker exec $OTROS_BROKERS[0] /opt/kafka/bin/kafka-topics.sh `
    --bootstrap-server "localhost:$puertoRef" --describe 2>$null | Where-Object { $_ -notmatch "^__" }

$archivoDurante = "$RESULTADOS\estado-replicas-durante-fallo.txt"
"Estado del cluster DURANTE el fallo - $timestamp2" | Out-File -FilePath $archivoDurante -Encoding UTF8
"Broker caido: $BROKER_NAME" | Out-File -FilePath $archivoDurante -Append -Encoding UTF8
"=" * 60 | Out-File -FilePath $archivoDurante -Append -Encoding UTF8
$estadoDurante | Out-File -FilePath $archivoDurante -Append -Encoding UTF8
"" | Out-File -FilePath $archivoDurante -Append -Encoding UTF8
"Cambios de leader detectados:" | Out-File -FilePath $archivoDurante -Append -Encoding UTF8
$cambiosDetectados | Out-File -FilePath $archivoDurante -Append -Encoding UTF8
Write-OK "Estado guardado en: $archivoDurante"

# ─────────────────────────────────────────────────────────────────────────────
# Reporte final
# ─────────────────────────────────────────────────────────────────────────────
Write-Host ""
Write-Host "  ════════════════════════════════════════════════════════════" -ForegroundColor DarkGray
Write-Host "  REPORTE DEL FALLO SIMULADO" -ForegroundColor Magenta
Write-Host "  ════════════════════════════════════════════════════════════" -ForegroundColor DarkGray
Write-Host "  Broker detenido    : $BROKER_NAME" -ForegroundColor White
Write-Host "  Particiones lideradas antes del fallo: $($particionesAfectadas.Count)" -ForegroundColor White
Write-Host "  Failovers detectados: $($cambiosDetectados.Count)" -ForegroundColor $(if ($cambiosDetectados.Count -gt 0) { "Green" } else { "Yellow" })

if ($cambiosDetectados.Count -gt 0) {
    Write-Host ""
    Write-Host "  Cambios de leader:" -ForegroundColor Cyan
    foreach ($c in $cambiosDetectados) { Write-Host "    $c" -ForegroundColor White }
}

Write-Host ""
Write-Host "  Estado actual: cluster operando con 2 de 3 brokers" -ForegroundColor Yellow
Write-Host ""
Write-Host "  Para continuar:" -ForegroundColor Cyan
Write-Host "    .\scripts\powershell\34-verificar-isr.ps1       (ver ISR actual)" -ForegroundColor White
Write-Host "    .\scripts\powershell\33-recuperar-broker.ps1 -Broker $Broker  (recuperar broker)" -ForegroundColor White
Write-Host ""
