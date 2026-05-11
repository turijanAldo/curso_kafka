# =============================================================
# 20-iniciar-cluster.ps1
# Inicia el cluster Kafka de 3 brokers en modo KRaft.
# Verifica puertos, levanta los contenedores y espera a que
# los 3 brokers esten activos y comunicandose entre si.
# =============================================================

$ErrorActionPreference = "Continue"

function Write-OK    { param($msg) Write-Host "[  OK  ] $msg" -ForegroundColor Green }
function Write-ERROR { param($msg) Write-Host "[ ERROR] $msg" -ForegroundColor Red }
function Write-INFO  { param($msg) Write-Host "[ INFO ] $msg" -ForegroundColor Cyan }
function Write-WARN  { param($msg) Write-Host "[ WARN ] $msg" -ForegroundColor Yellow }
function Write-STEP  { param($msg) Write-Host "`n>>> $msg" -ForegroundColor Yellow }

$BROKERS = @(
    @{ Id=1; Container="kafka-broker-1"; Puerto=9092 },
    @{ Id=2; Container="kafka-broker-2"; Puerto=9093 },
    @{ Id=3; Container="kafka-broker-3"; Puerto=9094 }
)

Write-Host ""
Write-Host "============================================================" -ForegroundColor Cyan
Write-Host "  Kafka Nivel 3: Iniciando Cluster Multi-Broker (3 nodos) " -ForegroundColor Cyan
Write-Host "============================================================" -ForegroundColor Cyan

# ── Localizar directorio raiz ────────────────────────────────
$scriptDir   = Split-Path -Parent $MyInvocation.MyCommand.Path
$rootDir     = Split-Path -Parent (Split-Path -Parent $scriptDir)
$composeFile = Join-Path $rootDir "docker\docker-compose-cluster.yml"

if (-not (Test-Path $composeFile)) {
    Write-ERROR "No se encontro docker\docker-compose-cluster.yml en: $rootDir"
    exit 1
}
Write-OK "Archivo compose encontrado: $composeFile"

# ── Verificar que Docker esta corriendo ──────────────────────
Write-STEP "Verificando Docker Desktop"
docker ps > $null 2>&1
if ($LASTEXITCODE -ne 0) {
    Write-ERROR "Docker Desktop no esta corriendo. Inicialo y vuelve a intentar."
    exit 1
}
Write-OK "Docker Desktop activo"

# ── Detener el cluster del Nivel 1 si esta corriendo ─────────
Write-STEP "Verificando conflictos con el cluster del Nivel 1"
$nivel1 = docker inspect kafka-nivel1 --format "{{.State.Status}}" 2>&1
if ($nivel1 -eq "running") {
    Write-WARN "El broker kafka-nivel1 (Nivel 1) esta corriendo en el puerto 9092."
    Write-WARN "Deteniendolo para evitar conflictos de puertos..."
    $nivel1ComposeDir = Join-Path (Split-Path $rootDir -Parent) "kafka-lab-nivel-1\docker"
    if (Test-Path $nivel1ComposeDir) {
        Push-Location $nivel1ComposeDir
        docker-compose down 2>&1 | Out-Null
        Pop-Location
    }
    Write-OK "Broker Nivel 1 detenido"
}

# ── Verificar puertos disponibles ────────────────────────────
Write-STEP "Verificando disponibilidad de puertos"

foreach ($broker in $BROKERS) {
    $puerto = $broker.Puerto
    $ocupado = Get-NetTCPConnection -LocalPort $puerto -ErrorAction SilentlyContinue
    if ($ocupado) {
        $proc = Get-Process -Id $ocupado.OwningProcess -ErrorAction SilentlyContinue
        Write-ERROR "Puerto $puerto ocupado por: $($proc.ProcessName) (PID $($ocupado.OwningProcess))"
        Write-Host "  Detiene ese proceso antes de continuar" -ForegroundColor Yellow
        exit 1
    }
    Write-OK "Puerto $puerto disponible (Broker $($broker.Id))"
}

# ── Levantar el cluster ──────────────────────────────────────
Write-STEP "Levantando 3 brokers con docker-compose"

Push-Location (Join-Path $rootDir "docker")
try {
    docker-compose -f docker-compose-cluster.yml up -d 2>&1
    if ($LASTEXITCODE -ne 0) {
        Write-ERROR "docker-compose up fallo con codigo $LASTEXITCODE"
        exit 1
    }
} finally {
    Pop-Location
}
Write-OK "docker-compose up ejecutado"

# ── Esperar a que los 3 brokers esten listos ─────────────────
Write-STEP "Esperando que los 3 brokers esten operativos (hasta 120 segundos)"
Write-INFO "Los brokers necesitan tiempo para hacer el KRaft leader election..."

$maxEspera    = 120
$intervalo    = 5
$transcurrido = 0
$brokersListos = 0

while ($transcurrido -lt $maxEspera) {
    $brokersListos = 0

    foreach ($broker in $BROKERS) {
        $conn = Test-NetConnection -ComputerName localhost -Port $broker.Puerto `
                    -WarningAction SilentlyContinue -ErrorAction SilentlyContinue
        if ($conn.TcpTestSucceeded) { $brokersListos++ }
    }

    $pct  = [math]::Round(($transcurrido / $maxEspera) * 100)
    $barra = ("[" + ("#" * [math]::Round($pct/5)) + ("." * (20 - [math]::Round($pct/5))) + "]")
    Write-Host "`r  $barra $pct% - Brokers listos: $brokersListos/3 ($transcurrido s)" -NoNewline

    if ($brokersListos -eq 3) { break }

    Start-Sleep -Seconds $intervalo
    $transcurrido += $intervalo
}
Write-Host ""

if ($brokersListos -lt 3) {
    Write-ERROR "Solo $brokersListos/3 brokers respondieron en $maxEspera segundos"
    Write-Host "  Revisa los logs con: .\scripts\powershell\24-ver-logs-brokers.ps1" -ForegroundColor Yellow
    exit 1
}
Write-OK "Los 3 brokers estan respondiendo"

# ── Verificar comunicacion interna del cluster ───────────────
Write-STEP "Verificando que los brokers se conocen entre si"

Start-Sleep -Seconds 5   # Dar tiempo al leader election de KRaft

$clusterInfo = docker exec kafka-broker-1 /opt/kafka/bin/kafka-broker-api-versions.sh `
    --bootstrap-server localhost:9092 2>&1

$numBrokers = ($clusterInfo | Select-String "id:").Count
Write-INFO "Brokers registrados en el cluster: $numBrokers"

# ── Estado del quorum KRaft ──────────────────────────────────
Write-STEP "Estado del quorum KRaft"
docker exec kafka-broker-1 /opt/kafka/bin/kafka-metadata-quorum.sh `
    --bootstrap-server localhost:9092 `
    describe --status 2>&1

# ── Estado de los contenedores ───────────────────────────────
Write-STEP "Estado de los 3 contenedores"
docker ps --filter "name=kafka-broker" --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}" 2>&1

# ── Resumen ──────────────────────────────────────────────────
Write-Host ""
Write-Host "============================================================" -ForegroundColor Green
Write-Host "  CLUSTER MULTI-BROKER INICIADO CORRECTAMENTE             " -ForegroundColor Green
Write-Host "============================================================" -ForegroundColor Green
Write-Host ""
Write-Host "  Broker 1 -> localhost:9092  (node.id=1)" -ForegroundColor White
Write-Host "  Broker 2 -> localhost:9093  (node.id=2)" -ForegroundColor White
Write-Host "  Broker 3 -> localhost:9094  (node.id=3)" -ForegroundColor White
Write-Host ""
Write-Host "  Bootstrap servers para Java:" -ForegroundColor Cyan
Write-Host "    localhost:9092,localhost:9093,localhost:9094" -ForegroundColor Cyan
Write-Host ""
Write-Host "  Siguientes pasos:" -ForegroundColor White
Write-Host "    1. .\scripts\powershell\21-verificar-cluster.ps1" -ForegroundColor Gray
Write-Host "    2. Compila Java: cd java && mvn clean package" -ForegroundColor Gray
Write-Host "    3. Sigue INSTRUCCIONES-NIVEL-3.md" -ForegroundColor Gray
Write-Host ""
