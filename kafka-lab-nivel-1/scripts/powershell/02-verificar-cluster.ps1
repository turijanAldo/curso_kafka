# =============================================================
# 02-verificar-cluster.ps1
# Ejecuta comandos de verificacion dentro del contenedor Kafka
# =============================================================

$ErrorActionPreference = "Continue"

function Write-OK    { param($msg) Write-Host "[  OK  ] $msg" -ForegroundColor Green }
function Write-ERROR { param($msg) Write-Host "[ ERROR] $msg" -ForegroundColor Red }
function Write-INFO  { param($msg) Write-Host "[ INFO ] $msg" -ForegroundColor Cyan }
function Write-STEP  { param($msg) Write-Host "`n>>> $msg" -ForegroundColor Yellow }
function Write-SEP   { Write-Host "------------------------------------------------------------" -ForegroundColor DarkGray }

$CONTENEDOR = "kafka-nivel1"
$BOOTSTRAP  = "localhost:9092"

Write-Host ""
Write-Host "============================================================" -ForegroundColor Cyan
Write-Host "  Laboratorio Kafka - Nivel 1: Verificacion del Cluster    " -ForegroundColor Cyan
Write-Host "============================================================" -ForegroundColor Cyan

# ── Verificar que el contenedor existe y corre ───────────────
Write-STEP "Verificando que el contenedor esta activo"

$estado = docker inspect $CONTENEDOR --format "{{.State.Status}}" 2>&1
if ($LASTEXITCODE -ne 0 -or $estado -ne "running") {
    Write-ERROR "El contenedor '$CONTENEDOR' no esta corriendo (estado: $estado)"
    Write-Host "  Ejecuta primero: .\scripts\powershell\01-iniciar-kafka.ps1" -ForegroundColor Yellow
    exit 1
}
Write-OK "Contenedor '$CONTENEDOR' en estado: $estado"

# ── 1. Versiones de API del broker ──────────────────────────
Write-STEP "1. Versiones de API soportadas por el broker"
Write-SEP
docker exec $CONTENEDOR /opt/kafka/bin/kafka-broker-api-versions.sh `
    --bootstrap-server $BOOTSTRAP 2>&1
Write-SEP

# ── 2. Listar topics ─────────────────────────────────────────
Write-STEP "2. Topics existentes en el cluster"
Write-SEP
$topics = docker exec $CONTENEDOR /opt/kafka/bin/kafka-topics.sh `
    --bootstrap-server $BOOTSTRAP `
    --list 2>&1

if ($topics -match "^\s*$" -or -not $topics) {
    Write-INFO "No hay topics de usuario creados aun (esto es normal en inicio limpio)"
} else {
    Write-INFO "Topics encontrados:"
    Write-Host $topics
}
Write-SEP

# ── 3. Configuracion del broker ──────────────────────────────
Write-STEP "3. Configuracion del broker (broker id=1)"
Write-SEP
docker exec $CONTENEDOR /opt/kafka/bin/kafka-configs.sh `
    --bootstrap-server $BOOTSTRAP `
    --describe `
    --entity-type brokers `
    --entity-name 1 2>&1
Write-SEP

# ── 4. Metadata del cluster (KRaft) ─────────────────────────
Write-STEP "4. Metadata del cluster KRaft"
Write-SEP
docker exec $CONTENEDOR /opt/kafka/bin/kafka-metadata-quorum.sh `
    --bootstrap-server $BOOTSTRAP `
    describe --status 2>&1
Write-SEP

# ── 5. Informacion del log de datos ─────────────────────────
Write-STEP "5. Informacion de los directorios de log"
Write-SEP
docker exec $CONTENEDOR /opt/kafka/bin/kafka-log-dirs.sh `
    --bootstrap-server $BOOTSTRAP `
    --describe 2>&1
Write-SEP

# ── Resumen ──────────────────────────────────────────────────
Write-Host ""
Write-Host "============================================================" -ForegroundColor Green
Write-Host "  VERIFICACION COMPLETADA                                  " -ForegroundColor Green
Write-Host "============================================================" -ForegroundColor Green
Write-Host ""
Write-Host "  El cluster de Kafka esta operativo." -ForegroundColor White
Write-Host "  Siguiente paso: sigue el experimento en experimentos\exp-01-primer-mensaje.md" -ForegroundColor Cyan
Write-Host ""
