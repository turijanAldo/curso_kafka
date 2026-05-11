# =============================================================
# 03-detener-kafka.ps1
# Detiene el cluster de Kafka de forma limpia
# Los datos en volumenes Docker persisten
# =============================================================

$ErrorActionPreference = "Continue"

function Write-OK    { param($msg) Write-Host "[  OK  ] $msg" -ForegroundColor Green }
function Write-ERROR { param($msg) Write-Host "[ ERROR] $msg" -ForegroundColor Red }
function Write-INFO  { param($msg) Write-Host "[ INFO ] $msg" -ForegroundColor Cyan }
function Write-STEP  { param($msg) Write-Host "`n>>> $msg" -ForegroundColor Yellow }

Write-Host ""
Write-Host "============================================================" -ForegroundColor Cyan
Write-Host "  Laboratorio Kafka - Nivel 1: Deteniendo Cluster          " -ForegroundColor Cyan
Write-Host "============================================================" -ForegroundColor Cyan

# ── Localizar directorio raiz ────────────────────────────────
$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$rootDir   = Split-Path -Parent (Split-Path -Parent $scriptDir)
$composeFile = Join-Path $rootDir "docker\docker-compose.yml"

if (-not (Test-Path $composeFile)) {
    Write-ERROR "No se encontro docker\docker-compose.yml. Ejecuta desde la raiz del laboratorio."
    exit 1
}

# ── Verificar que el contenedor existe ───────────────────────
Write-STEP "Verificando estado actual del contenedor"

$estado = docker inspect kafka-nivel1 --format "{{.State.Status}}" 2>&1
if ($LASTEXITCODE -ne 0) {
    Write-INFO "El contenedor kafka-nivel1 no existe. Nada que detener."
    exit 0
}
Write-INFO "Estado actual del contenedor: $estado"

# ── Detener con docker-compose down ─────────────────────────
Write-STEP "Deteniendo contenedor con docker-compose down"
Write-INFO "Nota: Los datos en volumenes Docker se conservan."

Push-Location (Join-Path $rootDir "docker")
try {
    docker-compose down
    if ($LASTEXITCODE -ne 0) {
        Write-ERROR "Error al detener el contenedor (codigo: $LASTEXITCODE)"
        exit 1
    }
} finally {
    Pop-Location
}

# ── Verificar que se detuvo ──────────────────────────────────
Write-STEP "Verificando que el contenedor se detuvo"

$estadoPost = docker inspect kafka-nivel1 --format "{{.State.Status}}" 2>&1
if ($LASTEXITCODE -ne 0) {
    Write-OK "Contenedor kafka-nivel1 eliminado correctamente"
} elseif ($estadoPost -eq "exited") {
    Write-OK "Contenedor detenido (estado: exited)"
} else {
    Write-INFO "Estado post-detener: $estadoPost"
}

# ── Resumen ──────────────────────────────────────────────────
Write-Host ""
Write-Host "============================================================" -ForegroundColor Green
Write-Host "  CLUSTER DETENIDO CORRECTAMENTE                          " -ForegroundColor Green
Write-Host "============================================================" -ForegroundColor Green
Write-Host ""
Write-Host "  Los datos persisten en el volumen Docker 'kafka_data'." -ForegroundColor White
Write-Host "  Para reiniciar el cluster: .\scripts\powershell\01-iniciar-kafka.ps1" -ForegroundColor Cyan
Write-Host "  Para eliminar TODO (incluyendo datos): .\scripts\powershell\04-limpiar-todo.ps1" -ForegroundColor Yellow
Write-Host ""
