# =============================================================
# 04-limpiar-todo.ps1
# Elimina TODOS los datos del laboratorio (contenedores + volumenes)
# ADVERTENCIA: Esta accion es IRREVERSIBLE
# =============================================================

$ErrorActionPreference = "Continue"

function Write-OK    { param($msg) Write-Host "[  OK  ] $msg" -ForegroundColor Green }
function Write-ERROR { param($msg) Write-Host "[ ERROR] $msg" -ForegroundColor Red }
function Write-WARN  { param($msg) Write-Host "[ WARN ] $msg" -ForegroundColor Yellow }
function Write-INFO  { param($msg) Write-Host "[ INFO ] $msg" -ForegroundColor Cyan }
function Write-STEP  { param($msg) Write-Host "`n>>> $msg" -ForegroundColor Yellow }

Write-Host ""
Write-Host "============================================================" -ForegroundColor Red
Write-Host "  ADVERTENCIA: LIMPIEZA TOTAL DEL LABORATORIO             " -ForegroundColor Red
Write-Host "============================================================" -ForegroundColor Red
Write-Host ""
Write-Host "  Esta operacion realizara:" -ForegroundColor White
Write-Host "    - Detener y eliminar el contenedor kafka-nivel1" -ForegroundColor Yellow
Write-Host "    - ELIMINAR el volumen Docker con todos los datos de Kafka" -ForegroundColor Red
Write-Host "    - Limpiar el directorio logs/" -ForegroundColor Yellow
Write-Host ""
Write-Host "  TODOS los topics y mensajes seran ELIMINADOS definitivamente." -ForegroundColor Red
Write-Host ""

# ── Confirmacion del usuario ─────────────────────────────────
$confirmacion = Read-Host "  Escribe 'SI' para confirmar (cualquier otra entrada cancela)"

if ($confirmacion -ne "SI") {
    Write-Host ""
    Write-INFO "Operacion cancelada. No se eliminaron datos."
    exit 0
}

Write-Host ""
Write-INFO "Confirmacion recibida. Procediendo con la limpieza..."

# ── Localizar directorio raiz ────────────────────────────────
$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$rootDir   = Split-Path -Parent (Split-Path -Parent $scriptDir)
$composeFile = Join-Path $rootDir "docker\docker-compose.yml"

if (-not (Test-Path $composeFile)) {
    Write-ERROR "No se encontro docker\docker-compose.yml"
    exit 1
}

# ── 1. Detener y eliminar contenedores + volumenes ───────────
Write-STEP "1. Ejecutando docker-compose down -v (elimina contenedores Y volumenes)"

Push-Location (Join-Path $rootDir "docker")
try {
    docker-compose down -v 2>&1
    if ($LASTEXITCODE -ne 0) {
        Write-WARN "docker-compose down -v termino con codigo $LASTEXITCODE (puede ser normal si ya estaba detenido)"
    } else {
        Write-OK "Contenedores y volumenes eliminados"
    }
} finally {
    Pop-Location
}

# ── 2. Limpiar directorio logs/ ──────────────────────────────
Write-STEP "2. Limpiando directorio logs/"

$logsDir = Join-Path $rootDir "logs"
if (Test-Path $logsDir) {
    Get-ChildItem $logsDir -Recurse -File | Where-Object { $_.Name -ne ".gitkeep" } | Remove-Item -Force
    Write-OK "Directorio logs/ limpiado"
} else {
    New-Item -ItemType Directory -Path $logsDir | Out-Null
    New-Item -ItemType File -Path "$logsDir\.gitkeep" | Out-Null
    Write-OK "Directorio logs/ recreado"
}

# ── 3. Limpiar resultados de experimentos ────────────────────
Write-STEP "3. Limpiando resultados de experimentos"

$resultadosDir = Join-Path $rootDir "experimentos\resultados"
if (Test-Path $resultadosDir) {
    Get-ChildItem $resultadosDir -Recurse -File | Where-Object { $_.Name -ne ".gitkeep" } | Remove-Item -Force
    Write-OK "Directorio experimentos/resultados/ limpiado"
}

# ── Resumen ──────────────────────────────────────────────────
Write-Host ""
Write-Host "============================================================" -ForegroundColor Green
Write-Host "  LIMPIEZA COMPLETADA                                      " -ForegroundColor Green
Write-Host "============================================================" -ForegroundColor Green
Write-Host ""
Write-Host "  El entorno esta como nuevo. Para reiniciar el laboratorio:" -ForegroundColor White
Write-Host "    1. .\scripts\powershell\00-verificar-requisitos.ps1" -ForegroundColor Cyan
Write-Host "    2. .\scripts\powershell\01-iniciar-kafka.ps1" -ForegroundColor Cyan
Write-Host ""
