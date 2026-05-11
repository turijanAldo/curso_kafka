# =============================================================
# 23-detener-cluster.ps1
# Detiene el cluster de 3 brokers de forma limpia.
# Los datos persisten en los volumenes kafka-data-1/2/3.
# =============================================================

$ErrorActionPreference = "Continue"

function Write-OK   { param($msg) Write-Host "[  OK  ] $msg" -ForegroundColor Green }
function Write-INFO { param($msg) Write-Host "[ INFO ] $msg" -ForegroundColor Cyan }
function Write-STEP { param($msg) Write-Host "`n>>> $msg" -ForegroundColor Yellow }

Write-Host ""
Write-Host "============================================================" -ForegroundColor Cyan
Write-Host "  Kafka Nivel 3: Deteniendo Cluster Multi-Broker          " -ForegroundColor Cyan
Write-Host "============================================================" -ForegroundColor Cyan
Write-Host ""
Write-INFO "Los datos en los volumenes kafka-data-1/2/3 se conservan."

$scriptDir   = Split-Path -Parent $MyInvocation.MyCommand.Path
$rootDir     = Split-Path -Parent (Split-Path -Parent $scriptDir)
$composeFile = Join-Path $rootDir "docker\docker-compose-cluster.yml"

if (-not (Test-Path $composeFile)) {
    Write-Host "[ ERROR] No se encontro docker-compose-cluster.yml" -ForegroundColor Red
    exit 1
}

Write-STEP "Ejecutando docker-compose down"
Push-Location (Join-Path $rootDir "docker")
try {
    docker-compose -f docker-compose-cluster.yml down 2>&1
    if ($LASTEXITCODE -eq 0) {
        Write-OK "docker-compose down completado"
    } else {
        Write-INFO "docker-compose down termino con advertencia (puede ser normal si ya estaba detenido)"
    }
} finally {
    Pop-Location
}

Write-STEP "Verificando que los contenedores se detuvieron"
$corriendo = docker ps --filter "name=kafka-broker" --format "{{.Names}}" 2>&1
if ($corriendo) {
    Write-Host "[ WARN ] Estos contenedores siguen corriendo: $corriendo" -ForegroundColor Yellow
} else {
    Write-OK "Todos los contenedores kafka-broker detenidos"
}

Write-STEP "Volumenes persistentes disponibles"
docker volume ls --filter "name=kafka-data" 2>&1

Write-Host ""
Write-Host "============================================================" -ForegroundColor Green
Write-Host "  CLUSTER DETENIDO CORRECTAMENTE                          " -ForegroundColor Green
Write-Host "============================================================" -ForegroundColor Green
Write-Host ""
Write-Host "  Datos conservados en: kafka-data-1, kafka-data-2, kafka-data-3" -ForegroundColor White
Write-Host "  Para reiniciar: .\scripts\powershell\20-iniciar-cluster.ps1" -ForegroundColor Cyan
Write-Host ""
