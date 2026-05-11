# =============================================================
# 00-verificar-requisitos.ps1
# Verifica que el sistema cumple los requisitos para el
# laboratorio de Kafka con KRaft en Windows 10
# =============================================================

$ErrorActionPreference = "Continue"
$errores = 0
$advertencias = 0

function Write-OK    { param($msg) Write-Host "[  OK  ] $msg" -ForegroundColor Green }
function Write-ERROR { param($msg) Write-Host "[ ERROR] $msg" -ForegroundColor Red }
function Write-WARN  { param($msg) Write-Host "[ WARN ] $msg" -ForegroundColor Yellow }
function Write-INFO  { param($msg) Write-Host "[ INFO ] $msg" -ForegroundColor Cyan }
function Write-TITULO { param($msg) Write-Host "`n=== $msg ===" -ForegroundColor White }

Write-Host ""
Write-Host "============================================================" -ForegroundColor Cyan
Write-Host "  Laboratorio Kafka - Nivel 1: Verificacion de Requisitos  " -ForegroundColor Cyan
Write-Host "============================================================" -ForegroundColor Cyan
Write-Host ""

# ── 1. Docker instalado ──────────────────────────────────────
Write-TITULO "1. Verificando instalacion de Docker"

$dockerPath = Get-Command docker -ErrorAction SilentlyContinue
if ($dockerPath) {
    $dockerVersion = docker --version 2>&1
    Write-OK "Docker encontrado: $dockerVersion"
} else {
    Write-ERROR "Docker no esta instalado o no esta en el PATH"
    Write-Host "       Descargalo desde: https://www.docker.com/products/docker-desktop" -ForegroundColor Yellow
    $errores++
}

# ── 2. Docker corriendo ──────────────────────────────────────
Write-TITULO "2. Verificando que Docker Desktop esta corriendo"

if ($dockerPath) {
    $dockerPs = docker ps 2>&1
    if ($LASTEXITCODE -eq 0) {
        Write-OK "Docker Desktop esta corriendo correctamente"
    } else {
        Write-ERROR "Docker Desktop no esta corriendo. Inicia Docker Desktop y vuelve a intentar"
        $errores++
    }
} else {
    Write-WARN "Saltando verificacion (Docker no instalado)"
    $advertencias++
}

# ── 3. docker-compose disponible ────────────────────────────
Write-TITULO "3. Verificando docker-compose"

$composeCheck = docker-compose --version 2>&1
if ($LASTEXITCODE -eq 0) {
    Write-OK "docker-compose disponible: $composeCheck"
} else {
    # Intentar con plugin integrado (Docker Desktop V2)
    $composeCheck2 = Invoke-Expression "docker compose version" 2>$null
    if ($LASTEXITCODE -eq 0) {
        Write-WARN "Solo disponible como 'docker compose' (plugin). Los scripts usan 'docker-compose'. Instala docker-compose standalone."
        $advertencias++
    } else {
        Write-ERROR "docker-compose no encontrado"
        $errores++
    }
}

# ── 4. Puerto 9092 disponible ────────────────────────────────
Write-TITULO "4. Verificando disponibilidad del puerto 9092"

$puerto9092 = Get-NetTCPConnection -LocalPort 9092 -ErrorAction SilentlyContinue
if ($puerto9092) {
    $proceso = Get-Process -Id $puerto9092.OwningProcess -ErrorAction SilentlyContinue
    Write-ERROR "Puerto 9092 ocupado por proceso: $($proceso.ProcessName) (PID: $($puerto9092.OwningProcess))"
    Write-Host "       Detiene ese proceso o cambia el puerto en docker-compose.yml" -ForegroundColor Yellow
    $errores++
} else {
    Write-OK "Puerto 9092 disponible"
}

# ── 5. Puerto 9093 disponible ────────────────────────────────
Write-TITULO "5. Verificando disponibilidad del puerto 9093 (KRaft controller)"

$puerto9093 = Get-NetTCPConnection -LocalPort 9093 -ErrorAction SilentlyContinue
if ($puerto9093) {
    Write-WARN "Puerto 9093 ocupado. Puede causar conflictos con el controlador KRaft"
    $advertencias++
} else {
    Write-OK "Puerto 9093 disponible"
}

# ── 6. Memoria disponible ────────────────────────────────────
Write-TITULO "6. Verificando memoria del sistema"

$memoria = Get-CimInstance Win32_OperatingSystem
$memoriaLibreMB = [math]::Round($memoria.FreePhysicalMemory / 1024)
$memoriaTotalMB = [math]::Round($memoria.TotalVisibleMemorySize / 1024)

Write-INFO "Memoria total del sistema: $memoriaTotalMB MB"
Write-INFO "Memoria libre actualmente: $memoriaLibreMB MB"

if ($memoriaLibreMB -ge 2048) {
    Write-OK "Memoria libre suficiente para Docker ($memoriaLibreMB MB disponibles, minimo recomendado: 2048 MB)"
} elseif ($memoriaLibreMB -ge 1024) {
    Write-WARN "Memoria libre justa ($memoriaLibreMB MB). Kafka puede ir lento. Cierra aplicaciones innecesarias."
    $advertencias++
} else {
    Write-ERROR "Memoria insuficiente ($memoriaLibreMB MB libres). Necesitas al menos 1 GB libre para ejecutar Kafka"
    $errores++
}

# ── 7. Recursos asignados a Docker ──────────────────────────
Write-TITULO "7. Verificando recursos de Docker"

if ($dockerPath -and $LASTEXITCODE -eq 0) {
    $dockerInfo = docker system info 2>&1
    $memoriaDockerLine = $dockerInfo | Select-String "Total Memory"
    $cpusDockerLine    = $dockerInfo | Select-String "CPUs"
    if ($memoriaDockerLine) { Write-INFO "Docker - $memoriaDockerLine" }
    if ($cpusDockerLine)    { Write-INFO "Docker - $cpusDockerLine" }
} else {
    Write-WARN "No se puede verificar recursos de Docker (Docker no disponible)"
    $advertencias++
}

# ── 8. WSL2 habilitado ───────────────────────────────────────
Write-TITULO "8. Verificando WSL2"

$wslStatus = wsl --status 2>&1
if ($LASTEXITCODE -eq 0) {
    Write-OK "WSL2 esta habilitado"
    $wslVersion = $wslStatus | Select-String "Default Version"
    if ($wslVersion) { Write-INFO $wslVersion }
} else {
    Write-WARN "WSL2 no disponible o no configurado. Docker Desktop puede requerirlo."
    Write-Host "       Ejecuta en PowerShell admin: wsl --install" -ForegroundColor Yellow
    $advertencias++
}

# ── Resumen ──────────────────────────────────────────────────
Write-Host ""
Write-Host "============================================================" -ForegroundColor White
Write-Host "  RESUMEN DE VERIFICACION" -ForegroundColor White
Write-Host "============================================================" -ForegroundColor White
Write-Host ""

if ($errores -eq 0 -and $advertencias -eq 0) {
    Write-Host "  SISTEMA LISTO. Puedes continuar con 01-iniciar-kafka.ps1" -ForegroundColor Green
} elseif ($errores -eq 0) {
    Write-Host "  SISTEMA CASI LISTO con $advertencias advertencia(s)." -ForegroundColor Yellow
    Write-Host "  Revisa los WARN anteriores. Puedes continuar pero puede haber problemas." -ForegroundColor Yellow
} else {
    Write-Host "  SISTEMA NO LISTO: $errores error(es) y $advertencias advertencia(s)." -ForegroundColor Red
    Write-Host "  Corrige los errores marcados con [ERROR] antes de continuar." -ForegroundColor Red
}

Write-Host ""
