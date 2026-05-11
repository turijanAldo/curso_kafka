# =============================================================
# 01-iniciar-kafka.ps1
# Inicia el cluster de Kafka usando Docker Compose
# =============================================================

$ErrorActionPreference = "Stop"

function Write-OK    { param($msg) Write-Host "[  OK  ] $msg" -ForegroundColor Green }
function Write-ERROR { param($msg) Write-Host "[ ERROR] $msg" -ForegroundColor Red }
function Write-INFO  { param($msg) Write-Host "[ INFO ] $msg" -ForegroundColor Cyan }
function Write-STEP  { param($msg) Write-Host "`n>>> $msg" -ForegroundColor Yellow }

Write-Host ""
Write-Host "============================================================" -ForegroundColor Cyan
Write-Host "  Laboratorio Kafka - Nivel 1: Iniciando Cluster           " -ForegroundColor Cyan
Write-Host "============================================================" -ForegroundColor Cyan

# ── 1. Verificar directorio correcto ─────────────────────────
Write-STEP "Paso 1: Verificando directorio de trabajo"

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$rootDir   = Split-Path -Parent (Split-Path -Parent $scriptDir)
$composeFile = Join-Path $rootDir "docker\docker-compose.yml"

if (-not (Test-Path $composeFile)) {
    Write-ERROR "No se encontro docker\docker-compose.yml en: $rootDir"
    Write-Host "  Ejecuta este script desde la raiz del laboratorio" -ForegroundColor Yellow
    exit 1
}

Write-OK "Directorio correcto: $rootDir"
Set-Location $rootDir

# ── 2. Levantar contenedor ───────────────────────────────────
Write-STEP "Paso 2: Levantando contenedor Kafka con docker-compose"

Write-INFO "Ejecutando: docker-compose up -d"
Push-Location "docker"
try {
    docker-compose up -d
    if ($LASTEXITCODE -ne 0) {
        Write-ERROR "docker-compose up fallo con codigo $LASTEXITCODE"
        exit 1
    }
} finally {
    Pop-Location
}

Write-OK "Comando docker-compose ejecutado"

# ── 3. Esperar a que Kafka este listo ────────────────────────
Write-STEP "Paso 3: Esperando que Kafka este listo (hasta 60 segundos)"

$maxEspera  = 60
$intervalo  = 5
$transcurrido = 0
$listo = $false

while ($transcurrido -lt $maxEspera) {
    $progreso = [math]::Round(($transcurrido / $maxEspera) * 100)
    $barraLlena  = [math]::Round($progreso / 5)
    $barraVacia  = 20 - $barraLlena
    $barra = ("[" + ("#" * $barraLlena) + ("." * $barraVacia) + "]")
    Write-Host "`r  $barra $progreso% - $transcurrido/$maxEspera segundos..." -NoNewline

    # Probar conexion al puerto 9092
    $conexion = Test-NetConnection -ComputerName localhost -Port 9092 -WarningAction SilentlyContinue -ErrorAction SilentlyContinue
    if ($conexion.TcpTestSucceeded) {
        $listo = $true
        break
    }

    Start-Sleep -Seconds $intervalo
    $transcurrido += $intervalo
}

Write-Host ""  # Nueva linea tras la barra de progreso

if ($listo) {
    Write-OK "Kafka respondiendo en localhost:9092 (tardó $transcurrido segundos)"
} else {
    Write-ERROR "Kafka no respondio en $maxEspera segundos. Revisa los logs:"
    Write-Host "  docker logs kafka-nivel1" -ForegroundColor Yellow
    exit 1
}

# ── 4. Verificar estado del contenedor ──────────────────────
Write-STEP "Paso 4: Estado del contenedor"

$contenedorInfo = docker ps --filter "name=kafka-nivel1" --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}" 2>&1
Write-INFO "Contenedores activos:"
Write-Host $contenedorInfo

# ── 5. Mostrar ultimos logs ──────────────────────────────────
Write-STEP "Paso 5: Ultimos logs del contenedor"

Write-INFO "Mostrando ultimas 20 lineas de log:"
docker logs kafka-nivel1 --tail 20 2>&1

# ── Resumen ──────────────────────────────────────────────────
Write-Host ""
Write-Host "============================================================" -ForegroundColor Green
Write-Host "  KAFKA INICIADO CORRECTAMENTE                             " -ForegroundColor Green
Write-Host "============================================================" -ForegroundColor Green
Write-Host ""
Write-Host "  Broker accesible en: localhost:9092" -ForegroundColor White
Write-Host ""
Write-Host "  Siguientes pasos:" -ForegroundColor White
Write-Host "    1. Ejecuta: .\scripts\powershell\02-verificar-cluster.ps1" -ForegroundColor Cyan
Write-Host "    2. O sigue las instrucciones en: INSTRUCCIONES-NIVEL-1.md" -ForegroundColor Cyan
Write-Host ""
