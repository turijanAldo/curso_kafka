# =============================================================
# 10-crear-topics-particionados.ps1
# Crea tres topics con diferente numero de particiones para
# demostrar el impacto del particionamiento en el paralelismo.
#
# Por que diferentes numeros de particiones?
#   - Mas particiones = mas paralelismo potencial
#   - Mas particiones = mas consumidores pueden trabajar en paralelo
#   - El limite es: 1 particion por consumidor en el mismo grupo
#   - Menos particiones = mas orden garantizado, menos paralelismo
# =============================================================

$ErrorActionPreference = "Continue"
$CONTENEDOR  = "kafka-nivel1"
$BOOTSTRAP   = "localhost:9092"

function Write-OK    { param($msg) Write-Host "[  OK  ] $msg" -ForegroundColor Green }
function Write-ERROR { param($msg) Write-Host "[ ERROR] $msg" -ForegroundColor Red }
function Write-INFO  { param($msg) Write-Host "[ INFO ] $msg" -ForegroundColor Cyan }
function Write-STEP  { param($msg) Write-Host "`n>>> $msg" -ForegroundColor Yellow }
function Write-SEP   { Write-Host "------------------------------------------------------------" -ForegroundColor DarkGray }

Write-Host ""
Write-Host "============================================================" -ForegroundColor Cyan
Write-Host "  Kafka Nivel 2: Creacion de Topics Particionados          " -ForegroundColor Cyan
Write-Host "============================================================" -ForegroundColor Cyan

# ── Verificar que el contenedor esta corriendo ───────────────
$estado = docker inspect $CONTENEDOR --format "{{.State.Status}}" 2>&1
if ($LASTEXITCODE -ne 0 -or $estado -ne "running") {
    Write-ERROR "El contenedor '$CONTENEDOR' no esta corriendo."
    Write-Host "  Inicia Kafka con: ..\kafka-lab-nivel-1\scripts\powershell\01-iniciar-kafka.ps1" -ForegroundColor Yellow
    exit 1
}
Write-OK "Contenedor '$CONTENEDOR' activo"

# ── Funcion para crear un topic ──────────────────────────────
function New-KafkaTopic {
    param(
        [string]$TopicName,
        [int]$Particiones,
        [int]$ReplicationFactor = 1
    )

    Write-STEP "Creando topic: $TopicName ($Particiones particion(es))"

    # Verificar si ya existe
    $existe = docker exec $CONTENEDOR /opt/kafka/bin/kafka-topics.sh `
        --bootstrap-server $BOOTSTRAP `
        --list 2>&1 | Select-String "^$TopicName$"

    if ($existe) {
        Write-INFO "El topic '$TopicName' ya existe. Eliminando para recrear limpio..."
        docker exec $CONTENEDOR /opt/kafka/bin/kafka-topics.sh `
            --bootstrap-server $BOOTSTRAP `
            --delete `
            --topic $TopicName 2>&1 | Out-Null
        Start-Sleep -Seconds 2
    }

    # Crear el topic
    docker exec $CONTENEDOR /opt/kafka/bin/kafka-topics.sh `
        --bootstrap-server $BOOTSTRAP `
        --create `
        --topic $TopicName `
        --partitions $Particiones `
        --replication-factor $ReplicationFactor 2>&1

    if ($LASTEXITCODE -eq 0) {
        Write-OK "Topic '$TopicName' creado con $Particiones particion(es)"
    } else {
        Write-ERROR "Error al crear '$TopicName'"
        return
    }

    # Describir el topic recien creado
    Write-INFO "Descripcion del topic '$TopicName':"
    Write-SEP
    docker exec $CONTENEDOR /opt/kafka/bin/kafka-topics.sh `
        --bootstrap-server $BOOTSTRAP `
        --describe `
        --topic $TopicName 2>&1
    Write-SEP
}

# ── Crear los tres topics ─────────────────────────────────────
# 1 particion: maximo orden, minimo paralelismo
New-KafkaTopic -TopicName "transacciones-1p" -Particiones 1

# 4 particiones: balance entre orden por clave y paralelismo
New-KafkaTopic -TopicName "transacciones-4p" -Particiones 4

# 8 particiones: maximo paralelismo en este laboratorio
New-KafkaTopic -TopicName "transacciones-8p" -Particiones 8

# ── Listar todos los topics creados ─────────────────────────
Write-STEP "Topics disponibles en el cluster"
Write-SEP
docker exec $CONTENEDOR /opt/kafka/bin/kafka-topics.sh `
    --bootstrap-server $BOOTSTRAP `
    --list 2>&1
Write-SEP

Write-Host ""
Write-Host "============================================================" -ForegroundColor Green
Write-Host "  TOPICS CREADOS CORRECTAMENTE                            " -ForegroundColor Green
Write-Host "============================================================" -ForegroundColor Green
Write-Host ""
Write-Host "  transacciones-1p  -> 1 particion   (1 consumer max en paralelo)" -ForegroundColor White
Write-Host "  transacciones-4p  -> 4 particiones (4 consumers max en paralelo)" -ForegroundColor White
Write-Host "  transacciones-8p  -> 8 particiones (8 consumers max en paralelo)" -ForegroundColor White
Write-Host ""
Write-Host "  Siguiente: ejecuta 11-describir-particiones.ps1" -ForegroundColor Cyan
Write-Host ""
