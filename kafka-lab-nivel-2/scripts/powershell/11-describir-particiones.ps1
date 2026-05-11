# =============================================================
# 11-describir-particiones.ps1
# Muestra informacion detallada de particiones para cada topic
# del Nivel 2. Ejecuta dentro del contenedor via docker exec.
# =============================================================

$ErrorActionPreference = "Continue"
$CONTENEDOR = "kafka-nivel1"
$BOOTSTRAP  = "localhost:9092"
$TOPICS     = @("transacciones-1p", "transacciones-4p", "transacciones-8p")

function Write-OK    { param($msg) Write-Host "[  OK  ] $msg" -ForegroundColor Green }
function Write-ERROR { param($msg) Write-Host "[ ERROR] $msg" -ForegroundColor Red }
function Write-INFO  { param($msg) Write-Host "[ INFO ] $msg" -ForegroundColor Cyan }
function Write-STEP  { param($msg) Write-Host "`n>>> $msg" -ForegroundColor Yellow }
function Write-SEP   { Write-Host "============================================================" -ForegroundColor DarkGray }

Write-Host ""
Write-Host "============================================================" -ForegroundColor Cyan
Write-Host "  Kafka Nivel 2: Descripcion de Particiones               " -ForegroundColor Cyan
Write-Host "============================================================" -ForegroundColor Cyan

# ── Verificar contenedor ─────────────────────────────────────
$estado = docker inspect $CONTENEDOR --format "{{.State.Status}}" 2>&1
if ($LASTEXITCODE -ne 0 -or $estado -ne "running") {
    Write-ERROR "Contenedor '$CONTENEDOR' no esta corriendo."
    exit 1
}

foreach ($topic in $TOPICS) {
    Write-STEP "Topic: $topic"

    # ── Descripcion completa del topic ───────────────────────
    Write-INFO "Estructura de particiones:"
    docker exec $CONTENEDOR /opt/kafka/bin/kafka-topics.sh `
        --bootstrap-server $BOOTSTRAP `
        --describe `
        --topic $topic 2>&1

    # ── Configuracion especifica del topic ───────────────────
    Write-INFO "Configuracion del topic:"
    docker exec $CONTENEDOR /opt/kafka/bin/kafka-configs.sh `
        --bootstrap-server $BOOTSTRAP `
        --describe `
        --entity-type topics `
        --entity-name $topic 2>&1

    # ── Estado de offsets usando herramientas Kafka 4.0 ───────
    Write-INFO "Estado de offsets:"
    
    # Método 1: Verificar posiciones actuales via consumer group
    $groupId = "offset-check-$(Get-Random)"
    docker exec $CONTENEDOR /opt/kafka/bin/kafka-consumer-groups.sh `
        --bootstrap-server $BOOTSTRAP `
        --group $groupId `
        --topic $topic `
        --reset-offsets `
        --to-latest `
        --dry-run 2>&1 | Select-String "%-"
    
    # Método 2: Mostrar info de segmentos de log
    Write-INFO "Segmentos de log:"
    docker exec $CONTENEDOR bash -c "find /tmp/kafka-logs -name '$topic-*' -type d 2>/dev/null | head -3" 2>&1

    Write-SEP
}

Write-Host ""
Write-Host "Lectura de la tabla de descripcion de particiones:" -ForegroundColor White
Write-Host "  Partition  -> numero de la particion (0-based)" -ForegroundColor Gray
Write-Host "  Leader     -> ID del broker que lidera esta particion" -ForegroundColor Gray
Write-Host "  Replicas   -> brokers que tienen copia de esta particion" -ForegroundColor Gray
Write-Host "  Isr        -> In-Sync Replicas: brokers con datos al dia" -ForegroundColor Gray
Write-Host ""
Write-Host "  Con 1 broker, Leader=Replicas=Isr siempre es el broker 1." -ForegroundColor Yellow
Write-Host "  En produccion con 3 brokers, verias valores distintos." -ForegroundColor Yellow
Write-Host ""
