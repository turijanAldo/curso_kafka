# =============================================================================
# 30-crear-topics-replicados.ps1
# Nivel 4 - Réplicas y Tolerancia a Fallos
# Crea topics con diferentes factores de replicación para experimentación
# =============================================================================

$ErrorActionPreference = "Stop"
$SCRIPT_DIR = Split-Path -Parent $MyInvocation.MyCommand.Path
$ROOT_DIR   = Split-Path -Parent (Split-Path -Parent $SCRIPT_DIR)
$RESULTADOS = Join-Path $ROOT_DIR "experimentos\resultados"

# Colores
function Write-Header  { param($m) Write-Host "`n$m" -ForegroundColor Cyan }
function Write-OK      { param($m) Write-Host "  [OK] $m" -ForegroundColor Green }
function Write-Info    { param($m) Write-Host "  [>>] $m" -ForegroundColor White }
function Write-Warn    { param($m) Write-Host "  [!] $m"  -ForegroundColor Yellow }
function Write-Titulo  { param($m) Write-Host $m -ForegroundColor Magenta }

Write-Host ""
Write-Titulo "╔══════════════════════════════════════════════════════════════╗"
Write-Titulo "║       NIVEL 4 - CREAR TOPICS REPLICADOS                      ║"
Write-Titulo "╚══════════════════════════════════════════════════════════════╝"

# ─────────────────────────────────────────────────────────────────────────────
# Verificar que el clúster está corriendo
# ─────────────────────────────────────────────────────────────────────────────
Write-Header "Verificando clúster de 3 brokers..."

$brokers = @("kafka-broker-1","kafka-broker-2","kafka-broker-3")
$activos = 0
foreach ($b in $brokers) {
    $estado = docker inspect --format='{{.State.Status}}' $b 2>$null
    if ($estado -eq "running") {
        Write-OK "$b esta corriendo"
        $activos++
    } else {
        Write-Warn "$b NO esta corriendo"
    }
}

if ($activos -lt 3) {
    Write-Host "`n  [ERROR] Se necesitan los 3 brokers. Ejecuta 20-iniciar-cluster.ps1 primero." -ForegroundColor Red
    exit 1
}

# Función auxiliar: crear topic con kafka-topics.sh dentro del broker-1
function New-KafkaTopic {
    param(
        [string]$Nombre,
        [int]$Particiones,
        [int]$ReplicationFactor,
        [string]$ConfigExtra = ""
    )

    # Verificar si ya existe
    $existe = docker exec kafka-broker-1 /opt/kafka/bin/kafka-topics.sh `
        --bootstrap-server localhost:9092 --list 2>$null | Select-String -Pattern "^$Nombre$"

    if ($existe) {
        Write-Warn "Topic '$Nombre' ya existe. Eliminando para recrear..."
        docker exec kafka-broker-1 /opt/kafka/bin/kafka-topics.sh `
            --bootstrap-server localhost:9092 --delete --topic $Nombre 2>$null | Out-Null
        Start-Sleep -Seconds 2
    }

    Write-Info "Creando topic '$Nombre' (particiones=$Particiones, RF=$ReplicationFactor)..."

    if ($ConfigExtra -ne "") {
        docker exec kafka-broker-1 /opt/kafka/bin/kafka-topics.sh `
            --bootstrap-server localhost:9092 `
            --create `
            --topic $Nombre `
            --partitions $Particiones `
            --replication-factor $ReplicationFactor `
            --config $ConfigExtra 2>&1 | Out-Null
    } else {
        docker exec kafka-broker-1 /opt/kafka/bin/kafka-topics.sh `
            --bootstrap-server localhost:9092 `
            --create `
            --topic $Nombre `
            --partitions $Particiones `
            --replication-factor $ReplicationFactor 2>&1 | Out-Null
    }

    Start-Sleep -Milliseconds 500
    Write-OK "Topic '$Nombre' creado"
}

# ─────────────────────────────────────────────────────────────────────────────
# Topic 1: transacciones-rf1 (RF=1 - línea base sin replicación real)
# ─────────────────────────────────────────────────────────────────────────────
Write-Header "Topic 1/4: transacciones-rf1 (RF=1 - sin redundancia)"
Write-Info "RF=1 significa que cada particion existe en UN SOLO broker."
Write-Info "Si ese broker falla, los datos de esa particion se pierden."
Write-Info "Usamos este topic como linea base para comparar con topics replicados."

New-KafkaTopic -Nombre "transacciones-rf1" -Particiones 4 -ReplicationFactor 1

$desc1 = docker exec kafka-broker-1 /opt/kafka/bin/kafka-topics.sh `
    --bootstrap-server localhost:9092 --describe --topic transacciones-rf1
Write-Host ($desc1 -join "`n") -ForegroundColor Gray
$desc1 | Out-File -FilePath "$RESULTADOS\topic-transacciones-rf1-estado-inicial.txt" -Encoding UTF8

# ─────────────────────────────────────────────────────────────────────────────
# Topic 2: transacciones-rf2 (RF=2 - tolera 1 fallo de broker)
# ─────────────────────────────────────────────────────────────────────────────
Write-Header "Topic 2/4: transacciones-rf2 (RF=2 - tolera 1 fallo)"
Write-Info "RF=2 significa que cada particion existe en DOS brokers:"
Write-Info "  - Uno como leader (sirve lecturas y escrituras)"
Write-Info "  - Uno como follower (replica datos del leader)"
Write-Info "Si el leader falla, el follower se convierte automaticamente en nuevo leader."
Write-Info "Puedes perder hasta 1 broker sin perder datos."

New-KafkaTopic -Nombre "transacciones-rf2" -Particiones 4 -ReplicationFactor 2

$desc2 = docker exec kafka-broker-1 /opt/kafka/bin/kafka-topics.sh `
    --bootstrap-server localhost:9092 --describe --topic transacciones-rf2
Write-Host ($desc2 -join "`n") -ForegroundColor Gray
$desc2 | Out-File -FilePath "$RESULTADOS\topic-transacciones-rf2-estado-inicial.txt" -Encoding UTF8

# ─────────────────────────────────────────────────────────────────────────────
# Topic 3: transacciones-rf3 (RF=3 - máxima redundancia con 3 brokers)
# ─────────────────────────────────────────────────────────────────────────────
Write-Header "Topic 3/4: transacciones-rf3 (RF=3 - maxima redundancia)"
Write-Info "RF=3 es el maximo posible con 3 brokers."
Write-Info "Cada particion existe en los 3 brokers SIMULTÁNEAMENTE."
Write-Info "Puedes perder hasta 2 brokers sin perder datos ni disponibilidad."
Write-Info "Precio: cada mensaje ocupa 3x mas espacio de disco en total."

New-KafkaTopic -Nombre "transacciones-rf3" -Particiones 4 -ReplicationFactor 3

$desc3 = docker exec kafka-broker-1 /opt/kafka/bin/kafka-topics.sh `
    --bootstrap-server localhost:9092 --describe --topic transacciones-rf3
Write-Host ($desc3 -join "`n") -ForegroundColor Gray
$desc3 | Out-File -FilePath "$RESULTADOS\topic-transacciones-rf3-estado-inicial.txt" -Encoding UTF8

# ─────────────────────────────────────────────────────────────────────────────
# Topic 4: critical-data (RF=3, min.insync.replicas=2)
# ─────────────────────────────────────────────────────────────────────────────
Write-Header "Topic 4/4: critical-data (RF=3 + min.insync.replicas=2)"
Write-Info "Este topic tiene una configuracion extra: min.insync.replicas=2"
Write-Info "Significa que el producer (con acks=all) NO confirmara la escritura"
Write-Info "hasta que AL MENOS 2 replicas hayan guardado el mensaje."
Write-Info "Esto garantiza que incluso si el leader falla inmediatamente despues"
Write-Info "de confirmar, hay al menos otra replica con el dato completo."
Write-Info "Ideal para datos criticos: transacciones, auditorias, comandos de control."

New-KafkaTopic -Nombre "critical-data" -Particiones 2 -ReplicationFactor 3 `
    -ConfigExtra "min.insync.replicas=2"

$desc4 = docker exec kafka-broker-1 /opt/kafka/bin/kafka-topics.sh `
    --bootstrap-server localhost:9092 --describe --topic critical-data
Write-Host ($desc4 -join "`n") -ForegroundColor Gray
$desc4 | Out-File -FilePath "$RESULTADOS\topic-critical-data-estado-inicial.txt" -Encoding UTF8

# ─────────────────────────────────────────────────────────────────────────────
# Resumen visual de distribución de réplicas por broker
# ─────────────────────────────────────────────────────────────────────────────
Write-Header "Resumen: distribucion de replicas por broker"
Write-Info "Contando cuantas replicas (como leader O follower) tiene cada broker..."

$todosTopics = @("transacciones-rf1","transacciones-rf2","transacciones-rf3","critical-data")
$contadorBroker = @{1=0; 2=0; 3=0}

foreach ($topic in $todosTopics) {
    $salida = docker exec kafka-broker-1 /opt/kafka/bin/kafka-topics.sh `
        --bootstrap-server localhost:9092 --describe --topic $topic 2>$null
    foreach ($linea in $salida) {
        if ($linea -match "Replicas:\s*([\d,]+)") {
            $replicas = $Matches[1] -split "," | ForEach-Object { $_.Trim() }
            foreach ($r in $replicas) {
                if ($contadorBroker.ContainsKey([int]$r)) {
                    $contadorBroker[[int]$r]++
                }
            }
        }
    }
}

$total = ($contadorBroker[1] + $contadorBroker[2] + $contadorBroker[3])
foreach ($id in 1,2,3) {
    $pct = if ($total -gt 0) { [math]::Round($contadorBroker[$id]*100.0/$total,1) } else { 0 }
    $barras = [math]::Round($contadorBroker[$id] / 2)
    $barra = "█" * $barras
    Write-Host ("  Broker {0}: {1,3} replicas ({2,5}%)  {3}" -f $id, $contadorBroker[$id], $pct, $barra) -ForegroundColor Cyan
}

if ([math]::Abs($contadorBroker[1] - $contadorBroker[2]) -le 2 -and
    [math]::Abs($contadorBroker[2] - $contadorBroker[3]) -le 2) {
    Write-OK "Distribucion de replicas balanceada entre los 3 brokers"
} else {
    Write-Warn "Distribucion levemente desbalanceada (normal con pocos topics)"
}

Write-Host ""
Write-Host "  Estados guardados en: experimentos\resultados\" -ForegroundColor Gray
Write-Host ""
Write-Host "  Proximos pasos:" -ForegroundColor Yellow
Write-Host "    .\scripts\powershell\31-describir-replicas.ps1   (analisis detallado del ISR)" -ForegroundColor White
Write-Host "    .\scripts\powershell\32-simular-fallo-broker.ps1 (simula caida de un broker)" -ForegroundColor White
Write-Host ""
