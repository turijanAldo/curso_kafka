# =============================================================
# 21-verificar-cluster.ps1
# Fotografia completa del estado del cluster de 3 brokers:
# brokers registrados, topics existentes, distribucion de
# leaders por particion y uso de recursos de cada contenedor.
# =============================================================

$ErrorActionPreference = "Continue"
$BOOTSTRAP = "localhost:9092,localhost:9093,localhost:9094"
$BROKERS   = @("kafka-broker-1", "kafka-broker-2", "kafka-broker-3")

function Write-OK   { param($msg) Write-Host "[  OK  ] $msg" -ForegroundColor Green }
function Write-INFO { param($msg) Write-Host "[ INFO ] $msg" -ForegroundColor Cyan }
function Write-STEP { param($msg) Write-Host "`n>>> $msg" -ForegroundColor Yellow }
function Write-SEP  { Write-Host "------------------------------------------------------------" -ForegroundColor DarkGray }

Write-Host ""
Write-Host "============================================================" -ForegroundColor Cyan
Write-Host "  Kafka Nivel 3: Verificacion del Cluster Multi-Broker    " -ForegroundColor Cyan
Write-Host "============================================================" -ForegroundColor Cyan

# ── Verificar que los 3 contenedores corren ──────────────────
Write-STEP "1. Estado de los contenedores"
foreach ($c in $BROKERS) {
    $estado = docker inspect $c --format "{{.State.Status}}" 2>&1
    if ($estado -eq "running") {
        Write-OK "$c -> $estado"
    } else {
        Write-Host "[ WARN ] $c -> $estado" -ForegroundColor Yellow
    }
}

# ── Brokers registrados en el cluster ───────────────────────
Write-STEP "2. Brokers registrados (desde perspectiva del Broker 1)"
Write-SEP
docker exec kafka-broker-1 /opt/kafka/bin/kafka-broker-api-versions.sh `
    --bootstrap-server localhost:9092 2>&1 | Select-String "id:|version" | Select-Object -First 20
Write-SEP

# ── Estado del quorum KRaft ──────────────────────────────────
Write-STEP "3. Estado del quorum KRaft (leader election)"
Write-SEP
docker exec kafka-broker-1 /opt/kafka/bin/kafka-metadata-quorum.sh `
    --bootstrap-server localhost:9092 `
    describe --status 2>&1
Write-SEP

# ── Topics existentes ────────────────────────────────────────
Write-STEP "4. Topics en el cluster"
$topics = docker exec kafka-broker-1 /opt/kafka/bin/kafka-topics.sh `
    --bootstrap-server localhost:9092 `
    --list 2>&1

$userTopics = $topics | Where-Object { $_ -notmatch "^__" -and $_ -ne "" }
if ($userTopics) {
    Write-INFO "Topics de usuario encontrados:"
    $userTopics | ForEach-Object { Write-Host "    - $_" -ForegroundColor White }
} else {
    Write-INFO "No hay topics de usuario creados aun."
}

# ── Descripcion detallada de cada topic ──────────────────────
if ($userTopics) {
    Write-STEP "5. Distribucion de particiones por topic"

    foreach ($topic in $userTopics) {
        Write-Host "`n  📊 Topic: $topic" -ForegroundColor Yellow
        Write-SEP

        $describe = docker exec kafka-broker-1 /opt/kafka/bin/kafka-topics.sh `
            --bootstrap-server localhost:9092 `
            --describe `
            --topic $topic 2>&1

        $describe | ForEach-Object { Write-Host "  $_" }

        # ── Parsear y resumir distribucion de leaders ────────
        $leaderCount = @{1=0; 2=0; 3=0}
        $describe | Where-Object { $_ -match "Leader:\s*(\d+)" } | ForEach-Object {
            if ($_ -match "Leader:\s*(\d+)") {
                $lid = [int]$Matches[1]
                if ($leaderCount.ContainsKey($lid)) { $leaderCount[$lid]++ }
            }
        }
        $total = ($leaderCount.Values | Measure-Object -Sum).Sum
        if ($total -gt 0) {
            Write-Host "`n  Resumen de leaders:" -ForegroundColor Cyan
            foreach ($bid in 1..3) {
                $cnt = $leaderCount[$bid]
                $pct = if ($total -gt 0) { [math]::Round(($cnt / $total) * 100, 1) } else { 0 }
                $bar = "█" * $cnt
                Write-Host "    Broker $bid : $bar $cnt particiones ($pct%)" -ForegroundColor White
            }
        }
        Write-SEP
    }
}

# ── Recursos de los contenedores ─────────────────────────────
Write-STEP "6. Uso de recursos de los 3 brokers"
Write-INFO "Ejecutando docker stats (snapshot unico, no interactivo)"
docker stats kafka-broker-1 kafka-broker-2 kafka-broker-3 --no-stream `
    --format "table {{.Name}}\t{{.CPUPerc}}\t{{.MemUsage}}\t{{.NetIO}}" 2>&1

Write-Host ""
Write-Host "============================================================" -ForegroundColor Green
Write-Host "  VERIFICACION COMPLETADA                                  " -ForegroundColor Green
Write-Host "============================================================" -ForegroundColor Green
Write-Host ""
