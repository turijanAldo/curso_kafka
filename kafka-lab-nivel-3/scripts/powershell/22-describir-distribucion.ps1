# =============================================================
# 22-describir-distribucion.ps1
# Analisis detallado de como estan distribuidas las particiones
# entre los 3 brokers. Calcula estadisticas de balance y genera
# visualizacion ASCII de la carga por broker.
#
# Uso:
#   .\22-describir-distribucion.ps1              # analiza todos los topics
#   .\22-describir-distribucion.ps1 mi-topic     # analiza un topic especifico
# =============================================================

param(
    [string]$TopicFiltro = ""   # Si se especifica, analiza solo ese topic
)

$ErrorActionPreference = "Continue"
$RESULTADOS_FILE = "$PSScriptRoot\..\..\experimentos\resultados\distribucion-leaders.txt"

function Write-INFO  { param($msg) Write-Host "[ INFO ] $msg" -ForegroundColor Cyan }
function Write-WARN  { param($msg) Write-Host "[ WARN ] $msg" -ForegroundColor Yellow }
function Write-OK    { param($msg) Write-Host "[  OK  ] $msg" -ForegroundColor Green }
function Write-SEP   { Write-Host "============================================================" -ForegroundColor DarkGray }

Write-Host ""
Write-Host "============================================================" -ForegroundColor Cyan
Write-Host "  Kafka Nivel 3: Analisis de Distribucion de Particiones  " -ForegroundColor Cyan
Write-Host "============================================================" -ForegroundColor Cyan

# ── Verificar que al menos broker-1 corre ────────────────────
$estado = docker inspect kafka-broker-1 --format "{{.State.Status}}" 2>&1
if ($estado -ne "running") {
    Write-Host "[ ERROR] kafka-broker-1 no esta corriendo." -ForegroundColor Red
    exit 1
}

# ── Obtener lista de topics ───────────────────────────────────
$todosTopics = docker exec kafka-broker-1 /opt/kafka/bin/kafka-topics.sh `
    --bootstrap-server localhost:9092 `
    --list 2>&1 | Where-Object { $_ -notmatch "^__" -and $_ -ne "" }

if ($TopicFiltro) {
    $topics = $todosTopics | Where-Object { $_ -eq $TopicFiltro }
    if (-not $topics) {
        Write-Host "[ ERROR] Topic '$TopicFiltro' no encontrado." -ForegroundColor Red
        exit 1
    }
} else {
    $topics = $todosTopics
}

if (-not $topics) {
    Write-INFO "No hay topics de usuario. Crealos con exp-05 primero."
    exit 0
}

# ── Iniciar archivo de resultados ────────────────────────────
$timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
"=== Reporte de Distribucion de Leaders ===" | Out-File $RESULTADOS_FILE -Encoding utf8
"Generado: $timestamp"                       | Out-File $RESULTADOS_FILE -Append -Encoding utf8
""                                           | Out-File $RESULTADOS_FILE -Append -Encoding utf8

# ── Analizar cada topic ──────────────────────────────────────
foreach ($topic in $topics) {
    Write-Host ""
    Write-Host "  [TOPIC] $topic" -ForegroundColor Yellow
    Write-SEP

    $describe = docker exec kafka-broker-1 /opt/kafka/bin/kafka-topics.sh `
        --bootstrap-server localhost:9092 `
        --describe `
        --topic $topic 2>&1

    # ── Extraer datos por particion ──────────────────────────
    $particiones = @()
    $describe | Where-Object { $_ -match "Partition:\s*\d+" -and $_ -match "Leader:" } | ForEach-Object {
        $linea = $_
        $regex_replicas = 'Replicas:\s*([\d,]+)'
        $regex_isr      = 'Isr:\s*([\d,]+)'

        $p_val = if ($linea -match 'Partition:\s*(\d+)') { $Matches[1] } else { 0 }
        $l_val = if ($linea -match 'Leader:\s*(\d+)')    { $Matches[1] } else { 0 }
        $r_val = if ($linea -match $regex_replicas)      { $Matches[1] } else { "" }
        $i_val = if ($linea -match $regex_isr)           { $Matches[1] } else { "" }

        $particiones += [PSCustomObject]@{
            Part     = [int]$p_val
            Leader   = [int]$l_val
            Replicas = $r_val
            ISR      = $i_val
        }
    }

    # ── Mostrar tabla de particiones ─────────────────────────
    Write-Host "  Partition  Leader   Replicas   ISR" -ForegroundColor White
    Write-Host "  ---------  ------   --------   ---" -ForegroundColor DarkGray
    foreach ($p in $particiones | Sort-Object Part) {
        Write-Host ("  {0,-10} {1,-8} {2,-10} {3}" -f $p.Part, "Broker $($p.Leader)", $p.Replicas, $p.ISR)
    }

    # ── Calcular estadisticas de balance ─────────────────────
    $leaderCount = @{}
    foreach ($p in $particiones) {
        if (-not $leaderCount.ContainsKey($p.Leader)) { $leaderCount[$p.Leader] = 0 }
        $leaderCount[$p.Leader]++
    }

    $totalParticiones = $particiones.Count
    $maxParts = ($leaderCount.Values | Measure-Object -Maximum).Maximum
    $minParts = ($leaderCount.Values | Measure-Object -Minimum).Minimum

    Write-Host ""
    Write-Host "  Distribucion de leaders por broker:" -ForegroundColor Cyan
    foreach ($bid in $leaderCount.Keys | Sort-Object) {
        $cnt  = $leaderCount[$bid]
        $pct  = [math]::Round(($cnt / $totalParticiones) * 100, 1)
        $bar  = "#" * $cnt + " " * ([math]::Max(0, 10 - $cnt))
        Write-Host "    Broker $bid : $bar $cnt particiones ($pct%)" -ForegroundColor White
    }

    # ── Evaluar balance ──────────────────────────────────────
    Write-Host ""
    if ($maxParts -le 0) {
        Write-WARN "No se pudieron leer las particiones"
    } elseif ($minParts -gt 0 -and ($maxParts / $minParts) -le 1.5) {
        Write-OK "Distribucion BALANCEADA (max=$maxParts, min=$minParts)"
    } else {
        $desviacion = $maxParts - $minParts
        Write-WARN "Distribucion DESBALANCEADA: diferencia de $desviacion particiones entre brokers"
        Write-Host "    Considera usar un numero de particiones multiplo de 3" -ForegroundColor Yellow
    }

    # ── Guardar en archivo ───────────────────────────────────
    "Topic: $topic ($totalParticiones particiones)" | Out-File $RESULTADOS_FILE -Append -Encoding utf8
    foreach ($p in $particiones | Sort-Object Part) {
        "  Partition $($p.Part) -> Leader: Broker $($p.Leader) | Replicas: [$($p.Replicas)] | ISR: [$($p.ISR)]" |
            Out-File $RESULTADOS_FILE -Append -Encoding utf8
    }
    "" | Out-File $RESULTADOS_FILE -Append -Encoding utf8
    Write-SEP
}

Write-OK "Reporte guardado en: experimentos\resultados\distribucion-leaders.txt"
Write-Host ""