# =============================================================================
# 31-describir-replicas.ps1
# Nivel 4 - Análisis detallado del estado de réplicas e ISR
# Uso: .\31-describir-replicas.ps1 [-Topic <nombre>]
# =============================================================================

param(
    [string]$Topic = ""
)

$ErrorActionPreference = "SilentlyContinue"
$SCRIPT_DIR = Split-Path -Parent $MyInvocation.MyCommand.Path
$ROOT_DIR   = Split-Path -Parent (Split-Path -Parent $SCRIPT_DIR)
$RESULTADOS = Join-Path $ROOT_DIR "experimentos\resultados"
$TIMESTAMP  = Get-Date -Format "yyyyMMdd-HHmmss"

function Write-OK   { param($m) Write-Host $m -ForegroundColor Green }
function Write-Warn { param($m) Write-Host $m -ForegroundColor Yellow }
function Write-Err  { param($m) Write-Host $m -ForegroundColor Red }
function Write-Info { param($m) Write-Host $m -ForegroundColor Cyan }

Write-Host ""
Write-Host "╔══════════════════════════════════════════════════════════════╗" -ForegroundColor Magenta
Write-Host "║       NIVEL 4 - ANALISIS DE REPLICAS E ISR                   ║" -ForegroundColor Magenta
Write-Host "╚══════════════════════════════════════════════════════════════╝" -ForegroundColor Magenta

# ─────────────────────────────────────────────────────────────────────────────
# Obtener lista de topics a analizar
# ─────────────────────────────────────────────────────────────────────────────
if ($Topic -ne "") {
    $topicsAnalizar = @($Topic)
} else {
    Write-Info "`n  Sin topic especificado. Analizando todos los topics replicados..."
    $todosTopics = docker exec kafka-broker-1 /opt/kafka/bin/kafka-topics.sh `
        --bootstrap-server localhost:9092 --list 2>$null
    # Filtrar topics internos de Kafka
    $topicsAnalizar = $todosTopics | Where-Object {
        $_ -ne "" -and $_ -notmatch "^__" -and $_ -notmatch "^\s*$"
    }
}

if (-not $topicsAnalizar -or $topicsAnalizar.Count -eq 0) {
    Write-Warn "  No hay topics para analizar. Crea topics con 30-crear-topics-replicados.ps1"
    exit 0
}

# Contadores globales
$totalParticiones  = 0
$saludables        = 0
$degradadas        = 0
$criticasCount     = 0
$replicasPorBroker = @{1=0; 2=0; 3=0}
$isrPorBroker      = @{1=0; 2=0; 3=0}

$reporteLineas = @()
$reporteLineas += "REPORTE ISR - $TIMESTAMP"
$reporteLineas += "=" * 60

# ─────────────────────────────────────────────────────────────────────────────
# Analizar cada topic
# ─────────────────────────────────────────────────────────────────────────────
foreach ($topicNombre in $topicsAnalizar) {
    $topicNombre = $topicNombre.Trim()
    if ($topicNombre -eq "") { continue }

    Write-Host "`n  ─────────────────────────────────────────────────────────" -ForegroundColor DarkGray
    Write-Host "  Topic: $topicNombre" -ForegroundColor White

    $describe = docker exec kafka-broker-1 /opt/kafka/bin/kafka-topics.sh `
        --bootstrap-server localhost:9092 --describe --topic $topicNombre 2>$null

    if (-not $describe) {
        Write-Warn "    [!] No se pudo obtener informacion del topic '$topicNombre'"
        continue
    }

    # Parsear las líneas de partición
    $partLines = $describe | Where-Object { $_ -match "^\s+Topic:" -and $_ -match "Partition:" }

    foreach ($linea in $partLines) {
        # Extraer campos
        $partNum = if ($linea -match "Partition:\s*(\d+)") { [int]$Matches[1] } else { -1 }
        $leader  = if ($linea -match "Leader:\s*(\d+)") { [int]$Matches[1] } else { -1 }

        $replicasStr = if ($linea -match "Replicas:\s*([\d,]+)") { $Matches[1] } else { "" }
        $isrStr      = if ($linea -match "Isr:\s*([\d,]+)") { $Matches[1] } else { "" }

        $replicasList = $replicasStr -split "," | ForEach-Object { [int]$_.Trim() } | Where-Object { $_ -gt 0 }
        $isrList      = $isrStr      -split "," | ForEach-Object { [int]$_.Trim() } | Where-Object { $_ -gt 0 }

        $rfConfigurado = $replicasList.Count
        $isrActual     = $isrList.Count

        # Contabilizar por broker
        foreach ($r in $replicasList) {
            if ($replicasPorBroker.ContainsKey($r)) { $replicasPorBroker[$r]++ }
        }
        foreach ($i in $isrList) {
            if ($isrPorBroker.ContainsKey($i)) { $isrPorBroker[$i]++ }
        }

        # Determinar estado
        $fueraDeSync = $replicasList | Where-Object { $_ -notin $isrList }
        $estado = ""
        if ($fueraDeSync.Count -eq 0) {
            $estado = "SALUDABLE"
            $saludables++
            $icono = "  [OK]"
            $color = "Green"
        } elseif ($isrActual -le 1) {
            $estado = "CRITICO"
            $criticasCount++
            $icono = "  [!!]"
            $color = "Red"
        } else {
            $estado = "DEGRADADO"
            $degradadas++
            $icono = "  [!] "
            $color = "Yellow"
        }

        $totalParticiones++

        # Mostrar resultado de esta partición
        $replicasTexto = "[" + ($replicasList -join ", ") + "]"
        $isrTexto      = "[" + ($isrList      -join ", ") + "]"

        Write-Host ("    Partition {0,2}: Leader=Broker{1}" -f $partNum, $leader) -ForegroundColor White
        Write-Host ("      Replicas: {0}  |  ISR: {1}" -f $replicasTexto, $isrTexto) -NoNewline
        if ($fueraDeSync.Count -eq 0) {
            Write-Host "  [todas in-sync]" -ForegroundColor Green
        } else {
            Write-Host ("  [FUERA: Broker(s) {0}]" -f ($fueraDeSync -join ",")) -ForegroundColor Yellow
        }
        Write-Host ("    $icono Estado: $estado") -ForegroundColor $color

        # Agregar al reporte
        $reporteLineas += ""
        $reporteLineas += "Topic: $topicNombre | Partition: $partNum | Leader: Broker$leader"
        $reporteLineas += "  Replicas: $replicasTexto  |  ISR: $isrTexto"
        $reporteLineas += "  Estado: $estado"
    }
}

# ─────────────────────────────────────────────────────────────────────────────
# Estadísticas globales
# ─────────────────────────────────────────────────────────────────────────────
Write-Host ""
Write-Host "  ════════════════════════════════════════════════════════════" -ForegroundColor DarkGray
Write-Host "  ESTADISTICAS GLOBALES DEL CLUSTER" -ForegroundColor Magenta
Write-Host "  ════════════════════════════════════════════════════════════" -ForegroundColor DarkGray

$pctSaludables = if ($totalParticiones -gt 0) { [math]::Round($saludables*100.0/$totalParticiones,1) } else { 0 }
$pctDegradadas = if ($totalParticiones -gt 0) { [math]::Round($degradadas*100.0/$totalParticiones,1) } else { 0 }
$pctCriticas   = if ($totalParticiones -gt 0) { [math]::Round($criticasCount*100.0/$totalParticiones,1) } else { 0 }

Write-Host ("  Total particiones analizadas : {0}" -f $totalParticiones) -ForegroundColor White
Write-Host ("  [OK] Saludables (todas ISR)  : {0} ({1}%)" -f $saludables,   $pctSaludables) -ForegroundColor Green
Write-Host ("  [!]  Degradadas (ISR parcial): {0} ({1}%)" -f $degradadas,   $pctDegradadas) -ForegroundColor Yellow
Write-Host ("  [!!] Criticas   (solo leader): {0} ({1}%)" -f $criticasCount, $pctCriticas)  -ForegroundColor Red

Write-Host ""
Write-Host "  Replicas por broker (total almacenadas):" -ForegroundColor Cyan
foreach ($id in 1,2,3) {
    $total = $replicasPorBroker[$id]
    $isr   = $isrPorBroker[$id]
    $barra = "█" * $total
    Write-Host ("    Broker {0}: {1,3} replicas totales, {2,3} in-sync  {3}" -f $id, $total, $isr, $barra) -ForegroundColor White
}

if ($criticasCount -eq 0 -and $degradadas -eq 0) {
    Write-Host ""
    Write-OK "  CLUSTER COMPLETAMENTE SALUDABLE - Todas las replicas in-sync"
} elseif ($criticasCount -gt 0) {
    Write-Host ""
    Write-Err "  ALERTA: $criticasCount particion(es) CRITICA(S) - Riesgo de perdida de datos si falla el leader"
} else {
    Write-Host ""
    Write-Warn "  ADVERTENCIA: $degradadas particion(es) degradadas - Algunas replicas fuera de sync"
}

# ─────────────────────────────────────────────────────────────────────────────
# Guardar snapshot
# ─────────────────────────────────────────────────────────────────────────────
$reporteLineas += ""
$reporteLineas += "─" * 60
$reporteLineas += "Total: $totalParticiones  Saludables: $saludables  Degradadas: $degradadas  Criticas: $criticasCount"

$archivoSnap = "$RESULTADOS\isr-snapshot-$TIMESTAMP.txt"
$reporteLineas | Out-File -FilePath $archivoSnap -Encoding UTF8
Write-Host ""
Write-Host "  Snapshot guardado: $archivoSnap" -ForegroundColor Gray
Write-Host ""
