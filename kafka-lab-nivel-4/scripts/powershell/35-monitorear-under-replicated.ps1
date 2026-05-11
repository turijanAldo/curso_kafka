# =============================================================================
# 35-monitorear-under-replicated.ps1
# Nivel 4 - Monitorear particiones under-replicated con alertas y tendencias
# Uso: .\35-monitorear-under-replicated.ps1 [-UmbralWarning <pct>] [-UmbralCritico <pct>]
# =============================================================================

param(
    [int]$UmbralWarning = 5,   # % de particiones under-replicated para warning
    [int]$UmbralCritico = 20   # % de particiones under-replicated para alerta critica
)

$ErrorActionPreference = "SilentlyContinue"
$SCRIPT_DIR = Split-Path -Parent $MyInvocation.MyCommand.Path
$ROOT_DIR   = Split-Path -Parent (Split-Path -Parent $SCRIPT_DIR)
$RESULTADOS = Join-Path $ROOT_DIR "experimentos\resultados"

Write-Host ""
Write-Host "╔══════════════════════════════════════════════════════════════╗" -ForegroundColor Magenta
Write-Host "║       NIVEL 4 - MONITOR DE UNDER-REPLICATED PARTITIONS       ║" -ForegroundColor Magenta
Write-Host "╚══════════════════════════════════════════════════════════════╝" -ForegroundColor Magenta

# ─────────────────────────────────────────────────────────────────────────────
# Determinar broker de referencia
# ─────────────────────────────────────────────────────────────────────────────
$brokerRef = $null
foreach ($id in 1,2,3) {
    $est = docker inspect --format='{{.State.Status}}' "kafka-broker-$id" 2>$null
    if ($est -eq "running") { $brokerRef = $id; break }
}

if (-not $brokerRef) {
    Write-Host "  [ERROR] Ningun broker esta corriendo." -ForegroundColor Red
    exit 1
}
$puertoRef = "909$brokerRef"
Write-Host "  Usando Broker $brokerRef como referencia (puerto $puertoRef)" -ForegroundColor Gray

# ─────────────────────────────────────────────────────────────────────────────
# Obtener todas las particiones y clasificarlas
# ─────────────────────────────────────────────────────────────────────────────
Write-Host ""
Write-Host "  Analizando estado de replicacion..." -ForegroundColor Cyan

$describe = docker exec "kafka-broker-$brokerRef" /opt/kafka/bin/kafka-topics.sh `
    --bootstrap-server "localhost:$puertoRef" --describe 2>$null

$lineasParticion = $describe | Where-Object {
    $_ -match "Partition:" -and $_ -match "Replicas:" -and $_ -notmatch "__consumer"
}

$totalParticiones = 0
$underReplicated  = @()
$brokeresFaltantes = @{}

foreach ($linea in $lineasParticion) {
    $topicN  = if ($linea -match "Topic:\s*(\S+)")       { $Matches[1] } else { "?" }
    $partN   = if ($linea -match "Partition:\s*(\d+)")   { $Matches[1] } else { "?" }
    $leaderN = if ($linea -match "Leader:\s*(\d+)")      { $Matches[1] } else { "-1" }
    $repStr  = if ($linea -match "Replicas:\s*([\d,]+)") { $Matches[1] } else { "" }
    $isrStr  = if ($linea -match "Isr:\s*([\d,]+)")      { $Matches[1] } else { "" }

    $rList = ($repStr -split "," | ForEach-Object { $_.Trim() } | Where-Object { $_ -ne "" })
    $iList = ($isrStr -split "," | ForEach-Object { $_.Trim() } | Where-Object { $_ -ne "" })

    $totalParticiones++

    $faltantes = $rList | Where-Object { $_ -notin $iList }
    if ($faltantes.Count -gt 0) {
        $underReplicated += [PSCustomObject]@{
            Topic     = $topicN
            Part      = $partN
            Leader    = $leaderN
            RFConfig  = $rList.Count
            ISRActual = $iList.Count
            Faltantes = $faltantes
        }
        foreach ($f in $faltantes) {
            if (-not $brokeresFaltantes.ContainsKey($f)) { $brokeresFaltantes[$f] = 0 }
            $brokeresFaltantes[$f]++
        }
    }
}

# ─────────────────────────────────────────────────────────────────────────────
# Mostrar resultados
# ─────────────────────────────────────────────────────────────────────────────
$countUR  = $underReplicated.Count
$pct      = if ($totalParticiones -gt 0) { [math]::Round($countUR*100.0/$totalParticiones,1) } else { 0 }
$timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"

Write-Host ""
Write-Host ("  Total de particiones : {0}" -f $totalParticiones) -ForegroundColor White
Write-Host ("  Under-replicated     : {0} ({1}%)" -f $countUR, $pct) -ForegroundColor $(
    if ($pct -eq 0) { "Green" } elseif ($pct -lt $UmbralWarning) { "Yellow" } elseif ($pct -lt $UmbralCritico) { "Red" } else { "DarkRed" }
)
Write-Host ("  Completamente sanas  : {0} ({1}%)" -f ($totalParticiones - $countUR), (100 - $pct)) -ForegroundColor Green

# Nivel de alerta
Write-Host ""
if ($pct -eq 0) {
    Write-Host "  ✅ CLUSTER COMPLETAMENTE REPLICADO - Sin under-replicated partitions" -ForegroundColor Green
} elseif ($pct -lt $UmbralWarning) {
    Write-Host "  ⚠  INFO: $pct% de particiones under-replicated (por debajo del umbral de warning $UmbralWarning%)" -ForegroundColor Yellow
} elseif ($pct -lt $UmbralCritico) {
    Write-Host "  🟠 WARNING: $pct% de particiones under-replicated - El cluster necesita atencion" -ForegroundColor Red
} else {
    Write-Host "  🔴 CRITICO: $pct% de particiones under-replicated - ACCION INMEDIATA REQUERIDA" -ForegroundColor DarkRed
}

# ─────────────────────────────────────────────────────────────────────────────
# Detalle de particiones under-replicated
# ─────────────────────────────────────────────────────────────────────────────
if ($underReplicated.Count -gt 0) {
    Write-Host ""
    Write-Host "  Detalle de particiones under-replicated:" -ForegroundColor Cyan
    Write-Host "  ──────────────────────────────────────────────────────────" -ForegroundColor DarkGray

    foreach ($ur in $underReplicated) {
        $fStr = "Broker(s) " + ($ur.Faltantes -join ", ")
        Write-Host ("  Topic: {0}" -f $ur.Topic) -ForegroundColor White
        Write-Host ("    Partition {0}: RF configurado={1}  ISR actual={2}" -f $ur.Part, $ur.RFConfig, $ur.ISRActual) -ForegroundColor Yellow
        Write-Host ("    Replicas faltantes: {0}" -f $fStr) -ForegroundColor Red

        # Verificar si el broker que falta está caído
        foreach ($f in $ur.Faltantes) {
            $estF = docker inspect --format='{{.State.Status}}' "kafka-broker-$f" 2>$null
            if ($estF -ne "running") {
                Write-Host ("    Causa: kafka-broker-$f esta DETENIDO - ejecuta 33-recuperar-broker.ps1 -Broker $f") -ForegroundColor Red
            } else {
                Write-Host ("    kafka-broker-$f esta corriendo pero la replica esta retrasada") -ForegroundColor Yellow
                Write-Host "    Verifica logs: docker logs kafka-broker-$f --tail 50" -ForegroundColor Gray
            }
        }
    }

    # Brokers con más problemas
    if ($brokeresFaltantes.Count -gt 0) {
        Write-Host ""
        Write-Host "  Brokers con replicas fuera de sync:" -ForegroundColor Cyan
        foreach ($kv in ($brokeresFaltantes.GetEnumerator() | Sort-Object Value -Descending)) {
            Write-Host ("    Broker {0}: {1} particion(es) sin sincronizar" -f $kv.Key, $kv.Value) -ForegroundColor Yellow
        }
    }

    # Recomendaciones
    Write-Host ""
    Write-Host "  Recomendaciones:" -ForegroundColor Cyan
    if ($brokeresFaltantes.Count -gt 0) {
        foreach ($kv in $brokeresFaltantes.GetEnumerator()) {
            $estFalt = docker inspect --format='{{.State.Status}}' "kafka-broker-$($kv.Key)" 2>$null
            if ($estFalt -ne "running") {
                Write-Host "    -> Recuperar Broker $($kv.Key): .\scripts\powershell\33-recuperar-broker.ps1 -Broker $($kv.Key)" -ForegroundColor White
            } else {
                Write-Host "    -> Revisar logs de Broker $($kv.Key): docker logs kafka-broker-$($kv.Key) --tail 100" -ForegroundColor White
            }
        }
    }
}

# ─────────────────────────────────────────────────────────────────────────────
# Guardar reporte histórico
# ─────────────────────────────────────────────────────────────────────────────
$archivoHistorico = "$RESULTADOS\under-replicated-historico.csv"
$cabeceraCSV = "Timestamp,Total,UnderReplicated,Porcentaje"
if (-not (Test-Path $archivoHistorico)) {
    $cabeceraCSV | Out-File -FilePath $archivoHistorico -Encoding UTF8
}
"$timestamp,$totalParticiones,$countUR,$pct" | Out-File -FilePath $archivoHistorico -Append -Encoding UTF8

Write-Host ""
Write-Host "  Registro historico actualizado: $archivoHistorico" -ForegroundColor Gray

# Mostrar últimas 5 entradas del historial
$historial = Get-Content $archivoHistorico -ErrorAction SilentlyContinue
if ($historial -and $historial.Count -gt 2) {
    Write-Host ""
    Write-Host "  Historial reciente (ultimas entradas):" -ForegroundColor Cyan
    $historial | Select-Object -Last 5 | ForEach-Object {
        Write-Host "    $_" -ForegroundColor Gray
    }
}

Write-Host ""
Write-Host "  Umbrales configurados: Warning=$UmbralWarning%  Critico=$UmbralCritico%" -ForegroundColor Gray
Write-Host "  Para cambiar: .\35-monitorear-under-replicated.ps1 -UmbralWarning 10 -UmbralCritico 30" -ForegroundColor Gray
Write-Host ""
