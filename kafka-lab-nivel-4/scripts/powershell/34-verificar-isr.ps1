# =============================================================================
# 34-verificar-isr.ps1
# Nivel 4 - Verificación detallada del estado ISR con monitoreo continuo
# Uso: .\34-verificar-isr.ps1 [-Watch] [-Topic <nombre>]
# =============================================================================

param(
    [switch]$Watch,
    [string]$Topic = ""
)

$ErrorActionPreference = "SilentlyContinue"
$SCRIPT_DIR = Split-Path -Parent $MyInvocation.MyCommand.Path
$ROOT_DIR   = Split-Path -Parent (Split-Path -Parent $SCRIPT_DIR)
$RESULTADOS = Join-Path $ROOT_DIR "experimentos\resultados"

function Invoke-ISRCheck {
    param([string]$TopicFiltro = "")

    if (-not $Watch) {
        Write-Host ""
        Write-Host "╔══════════════════════════════════════════════════════════════╗" -ForegroundColor Magenta
        Write-Host "║       NIVEL 4 - VERIFICACION DE ISR                          ║" -ForegroundColor Magenta
        Write-Host "╚══════════════════════════════════════════════════════════════╝" -ForegroundColor Magenta
    }

    # Obtener información de brokers activos
    $brokersActivos = @()
    foreach ($id in 1,2,3) {
        $est = docker inspect --format='{{.State.Status}}' "kafka-broker-$id" 2>$null
        if ($est -eq "running") { $brokersActivos += $id }
    }

    $brokerRef = $brokersActivos | Select-Object -First 1
    if (-not $brokerRef) {
        Write-Host "  [ERROR] Ningun broker esta corriendo." -ForegroundColor Red
        return
    }
    $puertoRef = "909$brokerRef"

    # Obtener describe de todos los topics
    $describe = docker exec "kafka-broker-$brokerRef" /opt/kafka/bin/kafka-topics.sh `
        --bootstrap-server "localhost:$puertoRef" --describe 2>$null

    # Filtrar topics internos y aplicar filtro de topic si se especificó
    $lineasParticion = $describe | Where-Object {
        $_ -match "Partition:" -and $_ -match "Leader:" -and $_ -notmatch "__consumer"
    }

    if ($TopicFiltro -ne "") {
        $lineasParticion = $lineasParticion | Where-Object { $_ -match "Topic:\s*$TopicFiltro" }
    }

    # Clasificar particiones
    $verdes    = @()
    $amarillas = @()
    $rojas     = @()

    foreach ($linea in $lineasParticion) {
        $topicN  = if ($linea -match "Topic:\s*(\S+)")     { $Matches[1] } else { "?" }
        $partN   = if ($linea -match "Partition:\s*(\d+)") { $Matches[1] } else { "?" }
        $leaderN = if ($linea -match "Leader:\s*(\d+)")    { $Matches[1] } else { "-1" }
        $repStr  = if ($linea -match "Replicas:\s*([\d,]+)") { $Matches[1] } else { "" }
        $isrStr  = if ($linea -match "Isr:\s*([\d,]+)")      { $Matches[1] } else { "" }

        $rList = ($repStr -split "," | ForEach-Object { $_.Trim() } | Where-Object { $_ -ne "" })
        $iList = ($isrStr -split "," | ForEach-Object { $_.Trim() } | Where-Object { $_ -ne "" })

        $faltantes = $rList | Where-Object { $_ -notin $iList }

        $info = [PSCustomObject]@{
            Topic    = $topicN
            Part     = $partN
            Leader   = $leaderN
            Replicas = $rList
            ISR      = $iList
            Faltantes = $faltantes
        }

        if ($faltantes.Count -eq 0) {
            $verdes    += $info
        } elseif ($iList.Count -le 1) {
            $rojas     += $info
        } else {
            $amarillas += $info
        }
    }

    $total = $verdes.Count + $amarillas.Count + $rojas.Count
    $timestamp = Get-Date -Format "HH:mm:ss"

    if ($Watch) { Clear-Host }
    Write-Host ""
    Write-Host "  Estado del ISR en el cluster  [$timestamp]" -ForegroundColor Cyan
    Write-Host "  Brokers activos: [$($brokersActivos -join ', ')]" -ForegroundColor White
    Write-Host "  ──────────────────────────────────────────────────────────" -ForegroundColor DarkGray

    # ── VERDES ──
    Write-Host ""
    Write-Host ("  🟢 PARTICIONES SALUDABLES ({0} de {1})" -f $verdes.Count, $total) -ForegroundColor Green
    if ($verdes.Count -gt 0) {
        Write-Host "     Todas las replicas in-sync, sin problemas." -ForegroundColor Green
        # Mostrar detalle solo si pocas
        if ($verdes.Count -le 8) {
            foreach ($p in $verdes) {
                $rStr = "[" + ($p.Replicas -join ",") + "]"
                Write-Host ("     {0} P{1}: Replicas={2}  ISR={2}  ✅" -f $p.Topic, $p.Part, $rStr) -ForegroundColor DarkGreen
            }
        }
    }

    # ── AMARILLAS ──
    Write-Host ""
    Write-Host ("  🟡 PARTICIONES CON ADVERTENCIA ({0} de {1})" -f $amarillas.Count, $total) -ForegroundColor Yellow
    if ($amarillas.Count -gt 0) {
        foreach ($p in $amarillas) {
            $rStr = "[" + ($p.Replicas -join ",") + "]"
            $iStr = "[" + ($p.ISR -join ",") + "]"
            $fStr = "Broker(s) " + ($p.Faltantes -join ",")
            Write-Host ("     Topic: {0}, Partition: {1}" -f $p.Topic, $p.Part) -ForegroundColor Yellow
            Write-Host ("       Replicas configuradas: {0}" -f $rStr) -ForegroundColor White
            Write-Host ("       ISR actual           : {0}" -f $iStr) -ForegroundColor White
            Write-Host ("       Faltante             : {0}" -f $fStr) -ForegroundColor Yellow
            # Verificar si el broker está caído
            foreach ($f in $p.Faltantes) {
                $estF = docker inspect --format='{{.State.Status}}' "kafka-broker-$f" 2>$null
                if ($estF -ne "running") {
                    Write-Host ("       Causa probable: kafka-broker-$f esta DETENIDO") -ForegroundColor Red
                } else {
                    Write-Host ("       kafka-broker-$f esta corriendo pero retrasado") -ForegroundColor Yellow
                }
            }
        }
    } else {
        Write-Host "     Ninguna." -ForegroundColor DarkGray
    }

    # ── ROJAS ──
    Write-Host ""
    Write-Host ("  🔴 PARTICIONES CRITICAS ({0} de {1})" -f $rojas.Count, $total) -ForegroundColor Red
    if ($rojas.Count -gt 0) {
        foreach ($p in $rojas) {
            $rStr = "[" + ($p.Replicas -join ",") + "]"
            $iStr = "[" + ($p.ISR -join ",") + "]"
            Write-Host ("     Topic: {0}, Partition: {1}" -f $p.Topic, $p.Part) -ForegroundColor Red
            Write-Host ("       Replicas configuradas: {0}" -f $rStr) -ForegroundColor White
            Write-Host ("       ISR actual           : {0}" -f $iStr) -ForegroundColor White
            Write-Host ("       ⚠ RIESGO: Si el leader (Broker{0}) falla, esta particion sera INACCESIBLE" -f $p.Leader) -ForegroundColor Red
        }
    } else {
        Write-Host "     Ninguna." -ForegroundColor DarkGray
    }

    # ── RESUMEN ──
    Write-Host ""
    Write-Host "  ──────────────────────────────────────────────────────────" -ForegroundColor DarkGray
    $pctSano = if ($total -gt 0) { [math]::Round($verdes.Count*100.0/$total,1) } else { 100 }
    Write-Host ("  Salud global del cluster: {0}% ({1}/{2} particiones saludables)" -f $pctSano, $verdes.Count, $total) -ForegroundColor $(if ($pctSano -ge 90) { "Green" } elseif ($pctSano -ge 50) { "Yellow" } else { "Red" })

    # Guardar snapshot
    $snap = "$RESULTADOS\isr-estado-$(Get-Date -Format 'yyyyMMdd-HHmmss').txt"
    @(
        "ISR SNAPSHOT - $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')",
        "Saludables: $($verdes.Count)  Advertencia: $($amarillas.Count)  Criticas: $($rojas.Count)  Total: $total",
        "Salud: $pctSano%"
    ) | Out-File -FilePath $snap -Encoding UTF8

    if ($Watch) {
        Write-Host ""
        Write-Host "  Modo Watch activo - actualizando cada 10 segundos. Ctrl+C para salir." -ForegroundColor Gray
    }
}

# ─────────────────────────────────────────────────────────────────────────────
# Modo Watch vs ejecución única
# ─────────────────────────────────────────────────────────────────────────────
if ($Watch) {
    Write-Host "  Iniciando monitoreo continuo del ISR (Ctrl+C para detener)..." -ForegroundColor Cyan
    while ($true) {
        Invoke-ISRCheck -TopicFiltro $Topic
        Start-Sleep -Seconds 10
    }
} else {
    Invoke-ISRCheck -TopicFiltro $Topic
    Write-Host ""
    Write-Host "  Tip: Usa -Watch para monitoreo continuo cada 10 segundos." -ForegroundColor Gray
    Write-Host "  Tip: Usa -Topic <nombre> para filtrar un topic especifico." -ForegroundColor Gray
    Write-Host ""
}
