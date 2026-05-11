# =============================================================================
# 33-recuperar-broker.ps1
# Nivel 4 - Recuperar broker y monitorear re-sincronización de réplicas
# Uso: .\33-recuperar-broker.ps1 -Broker <1|2|3>
# =============================================================================

param(
    [Parameter(Mandatory=$true)]
    [ValidateSet("1","2","3")]
    [string]$Broker
)

$ErrorActionPreference = "SilentlyContinue"
$SCRIPT_DIR  = Split-Path -Parent $MyInvocation.MyCommand.Path
$ROOT_DIR    = Split-Path -Parent (Split-Path -Parent $SCRIPT_DIR)
$RESULTADOS  = Join-Path $ROOT_DIR "experimentos\resultados"
$BROKER_NAME = "kafka-broker-$Broker"
$OTROS       = @(1,2,3) | Where-Object { $_ -ne [int]$Broker }
$BROKER_REF  = "kafka-broker-$($OTROS[0])"
$PUERTO_REF  = "909$($OTROS[0])"

function Write-OK   { param($m) Write-Host "  [OK] $m" -ForegroundColor Green }
function Write-Info { param($m) Write-Host "  [>>] $m" -ForegroundColor White }
function Write-Warn { param($m) Write-Host "  [!]  $m" -ForegroundColor Yellow }
function Write-Err  { param($m) Write-Host "  [X]  $m" -ForegroundColor Red }

Write-Host ""
Write-Host "╔══════════════════════════════════════════════════════════════╗" -ForegroundColor Green
Write-Host "║       NIVEL 4 - RECUPERAR BROKER $Broker                            ║" -ForegroundColor Green
Write-Host "╚══════════════════════════════════════════════════════════════╝" -ForegroundColor Green

# ─────────────────────────────────────────────────────────────────────────────
# Verificar que el broker está realmente detenido
# ─────────────────────────────────────────────────────────────────────────────
Write-Host "`n  Verificando estado de $BROKER_NAME..." -ForegroundColor Cyan
$estado = docker inspect --format='{{.State.Status}}' $BROKER_NAME 2>$null
if ($estado -eq "running") {
    Write-Warn "$BROKER_NAME ya está corriendo. No hay nada que recuperar."
    exit 0
}
Write-OK "$BROKER_NAME está detenido - procediendo con la recuperación"

# ─────────────────────────────────────────────────────────────────────────────
# Explicar el proceso de re-sincronización
# ─────────────────────────────────────────────────────────────────────────────
Write-Host ""
Write-Host "  Proceso de recuperacion:" -ForegroundColor Cyan
Write-Host "    1. Docker inicia el contenedor de $BROKER_NAME" -ForegroundColor White
Write-Host "    2. Kafka dentro del contenedor arranca y se anuncia al cluster" -ForegroundColor White
Write-Host "    3. El controlador KRaft detecta que el broker regreso" -ForegroundColor White
Write-Host "    4. El broker identifica sus replicas y las compara con los leaders" -ForegroundColor White
Write-Host "    5. Empieza a copiar mensajes faltantes (los escritos mientras estuvo caido)" -ForegroundColor White
Write-Host "    6. Cuando cada replica se pone al dia, vuelve al ISR" -ForegroundColor White
Write-Host "    7. Una vez en ISR, puede ser candidata a convertirse en leader" -ForegroundColor White
Write-Host ""
Write-Host "  Tiempo esperado: 30-120 segundos dependiendo de los datos escritos durante el fallo." -ForegroundColor Yellow
Write-Host ""

# ─────────────────────────────────────────────────────────────────────────────
# Iniciar el broker
# ─────────────────────────────────────────────────────────────────────────────
Write-Host "  Iniciando $BROKER_NAME..." -ForegroundColor Cyan
$tiempoInicio = Get-Date
docker start $BROKER_NAME 2>&1 | Out-Null
Write-OK "$BROKER_NAME iniciado a las $(Get-Date -Format 'HH:mm:ss')"

Write-Host "  Esperando 10 segundos para que Kafka arranque dentro del contenedor..." -ForegroundColor Gray
Start-Sleep -Seconds 10

# ─────────────────────────────────────────────────────────────────────────────
# Función: obtener estado ISR actual
# ─────────────────────────────────────────────────────────────────────────────
function Get-ISREstado {
    $describe = docker exec $BROKER_REF /opt/kafka/bin/kafka-topics.sh `
        --bootstrap-server "localhost:$PUERTO_REF" --describe 2>$null | Where-Object { $_ -match "Leader:" }

    $totalReplicas  = 0
    $replicasEnISR  = 0
    $replicasBroker = 0
    $isrBroker      = 0

    foreach ($linea in $describe) {
        $replicasStr = if ($linea -match "Replicas:\s*([\d,]+)") { $Matches[1] } else { "" }
        $isrStr      = if ($linea -match "Isr:\s*([\d,]+)")      { $Matches[1] } else { "" }

        $rList = $replicasStr -split "," | ForEach-Object { $_.Trim() } | Where-Object { $_ -ne "" }
        $iList = $isrStr      -split "," | ForEach-Object { $_.Trim() } | Where-Object { $_ -ne "" }

        $totalReplicas += $rList.Count
        $replicasEnISR += $iList.Count

        if ($Broker -in $rList) { $replicasBroker++ }
        if ($Broker -in $iList) { $isrBroker++ }
    }

    return @{
        TotalReplicas  = $totalReplicas
        ReplicasEnISR  = $replicasEnISR
        ReplicasBroker = $replicasBroker
        ISRBroker      = $isrBroker
    }
}

# ─────────────────────────────────────────────────────────────────────────────
# Loop de monitoreo de re-sincronización (2 min, cada 5 seg)
# ─────────────────────────────────────────────────────────────────────────────
Write-Host ""
Write-Host "  Monitoreando re-sincronizacion (hasta 2 minutos)..." -ForegroundColor Cyan
Write-Host "  ──────────────────────────────────────────────────────────" -ForegroundColor DarkGray

$sincronizacionCompleta = $false
$totalIteraciones = 24  # 2 minutos / 5 segundos

for ($i = 1; $i -le $totalIteraciones; $i++) {
    Start-Sleep -Seconds 5
    $ahora = Get-Date -Format "HH:mm:ss"
    $estado = Get-ISREstado

    $pct = if ($estado.ReplicasBroker -gt 0) {
        [math]::Round($estado.ISRBroker * 100.0 / $estado.ReplicasBroker, 1)
    } else { 0 }

    $barraLen = 20
    $llenos = [math]::Round($pct / 100 * $barraLen)
    $vacios  = $barraLen - $llenos
    $barra = "█" * $llenos + "░" * $vacios

    Write-Host ("  [{0}] Broker{1}: {2}/{3} replicas in-sync [{4}] {5}%" -f `
        $ahora, $Broker, $estado.ISRBroker, $estado.ReplicasBroker, $barra, $pct) -ForegroundColor White

    if ($estado.ReplicasBroker -gt 0 -and $estado.ISRBroker -eq $estado.ReplicasBroker) {
        $sincronizacionCompleta = $true
        break
    }
}

# ─────────────────────────────────────────────────────────────────────────────
# Resultado final
# ─────────────────────────────────────────────────────────────────────────────
$tiempoTotal = [math]::Round(((Get-Date) - $tiempoInicio).TotalSeconds, 1)
Write-Host ""
Write-Host "  ════════════════════════════════════════════════════════════" -ForegroundColor DarkGray

if ($sincronizacionCompleta) {
    Write-OK "Re-sincronizacion COMPLETADA en ${tiempoTotal} segundos"
    Write-OK "Broker $Broker esta completamente recuperado y todas sus replicas estan in-sync"
} else {
    Write-Warn "Re-sincronizacion NO completada en el tiempo esperado (${tiempoTotal}s)"
    Write-Warn "El proceso puede seguir en segundo plano. Verifica con:"
    Write-Warn "  .\scripts\powershell\34-verificar-isr.ps1"
}

# Capturar estado final
Write-Host ""
Write-Host "  Capturando estado final del cluster..." -ForegroundColor Cyan
$timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
$estadoFinal = docker exec $BROKER_REF /opt/kafka/bin/kafka-topics.sh `
    --bootstrap-server "localhost:$PUERTO_REF" --describe 2>$null | Where-Object { $_ -notmatch "^__" }

$archivoFinal = "$RESULTADOS\estado-replicas-despues-recuperacion.txt"
"Estado del cluster DESPUES de recuperacion - $timestamp" | Out-File -FilePath $archivoFinal -Encoding UTF8
"Broker recuperado: $BROKER_NAME" | Out-File -FilePath $archivoFinal -Append -Encoding UTF8
"Tiempo de recuperacion: ${tiempoTotal}s" | Out-File -FilePath $archivoFinal -Append -Encoding UTF8
"=" * 60 | Out-File -FilePath $archivoFinal -Append -Encoding UTF8
$estadoFinal | Out-File -FilePath $archivoFinal -Append -Encoding UTF8
Write-OK "Estado guardado en: $archivoFinal"

Write-Host ""
Write-Host "  Comparar estados con:" -ForegroundColor Cyan
Write-Host "    Get-Content $RESULTADOS\estado-replicas-antes-fallo.txt" -ForegroundColor Gray
Write-Host "    Get-Content $RESULTADOS\estado-replicas-durante-fallo.txt" -ForegroundColor Gray
Write-Host "    Get-Content $RESULTADOS\estado-replicas-despues-recuperacion.txt" -ForegroundColor Gray
Write-Host ""
