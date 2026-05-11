# =============================================================
# 24-ver-logs-brokers.ps1
# Inspecciona los logs de uno o todos los brokers del cluster.
# Resalta lineas importantes (ERROR, WARN, leader election).
#
# Uso:
#   .\24-ver-logs-brokers.ps1                 # logs de los 3 brokers
#   .\24-ver-logs-brokers.ps1 -Broker 2       # solo broker 2
#   .\24-ver-logs-brokers.ps1 -Lineas 100     # ultimas 100 lineas
#   .\24-ver-logs-brokers.ps1 -Filtro "partition"  # filtrar por palabra
#   .\24-ver-logs-brokers.ps1 -Seguir         # modo follow (Ctrl+C para salir)
# =============================================================

param(
    [ValidateSet("1","2","3","todos")]
    [string]$Broker  = "todos",
    [int]   $Lineas  = 50,
    [string]$Filtro  = "",
    [switch]$Seguir  = $false
)

$ErrorActionPreference = "Continue"

function Show-BrokerLog {
    param([string]$Container, [int]$Tail, [string]$Filter, [bool]$Follow)

    Write-Host ""
    Write-Host "════════════════════════════════════════════════════════" -ForegroundColor Cyan
    Write-Host "  LOGS: $Container" -ForegroundColor Cyan
    Write-Host "════════════════════════════════════════════════════════" -ForegroundColor Cyan

    $estado = docker inspect $Container --format "{{.State.Status}}" 2>&1
    if ($estado -ne "running") {
        Write-Host "  [ WARN ] $Container no esta corriendo (estado: $estado)" -ForegroundColor Yellow
        return
    }

    if ($Follow) {
        Write-Host "  [Modo FOLLOW activo - presiona Ctrl+C para salir]" -ForegroundColor Yellow
        docker logs $Container --follow 2>&1
        return
    }

    $logs = docker logs $Container --tail $Tail 2>&1

    # Aplicar filtro si se especifico
    if ($Filter) {
        $logs = $logs | Select-String -Pattern $Filter -SimpleMatch | ForEach-Object { $_.Line }
    }

    foreach ($linea in $logs) {
        # Resaltar lineas importantes con colores
        if ($linea -match "ERROR") {
            Write-Host "  $linea" -ForegroundColor Red
        } elseif ($linea -match "WARN") {
            Write-Host "  $linea" -ForegroundColor Yellow
        } elseif ($linea -match "Leader election|Partition assignment|became leader|Replication|KRaft") {
            Write-Host "  $linea" -ForegroundColor Cyan
        } elseif ($linea -match "started|ready|registered") {
            Write-Host "  $linea" -ForegroundColor Green
        } else {
            Write-Host "  $linea" -ForegroundColor Gray
        }
    }
}

Write-Host ""
Write-Host "============================================================" -ForegroundColor Cyan
Write-Host "  Kafka Nivel 3: Inspeccion de Logs de Brokers            " -ForegroundColor Cyan
Write-Host "============================================================" -ForegroundColor Cyan
Write-Host "  Broker: $Broker | Ultimas lineas: $Lineas | Filtro: $(if($Filtro){"'$Filtro'"}else{'ninguno'})" -ForegroundColor White

$brokersAMostrar = switch ($Broker) {
    "1"     { @("kafka-broker-1") }
    "2"     { @("kafka-broker-2") }
    "3"     { @("kafka-broker-3") }
    default { @("kafka-broker-1", "kafka-broker-2", "kafka-broker-3") }
}

foreach ($container in $brokersAMostrar) {
    Show-BrokerLog -Container $container -Tail $Lineas -Filter $Filtro -Follow $Seguir
}

Write-Host ""
Write-Host "Tip: usa -Filtro 'partition' para ver solo eventos de particiones" -ForegroundColor DarkGray
Write-Host "Tip: usa -Seguir para ver logs en tiempo real (Ctrl+C para salir)" -ForegroundColor DarkGray
Write-Host ""
