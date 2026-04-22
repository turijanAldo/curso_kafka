# ================================================================
# wait-for-kafka.ps1
#
# Espera a que el broker Kafka esté listo para aceptar conexiones
# antes de arrancar los microservicios Spring Boot.
#
# Uso:
#   .\wait-for-kafka.ps1
#   .\wait-for-kafka.ps1 -Host "localhost" -Port 9092 -TimeoutSec 120
#
# El script hace polling al puerto TCP de Kafka cada segundo.
# Sale con código 0 si Kafka responde, 1 si agota el timeout.
# ================================================================

param(
    [string]$KafkaHost   = "localhost",
    [int]$Port           = 9092,
    [int]$TimeoutSec     = 120,      # Máximo de espera en segundos
    [int]$IntervalSec    = 2         # Intervalo entre intentos
)

$startTime  = Get-Date
$deadline   = $startTime.AddSeconds($TimeoutSec)
$attempt    = 0

Write-Host ""
Write-Host "⏳  Esperando a Kafka en $KafkaHost`:$Port ..." -ForegroundColor Cyan
Write-Host "    Timeout máximo: $TimeoutSec segundos"
Write-Host ""

while ((Get-Date) -lt $deadline) {
    $attempt++

    try {
        # Intenta abrir una conexión TCP al puerto de Kafka
        $tcpClient = New-Object System.Net.Sockets.TcpClient
        $connect   = $tcpClient.BeginConnect($KafkaHost, $Port, $null, $null)
        $waited    = $connect.AsyncWaitHandle.WaitOne(1000, $false)

        if ($waited -and $tcpClient.Connected) {
            $tcpClient.Close()
            $elapsed = [int]((Get-Date) - $startTime).TotalSeconds

            Write-Host ""
            Write-Host "✅  Kafka está listo! (intento $attempt, ${elapsed}s)" -ForegroundColor Green
            Write-Host ""
            exit 0
        }

        $tcpClient.Close()
    }
    catch {
        # Conexión rechazada — Kafka aún no está listo
    }

    $remaining = [int]($deadline - (Get-Date)).TotalSeconds
    Write-Host "    [intento $attempt] Kafka no disponible — reintentando en ${IntervalSec}s (quedan ${remaining}s)" `
        -ForegroundColor Yellow

    Start-Sleep -Seconds $IntervalSec
}

# ── Timeout alcanzado ──────────────────────────────────────────
Write-Host ""
Write-Host "❌  Timeout: Kafka no respondió en $TimeoutSec segundos." -ForegroundColor Red
Write-Host "    Verifica que el contenedor esté corriendo:"
Write-Host "    docker ps | Select-String 'kafka'"
Write-Host "    docker logs kafka --tail 30"
Write-Host ""
exit 1
