# =============================================================
# 12-producer-con-claves.ps1
# Ejecuta KeyedProducer 20 veces con claves user-001..user-020
# Captura a que particion va cada mensaje y guarda resultados.
# =============================================================

$ErrorActionPreference = "Continue"
$TOPIC       = "transacciones-4p"
$JAR_PATH    = "$PSScriptRoot\..\..\java\target\kafka-lab-nivel-2-1.0.0.jar"
$RESULTADOS  = "$PSScriptRoot\..\..\experimentos\resultados\distribucion-mensajes.txt"

function Write-OK    { param($msg) Write-Host "[  OK  ] $msg" -ForegroundColor Green }
function Write-ERROR { param($msg) Write-Host "[ ERROR] $msg" -ForegroundColor Red }
function Write-INFO  { param($msg) Write-Host "[ INFO ] $msg" -ForegroundColor Cyan }
function Write-STEP  { param($msg) Write-Host "`n>>> $msg" -ForegroundColor Yellow }

Write-Host ""
Write-Host "============================================================" -ForegroundColor Cyan
Write-Host "  Kafka Nivel 2: Producer con Claves (20 mensajes)        " -ForegroundColor Cyan
Write-Host "============================================================" -ForegroundColor Cyan

# ── Verificar JAVA_HOME ──────────────────────────────────────
if (-not $env:JAVA_HOME) {
    Write-ERROR "JAVA_HOME no esta configurado."
    Write-Host "  Configura JAVA_HOME apuntando a tu instalacion de Java 17." -ForegroundColor Yellow
    Write-Host "  Ejemplo: `$env:JAVA_HOME = 'C:\Program Files\Java\jdk-17'" -ForegroundColor Yellow
    exit 1
}
$JAVA_EXE = "$env:JAVA_HOME\bin\java.exe"
if (-not (Test-Path $JAVA_EXE)) {
    Write-ERROR "No se encontro java.exe en: $JAVA_EXE"
    exit 1
}
Write-OK "JAVA_HOME: $env:JAVA_HOME"

# ── Verificar JAR compilado ──────────────────────────────────
$JAR_REAL = Resolve-Path $JAR_PATH -ErrorAction SilentlyContinue
if (-not $JAR_REAL) {
    Write-ERROR "JAR no encontrado en: $JAR_PATH"
    Write-Host "  Compila primero: cd java && mvn clean package" -ForegroundColor Yellow
    exit 1
}
Write-OK "JAR encontrado: $JAR_REAL"

# ── Preparar archivo de resultados ──────────────────────────
$timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
"=== Resultados de distribucion por clave ===" | Out-File $RESULTADOS -Encoding utf8
"Fecha: $timestamp" | Out-File $RESULTADOS -Append -Encoding utf8
"Topic: $TOPIC" | Out-File $RESULTADOS -Append -Encoding utf8
"" | Out-File $RESULTADOS -Append -Encoding utf8

Write-STEP "Enviando 20 mensajes con claves user-001 a user-020"
Write-INFO "Pausa de 500ms entre cada envio para observabilidad"
Write-INFO "Resultados se guardan en: experimentos\resultados\distribucion-mensajes.txt"
Write-Host ""

$contadorParticion = @{0=0; 1=0; 2=0; 3=0}

for ($i = 1; $i -le 20; $i++) {
    $clave  = "user-{0:D3}" -f $i
    $valor  = "{`"userId`": `"$clave`", `"monto`": $($i * 10.0), `"tipo`": `"compra`"}"

    Write-Host "  [$i/20] Enviando clave: $clave ..." -NoNewline -ForegroundColor White

    # Capturar output del KeyedProducer
    $output = & "$JAVA_EXE" `
        -cp "$JAR_REAL" `
        com.nexus.kafka.nivel2.KeyedProducer `
        $TOPIC $clave $valor 2>&1

    # Mostrar output en consola
    $output | ForEach-Object { Write-Host "`n    $_" -ForegroundColor Gray }

    # Extraer el numero de particion del output para estadisticas
    $lineaParticion = $output | Select-String "Partition:" | Select-Object -First 1
    if ($lineaParticion -match "Partition:\s*(\d+)") {
        $part = [int]$Matches[1]
        $contadorParticion[$part]++
    }

    # Guardar en archivo de resultados
    "[$i/20] Clave: $clave" | Out-File $RESULTADOS -Append -Encoding utf8
    $output | Out-File $RESULTADOS -Append -Encoding utf8
    "" | Out-File $RESULTADOS -Append -Encoding utf8

    Start-Sleep -Milliseconds 500
}

# ── Resumen de distribucion ──────────────────────────────────
Write-Host ""
Write-Host "============================================================" -ForegroundColor Green
Write-Host "  RESUMEN DE DISTRIBUCION POR PARTICION" -ForegroundColor Green
Write-Host "============================================================" -ForegroundColor Green
Write-Host ""

$resumen = "=== Distribucion final ===" + [System.Environment]::NewLine
foreach ($p in $contadorParticion.Keys | Sort-Object) {
    $linea = "  Particion $p : $($contadorParticion[$p]) mensajes"
    Write-Host $linea -ForegroundColor White
    $resumen += $linea + [System.Environment]::NewLine
}
$resumen | Out-File $RESULTADOS -Append -Encoding utf8

Write-Host ""
Write-Host "  Nota: mensajes con la misma clave siempre van a la MISMA particion." -ForegroundColor Yellow
Write-Host "  Resultados guardados en: experimentos\resultados\distribucion-mensajes.txt" -ForegroundColor Cyan
Write-Host ""
