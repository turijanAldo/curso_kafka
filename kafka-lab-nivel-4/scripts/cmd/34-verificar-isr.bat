@echo off
REM =============================================================================
REM 34-verificar-isr.bat
REM Nivel 4 - Verificar estado del ISR de todos los topics
REM Uso: 34-verificar-isr.bat [nombre-topic]
REM =============================================================================

setlocal EnableDelayedExpansion

for %%i in ("%~dp0..\..") do set "ROOT_DIR=%%~fi"
set "RESULTADOS=%ROOT_DIR%\experimentos\resultados"
set "TOPIC_FILTRO=%~1"

echo.
echo ==============================================================
echo   NIVEL 4 - VERIFICACION DEL ISR
echo ==============================================================
echo   ISR = In-Sync Replicas
echo   Una particion es "under-replicated" si Replicas != Isr
echo ==============================================================
echo.

REM Verificar cluster disponible
set "BROKER_REF=1"
docker inspect --format={{.State.Status}} kafka-broker-1 2>nul | findstr "running" >nul
if errorlevel 1 (
    set "BROKER_REF=2"
    docker inspect --format={{.State.Status}} kafka-broker-2 2>nul | findstr "running" >nul
    if errorlevel 1 (
        set "BROKER_REF=3"
    )
)
set "PUERTO_REF=909%BROKER_REF%"
echo [>>] Usando Broker %BROKER_REF% como referencia (puerto %PUERTO_REF%)
echo.

REM Obtener describe y guardar en archivo temporal
set "TMP_FILE=%TEMP%\kafka_describe_%RANDOM%.txt"
docker exec kafka-broker-%BROKER_REF% /opt/kafka/bin/kafka-topics.sh ^
    --bootstrap-server localhost:%PUERTO_REF% --describe 2>nul ^
    | findstr /v "__consumer_offsets" > "%TMP_FILE%"

if "%TOPIC_FILTRO%"=="" (
    echo [>>] Mostrando todos los topics:
) else (
    echo [>>] Filtrando topic: %TOPIC_FILTRO%
)

echo.
echo   Particiones y estado de ISR:
echo   --------------------------------------------------------------
type "%TMP_FILE%"

echo.
echo   --------------------------------------------------------------
echo   Para identificar particiones under-replicated, busca filas
echo   donde "Replicas:" tiene mas numeros que "Isr:".
echo.
echo   Ejemplo SALUDABLE:  Replicas: 1,2,3  Isr: 1,2,3
echo   Ejemplo DEGRADADO:  Replicas: 1,2,3  Isr: 1,3     (broker 2 fuera)
echo   Ejemplo CRITICO:    Replicas: 1,2,3  Isr: 1        (solo leader in-sync)
echo.

REM Guardar snapshot con timestamp
set "HH=%time:~0,2%"
set "MM=%time:~3,2%"
set "SS=%time:~6,2%"
set "TS=%date:~-4%%date:~3,2%%date:~0,2%-%HH%%MM%%SS%"
set "TS=%TS: =0%"
copy "%TMP_FILE%" "%RESULTADOS%\isr-estado-%TS%.txt" >nul
echo   Snapshot guardado: %RESULTADOS%\isr-estado-%TS%.txt
echo.

REM Verificar estado de cada broker
echo   Estado de los brokers:
for /l %%b in (1,1,3) do (
    docker inspect --format={{.State.Status}} kafka-broker-%%b 2>nul | findstr "running" >nul
    if errorlevel 1 (
        echo   [!!] kafka-broker-%%b: DETENIDO
    ) else (
        echo   [OK] kafka-broker-%%b: corriendo
    )
)

del "%TMP_FILE%" 2>nul
echo.
echo   Usa 35-monitorear-under-replicated.bat para deteccion automatica
echo   de particiones under-replicated con alertas.
echo.
endlocal
