@echo off
REM =============================================================================
REM 31-describir-replicas.bat
REM Nivel 4 - Describir estado de replicas e ISR de todos los topics
REM Uso: 31-describir-replicas.bat [nombre-topic]
REM =============================================================================

setlocal EnableDelayedExpansion

for %%i in ("%~dp0..\..") do set "ROOT_DIR=%%~fi"
set "RESULTADOS=%ROOT_DIR%\experimentos\resultados"
set "TOPIC_FILTRO=%~1"

echo.
echo ==============================================================
echo   NIVEL 4 - ANALISIS DE REPLICAS E ISR
echo ==============================================================
echo.

REM Verificar cluster
docker inspect --format={{.State.Status}} kafka-broker-1 2>nul | findstr "running" >nul
if errorlevel 1 (
    echo [ERROR] kafka-broker-1 no responde. Verifica el cluster.
    exit /b 1
)

if "%TOPIC_FILTRO%"=="" (
    echo [>>] Analizando TODOS los topics...
    echo.
    docker exec kafka-broker-1 /opt/kafka/bin/kafka-topics.sh ^
        --bootstrap-server localhost:9092 ^
        --describe 2>nul | findstr /v "__consumer_offsets" | findstr /v "^$"
) else (
    echo [>>] Analizando topic: %TOPIC_FILTRO%
    echo.
    docker exec kafka-broker-1 /opt/kafka/bin/kafka-topics.sh ^
        --bootstrap-server localhost:9092 ^
        --describe ^
        --topic %TOPIC_FILTRO% 2>nul
)

echo.
echo ==============================================================
echo   Buscar particiones con ISR incompleto:
echo ==============================================================
echo [>>] Una particion tiene ISR incompleto si la lista "Isr:"
echo [>>] tiene menos brokers que la lista "Replicas:".
echo [>>] Eso significa que hay replicas fuera de sincronizacion.
echo.
echo   Ejecuta 34-verificar-isr.bat para analisis detallado con alertas.
echo.

REM Guardar snapshot con timestamp
set "HH=%time:~0,2%"
set "MM=%time:~3,2%"
set "SS=%time:~6,2%"
set "TS=%date:~-4%%date:~3,2%%date:~0,2%-%HH%%MM%%SS%"
set "TS=%TS: =0%"

docker exec kafka-broker-1 /opt/kafka/bin/kafka-topics.sh ^
    --bootstrap-server localhost:9092 --describe 2>nul ^
    | findstr /v "__consumer_offsets" ^
    > "%RESULTADOS%\isr-snapshot-%TS%.txt"

echo   Snapshot guardado: %RESULTADOS%\isr-snapshot-%TS%.txt
echo.
endlocal
