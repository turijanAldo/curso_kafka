@echo off
REM =============================================================================
REM 35-monitorear-under-replicated.bat
REM Nivel 4 - Detectar y reportar particiones under-replicated
REM =============================================================================

setlocal EnableDelayedExpansion

for %%i in ("%~dp0..\..") do set "ROOT_DIR=%%~fi"
set "RESULTADOS=%ROOT_DIR%\experimentos\resultados"

echo.
echo ==============================================================
echo   NIVEL 4 - MONITOR DE UNDER-REPLICATED PARTITIONS
echo ==============================================================
echo.

REM Buscar broker disponible
set "BROKER_REF="
for /l %%b in (1,1,3) do (
    if not defined BROKER_REF (
        docker inspect --format={{.State.Status}} kafka-broker-%%b 2>nul | findstr "running" >nul
        if not errorlevel 1 set "BROKER_REF=%%b"
    )
)

if not defined BROKER_REF (
    echo [ERROR] Ningun broker esta corriendo.
    exit /b 1
)

set "PUERTO_REF=909%BROKER_REF%"
echo [>>] Broker de referencia: kafka-broker-%BROKER_REF% (puerto %PUERTO_REF%)
echo.

REM Estado de brokers
echo   Estado de los 3 brokers:
set "BROKERS_CAIDOS="
for /l %%b in (1,1,3) do (
    docker inspect --format={{.State.Status}} kafka-broker-%%b 2>nul | findstr "running" >nul
    if errorlevel 1 (
        echo   [!!] kafka-broker-%%b: DETENIDO ^<-- causa probable de under-replicated
        set "BROKERS_CAIDOS=!BROKERS_CAIDOS! %%b"
    ) else (
        echo   [OK] kafka-broker-%%b: corriendo
    )
)
echo.

REM Obtener describe en archivo temporal
set "TMP=%TEMP%\ur_%RANDOM%.txt"
docker exec kafka-broker-%BROKER_REF% /opt/kafka/bin/kafka-topics.sh ^
    --bootstrap-server localhost:%PUERTO_REF% --describe 2>nul ^
    | findstr /v "__consumer_offsets" | findstr "Partition:" > "%TMP%"

set "TOTAL=0"
set "UNDER=0"

REM Contar particiones totales y under-replicated
REM (Batch no puede parsear facilmente, mostramos la informacion raw y contamos lineas)
for /f %%L in ('type "%TMP%" ^| find /c /v ""') do set "TOTAL=%%L"

echo   Total de particiones encontradas: %TOTAL%
echo.

if %TOTAL%==0 (
    echo   [!] No se encontraron topics. Crea topics con 30-crear-topics-replicados.bat
    del "%TMP%" 2>nul
    exit /b 0
)

echo   Todas las particiones y su estado ISR:
echo   --------------------------------------------------------------
type "%TMP%"
echo   --------------------------------------------------------------
echo.

if defined BROKERS_CAIDOS (
    echo   [!!] BROKERS CAIDOS DETECTADOS:%BROKERS_CAIDOS%
    echo   [!!] Las replicas en esos brokers estan fuera del ISR.
    echo.
    for %%b in (%BROKERS_CAIDOS%) do (
        echo   Para recuperar Broker %%b:
        echo     33-recuperar-broker.bat %%b
    )
) else (
    echo   [OK] Todos los brokers estan corriendo.
    echo   [>>] Si hay under-replicated, puede ser por retraso temporal.
    echo   [>>] Verifica los logs: docker logs kafka-broker-X --tail 50
)

REM Guardar en historico
set "HH=%time:~0,2%"
set "MM=%time:~3,2%"
set "TS=%date:~-4%%date:~3,2%%date:~0,2%-%HH%%MM%"
set "TS=%TS: =0%"
set "CSV=%RESULTADOS%\under-replicated-historico.csv"
if not exist "%CSV%" echo Timestamp,Total,Brokers_caidos >> "%CSV%"
echo %date% %time%,%TOTAL%,%BROKERS_CAIDOS% >> "%CSV%"
echo.
echo   Registro guardado en: %CSV%

del "%TMP%" 2>nul
echo.
endlocal
