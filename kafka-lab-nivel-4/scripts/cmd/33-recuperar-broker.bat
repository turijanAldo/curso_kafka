@echo off
REM =============================================================================
REM 33-recuperar-broker.bat
REM Nivel 4 - Recuperar broker detenido y monitorear re-sincronizacion
REM Uso: 33-recuperar-broker.bat <1|2|3>
REM =============================================================================

setlocal EnableDelayedExpansion

if "%~1"=="" (
    echo   Uso: 33-recuperar-broker.bat ^<1^|2^|3^>
    exit /b 1
)

set "BROKER=%~1"
set "BROKER_NAME=kafka-broker-%BROKER%"
for %%i in ("%~dp0..\..") do set "ROOT_DIR=%%~fi"
set "RESULTADOS=%ROOT_DIR%\experimentos\resultados"

echo.
echo ==============================================================
echo   NIVEL 4 - RECUPERAR BROKER %BROKER%
echo ==============================================================
echo.

REM Verificar que el broker esta realmente detenido
docker inspect --format={{.State.Status}} %BROKER_NAME% 2>nul | findstr "running" >nul
if not errorlevel 1 (
    echo [!] %BROKER_NAME% ya esta corriendo. No hay nada que recuperar.
    exit /b 0
)
echo [OK] %BROKER_NAME% esta detenido - iniciando recuperacion

echo.
echo [>>] Proceso de recuperacion:
echo      1. Docker inicia el contenedor
echo      2. Kafka arranca y se anuncia al cluster
echo      3. El broker identifica sus replicas desactualizadas
echo      4. Copia mensajes faltantes de los leaders actuales
echo      5. Cuando cada replica se pone al dia, vuelve al ISR
echo.

REM Iniciar el broker
echo [>>] Iniciando %BROKER_NAME%...
docker start %BROKER_NAME%
echo [OK] Contenedor iniciado. Esperando que Kafka arranque (15 segundos)...
timeout /t 15 /nobreak >nul

REM Determinar broker de referencia para consultas
set "BROKER_REF=1"
if "%BROKER%"=="1" set "BROKER_REF=2"
set "PUERTO_REF=909%BROKER_REF%"

REM Monitoreo de re-sincronizacion (24 iteraciones de 5 segundos = 2 minutos)
echo.
echo [>>] Monitoreando re-sincronizacion (hasta 2 minutos)...
echo      Ctrl+C para detener el monitoreo
echo.

for /l %%i in (1,1,24) do (
    timeout /t 5 /nobreak >nul
    set /a SEG=%%i*5
    echo [!SEG! seg] Estado ISR del broker %BROKER%:
    docker exec kafka-broker-%BROKER_REF% /opt/kafka/bin/kafka-topics.sh ^
        --bootstrap-server localhost:%PUERTO_REF% --describe 2>nul ^
        | findstr /v "__consumer_offsets" | findstr "Isr:"
    echo.
)

REM Guardar estado final
echo [>>] Capturando estado final...
echo Estado DESPUES de recuperacion - %date% %time% > "%RESULTADOS%\estado-replicas-despues-recuperacion.txt"
echo Broker recuperado: %BROKER_NAME% >> "%RESULTADOS%\estado-replicas-despues-recuperacion.txt"
echo ============================================================ >> "%RESULTADOS%\estado-replicas-despues-recuperacion.txt"
docker exec kafka-broker-%BROKER_REF% /opt/kafka/bin/kafka-topics.sh ^
    --bootstrap-server localhost:%PUERTO_REF% --describe 2>nul ^
    | findstr /v "__consumer_offsets" >> "%RESULTADOS%\estado-replicas-despues-recuperacion.txt"
echo [OK] Estado guardado en estado-replicas-despues-recuperacion.txt

echo.
echo ==============================================================
echo   Recuperacion completada
echo ==============================================================
echo.
echo   Compara los 3 estados:
echo   type "%RESULTADOS%\estado-replicas-antes-fallo.txt"
echo   type "%RESULTADOS%\estado-replicas-durante-fallo.txt"
echo   type "%RESULTADOS%\estado-replicas-despues-recuperacion.txt"
echo.
endlocal
