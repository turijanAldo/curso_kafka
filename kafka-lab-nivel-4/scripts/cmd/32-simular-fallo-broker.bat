@echo off
REM =============================================================================
REM 32-simular-fallo-broker.bat
REM Nivel 4 - Simular fallo de un broker especifico
REM Uso: 32-simular-fallo-broker.bat <1|2|3>
REM =============================================================================

setlocal EnableDelayedExpansion

if "%~1"=="" (
    echo.
    echo   Uso: 32-simular-fallo-broker.bat ^<1^|2^|3^>
    echo   Ejemplo: 32-simular-fallo-broker.bat 2
    echo.
    exit /b 1
)

set "BROKER=%~1"
if not "%BROKER%"=="1" if not "%BROKER%"=="2" if not "%BROKER%"=="3" (
    echo [ERROR] Broker debe ser 1, 2 o 3.
    exit /b 1
)

set "BROKER_NAME=kafka-broker-%BROKER%"
for %%i in ("%~dp0..\..") do set "ROOT_DIR=%%~fi"
set "RESULTADOS=%ROOT_DIR%\experimentos\resultados"

echo.
echo ==============================================================
echo   NIVEL 4 - SIMULAR FALLO DEL BROKER %BROKER%
echo ==============================================================
echo.

REM Verificar que el broker a detener esta corriendo
docker inspect --format={{.State.Status}} %BROKER_NAME% 2>nul | findstr "running" >nul
if errorlevel 1 (
    echo [ERROR] %BROKER_NAME% no esta corriendo. No hay nada que simular.
    exit /b 1
)
echo [OK] %BROKER_NAME% esta corriendo

REM Guardar estado antes del fallo
echo.
echo [>>] Capturando estado ANTES del fallo...
set "HH=%time:~0,2%"
set "ANTES_TS=%date:~-4%%date:~3,2%%date:~0,2%-%HH%%time:~3,2%%time:~6,2%"
set "ANTES_TS=%ANTES_TS: =0%"

echo Estado ANTES del fallo - %date% %time% > "%RESULTADOS%\estado-replicas-antes-fallo.txt"
echo Broker a detener: %BROKER_NAME% >> "%RESULTADOS%\estado-replicas-antes-fallo.txt"
echo ============================================================ >> "%RESULTADOS%\estado-replicas-antes-fallo.txt"
docker exec kafka-broker-1 /opt/kafka/bin/kafka-topics.sh ^
    --bootstrap-server localhost:9092 --describe 2>nul ^
    | findstr /v "__consumer_offsets" >> "%RESULTADOS%\estado-replicas-antes-fallo.txt"
echo [OK] Estado guardado en estado-replicas-antes-fallo.txt

REM Advertencia
echo.
echo ==============================================================
echo   ADVERTENCIA
echo ==============================================================
echo.
echo   Este script va a detener %BROKER_NAME% simulando un fallo.
echo   - Topics con RF mayor a 1 continuaran operando
echo   - Topics con RF=1 tendran particiones inaccesibles
echo   - El cluster seguira con 2 de 3 brokers
echo.
echo   Para recuperar ejecuta: 33-recuperar-broker.bat %BROKER%
echo.
set /p CONFIRM="  Confirmas? (escribe SI para confirmar): "
if /i not "%CONFIRM%"=="SI" (
    echo Operacion cancelada.
    exit /b 0
)

REM Detener broker (sin parametro de tiempo = fallo abrupto)
echo.
echo [>>] Deteniendo %BROKER_NAME%...
docker stop %BROKER_NAME%
echo [OK] %BROKER_NAME% detenido

REM Monitoreo simple (10 iteraciones de 3 segundos = 30 segundos)
echo.
echo [>>] Monitoreando failover durante 30 segundos...
echo      (Kafka elegira nuevos leaders automaticamente)

for /l %%i in (1,1,10) do (
    timeout /t 3 /nobreak >nul
    set /a SEG=%%i*3
    echo   [!SEG! seg] Verificando estado del cluster...
    docker exec kafka-broker-2 /opt/kafka/bin/kafka-topics.sh ^
        --bootstrap-server localhost:9093 --describe 2>nul ^
        | findstr "Leader:" | findstr /v "__consumer_offsets"
)

REM Guardar estado durante el fallo
echo.
echo [>>] Capturando estado DURANTE el fallo...
echo Estado DURANTE el fallo - %date% %time% > "%RESULTADOS%\estado-replicas-durante-fallo.txt"
echo Broker caido: %BROKER_NAME% >> "%RESULTADOS%\estado-replicas-durante-fallo.txt"
echo ============================================================ >> "%RESULTADOS%\estado-replicas-durante-fallo.txt"
docker exec kafka-broker-2 /opt/kafka/bin/kafka-topics.sh ^
    --bootstrap-server localhost:9093 --describe 2>nul ^
    | findstr /v "__consumer_offsets" >> "%RESULTADOS%\estado-replicas-durante-fallo.txt"
echo [OK] Estado guardado en estado-replicas-durante-fallo.txt

echo.
echo ==============================================================
echo   Fallo simulado. Estado actual: 2 de 3 brokers operativos
echo ==============================================================
echo.
echo   Para recuperar el broker: 33-recuperar-broker.bat %BROKER%
echo   Para ver ISR actual:      34-verificar-isr.bat
echo.
endlocal
