@echo off
chcp 65001 >nul 2>&1
setlocal

:: Parametros: broker (1|2|3|todos) y lineas (default 50)
set "BROKER=%~1"
set "LINEAS=%~2"
if "%BROKER%"=="" set "BROKER=todos"
if "%LINEAS%"=="" set "LINEAS=50"

echo.
echo ============================================================
echo   Kafka Nivel 3: Inspeccion de Logs de Brokers
echo ============================================================
echo   Broker: %BROKER%  ^| Ultimas lineas: %LINEAS%
echo.

if "%BROKER%"=="1" goto LOG1
if "%BROKER%"=="2" goto LOG2
if "%BROKER%"=="3" goto LOG3

:: Mostrar los 3 brokers
:LOG1
echo ============================================================
echo   LOGS: kafka-broker-1
echo ============================================================
docker inspect kafka-broker-1 --format "{{.State.Status}}" 2>nul | findstr "running" >nul
if %ERRORLEVEL% equ 0 (
    docker logs kafka-broker-1 --tail %LINEAS% 2>&1
) else (
    echo   [ WARN ] kafka-broker-1 no esta corriendo
)
if "%BROKER%"=="1" goto FIN

:LOG2
echo.
echo ============================================================
echo   LOGS: kafka-broker-2
echo ============================================================
docker inspect kafka-broker-2 --format "{{.State.Status}}" 2>nul | findstr "running" >nul
if %ERRORLEVEL% equ 0 (
    docker logs kafka-broker-2 --tail %LINEAS% 2>&1
) else (
    echo   [ WARN ] kafka-broker-2 no esta corriendo
)
if "%BROKER%"=="2" goto FIN

:LOG3
echo.
echo ============================================================
echo   LOGS: kafka-broker-3
echo ============================================================
docker inspect kafka-broker-3 --format "{{.State.Status}}" 2>nul | findstr "running" >nul
if %ERRORLEVEL% equ 0 (
    docker logs kafka-broker-3 --tail %LINEAS% 2>&1
) else (
    echo   [ WARN ] kafka-broker-3 no esta corriendo
)

:FIN
echo.
echo Uso: 24-ver-logs-brokers.bat [1^|2^|3^|todos] [numero-de-lineas]
echo Ejemplo: 24-ver-logs-brokers.bat 2 100
echo.
pause
endlocal
