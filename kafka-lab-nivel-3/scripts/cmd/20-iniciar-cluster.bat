@echo off
chcp 65001 >nul 2>&1
setlocal EnableDelayedExpansion

echo.
echo ============================================================
echo   Kafka Nivel 3: Iniciando Cluster Multi-Broker (3 nodos)
echo ============================================================
echo.

:: ── Localizar directorio raiz ────────────────────────────────
set "SCRIPT_DIR=%~dp0"
for %%i in ("%SCRIPT_DIR%..\..") do set "ROOT_DIR=%%~fi\"
set "COMPOSE_FILE=%ROOT_DIR%docker\docker-compose-cluster.yml"

if not exist "%COMPOSE_FILE%" (
    echo [ ERROR] No se encontro docker-compose-cluster.yml en: %ROOT_DIR%docker\
    pause & exit /b 1
)

:: ── Verificar Docker ─────────────────────────────────────────
docker ps >nul 2>&1
if %ERRORLEVEL% neq 0 (
    echo [ ERROR] Docker Desktop no esta corriendo. Inicialo primero.
    pause & exit /b 1
)
echo [  OK  ] Docker Desktop activo

:: ── Verificar puertos ────────────────────────────────────────
echo.
echo ^>^>^> Verificando disponibilidad de puertos 9092, 9093, 9094
for %%P in (9092 9093 9094) do (
    netstat -an | findstr ":%%P " | findstr "LISTENING" >nul 2>&1
    if !ERRORLEVEL! equ 0 (
        echo [ ERROR] Puerto %%P ya esta en uso. Libera ese puerto antes de continuar.
        pause & exit /b 1
    ) else (
        echo [  OK  ] Puerto %%P disponible
    )
)

:: ── Levantar el cluster ──────────────────────────────────────
echo.
echo ^>^>^> Levantando 3 brokers con docker-compose
pushd "%ROOT_DIR%docker"
docker-compose -f docker-compose-cluster.yml up -d
if %ERRORLEVEL% neq 0 (
    echo [ ERROR] docker-compose up fallo.
    popd & pause & exit /b 1
)
popd
echo [  OK  ] docker-compose up ejecutado

:: ── Esperar a que los 3 brokers esten listos ─────────────────
echo.
echo ^>^>^> Esperando que los 3 brokers inicien (puede tomar ~90 segundos)...
echo [ INFO ] Los brokers necesitan hacer KRaft leader election entre si

set /A intentos=0
:ESPERAR_BROKERS
if %intentos% geq 18 goto TIMEOUT_BROKERS
set /A segundos=%intentos% * 10
echo [ INFO ] Esperando... !segundos! de 120 segundos
timeout /t 10 /nobreak >nul
set /A intentos+=1

:: Verificar si el puerto 9092 responde (proxy para "cluster listo")
powershell -Command "Test-NetConnection -ComputerName localhost -Port 9092 -WarningAction SilentlyContinue | Select-Object -ExpandProperty TcpTestSucceeded" 2>nul | findstr "True" >nul
if %ERRORLEVEL% equ 0 (
    powershell -Command "Test-NetConnection -ComputerName localhost -Port 9093 -WarningAction SilentlyContinue | Select-Object -ExpandProperty TcpTestSucceeded" 2>nul | findstr "True" >nul
    if !ERRORLEVEL! equ 0 (
        powershell -Command "Test-NetConnection -ComputerName localhost -Port 9094 -WarningAction SilentlyContinue | Select-Object -ExpandProperty TcpTestSucceeded" 2>nul | findstr "True" >nul
        if !ERRORLEVEL! equ 0 goto BROKERS_LISTOS
    )
)
goto ESPERAR_BROKERS

:TIMEOUT_BROKERS
echo [ WARN ] Timeout esperando brokers. Verifica con: docker ps
goto RESUMEN

:BROKERS_LISTOS
echo [  OK  ] Los 3 brokers responden en sus puertos

:: ── Estado del quorum KRaft ──────────────────────────────────
echo.
echo ^>^>^> Estado del quorum KRaft
docker exec kafka-broker-1 /opt/kafka/bin/kafka-metadata-quorum.sh --bootstrap-server localhost:9092 describe --status 2>&1

:: ── Estado de los contenedores ───────────────────────────────
echo.
echo ^>^>^> Estado de los contenedores
docker ps --filter "name=kafka-broker"

:RESUMEN
echo.
echo ============================================================
echo   CLUSTER MULTI-BROKER INICIADO
echo ============================================================
echo.
echo   Broker 1 -^> localhost:9092  (node.id=1)
echo   Broker 2 -^> localhost:9093  (node.id=2)
echo   Broker 3 -^> localhost:9094  (node.id=3)
echo.
echo   Bootstrap servers: localhost:9092,localhost:9093,localhost:9094
echo.
echo   Siguiente: ejecuta 21-verificar-cluster.bat
echo.
pause
endlocal
