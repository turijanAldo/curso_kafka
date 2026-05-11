@echo off
chcp 65001 >nul 2>&1
setlocal EnableDelayedExpansion

echo.
echo ============================================================
echo   Laboratorio Kafka - Nivel 1: Iniciando Cluster
echo ============================================================
echo.

:: ── 1. Verificar directorio correcto ─────────────────────────
echo ^>^>^> Paso 1: Verificando directorio de trabajo

:: Subir dos niveles desde scripts\cmd\ para llegar a la raiz
set "SCRIPT_DIR=%~dp0"
:: Resolvemos la ruta absoluta para evitar problemas con rutas relativas
for %%i in ("%SCRIPT_DIR%..\..") do set "ROOT_DIR=%%~fi\"
set "COMPOSE_FILE=%ROOT_DIR%docker\docker-compose.yml"

if not exist "%COMPOSE_FILE%" (
    echo [ ERROR] No se encontro docker\docker-compose.yml
    echo          Ejecuta este script desde la carpeta del laboratorio.
    pause
    exit /b 1
)
echo [  OK  ] Directorio encontrado

:: ── 2. Levantar contenedor ───────────────────────────────────
echo.
echo ^>^>^> Paso 2: Levantando contenedor Kafka

pushd "%ROOT_DIR%docker" || exit /b 1
docker-compose up -d
if %ERRORLEVEL% neq 0 (
    echo [ ERROR] docker-compose up fallo. Revisa que Docker Desktop este corriendo.
    popd
    pause
    exit /b 1
)
popd
echo [  OK  ] Comando docker-compose up -d ejecutado

:: ── 3. Esperar a que Kafka este listo ────────────────────────
echo.
echo ^>^>^> Paso 3: Esperando que Kafka inicie (30 segundos)...

set /A contador=0
:ESPERAR
if %contador% geq 6 goto PROBAR_CONEXION
set /A segundos=%contador% * 5
echo [ INFO ] Esperando... !segundos! de 30 segundos
timeout /t 5 /nobreak >nul
set /A contador+=1
goto ESPERAR

:PROBAR_CONEXION
echo [ INFO ] Verificando conexion al puerto 9092...

:: Intentar conexion al puerto 9092
powershell -Command "Test-NetConnection -ComputerName localhost -Port 9092 -WarningAction SilentlyContinue | Select-Object -ExpandProperty TcpTestSucceeded" 2>nul | findstr "True" >nul
if %ERRORLEVEL% equ 0 (
    echo [  OK  ] Kafka responde en localhost:9092
) else (
    echo [ WARN ] No se pudo verificar conexion a 9092. Kafka puede estar aun iniciando.
    echo          Espera unos segundos mas y ejecuta 02-verificar-cluster.bat
)

:: ── 4. Estado del contenedor ────────────────────────────────
echo.
echo ^>^>^> Paso 4: Estado del contenedor
docker ps --filter "name=kafka-nivel1"

:: ── 5. Ultimos logs ──────────────────────────────────────────
echo.
echo ^>^>^> Paso 5: Ultimos logs del contenedor
docker logs kafka-nivel1 --tail 15 2>&1

echo.
echo ============================================================
echo   KAFKA INICIADO
echo ============================================================
echo.
echo   Broker en: localhost:9092
echo   Siguiente: ejecuta 02-verificar-cluster.bat
echo.
pause
endlocal
