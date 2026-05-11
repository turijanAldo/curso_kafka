@echo off
chcp 65001 >nul 2>&1
setlocal

echo.
echo ============================================================
echo   Kafka Nivel 3: Deteniendo Cluster Multi-Broker
echo ============================================================
echo.
echo [ INFO ] Los datos en los volumenes kafka-data-1/2/3 se conservan.
echo.

set "SCRIPT_DIR=%~dp0"
for %%i in ("%SCRIPT_DIR%..\..") do set "ROOT_DIR=%%~fi\"

echo ^>^>^> Ejecutando docker-compose down
pushd "%ROOT_DIR%docker"
docker-compose -f docker-compose-cluster.yml down
if %ERRORLEVEL% neq 0 (
    echo [ WARN ] docker-compose down termino con advertencia (puede ser normal)
) else (
    echo [  OK  ] Contenedores detenidos
)
popd

echo.
echo ^>^>^> Verificando estado
docker ps --filter "name=kafka-broker"

echo.
echo ^>^>^> Volumenes persistentes disponibles
docker volume ls --filter "name=kafka-data"

echo.
echo ============================================================
echo   CLUSTER DETENIDO
echo ============================================================
echo   Para reiniciar: 20-iniciar-cluster.bat
echo.
pause
endlocal
