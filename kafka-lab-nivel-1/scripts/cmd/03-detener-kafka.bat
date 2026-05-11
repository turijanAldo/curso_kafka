@echo off
chcp 65001 >nul 2>&1
setlocal

echo.
echo ============================================================
echo   Laboratorio Kafka - Nivel 1: Deteniendo Cluster
echo ============================================================
echo.
echo [ INFO ] Los datos en volumenes Docker se conservan.
echo.

:: ── Localizar directorio raiz ────────────────────────────────
set "SCRIPT_DIR=%~dp0"
set "ROOT_DIR=%SCRIPT_DIR%..\.."
set "COMPOSE_FILE=%ROOT_DIR%docker\docker-compose.yml"

if not exist "%COMPOSE_FILE%" (
    echo [ ERROR] No se encontro docker\docker-compose.yml
    pause
    exit /b 1
)

:: ── Detener con docker-compose down ─────────────────────────
echo ^>^>^> Ejecutando docker-compose down...

pushd "%ROOT_DIR%docker"
docker-compose down
if %ERRORLEVEL% neq 0 (
    echo [ WARN ] docker-compose down termino con error (puede ser normal si ya estaba detenido)
) else (
    echo [  OK  ] Contenedor detenido correctamente
)
popd

echo.
echo ============================================================
echo   CLUSTER DETENIDO
echo ============================================================
echo.
echo   Datos conservados en volumen Docker.
echo   Para reiniciar: 01-iniciar-kafka.bat
echo   Para limpiar todo: 04-limpiar-todo.bat
echo.
pause
endlocal
