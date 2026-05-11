@echo off
chcp 65001 >nul 2>&1
setlocal

echo.
echo ============================================================
echo   ADVERTENCIA: LIMPIEZA TOTAL DEL LABORATORIO
echo ============================================================
echo.
echo   Esta operacion realizara:
echo     - Eliminar el contenedor kafka-nivel1
echo     - ELIMINAR el volumen Docker con todos los datos
echo     - Limpiar el directorio logs/
echo.
echo   TODOS los topics y mensajes seran ELIMINADOS DEFINITIVAMENTE.
echo.

set /P CONFIRMACION=Escribe SI para confirmar (cualquier otra entrada cancela):

if /I not "%CONFIRMACION%"=="SI" (
    echo.
    echo [ INFO ] Operacion cancelada. No se eliminaron datos.
    pause
    exit /b 0
)

echo.
echo [ INFO ] Confirmacion recibida. Procediendo con la limpieza...

:: ── Localizar directorio raiz ────────────────────────────────
set "SCRIPT_DIR=%~dp0"
set "ROOT_DIR=%SCRIPT_DIR%..\.."

:: ── 1. Detener y eliminar contenedores + volumenes ───────────
echo.
echo ^>^>^> 1. Ejecutando docker-compose down -v

pushd "%ROOT_DIR%docker"
docker-compose down -v
if %ERRORLEVEL% neq 0 (
    echo [ WARN ] docker-compose down -v termino con advertencia (puede ser normal)
) else (
    echo [  OK  ] Contenedores y volumenes eliminados
)
popd

:: ── 2. Limpiar directorio logs/ ──────────────────────────────
echo.
echo ^>^>^> 2. Limpiando directorio logs/

if exist "%ROOT_DIR%logs" (
    for /f "delims=" %%f in ('dir /b /a-d "%ROOT_DIR%logs\*" 2^>nul ^| findstr /v ".gitkeep"') do (
        del /f /q "%ROOT_DIR%logs\%%f" >nul 2>&1
    )
    echo [  OK  ] Directorio logs/ limpiado
)

:: ── 3. Limpiar resultados de experimentos ────────────────────
echo.
echo ^>^>^> 3. Limpiando resultados de experimentos

if exist "%ROOT_DIR%experimentos\resultados" (
    for /f "delims=" %%f in ('dir /b /a-d "%ROOT_DIR%experimentos\resultados\*" 2^>nul ^| findstr /v ".gitkeep"') do (
        del /f /q "%ROOT_DIR%experimentos\resultados\%%f" >nul 2>&1
    )
    echo [  OK  ] Directorio experimentos/resultados/ limpiado
)

echo.
echo ============================================================
echo   LIMPIEZA COMPLETADA
echo ============================================================
echo.
echo   El entorno esta como nuevo.
echo   Para reiniciar:
echo     1. 00-verificar-requisitos.bat
echo     2. 01-iniciar-kafka.bat
echo.
pause
endlocal
