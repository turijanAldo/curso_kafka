@echo off
chcp 65001 >nul 2>&1
setlocal EnableDelayedExpansion
set ERRORES=0
set ADVERTENCIAS=0

echo.
echo ============================================================
echo   Laboratorio Kafka - Nivel 1: Verificacion de Requisitos
echo ============================================================
echo.

:: ── 1. Docker instalado ──────────────────────────────────────
echo === 1. Verificando instalacion de Docker ===
where docker >nul 2>&1
if %ERRORLEVEL% neq 0 (
    echo [ ERROR] Docker no esta instalado o no esta en el PATH
    echo          Descargalo desde: https://www.docker.com/products/docker-desktop
    set /A ERRORES+=1
) else (
    for /f "tokens=*" %%v in ('docker --version 2^>^&1') do (
        echo [  OK  ] Docker encontrado: %%v
    )
)

echo.

:: ── 2. Docker corriendo ──────────────────────────────────────
echo === 2. Verificando que Docker Desktop esta corriendo ===
docker ps >nul 2>&1
if %ERRORLEVEL% neq 0 (
    echo [ ERROR] Docker Desktop no esta corriendo. Inicia Docker Desktop.
    set /A ERRORES+=1
) else (
    echo [  OK  ] Docker Desktop esta corriendo correctamente
)

echo.

:: ── 3. docker-compose disponible ────────────────────────────
echo === 3. Verificando docker-compose ===
docker-compose --version >nul 2>&1
if %ERRORLEVEL% neq 0 (
    echo [ ERROR] docker-compose no encontrado. Instala Docker Desktop con Compose.
    set /A ERRORES+=1
) else (
    for /f "tokens=*" %%v in ('docker-compose --version 2^>^&1') do (
        echo [  OK  ] docker-compose disponible: %%v
    )
)

echo.

:: ── 4. Puerto 9092 disponible ────────────────────────────────
echo === 4. Verificando disponibilidad del puerto 9092 ===
netstat -an | findstr ":9092 " | findstr "LISTENING" >nul 2>&1
if %ERRORLEVEL% equ 0 (
    echo [ ERROR] Puerto 9092 ya esta en uso. Cierra la aplicacion que lo ocupa.
    set /A ERRORES+=1
) else (
    echo [  OK  ] Puerto 9092 disponible
)

echo.

:: ── 5. Puerto 9093 disponible ────────────────────────────────
echo === 5. Verificando disponibilidad del puerto 9093 ===
netstat -an | findstr ":9093 " | findstr "LISTENING" >nul 2>&1
if %ERRORLEVEL% equ 0 (
    echo [ WARN ] Puerto 9093 ocupado. Puede causar conflictos con KRaft controller.
    set /A ADVERTENCIAS+=1
) else (
    echo [  OK  ] Puerto 9093 disponible
)

echo.

:: ── 6. Informacion de memoria ───────────────────────────────
echo === 6. Informacion del sistema ===
:: Usamos wmic porque systeminfo depende del idioma del sistema operativo
wmic OS get FreePhysicalMemory,TotalVisibleMemorySize /Value

echo.

:: ── Resumen ──────────────────────────────────────────────────
echo ============================================================
echo   RESUMEN DE VERIFICACION
echo ============================================================
echo.
if %ERRORES% equ 0 (
    if %ADVERTENCIAS% equ 0 (
        echo   SISTEMA LISTO. Ejecuta: 01-iniciar-kafka.bat
    ) else (
        echo   SISTEMA CASI LISTO con %ADVERTENCIAS% advertencia(s).
        echo   Revisa los mensajes WARN anteriores.
    )
) else (
    echo   SISTEMA NO LISTO: %ERRORES% error(es) encontrado(s).
    echo   Corrige los errores marcados con ERROR antes de continuar.
)
echo.
pause
endlocal
