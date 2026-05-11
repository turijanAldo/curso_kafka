@echo off
chcp 65001 >nul 2>&1
setlocal

set CONTENEDOR=kafka-nivel1
set BOOTSTRAP=localhost:9092

echo.
echo ============================================================
echo   Laboratorio Kafka - Nivel 1: Verificacion del Cluster
echo ============================================================
echo.

:: ── Verificar que el contenedor corre ───────────────────────
docker inspect %CONTENEDOR% --format "{{.State.Status}}" 2>nul | findstr "running" >nul
if %ERRORLEVEL% neq 0 (
    echo [ ERROR] El contenedor %CONTENEDOR% no esta corriendo.
    echo          Ejecuta primero: 01-iniciar-kafka.bat
    pause
    exit /b 1
)
echo [  OK  ] Contenedor %CONTENEDOR% esta corriendo

:: ── 1. Versiones de API ──────────────────────────────────────
echo.
echo ^>^>^> 1. Versiones de API del broker
echo ------------------------------------------------------------
docker exec %CONTENEDOR% /opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server %BOOTSTRAP%
echo ------------------------------------------------------------

:: ── 2. Listar topics ─────────────────────────────────────────
echo.
echo ^>^>^> 2. Topics existentes
echo ------------------------------------------------------------
docker exec %CONTENEDOR% /opt/kafka/bin/kafka-topics.sh --bootstrap-server %BOOTSTRAP% --list
echo (si no aparece nada, no hay topics de usuario aun - esto es normal)
echo ------------------------------------------------------------

:: ── 3. Configuracion del broker ──────────────────────────────
echo.
echo ^>^>^> 3. Configuracion del broker
echo ------------------------------------------------------------
docker exec %CONTENEDOR% /opt/kafka/bin/kafka-configs.sh --bootstrap-server %BOOTSTRAP% --describe --entity-type brokers --entity-name 1
echo ------------------------------------------------------------

:: ── 4. Metadata KRaft ────────────────────────────────────────
echo.
echo ^>^>^> 4. Estado del quorum KRaft
echo ------------------------------------------------------------
docker exec %CONTENEDOR% /opt/kafka/bin/kafka-metadata-quorum.sh --bootstrap-server %BOOTSTRAP% describe --status
echo ------------------------------------------------------------

echo.
echo ============================================================
echo   VERIFICACION COMPLETADA
echo ============================================================
echo   El cluster esta operativo.
echo   Siguiente: sigue experimentos\exp-01-primer-mensaje.md
echo.
pause
endlocal
