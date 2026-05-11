@echo off
chcp 65001 >nul 2>&1
setlocal

set CONTENEDOR=kafka-nivel1
set BOOTSTRAP=localhost:9092

echo.
echo ============================================================
echo   Kafka Nivel 2: Descripcion de Particiones
echo ============================================================

docker inspect %CONTENEDOR% --format "{{.State.Status}}" 2>nul | findstr "running" >nul
if %ERRORLEVEL% neq 0 (
    echo [ ERROR] Contenedor %CONTENEDOR% no esta corriendo.
    pause & exit /b 1
)

:: ── transacciones-1p ─────────────────────────────────────────
echo.
echo ^>^>^> Topic: transacciones-1p
echo ------------------------------------------------------------
docker exec %CONTENEDOR% /opt/kafka/bin/kafka-topics.sh --bootstrap-server %BOOTSTRAP% --describe --topic transacciones-1p
echo.
docker exec %CONTENEDOR% /opt/kafka/bin/kafka-configs.sh --bootstrap-server %BOOTSTRAP% --describe --entity-type topics --entity-name transacciones-1p
echo ------------------------------------------------------------

:: ── transacciones-4p ─────────────────────────────────────────
echo.
echo ^>^>^> Topic: transacciones-4p
echo ------------------------------------------------------------
docker exec %CONTENEDOR% /opt/kafka/bin/kafka-topics.sh --bootstrap-server %BOOTSTRAP% --describe --topic transacciones-4p
echo.
docker exec %CONTENEDOR% /opt/kafka/bin/kafka-configs.sh --bootstrap-server %BOOTSTRAP% --describe --entity-type topics --entity-name transacciones-4p
echo ------------------------------------------------------------

:: ── transacciones-8p ─────────────────────────────────────────
echo.
echo ^>^>^> Topic: transacciones-8p
echo ------------------------------------------------------------
docker exec %CONTENEDOR% /opt/kafka/bin/kafka-topics.sh --bootstrap-server %BOOTSTRAP% --describe --topic transacciones-8p
echo.
docker exec %CONTENEDOR% /opt/kafka/bin/kafka-configs.sh --bootstrap-server %BOOTSTRAP% --describe --entity-type topics --entity-name transacciones-8p
echo ------------------------------------------------------------

echo.
echo Guia de la tabla:
echo   Partition -^> numero de la particion (0-based)
echo   Leader    -^> ID del broker lider de esta particion
echo   Replicas  -^> brokers con copia de esta particion
echo   Isr       -^> In-Sync Replicas: brokers con datos al dia
echo.
pause
endlocal
