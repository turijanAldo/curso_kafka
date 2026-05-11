@echo off
chcp 65001 >nul 2>&1
setlocal

echo.
echo ============================================================
echo   Kafka Nivel 3: Verificacion del Cluster Multi-Broker
echo ============================================================

:: ── Verificar contenedores ───────────────────────────────────
echo.
echo ^>^>^> 1. Estado de los contenedores
docker ps --filter "name=kafka-broker"

:: ── Quorum KRaft ─────────────────────────────────────────────
echo.
echo ^>^>^> 2. Estado del quorum KRaft
echo ------------------------------------------------------------
docker exec kafka-broker-1 /opt/kafka/bin/kafka-metadata-quorum.sh --bootstrap-server localhost:9092 describe --status
echo ------------------------------------------------------------

:: ── Topics ───────────────────────────────────────────────────
echo.
echo ^>^>^> 3. Topics en el cluster
echo ------------------------------------------------------------
docker exec kafka-broker-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
echo (los __consumer_offsets y similares son internos de Kafka)
echo ------------------------------------------------------------

:: ── Describe todos los topics de usuario ─────────────────────
echo.
echo ^>^>^> 4. Descripcion de particiones por topic
echo ------------------------------------------------------------
for /f "tokens=*" %%t in ('docker exec kafka-broker-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list 2^>nul ^| findstr /V "^__"') do (
    echo.
    echo   Topic: %%t
    docker exec kafka-broker-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic %%t
)
echo ------------------------------------------------------------

:: ── Recursos de los brokers ──────────────────────────────────
echo.
echo ^>^>^> 5. Uso de recursos de los 3 brokers (snapshot)
docker stats kafka-broker-1 kafka-broker-2 kafka-broker-3 --no-stream

echo.
echo ============================================================
echo   VERIFICACION COMPLETADA
echo ============================================================
echo.
pause
endlocal
