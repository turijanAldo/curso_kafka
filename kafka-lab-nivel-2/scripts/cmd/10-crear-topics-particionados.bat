@echo off
chcp 65001 >nul 2>&1
setlocal

set CONTENEDOR=kafka-nivel1
set BOOTSTRAP=localhost:9092

echo.
echo ============================================================
echo   Kafka Nivel 2: Creacion de Topics Particionados
echo ============================================================
echo.

:: ── Verificar que el contenedor corre ───────────────────────
docker inspect %CONTENEDOR% --format "{{.State.Status}}" 2>nul | findstr "running" >nul
if %ERRORLEVEL% neq 0 (
    echo [ ERROR] El contenedor %CONTENEDOR% no esta corriendo.
    echo          Inicia Kafka con: ..\kafka-lab-nivel-1\scripts\cmd\01-iniciar-kafka.bat
    pause & exit /b 1
)
echo [  OK  ] Contenedor %CONTENEDOR% activo

:: ── Topic 1: 1 particion ─────────────────────────────────────
echo.
echo ^>^>^> Creando topic transacciones-1p (1 particion)
docker exec %CONTENEDOR% /opt/kafka/bin/kafka-topics.sh --bootstrap-server %BOOTSTRAP% --delete --topic transacciones-1p 2>nul
timeout /t 2 /nobreak >nul
docker exec %CONTENEDOR% /opt/kafka/bin/kafka-topics.sh --bootstrap-server %BOOTSTRAP% --create --topic transacciones-1p --partitions 1 --replication-factor 1
echo.
echo [ INFO ] Descripcion del topic transacciones-1p:
docker exec %CONTENEDOR% /opt/kafka/bin/kafka-topics.sh --bootstrap-server %BOOTSTRAP% --describe --topic transacciones-1p

:: ── Topic 2: 4 particiones ───────────────────────────────────
echo.
echo ^>^>^> Creando topic transacciones-4p (4 particiones)
docker exec %CONTENEDOR% /opt/kafka/bin/kafka-topics.sh --bootstrap-server %BOOTSTRAP% --delete --topic transacciones-4p 2>nul
timeout /t 2 /nobreak >nul
docker exec %CONTENEDOR% /opt/kafka/bin/kafka-topics.sh --bootstrap-server %BOOTSTRAP% --create --topic transacciones-4p --partitions 4 --replication-factor 1
echo.
echo [ INFO ] Descripcion del topic transacciones-4p:
docker exec %CONTENEDOR% /opt/kafka/bin/kafka-topics.sh --bootstrap-server %BOOTSTRAP% --describe --topic transacciones-4p

:: ── Topic 3: 8 particiones ───────────────────────────────────
echo.
echo ^>^>^> Creando topic transacciones-8p (8 particiones)
docker exec %CONTENEDOR% /opt/kafka/bin/kafka-topics.sh --bootstrap-server %BOOTSTRAP% --delete --topic transacciones-8p 2>nul
timeout /t 2 /nobreak >nul
docker exec %CONTENEDOR% /opt/kafka/bin/kafka-topics.sh --bootstrap-server %BOOTSTRAP% --create --topic transacciones-8p --partitions 8 --replication-factor 1
echo.
echo [ INFO ] Descripcion del topic transacciones-8p:
docker exec %CONTENEDOR% /opt/kafka/bin/kafka-topics.sh --bootstrap-server %BOOTSTRAP% --describe --topic transacciones-8p

:: ── Listar todos los topics ──────────────────────────────────
echo.
echo ^>^>^> Topics disponibles en el cluster:
docker exec %CONTENEDOR% /opt/kafka/bin/kafka-topics.sh --bootstrap-server %BOOTSTRAP% --list

echo.
echo ============================================================
echo   TOPICS CREADOS CORRECTAMENTE
echo ============================================================
echo   transacciones-1p  -^> 1 particion
echo   transacciones-4p  -^> 4 particiones
echo   transacciones-8p  -^> 8 particiones
echo.
echo   Siguiente: ejecuta 11-describir-particiones.bat
echo.
pause
endlocal
