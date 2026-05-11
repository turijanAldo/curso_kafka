@echo off
REM =============================================================================
REM 30-crear-topics-replicados.bat
REM Nivel 4 - Crear topics con diferentes factores de replicacion
REM =============================================================================

setlocal EnableDelayedExpansion

for %%i in ("%~dp0..\..") do set "ROOT_DIR=%%~fi"
set "RESULTADOS=%ROOT_DIR%\experimentos\resultados"

echo.
echo ==============================================================
echo   NIVEL 4 - CREAR TOPICS REPLICADOS
echo ==============================================================
echo.

REM Verificar que el cluster esta corriendo
echo [>>] Verificando cluster de 3 brokers...
docker inspect --format={{.State.Status}} kafka-broker-1 2>nul | findstr "running" >nul
if errorlevel 1 (
    echo [ERROR] kafka-broker-1 no esta corriendo.
    echo         Ejecuta: cd ..\kafka-lab-nivel-3 ^& .\scripts\powershell\20-iniciar-cluster.ps1
    exit /b 1
)
echo [OK] Cluster de 3 brokers detectado

echo.
echo ==============================================================
echo   Topic 1/4: transacciones-rf1 (RF=1 - sin redundancia)
echo ==============================================================
echo [>>] RF=1: cada particion existe solo en UN broker.
echo [>>] Si ese broker falla, los datos se pierden.
echo [>>] Sirve como linea base para comparar rendimiento.

docker exec kafka-broker-1 /opt/kafka/bin/kafka-topics.sh ^
    --bootstrap-server localhost:9092 ^
    --delete --topic transacciones-rf1 2>nul
timeout /t 2 /nobreak >nul

docker exec kafka-broker-1 /opt/kafka/bin/kafka-topics.sh ^
    --bootstrap-server localhost:9092 ^
    --create ^
    --topic transacciones-rf1 ^
    --partitions 4 ^
    --replication-factor 1

echo.
echo [>>] Estado del topic transacciones-rf1:
docker exec kafka-broker-1 /opt/kafka/bin/kafka-topics.sh ^
    --bootstrap-server localhost:9092 ^
    --describe ^
    --topic transacciones-rf1 > "%RESULTADOS%\topic-transacciones-rf1-estado-inicial.txt"
type "%RESULTADOS%\topic-transacciones-rf1-estado-inicial.txt"

echo.
echo ==============================================================
echo   Topic 2/4: transacciones-rf2 (RF=2 - tolera 1 fallo)
echo ==============================================================
echo [>>] RF=2: cada particion existe en DOS brokers.
echo [>>] Si el leader falla, el follower toma el rol de leader.
echo [>>] Tolera perder 1 broker sin perder datos.

docker exec kafka-broker-1 /opt/kafka/bin/kafka-topics.sh ^
    --bootstrap-server localhost:9092 ^
    --delete --topic transacciones-rf2 2>nul
timeout /t 2 /nobreak >nul

docker exec kafka-broker-1 /opt/kafka/bin/kafka-topics.sh ^
    --bootstrap-server localhost:9092 ^
    --create ^
    --topic transacciones-rf2 ^
    --partitions 4 ^
    --replication-factor 2

docker exec kafka-broker-1 /opt/kafka/bin/kafka-topics.sh ^
    --bootstrap-server localhost:9092 ^
    --describe ^
    --topic transacciones-rf2 > "%RESULTADOS%\topic-transacciones-rf2-estado-inicial.txt"
type "%RESULTADOS%\topic-transacciones-rf2-estado-inicial.txt"

echo.
echo ==============================================================
echo   Topic 3/4: transacciones-rf3 (RF=3 - maxima redundancia)
echo ==============================================================
echo [>>] RF=3: cada particion existe en los 3 brokers.
echo [>>] Tolera perder hasta 2 brokers sin perder datos.
echo [>>] Precio: 3x mas espacio de disco en total.

docker exec kafka-broker-1 /opt/kafka/bin/kafka-topics.sh ^
    --bootstrap-server localhost:9092 ^
    --delete --topic transacciones-rf3 2>nul
timeout /t 2 /nobreak >nul

docker exec kafka-broker-1 /opt/kafka/bin/kafka-topics.sh ^
    --bootstrap-server localhost:9092 ^
    --create ^
    --topic transacciones-rf3 ^
    --partitions 4 ^
    --replication-factor 3

docker exec kafka-broker-1 /opt/kafka/bin/kafka-topics.sh ^
    --bootstrap-server localhost:9092 ^
    --describe ^
    --topic transacciones-rf3 > "%RESULTADOS%\topic-transacciones-rf3-estado-inicial.txt"
type "%RESULTADOS%\topic-transacciones-rf3-estado-inicial.txt"

echo.
echo ==============================================================
echo   Topic 4/4: critical-data (RF=3 + min.insync.replicas=2)
echo ==============================================================
echo [>>] Configuracion extra: min.insync.replicas=2
echo [>>] Con acks=all, el producer espera que AL MENOS 2 replicas
echo [>>] confirmen antes de considerar la escritura exitosa.
echo [>>] Maxima garantia de durabilidad para datos criticos.

docker exec kafka-broker-1 /opt/kafka/bin/kafka-topics.sh ^
    --bootstrap-server localhost:9092 ^
    --delete --topic critical-data 2>nul
timeout /t 2 /nobreak >nul

docker exec kafka-broker-1 /opt/kafka/bin/kafka-topics.sh ^
    --bootstrap-server localhost:9092 ^
    --create ^
    --topic critical-data ^
    --partitions 2 ^
    --replication-factor 3 ^
    --config min.insync.replicas=2

docker exec kafka-broker-1 /opt/kafka/bin/kafka-topics.sh ^
    --bootstrap-server localhost:9092 ^
    --describe ^
    --topic critical-data > "%RESULTADOS%\topic-critical-data-estado-inicial.txt"
type "%RESULTADOS%\topic-critical-data-estado-inicial.txt"

echo.
echo ==============================================================
echo   Todos los topics creados exitosamente
echo ==============================================================
echo.
echo   Estados iniciales guardados en:
echo   %RESULTADOS%\
echo.
echo   Proximos pasos:
echo   31-describir-replicas.bat   (analisis del ISR)
echo   32-simular-fallo-broker.bat (simular fallo)
echo.
endlocal
