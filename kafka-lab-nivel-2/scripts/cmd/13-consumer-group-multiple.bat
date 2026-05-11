@echo off
chcp 65001 >nul 2>&1
setlocal

set TOPIC=transacciones-4p
set GROUP_ID=grupo-nivel-2
set "SCRIPT_DIR=%~dp0"
for %%i in ("%SCRIPT_DIR%..\..") do set "ROOT_DIR=%%~fi\"
set "JAR=%ROOT_DIR%java\target\kafka-lab-nivel-2-1.0.0.jar"

echo.
echo ============================================================
echo   Kafka Nivel 2: Consumer Group con Multiples Instancias
echo ============================================================
echo.

:: ── Verificar JAVA_HOME ──────────────────────────────────────
if "%JAVA_HOME%"=="" (
    echo [ ERROR] JAVA_HOME no configurado.
    pause & exit /b 1
)
set "JAVA_EXE=%JAVA_HOME%\bin\java.exe"
if not exist "%JAVA_EXE%" (
    echo [ ERROR] java.exe no encontrado en: %JAVA_EXE%
    pause & exit /b 1
)

if not exist "%JAR%" (
    echo [ ERROR] JAR no encontrado: %JAR%
    echo          Compila primero: cd java ^&^& mvn clean package
    pause & exit /b 1
)

echo [  OK  ] JAVA_HOME y JAR verificados
echo.
echo   Se abriran 4 ventanas CMD.
echo   Observa en cada ventana que particiones le fueron asignadas.
echo.
pause

:: ── Lanzar 4 consumers en ventanas CMD separadas ─────────────
echo ^>^>^> Iniciando consumer-1...
start "Kafka Consumer 1" cmd /k ""%JAVA_EXE%" -cp "%JAR%" com.nexus.kafka.nivel2.InstrumentedConsumer %TOPIC% %GROUP_ID% consumer-1"
timeout /t 1 /nobreak >nul

echo ^>^>^> Iniciando consumer-2...
start "Kafka Consumer 2" cmd /k ""%JAVA_EXE%" -cp "%JAR%" com.nexus.kafka.nivel2.InstrumentedConsumer %TOPIC% %GROUP_ID% consumer-2"
timeout /t 1 /nobreak >nul

echo ^>^>^> Iniciando consumer-3...
start "Kafka Consumer 3" cmd /k ""%JAVA_EXE%" -cp "%JAR%" com.nexus.kafka.nivel2.InstrumentedConsumer %TOPIC% %GROUP_ID% consumer-3"
timeout /t 1 /nobreak >nul

echo ^>^>^> Iniciando consumer-4...
start "Kafka Consumer 4" cmd /k ""%JAVA_EXE%" -cp "%JAR%" com.nexus.kafka.nivel2.InstrumentedConsumer %TOPIC% %GROUP_ID% consumer-4"

echo.
echo ============================================================
echo   4 CONSUMERS INICIADOS
echo ============================================================
echo.
echo   QUE HACER AHORA:
echo   1. Espera ~5 segundos al rebalanceo inicial
echo   2. Observa el mensaje ASIGNADO A en cada ventana
echo   3. Verifica que cada consumer tiene 1 particion
echo   4. Ejecuta 12-producer-con-claves.bat para enviar mensajes
echo   5. Cierra una ventana y observa el rebalanceo automatico
echo.

timeout /t 6 /nobreak >nul

echo ^>^>^> Estado del consumer group en el broker:
docker exec kafka-nivel1 /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group %GROUP_ID%

echo.
pause
endlocal
