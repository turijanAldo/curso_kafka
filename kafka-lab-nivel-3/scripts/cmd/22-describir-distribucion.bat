@echo off
chcp 65001 >nul 2>&1
setlocal

:: Parametro opcional: nombre del topic a analizar
set "TOPIC_FILTRO=%~1"
set "SCRIPT_DIR=%~dp0"
for %%i in ("%SCRIPT_DIR%..\..") do set "ROOT_DIR=%%~fi\"
set "RESULTADOS=%ROOT_DIR%experimentos\resultados\distribucion-leaders.txt"

echo.
echo ============================================================
echo   Kafka Nivel 3: Analisis de Distribucion de Particiones
echo ============================================================

docker inspect kafka-broker-1 --format "{{.State.Status}}" 2>nul | findstr "running" >nul
if %ERRORLEVEL% neq 0 (
    echo [ ERROR] kafka-broker-1 no esta corriendo.
    pause & exit /b 1
)

:: Encabezado del archivo de resultados
echo === Reporte Distribucion de Leaders === > "%RESULTADOS%"
echo Fecha: %DATE% %TIME% >> "%RESULTADOS%"
echo. >> "%RESULTADOS%"

if "%TOPIC_FILTRO%"=="" (
    echo [ INFO ] Analizando todos los topics de usuario...
    echo.
    for /f "tokens=*" %%t in ('docker exec kafka-broker-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list 2^>nul ^| findstr /V "^__"') do (
        call :ANALIZAR_TOPIC %%t
    )
) else (
    call :ANALIZAR_TOPIC %TOPIC_FILTRO%
)

echo.
echo [  OK  ] Reporte guardado en: experimentos\resultados\distribucion-leaders.txt
echo.
pause
endlocal
goto :EOF

:ANALIZAR_TOPIC
set "T=%~1"
echo.
echo   === Topic: %T% ===
echo   Topic: %T% >> "%RESULTADOS%"
docker exec kafka-broker-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic %T%
docker exec kafka-broker-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic %T% >> "%RESULTADOS%" 2>&1
echo. >> "%RESULTADOS%"
goto :EOF
