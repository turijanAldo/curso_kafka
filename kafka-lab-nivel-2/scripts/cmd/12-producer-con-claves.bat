@echo off
chcp 65001 >nul 2>&1
setlocal EnableDelayedExpansion

set TOPIC=transacciones-4p
set "SCRIPT_DIR=%~dp0"
for %%i in ("%SCRIPT_DIR%..\..") do set "ROOT_DIR=%%~fi\"
set "JAR=%ROOT_DIR%java\target\kafka-lab-nivel-2-1.0.0.jar"
set "RESULTADOS=%ROOT_DIR%experimentos\resultados\distribucion-mensajes.txt"

echo.
echo ============================================================
echo   Kafka Nivel 2: Producer con Claves (20 mensajes)
echo ============================================================
echo.

:: ── Verificar JAVA_HOME ──────────────────────────────────────
if "%JAVA_HOME%"=="" (
    echo [ ERROR] JAVA_HOME no configurado.
    echo          Ejemplo: set JAVA_HOME=C:\Program Files\Java\jdk-17
    pause & exit /b 1
)
set "JAVA_EXE=%JAVA_HOME%\bin\java.exe"
if not exist "%JAVA_EXE%" (
    echo [ ERROR] java.exe no encontrado en: %JAVA_EXE%
    pause & exit /b 1
)
echo [  OK  ] JAVA_HOME: %JAVA_HOME%

:: ── Verificar JAR ────────────────────────────────────────────
if not exist "%JAR%" (
    echo [ ERROR] JAR no encontrado: %JAR%
    echo          Compila primero: cd java ^&^& mvn clean package
    pause & exit /b 1
)
echo [  OK  ] JAR encontrado

:: ── Iniciar archivo de resultados ───────────────────────────
echo === Resultados distribucion por clave === > "%RESULTADOS%"
echo Fecha: %DATE% %TIME% >> "%RESULTADOS%"
echo Topic: %TOPIC% >> "%RESULTADOS%"
echo. >> "%RESULTADOS%"

echo.
echo ^>^>^> Enviando 20 mensajes con claves user-001 a user-020
echo [ INFO ] Pausa de 500ms entre cada envio
echo.

set /A contador=1
:LOOP
if %contador% gtr 20 goto FIN_LOOP

:: Formatear numero con ceros (001-020)
set "num=00%contador%"
set "clave=user-!num:~-3!"
set "valor={""userId"": ""!clave!"", ""monto"": !contador!0, ""tipo"": ""compra""}"

echo   [!contador!/20] Enviando clave: !clave! ...
echo [!contador!/20] Clave: !clave! >> "%RESULTADOS%"

"%JAVA_EXE%" -cp "%JAR%" com.nexus.kafka.nivel2.KeyedProducer %TOPIC% !clave! "!valor!" >> "%RESULTADOS%" 2>&1
"%JAVA_EXE%" -cp "%JAR%" com.nexus.kafka.nivel2.KeyedProducer %TOPIC% !clave! "!valor!"

echo. >> "%RESULTADOS%"

:: Pausa de 500ms
timeout /t 1 /nobreak >nul

set /A contador+=1
goto LOOP

:FIN_LOOP
echo.
echo ============================================================
echo   20 MENSAJES ENVIADOS
echo ============================================================
echo.
echo   Resultados en: experimentos\resultados\distribucion-mensajes.txt
echo   Nota: mensajes con la misma clave siempre van a la misma particion
echo.
pause
endlocal
