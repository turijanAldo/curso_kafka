# Reset de Niveles - Laboratorio Kafka

Guía para resetear cada nivel desde cero. Úsala cuando quieras repetir un experimento
con el entorno limpio, o cuando algo no funciona como se espera.

Cada sección indica exactamente qué se borra y qué se conserva.

---

## Índice

- [Antes de empezar: diagnóstico rápido](#antes-de-empezar-diagnóstico-rápido)
- [Reset Nivel 1 — Broker único](#reset-nivel-1--broker-único)
- [Reset Nivel 2 — Particiones y consumer groups](#reset-nivel-2--particiones-y-consumer-groups)
- [Reset Nivel 3 — Clúster multi-broker](#reset-nivel-3--clúster-multi-broker)
- [Reset Nivel 4 — Réplicas y failover](#reset-nivel-4--réplicas-y-failover)
- [Reset total — Todo el laboratorio](#reset-total--todo-el-laboratorio)

---

## Antes de empezar: diagnóstico rápido

Ejecuta esto para ver el estado actual de todos los contenedores y volúmenes:

```powershell
# ¿Qué contenedores Kafka están corriendo?
docker ps --filter "name=kafka" --format "table {{.Names}}\t{{.Status}}"

# ¿Qué volúmenes de datos existen?
docker volume ls --filter "name=kafka"

# ¿Cuántos topics hay en el broker único (Nivel 1/2)?
docker exec kafka-broker /opt/kafka/bin/kafka-topics.sh `
    --bootstrap-server localhost:9092 --list 2>$null

# ¿Cuántos topics hay en el clúster (Nivel 3/4)?
docker exec kafka-broker-1 /opt/kafka/bin/kafka-topics.sh `
    --bootstrap-server localhost:9092 --list 2>$null
```

---

## Reset Nivel 1 — Broker único

**Qué resetea:** broker, todos los topics, todos los mensajes, consumer group offsets.
**Qué conserva:** el código Java compilado (`kafka-lab-nivel-1-1.0.0.jar`).

### Paso 1 — Ir al directorio del Nivel 1

```powershell
cd C:\Users\aldo_\Documents\SIIE\kafka_laboratorio\kafka-lab-nivel-1
```

### Paso 2 — Detener el broker

```powershell
# PowerShell
docker-compose -f docker/docker-compose.yml down

# CMD
docker-compose -f docker\docker-compose.yml down
```

> Si no tienes `docker-compose.yml` en esa ruta, usa:
> ```powershell
> docker stop kafka-broker
> docker rm kafka-broker
> ```

### Paso 3 — Eliminar el volumen de datos

```powershell
# PowerShell — borra todos los mensajes y configuración del broker
docker-compose -f docker/docker-compose.yml down -v

# CMD
docker-compose -f docker\docker-compose.yml down -v
```

> Si el paso anterior ya hizo `down`, solo elimina el volumen:
> ```powershell
> docker volume rm kafka-lab-nivel-1_kafka_data
> # o el nombre que aparezca en:
> docker volume ls --filter "name=kafka"
> ```

### Paso 4 — Limpiar archivos de resultados (opcional)

```powershell
# PowerShell
Remove-Item -Path "experimentos\resultados\*" -Force -ErrorAction SilentlyContinue
# CMD
del /q experimentos\resultados\* 2>nul
```

### Paso 5 — Verificar que el volumen fue eliminado

```powershell
docker volume ls --filter "name=kafka"
# No debe aparecer ningún volumen del Nivel 1
```

### Paso 6 — Volver a levantar el broker limpio

```powershell
# PowerShell
.\scripts\powershell\01-iniciar-kafka.ps1

# CMD
.\scripts\cmd\01-iniciar-kafka.bat
```

### Paso 7 — Verificar que el broker está limpio

```powershell
# No debe haber ningún topic (solo __consumer_offsets si existe)
docker exec kafka-broker /opt/kafka/bin/kafka-topics.sh `
    --bootstrap-server localhost:9092 --list
```

**El Nivel 1 está listo para empezar desde cero.**

---

## Reset Nivel 2 — Particiones y consumer groups

**Qué resetea:** todos los topics del Nivel 2, mensajes, offsets de consumer groups.
**Qué conserva:** el broker del Nivel 1 si está corriendo (comparte infraestructura), el JAR compilado.

> El Nivel 2 usa el mismo broker del Nivel 1. Un reset del broker borra topics de ambos niveles.

### Opción A — Solo borrar los topics del Nivel 2 (broker sigue corriendo)

```powershell
# Paso 1: ir al directorio del Nivel 2
cd C:\Users\aldo_\Documents\SIIE\kafka_laboratorio\kafka-lab-nivel-2

# Paso 2: eliminar los topics de los experimentos
$topics = @("transacciones-1p","transacciones-4p","transacciones-8p")
foreach ($t in $topics) {
    docker exec kafka-broker /opt/kafka/bin/kafka-topics.sh `
        --bootstrap-server localhost:9092 --delete --topic $t 2>$null
    Write-Host "Eliminado: $t"
}
```

```cmd
REM CMD
docker exec kafka-broker /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic transacciones-1p
docker exec kafka-broker /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic transacciones-4p
docker exec kafka-broker /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic transacciones-8p
```

```powershell
# Paso 3: esperar a que la eliminación se propague
Start-Sleep -Seconds 3

# Paso 4: verificar que no quedan topics
docker exec kafka-broker /opt/kafka/bin/kafka-topics.sh `
    --bootstrap-server localhost:9092 --list
```

### Opción B — Reset completo (broker + datos)

Ejecuta el **Reset Nivel 1** completo. Al volver a levantar el broker, el Nivel 2 también tiene el entorno limpio.

### Limpiar archivos de resultados del Nivel 2

```powershell
# PowerShell
Remove-Item -Path "experimentos\resultados\*" -Force -ErrorAction SilentlyContinue
Remove-Item -Path "experimentos\resultados\distribucion-mensajes.txt" -Force -ErrorAction SilentlyContinue

# CMD
del /q experimentos\resultados\* 2>nul
```

### Recrear los topics del Nivel 2 desde cero

```powershell
# Una vez que el broker está limpio, recrea los topics
.\scripts\powershell\10-crear-topics-particionados.ps1

# CMD
.\scripts\cmd\10-crear-topics-particionados.bat
```

**El Nivel 2 está listo para empezar desde cero.**

---

## Reset Nivel 3 — Clúster multi-broker

**Qué resetea:** los 3 brokers, todos los topics y mensajes del clúster, offsets de consumer groups.
**Qué conserva:** el JAR compilado, los archivos markdown de experimentos.

### Paso 1 — Ir al directorio del Nivel 3

```powershell
cd C:\Users\aldo_\Documents\SIIE\kafka_laboratorio\kafka-lab-nivel-3
```

### Paso 2 — Detener el clúster y eliminar los volúmenes

```powershell
# PowerShell — detiene los 3 brokers Y elimina los 3 volúmenes de datos
cd docker
docker-compose -f docker-compose-cluster.yml down -v
cd ..
```

```cmd
REM CMD
cd docker
docker-compose -f docker-compose-cluster.yml down -v
cd ..
```

> Esto elimina los volúmenes `kafka-data-1`, `kafka-data-2` y `kafka-data-3`.
> Todos los topics y mensajes desaparecen.

### Paso 3 — Verificar que los volúmenes fueron eliminados

```powershell
docker volume ls --filter "name=kafka-data"
# No debe aparecer ninguno
```

### Paso 4 — Verificar que los contenedores fueron eliminados

```powershell
docker ps -a --filter "name=kafka-broker" --format "table {{.Names}}\t{{.Status}}"
# No debe aparecer ninguno
```

### Paso 5 — Limpiar archivos de resultados (opcional)

```powershell
# PowerShell
Remove-Item -Path "experimentos\resultados\*" -Force -ErrorAction SilentlyContinue

# CMD
del /q experimentos\resultados\* 2>nul
```

### Paso 6 — Volver a levantar el clúster limpio

```powershell
.\scripts\powershell\20-iniciar-cluster.ps1

# CMD
.\scripts\cmd\20-iniciar-cluster.bat
```

### Paso 7 — Verificar el clúster y que no hay topics

```powershell
.\scripts\powershell\21-verificar-cluster.ps1

# Confirmar que no hay topics
docker exec kafka-broker-1 /opt/kafka/bin/kafka-topics.sh `
    --bootstrap-server localhost:9092 --list
```

### Paso 8 — Recrear los topics del Nivel 3

```powershell
# Crea transacciones-6p, transacciones-12p, transacciones-5p
docker exec kafka-broker-1 /opt/kafka/bin/kafka-topics.sh `
    --bootstrap-server localhost:9092 --create `
    --topic transacciones-6p --partitions 6 --replication-factor 1

docker exec kafka-broker-1 /opt/kafka/bin/kafka-topics.sh `
    --bootstrap-server localhost:9092 --create `
    --topic transacciones-12p --partitions 12 --replication-factor 1

docker exec kafka-broker-1 /opt/kafka/bin/kafka-topics.sh `
    --bootstrap-server localhost:9092 --create `
    --topic transacciones-5p --partitions 5 --replication-factor 1
```

**El Nivel 3 está listo para empezar desde cero.**

---

## Reset Nivel 4 — Réplicas y failover

El Nivel 4 usa el mismo clúster de 3 brokers del Nivel 3. Un reset del Nivel 4 puede hacerse
en dos modos: **solo topics** (clúster sigue corriendo) o **reset completo** (igual que Nivel 3).

### Modo A — Solo borrar los topics del Nivel 4 (clúster sigue corriendo)

```powershell
# Paso 1: ir al directorio
cd C:\Users\aldo_\Documents\SIIE\kafka_laboratorio\kafka-lab-nivel-4

# Paso 2: asegurarse que los 3 brokers están corriendo
docker ps --filter "name=kafka-broker" --format "{{.Names}}"

# Paso 3: eliminar los topics replicados del Nivel 4
$topics = @("transacciones-rf1","transacciones-rf2","transacciones-rf3","critical-data")
foreach ($t in $topics) {
    docker exec kafka-broker-1 /opt/kafka/bin/kafka-topics.sh `
        --bootstrap-server localhost:9092 --delete --topic $t 2>$null
    Write-Host "Eliminado: $t"
}

# Paso 4: esperar propagación y verificar
Start-Sleep -Seconds 3
docker exec kafka-broker-1 /opt/kafka/bin/kafka-topics.sh `
    --bootstrap-server localhost:9092 --list
```

```cmd
REM CMD
docker exec kafka-broker-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic transacciones-rf1
docker exec kafka-broker-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic transacciones-rf2
docker exec kafka-broker-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic transacciones-rf3
docker exec kafka-broker-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic critical-data
```

```powershell
# Paso 5: limpiar archivos de resultados
Remove-Item -Path "experimentos\resultados\*.txt" -Force -ErrorAction SilentlyContinue
Remove-Item -Path "experimentos\resultados\*.csv" -Force -ErrorAction SilentlyContinue

# Paso 6: recrear los archivos placeholder vacíos
"" | Out-File "experimentos\resultados\estado-replicas-antes-fallo.txt"
"" | Out-File "experimentos\resultados\estado-replicas-durante-fallo.txt"
"" | Out-File "experimentos\resultados\estado-replicas-despues-recuperacion.txt"
"" | Out-File "experimentos\resultados\comparacion-acks.txt"
```

```powershell
# Paso 7: recrear los topics limpios
.\scripts\powershell\30-crear-topics-replicados.ps1
```

### Modo B — Recuperar un broker que quedó detenido

Si al terminar los experimentos el broker 2 (o cualquier otro) quedó detenido:

```powershell
# Verificar cuál está detenido
docker ps -a --filter "name=kafka-broker" --format "table {{.Names}}\t{{.Status}}"

# Recuperar el broker caído
docker start kafka-broker-2

# Esperar re-sincronización (30-60 segundos)
Start-Sleep -Seconds 30

# Verificar ISR desde el Nivel 4
.\scripts\powershell\34-verificar-isr.ps1
```

### Modo C — Reset completo del clúster (Nivel 3 + Nivel 4)

Ejecuta el **Reset Nivel 3** completo. Al volver a levantar el clúster con `20-iniciar-cluster.ps1`,
ambos niveles tienen el entorno limpio. Luego recrea los topics del Nivel 4:

```powershell
cd C:\Users\aldo_\Documents\SIIE\kafka_laboratorio\kafka-lab-nivel-4
.\scripts\powershell\30-crear-topics-replicados.ps1
```

**El Nivel 4 está listo para empezar desde cero.**

---

## Reset total — Todo el laboratorio

Borra absolutamente todo: contenedores, volúmenes, datos, resultados. El código fuente y los JARs
compilados se conservan a menos que ejecutes la limpieza de Maven.

### Paso 1 — Detener todos los contenedores Kafka

```powershell
# Detener broker único (Nivel 1/2) si está corriendo
cd C:\Users\aldo_\Documents\SIIE\kafka_laboratorio\kafka-lab-nivel-1\docker
docker-compose down 2>$null
cd ..\..

# Detener clúster multi-broker (Nivel 3/4) si está corriendo
cd C:\Users\aldo_\Documents\SIIE\kafka_laboratorio\kafka-lab-nivel-3\docker
docker-compose -f docker-compose-cluster.yml down 2>$null
cd ..\..
```

### Paso 2 — Eliminar TODOS los volúmenes de datos Kafka

```powershell
# Ver qué volúmenes existen
docker volume ls --filter "name=kafka"

# Eliminar todos los volúmenes Kafka de una vez
docker volume ls --filter "name=kafka" -q | ForEach-Object { docker volume rm $_ }
```

```cmd
REM CMD — eliminar uno por uno si el pipe no funciona
docker volume rm kafka-lab-nivel-1_kafka_data
docker volume rm kafka-data-1
docker volume rm kafka-data-2
docker volume rm kafka-data-3
```

### Paso 3 — Verificar que no quedan contenedores ni volúmenes

```powershell
docker ps -a --filter "name=kafka"
docker volume ls --filter "name=kafka"
# Ambos deben estar vacíos
```

### Paso 4 — Limpiar todos los archivos de resultados

```powershell
# PowerShell — limpia todos los resultados de los 4 niveles
$niveles = @("kafka-lab-nivel-1","kafka-lab-nivel-2","kafka-lab-nivel-3","kafka-lab-nivel-4")
$base = "C:\Users\aldo_\Documents\SIIE\kafka_laboratorio"

foreach ($nivel in $niveles) {
    $ruta = "$base\$nivel\experimentos\resultados"
    if (Test-Path $ruta) {
        Remove-Item -Path "$ruta\*" -Force -ErrorAction SilentlyContinue
        Write-Host "Limpiado: $ruta"
    }
}
```

### Paso 5 — Limpiar JARs compilados (opcional)

Solo si quieres forzar una recompilación desde cero:

```powershell
# PowerShell
foreach ($nivel in @(1,2,3,4)) {
    $javaDir = "$base\kafka-lab-nivel-$nivel\java"
    if (Test-Path $javaDir) {
        Push-Location $javaDir
        mvn clean -q 2>$null
        Pop-Location
        Write-Host "Maven clean: Nivel $nivel"
    }
}
```

```cmd
REM CMD — ejecutar en cada directorio java\
cd C:\Users\aldo_\Documents\SIIE\kafka_laboratorio\kafka-lab-nivel-1\java && mvn clean -q
cd C:\Users\aldo_\Documents\SIIE\kafka_laboratorio\kafka-lab-nivel-2\java && mvn clean -q
cd C:\Users\aldo_\Documents\SIIE\kafka_laboratorio\kafka-lab-nivel-3\java && mvn clean -q
cd C:\Users\aldo_\Documents\SIIE\kafka_laboratorio\kafka-lab-nivel-4\java && mvn clean -q
```

### Paso 6 — Verificación final

```powershell
Write-Host "=== Contenedores Kafka ===" -ForegroundColor Cyan
docker ps -a --filter "name=kafka" --format "table {{.Names}}\t{{.Status}}"

Write-Host "=== Volúmenes Kafka ===" -ForegroundColor Cyan
docker volume ls --filter "name=kafka"

Write-Host "=== Imágenes Kafka ===" -ForegroundColor Cyan
docker images --filter "reference=apache/kafka*" --format "table {{.Repository}}\t{{.Tag}}\t{{.Size}}"
```

> La imagen Docker `apache/kafka:4.0.0` **no se elimina** en este proceso.
> Si también quieres eliminarla: `docker rmi apache/kafka:4.0.0`
> (el próximo `docker-compose up` la descargará de nuevo, ~800 MB).

---

## Resumen visual

```
┌─────────────────┬───────────────┬────────────────┬──────────────────────────────┐
│ Nivel           │ Contenedor(s) │ Volumen(s)     │ Topics a eliminar            │
├─────────────────┼───────────────┼────────────────┼──────────────────────────────┤
│ Nivel 1         │ kafka-broker  │ kafka_data     │ primer-topic                 │
│ Nivel 2         │ kafka-broker  │ kafka_data     │ transacciones-1p/4p/8p       │
│ Nivel 3         │ kafka-broker-1│ kafka-data-1   │ transacciones-6p/12p/5p      │
│                 │ kafka-broker-2│ kafka-data-2   │ transacciones-1p-benchmark   │
│                 │ kafka-broker-3│ kafka-data-3   │                              │
│ Nivel 4         │ (mismo que 3) │ (mismo que 3)  │ transacciones-rf1/rf2/rf3    │
│                 │               │                │ critical-data                │
└─────────────────┴───────────────┴────────────────┴──────────────────────────────┘
```

```
┌──────────────────────┬────────────────────┬─────────────────────────────────────┐
│ Qué borrar           │ Comando            │ Efecto                              │
├──────────────────────┼────────────────────┼─────────────────────────────────────┤
│ Solo mensajes del    │ --delete --topic X │ Borra los mensajes de ese topic     │
│ topic                │                    │ El broker sigue corriendo           │
├──────────────────────┼────────────────────┼─────────────────────────────────────┤
│ Todo el broker/      │ docker-compose     │ Borra mensajes, topics, offsets     │
│ clúster              │ down -v            │ El contenedor desaparece            │
├──────────────────────┼────────────────────┼─────────────────────────────────────┤
│ Solo los JARs        │ mvn clean          │ Fuerza recompilación completa       │
│ compilados           │                    │ El código fuente no se toca         │
└──────────────────────┴────────────────────┴─────────────────────────────────────┘
```

---

## Problemas comunes al resetear

### El topic sigue apareciendo después de eliminarlo

```powershell
# Espera unos segundos y vuelve a listar
Start-Sleep -Seconds 5
docker exec kafka-broker-1 /opt/kafka/bin/kafka-topics.sh `
    --bootstrap-server localhost:9092 --list
```
> La eliminación de topics en Kafka es asíncrona. Puede tardar hasta 10 segundos en completarse.

---

### El volumen no se elimina porque un contenedor lo está usando

```powershell
# Primero asegúrate de que el contenedor está detenido Y eliminado
docker stop kafka-broker-1
docker rm kafka-broker-1
# Luego elimina el volumen
docker volume rm kafka-data-1
```

---

### El broker arranca pero no acepta conexiones

```powershell
# Ver si hay errores en los logs al arrancar
docker logs kafka-broker-1 --tail 30

# Si el volumen anterior está corrupto, elimínalo y recrea el contenedor
docker-compose -f docker-compose-cluster.yml down -v
docker-compose -f docker-compose-cluster.yml up -d
```

---

### `docker-compose down -v` no elimina los volúmenes del Nivel 3

Esto puede pasar si los volúmenes fueron creados con un nombre diferente. Verifica y elimina manualmente:

```powershell
# Ver todos los volúmenes con "kafka" en el nombre
docker volume ls | Select-String "kafka"

# Eliminar los que corresponden al Nivel 3
docker volume rm kafka-data-1
docker volume rm kafka-data-2
docker volume rm kafka-data-3
```

---

*Laboratorio Kafka — Niveles 1 al 4 — Kafka 4.0 KRaft — Windows 10 + Docker Desktop*
