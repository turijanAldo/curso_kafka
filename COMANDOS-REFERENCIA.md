# Referencia de Comandos - Laboratorio Kafka

Guía rápida de todos los comandos utilizados en los 4 niveles del laboratorio.
Cada comando incluye su explicación, y cómo ejecutarlo **desde Windows** (fuera del contenedor) y **desde dentro del contenedor**.

---

## Índice

1. [Docker — Gestión del clúster](#1-docker--gestión-del-clúster)
2. [Topics — Crear, listar, describir, eliminar](#2-topics--crear-listar-describir-eliminar)
3. [Producers — Enviar mensajes](#3-producers--enviar-mensajes)
4. [Consumers — Leer mensajes](#4-consumers--leer-mensajes)
5. [Consumer Groups — Gestión de grupos](#5-consumer-groups--gestión-de-grupos)
6. [Réplicas e ISR — Nivel 4](#6-réplicas-e-isr--nivel-4)
7. [KRaft — Metadatos del clúster](#7-kraft--metadatos-del-clúster)
8. [Offsets — Posiciones en el log](#8-offsets--posiciones-en-el-log)
9. [Configuración de topics](#9-configuración-de-topics)
10. [Java — Ejecutar clases del laboratorio](#10-java--ejecutar-clases-del-laboratorio)

---

## Convenciones

```
[FUERA]  = comando ejecutado en PowerShell/CMD de Windows
[DENTRO] = comando ejecutado dentro del contenedor (docker exec)

BROKER_SINGLE  = kafka-broker        (Nivel 1, puerto 9092)
BROKER_CLUSTER = kafka-broker-1/2/3  (Niveles 3 y 4, puertos 9092/9093/9094)
```

> **Atajo**: todos los comandos `[FUERA]` usan `docker exec <contenedor> /opt/kafka/bin/<comando>.sh`.
> Los comandos `[DENTRO]` se ejecutan después de entrar con `docker exec -it <contenedor> bash`.

---

## 1. Docker — Gestión del clúster

### Levantar el broker único (Nivel 1 y 2)

```powershell
# [FUERA] PowerShell
docker-compose -f docker/docker-compose.yml up -d

# [FUERA] CMD
docker-compose -f docker\docker-compose.yml up -d
```
> Levanta el broker en segundo plano. `-d` = detached (sin bloquear la terminal).

---

### Levantar el clúster de 3 brokers (Niveles 3 y 4)

```powershell
# [FUERA] PowerShell (desde kafka-lab-nivel-3\docker)
docker-compose -f docker-compose-cluster.yml up -d
```

---

### Ver contenedores corriendo

```powershell
# [FUERA]
docker ps

# Ver solo contenedores Kafka
docker ps --filter "name=kafka" --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"
```
> Muestra qué contenedores están activos, su estado y puertos expuestos.

---

### Ver logs de un broker

```powershell
# [FUERA] últimas 50 líneas
docker logs kafka-broker-1 --tail 50

# [FUERA] seguir logs en tiempo real
docker logs kafka-broker-1 -f

# [FUERA] filtrar líneas que contienen "ERROR"
docker logs kafka-broker-1 --tail 200 2>&1 | Select-String "ERROR"
# CMD:
docker logs kafka-broker-1 --tail 200 2>&1 | findstr "ERROR"
```

---

### Detener un broker (shutdown graceful)

```powershell
# [FUERA]
docker stop kafka-broker-1          # graceful (~10 seg timeout)
docker stop -t 0 kafka-broker-1     # forzado inmediato (simula fallo)
```
> `-t 0` envía SIGKILL inmediato, simulando un fallo catastrófico real.

---

### Iniciar un broker detenido

```powershell
# [FUERA]
docker start kafka-broker-2
```

---

### Detener y eliminar contenedores (conservando datos)

```powershell
# [FUERA]
docker-compose -f docker-compose-cluster.yml down
```

### Detener, eliminar contenedores Y volúmenes (reseteo total)

```powershell
# [FUERA] ⚠ BORRA TODOS LOS DATOS
docker-compose -f docker-compose-cluster.yml down -v
```

---

### Ver uso de recursos en tiempo real

```powershell
# [FUERA] — monitoreo continuo de CPU/RAM/Red
docker stats kafka-broker-1 kafka-broker-2 kafka-broker-3

# Una sola lectura (sin loop continuo)
docker stats --no-stream kafka-broker-1 kafka-broker-2 kafka-broker-3
```

---

### Entrar al shell de un contenedor

```powershell
# [FUERA] — abre bash dentro del contenedor
docker exec -it kafka-broker-1 bash

# Salir del contenedor
exit
```

---

## 2. Topics — Crear, listar, describir, eliminar

### Listar todos los topics

```powershell
# [FUERA]
docker exec kafka-broker-1 /opt/kafka/bin/kafka-topics.sh `
    --bootstrap-server localhost:9092 --list

# [DENTRO] (ya estás dentro del contenedor)
/opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
```

---

### Crear un topic básico

```powershell
# [FUERA] PowerShell — 1 partición, sin replicación
docker exec kafka-broker-1 /opt/kafka/bin/kafka-topics.sh `
    --bootstrap-server localhost:9092 `
    --create `
    --topic mi-topic `
    --partitions 1 `
    --replication-factor 1

# [FUERA] CMD
docker exec kafka-broker-1 /opt/kafka/bin/kafka-topics.sh ^
    --bootstrap-server localhost:9092 ^
    --create ^
    --topic mi-topic ^
    --partitions 1 ^
    --replication-factor 1
```

---

### Crear topic con múltiples particiones (Nivel 2+)

```powershell
# [FUERA] PowerShell — 4 particiones, sin replicación
docker exec kafka-broker-1 /opt/kafka/bin/kafka-topics.sh `
    --bootstrap-server localhost:9092 `
    --create `
    --topic transacciones-4p `
    --partitions 4 `
    --replication-factor 1
```

---

### Crear topic replicado (Nivel 4)

```powershell
# [FUERA] PowerShell — RF=3, con min.insync.replicas
docker exec kafka-broker-1 /opt/kafka/bin/kafka-topics.sh `
    --bootstrap-server localhost:9092 `
    --create `
    --topic critical-data `
    --partitions 2 `
    --replication-factor 3 `
    --config min.insync.replicas=2
```
> `replication-factor` no puede superar el número de brokers disponibles.

---

### Describir un topic (ver particiones, leaders, réplicas, ISR)

```powershell
# [FUERA] PowerShell — un topic específico
docker exec kafka-broker-1 /opt/kafka/bin/kafka-topics.sh `
    --bootstrap-server localhost:9092 `
    --describe `
    --topic transacciones-rf3

# [FUERA] — todos los topics
docker exec kafka-broker-1 /opt/kafka/bin/kafka-topics.sh `
    --bootstrap-server localhost:9092 `
    --describe
```

**Salida de ejemplo:**
```
Topic: transacciones-rf3  PartitionCount: 4  ReplicationFactor: 3
  Partition: 0  Leader: 1  Replicas: 1,2,3  Isr: 1,2,3
  Partition: 1  Leader: 2  Replicas: 2,3,1  Isr: 2,3,1
  Partition: 2  Leader: 3  Replicas: 3,1,2  Isr: 3,1,2
  Partition: 3  Leader: 1  Replicas: 1,3,2  Isr: 1,3,2
```
> **Leader**: broker que recibe escrituras. **Replicas**: todos los que deben tener copia. **Isr**: los que están sincronizados.

---

### Eliminar un topic

```powershell
# [FUERA] PowerShell
docker exec kafka-broker-1 /opt/kafka/bin/kafka-topics.sh `
    --bootstrap-server localhost:9092 `
    --delete `
    --topic mi-topic
```
> La eliminación es asíncrona; el topic puede tardar unos segundos en desaparecer del listado.

---

## 3. Producers — Enviar mensajes

### Producer de consola básico (Nivel 1)

```powershell
# [FUERA] PowerShell — escribe un mensaje por línea, Enter para enviar, Ctrl+C para salir
docker exec -it kafka-broker-1 /opt/kafka/bin/kafka-console-producer.sh `
    --bootstrap-server localhost:9092 `
    --topic primer-topic

# [DENTRO]
/opt/kafka/bin/kafka-console-producer.sh \
    --bootstrap-server localhost:9092 \
    --topic primer-topic
```

---

### Producer con clave (Nivel 2)

```powershell
# [FUERA] PowerShell — formato: clave:valor separados por ':'
docker exec -it kafka-broker-1 /opt/kafka/bin/kafka-console-producer.sh `
    --bootstrap-server localhost:9092 `
    --topic transacciones-4p `
    --property parse.key=true `
    --property key.separator=":"

# [DENTRO]
/opt/kafka/bin/kafka-console-producer.sh \
    --bootstrap-server localhost:9092 \
    --topic transacciones-4p \
    --property parse.key=true \
    --property key.separator=":"
```
> Escribe mensajes como: `user-001:{"monto":100}` y presiona Enter.

---

### Producer a clúster multi-broker (Niveles 3 y 4)

```powershell
# [FUERA] — especifica múltiples brokers (bootstrap servers)
docker exec -it kafka-broker-1 /opt/kafka/bin/kafka-console-producer.sh `
    --bootstrap-server localhost:9092,localhost:9093,localhost:9094 `
    --topic transacciones-6p
```
> Solo necesitas uno para arrancar; Kafka descubre el resto automáticamente.

---

## 4. Consumers — Leer mensajes

### Consumer de consola básico (Nivel 1)

```powershell
# [FUERA] PowerShell — lee desde el principio del topic
docker exec -it kafka-broker-1 /opt/kafka/bin/kafka-console-consumer.sh `
    --bootstrap-server localhost:9092 `
    --topic primer-topic `
    --from-beginning

# [DENTRO]
/opt/kafka/bin/kafka-console-consumer.sh \
    --bootstrap-server localhost:9092 \
    --topic primer-topic \
    --from-beginning
```
> `--from-beginning`: lee todos los mensajes desde el offset 0. Sin este flag, espera mensajes nuevos.

---

### Consumer mostrando clave, partición y offset (Nivel 2)

```powershell
# [FUERA] PowerShell
docker exec -it kafka-broker-1 /opt/kafka/bin/kafka-console-consumer.sh `
    --bootstrap-server localhost:9092 `
    --topic transacciones-4p `
    --from-beginning `
    --property print.key=true `
    --property print.partition=true `
    --property print.offset=true `
    --property key.separator=" | "
```

**Salida de ejemplo:**
```
Partition:0 | Offset:0 | user-001 | {"monto":100}
Partition:2 | Offset:0 | user-003 | {"monto":250}
```

---

### Consumer con grupo (Nivel 2)

```powershell
# [FUERA] PowerShell
docker exec -it kafka-broker-1 /opt/kafka/bin/kafka-console-consumer.sh `
    --bootstrap-server localhost:9092 `
    --topic transacciones-4p `
    --group mi-grupo-1 `
    --from-beginning
```
> Con `--group` el consumer registra su posición (offset). Si lo reinicias sin `--from-beginning`, continúa desde donde se quedó.

---

## 5. Consumer Groups — Gestión de grupos

### Listar todos los grupos

```powershell
# [FUERA] PowerShell
docker exec kafka-broker-1 /opt/kafka/bin/kafka-consumer-groups.sh `
    --bootstrap-server localhost:9092 `
    --list
```

---

### Describir un grupo (ver lag y asignación de particiones)

```powershell
# [FUERA] PowerShell
docker exec kafka-broker-1 /opt/kafka/bin/kafka-consumer-groups.sh `
    --bootstrap-server localhost:9092 `
    --describe `
    --group mi-grupo-1
```

**Salida de ejemplo:**
```
GROUP        TOPIC            PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG
mi-grupo-1   transacciones-4p     0          50              50           0
mi-grupo-1   transacciones-4p     1          48              48           0
```
> **LAG = 0**: el consumer está al día. **LAG > 0**: hay mensajes sin procesar.

---

### Resetear offsets de un grupo

```powershell
# [FUERA] PowerShell — volver al inicio
docker exec kafka-broker-1 /opt/kafka/bin/kafka-consumer-groups.sh `
    --bootstrap-server localhost:9092 `
    --group mi-grupo-1 `
    --topic transacciones-4p `
    --reset-offsets `
    --to-earliest `
    --execute

# [FUERA] — ir al final (ignorar mensajes existentes)
docker exec kafka-broker-1 /opt/kafka/bin/kafka-consumer-groups.sh `
    --bootstrap-server localhost:9092 `
    --group mi-grupo-1 `
    --topic transacciones-4p `
    --reset-offsets `
    --to-latest `
    --execute
```
> El consumer debe estar **detenido** para poder resetear sus offsets.

---

## 6. Réplicas e ISR — Nivel 4

### Ver estado completo de réplicas e ISR

```powershell
# [FUERA] PowerShell — todos los topics
docker exec kafka-broker-1 /opt/kafka/bin/kafka-topics.sh `
    --bootstrap-server localhost:9092 `
    --describe

# [FUERA] — un topic específico
docker exec kafka-broker-1 /opt/kafka/bin/kafka-topics.sh `
    --bootstrap-server localhost:9092 `
    --describe `
    --topic transacciones-rf3
```

---

### Ver particiones under-replicated (ISR incompleto)

```powershell
# [FUERA] PowerShell — filtra solo las líneas con Isr menor que Replicas
docker exec kafka-broker-1 /opt/kafka/bin/kafka-topics.sh `
    --bootstrap-server localhost:9092 `
    --describe `
    --under-replicated-partitions
```
> Sin output = clúster saludable. Con output = hay réplicas fuera de sync.

---

### Forzar re-elección de leaders preferidos

```powershell
# [FUERA] PowerShell — después de recuperar un broker caído
docker exec kafka-broker-1 /opt/kafka/bin/kafka-leader-election.sh `
    --bootstrap-server localhost:9092 `
    --election-type preferred `
    --all-topic-partitions
```
> Devuelve los leaders a sus "preferred leaders" (los que Kafka asignó originalmente). Útil tras recuperar un broker.

---

## 7. KRaft — Metadatos del clúster

### Ver estado del quorum KRaft

```powershell
# [FUERA] PowerShell
docker exec kafka-broker-1 /opt/kafka/bin/kafka-metadata-quorum.sh `
    --bootstrap-server localhost:9092 `
    describe --status

# [DENTRO]
/opt/kafka/bin/kafka-metadata-quorum.sh \
    --bootstrap-server localhost:9092 \
    describe --status
```

**Salida de ejemplo:**
```
ClusterId:              TzJkYzFlZDctMzJhNy00NTM
LeaderId:               1
LeaderEpoch:            5
HighWatermark:          1203
MaxFollowerLag:         0
MaxFollowerLagTimeMs:   0
CurrentVoters:          [1,2,3]
CurrentObservers:       []
```
> `LeaderId`: broker que actúa como controlador KRaft (gestiona metadatos). `MaxFollowerLag=0`: todos los brokers están al día.

---

### Ver brokers registrados en el clúster

```powershell
# [FUERA] PowerShell
docker exec kafka-broker-1 /opt/kafka/bin/kafka-broker-api-versions.sh `
    --bootstrap-server localhost:9092
```
> Lista los brokers accesibles y las versiones de API que soporta cada uno.

---

### Ver todos los metadatos del clúster (votantes, snapshots)

```powershell
# [FUERA] PowerShell — lista los votantes KRaft
docker exec kafka-broker-1 /opt/kafka/bin/kafka-metadata-quorum.sh `
    --bootstrap-server localhost:9092 `
    describe --replication

# [DENTRO]
/opt/kafka/bin/kafka-metadata-quorum.sh \
    --bootstrap-server localhost:9092 \
    describe --replication
```

---

## 8. Offsets — Posiciones en el log

### Ver el offset más alto (cantidad de mensajes) por partición

```powershell
# [FUERA] PowerShell
docker exec kafka-broker-1 /opt/kafka/bin/kafka-run-class.sh `
    kafka.tools.GetOffsetShell `
    --bootstrap-server localhost:9092 `
    --topic transacciones-4p `
    --time -1

# [FUERA] CMD
docker exec kafka-broker-1 /opt/kafka/bin/kafka-run-class.sh ^
    kafka.tools.GetOffsetShell ^
    --bootstrap-server localhost:9092 ^
    --topic transacciones-4p ^
    --time -1
```

**Salida de ejemplo:**
```
transacciones-4p:0:250
transacciones-4p:1:248
transacciones-4p:2:251
transacciones-4p:3:249
```
> `--time -1` = offset del final (mensaje más nuevo). `--time -2` = offset del inicio.

---

### Ver offsets del inicio (primer mensaje disponible)

```powershell
# [FUERA] PowerShell
docker exec kafka-broker-1 /opt/kafka/bin/kafka-run-class.sh `
    kafka.tools.GetOffsetShell `
    --bootstrap-server localhost:9092 `
    --topic transacciones-4p `
    --time -2
```
> El total de mensajes en el topic ≈ suma de (offset_final - offset_inicio) por partición.

---

## 9. Configuración de topics

### Ver configuración actual de un topic

```powershell
# [FUERA] PowerShell
docker exec kafka-broker-1 /opt/kafka/bin/kafka-configs.sh `
    --bootstrap-server localhost:9092 `
    --describe `
    --entity-type topics `
    --entity-name critical-data
```

---

### Cambiar configuración de un topic en caliente

```powershell
# [FUERA] PowerShell — cambiar retention a 1 hora
docker exec kafka-broker-1 /opt/kafka/bin/kafka-configs.sh `
    --bootstrap-server localhost:9092 `
    --alter `
    --entity-type topics `
    --entity-name mi-topic `
    --add-config retention.ms=3600000

# [FUERA] — cambiar min.insync.replicas
docker exec kafka-broker-1 /opt/kafka/bin/kafka-configs.sh `
    --bootstrap-server localhost:9092 `
    --alter `
    --entity-type topics `
    --entity-name transacciones-rf3 `
    --add-config min.insync.replicas=2
```

---

### Aumentar el número de particiones de un topic

```powershell
# [FUERA] PowerShell — de 4 a 6 particiones (solo se puede aumentar, nunca reducir)
docker exec kafka-broker-1 /opt/kafka/bin/kafka-topics.sh `
    --bootstrap-server localhost:9092 `
    --alter `
    --topic mi-topic `
    --partitions 6
```
> ⚠ Aumentar particiones cambia el hash de las claves existentes. Los mensajes con la misma clave pueden ir a particiones distintas antes y después del cambio.

---

## 10. Java — Ejecutar clases del laboratorio

> Todos los comandos Java se ejecutan desde el directorio `java\` de cada nivel.
> Siempre usa `"$env:JAVA_HOME\bin\java"` (PowerShell) o `"%JAVA_HOME%\bin\java"` (CMD) para garantizar que usas Java 17.

---

### Nivel 1 — Mensajes simples

```powershell
# Producer: envía 10 mensajes al topic
& "$env:JAVA_HOME\bin\java" -cp target\kafka-lab-nivel-1-1.0.0.jar `
    com.nexus.kafka.nivel1.SimpleProducer primer-topic 10

# Consumer: lee todos los mensajes del topic
& "$env:JAVA_HOME\bin\java" -cp target\kafka-lab-nivel-1-1.0.0.jar `
    com.nexus.kafka.nivel1.SimpleConsumer primer-topic
```

---

### Nivel 2 — Particiones y claves

```powershell
# KeyedProducer: envía mensajes con claves para ver distribución por hash
& "$env:JAVA_HOME\bin\java" -cp target\kafka-lab-nivel-2-1.0.0.jar `
    com.nexus.kafka.nivel2.KeyedProducer transacciones-4p user-123

# BatchProducer: envío masivo asíncrono con barra de progreso
& "$env:JAVA_HOME\bin\java" -cp target\kafka-lab-nivel-2-1.0.0.jar `
    com.nexus.kafka.nivel2.BatchProducer transacciones-4p 1000 true

# InstrumentedConsumer: muestra rebalancing y contador por partición
& "$env:JAVA_HOME\bin\java" -cp target\kafka-lab-nivel-2-1.0.0.jar `
    com.nexus.kafka.nivel2.InstrumentedConsumer transacciones-4p grupo-1

# PartitionAnalyzer: análisis de distribución de mensajes por partición
& "$env:JAVA_HOME\bin\java" -cp target\kafka-lab-nivel-2-1.0.0.jar `
    com.nexus.kafka.nivel2.PartitionAnalyzer transacciones-4p
```

---

### Nivel 3 — Clúster multi-broker

```powershell
# ClusterAnalyzer: muestra brokers, controlador y distribución de leaders
& "$env:JAVA_HOME\bin\java" -cp target\kafka-lab-nivel-3-1.3.0.jar `
    com.nexus.kafka.nivel3.ClusterAnalyzer

# ClusterAnalyzer con topic específico
& "$env:JAVA_HOME\bin\java" -cp target\kafka-lab-nivel-3-1.3.0.jar `
    com.nexus.kafka.nivel3.ClusterAnalyzer transacciones-6p

# LoadBalancedProducer: envío mostrando distribución por broker
# Estrategias: hash | secuencial | random
& "$env:JAVA_HOME\bin\java" -cp target\kafka-lab-nivel-3-1.3.0.jar `
    com.nexus.kafka.nivel3.LoadBalancedProducer transacciones-6p 600 hash

# ThroughputBenchmark: benchmark de rendimiento con múltiples threads
& "$env:JAVA_HOME\bin\java" -cp target\kafka-lab-nivel-3-1.3.0.jar `
    com.nexus.kafka.nivel3.ThroughputBenchmark `
    --topic transacciones-6p `
    --messages 10000 `
    --message-size 1024 `
    --threads 3 `
    --batch-size 100 `
    --linger-ms 10
```

---

### Nivel 4 — Réplicas y failover

```powershell
# ReplicaAnalyzer: análisis del estado de réplicas e ISR
& "$env:JAVA_HOME\bin\java" -cp target\kafka-lab-nivel-4-1.4.0.jar `
    com.nexus.kafka.nivel4.ReplicaAnalyzer

# ReplicaAnalyzer para un topic específico
& "$env:JAVA_HOME\bin\java" -cp target\kafka-lab-nivel-4-1.4.0.jar `
    com.nexus.kafka.nivel4.ReplicaAnalyzer transacciones-rf3

# DurableProducer: comparar acks=0, acks=1 y acks=all
# <topic> <cantidad|continuous> <acks>
& "$env:JAVA_HOME\bin\java" -cp target\kafka-lab-nivel-4-1.4.0.jar `
    com.nexus.kafka.nivel4.DurableProducer critical-data 5000 all

# DurableProducer modo continuo (Ctrl+C para detener)
& "$env:JAVA_HOME\bin\java" -cp target\kafka-lab-nivel-4-1.4.0.jar `
    com.nexus.kafka.nivel4.DurableProducer transacciones-rf3 continuous all

# FailoverMonitor: detecta fallos y failovers en tiempo real
& "$env:JAVA_HOME\bin\java" -cp target\kafka-lab-nivel-4-1.4.0.jar `
    com.nexus.kafka.nivel4.FailoverMonitor

# FailoverMonitor para topics específicos
& "$env:JAVA_HOME\bin\java" -cp target\kafka-lab-nivel-4-1.4.0.jar `
    com.nexus.kafka.nivel4.FailoverMonitor transacciones-rf3 critical-data

# ISRTracker: snapshot único del estado ISR
& "$env:JAVA_HOME\bin\java" -cp target\kafka-lab-nivel-4-1.4.0.jar `
    com.nexus.kafka.nivel4.ISRTracker transacciones-rf3

# ISRTracker: tracking continuo cada 5 segundos
& "$env:JAVA_HOME\bin\java" -cp target\kafka-lab-nivel-4-1.4.0.jar `
    com.nexus.kafka.nivel4.ISRTracker transacciones-rf3 --track --interval 5
```

---

## Referencia rápida — Tabla de comandos

| Acción | Comando (fuera del contenedor) |
|---|---|
| Listar topics | `docker exec kafka-broker-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list` |
| Crear topic | `... --create --topic X --partitions N --replication-factor R` |
| Describir topic | `... --describe --topic X` |
| Eliminar topic | `... --delete --topic X` |
| Under-replicated | `... --describe --under-replicated-partitions` |
| Ver offsets | `docker exec kafka-broker-1 /opt/kafka/bin/kafka-run-class.sh kafka.tools.GetOffsetShell --bootstrap-server localhost:9092 --topic X --time -1` |
| Listar grupos | `docker exec kafka-broker-1 /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --list` |
| Describir grupo | `... --describe --group mi-grupo` |
| Estado KRaft | `docker exec kafka-broker-1 /opt/kafka/bin/kafka-metadata-quorum.sh --bootstrap-server localhost:9092 describe --status` |
| Logs broker | `docker logs kafka-broker-1 --tail 100` |
| Stats recursos | `docker stats kafka-broker-1 kafka-broker-2 kafka-broker-3` |
| Detener broker | `docker stop kafka-broker-2` |
| Recuperar broker | `docker start kafka-broker-2` |

---

## Parámetros más usados

| Parámetro | Descripción | Ejemplo |
|---|---|---|
| `--bootstrap-server` | Dirección de uno o más brokers para conectarse | `localhost:9092` o `localhost:9092,localhost:9093,localhost:9094` |
| `--topic` | Nombre del topic | `--topic transacciones-4p` |
| `--partitions` | Número de particiones al crear | `--partitions 6` |
| `--replication-factor` | Copias de cada partición | `--replication-factor 3` |
| `--from-beginning` | Leer desde el offset 0 | (flag, sin valor) |
| `--group` | Nombre del consumer group | `--group mi-grupo` |
| `--time -1` | Offset final de cada partición | Solo para GetOffsetShell |
| `--time -2` | Offset inicial de cada partición | Solo para GetOffsetShell |
| `acks=0` | Producer no espera confirmación | Máximo throughput |
| `acks=1` | Solo el leader confirma | Balance |
| `acks=all` | Leader + ISR confirman | Máxima durabilidad |
| `min.insync.replicas` | Mínimo de ISR para acks=all | `--config min.insync.replicas=2` |

---

*Laboratorio Kafka — Niveles 1 al 4 — Kafka 4.0 KRaft — Windows 10 + Docker Desktop*
