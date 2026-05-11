# Instrucciones Detalladas - Laboratorio Kafka Nivel 3

## Introducción

En este nivel pasas de **1 broker a 3 brokers**. Verás cómo Kafka distribuye automáticamente las particiones entre múltiples servidores y cómo esto mejora el throughput y sienta las bases para la alta disponibilidad.

**Lo que aprenderás:**
- Configurar un clúster KRaft de 3 nodos desde cero
- Entender cómo Kafka asigna leaders a los brokers
- Ver la distribución de carga en tiempo real con `docker stats`
- Medir la mejora de throughput con `ThroughputBenchmark`

---

## Sección 1: Preparación del entorno

### 1.1 Detener el cluster del Nivel 1 (si está corriendo)

El broker del Nivel 1 usa el puerto 9092, que también usa el broker-1 del Nivel 3.

```powershell
cd ..\kafka-lab-nivel-1
.\scripts\powershell\03-detener-kafka.ps1
cd ..\kafka-lab-nivel-3
```

### 1.2 Asegurarse de que Docker Desktop tiene suficiente RAM

3 brokers Kafka necesitan al menos **6 GB** asignados a Docker.

1. Abre Docker Desktop → Settings → Resources
2. Memory: ajusta a 6 GB mínimo
3. Aplica y reinicia Docker Desktop

### 1.3 Configurar JAVA_HOME

```powershell
# Verifica si ya está configurado
echo $env:JAVA_HOME

# Si no está o está vacío, configúralo (ajusta la ruta a tu instalación):
$env:JAVA_HOME = "C:\Program Files\Java\jdk-17"
```

---

## Sección 2: Inicio del clúster multi-broker

### 2.1 Iniciar los 3 brokers

```powershell
.\scripts\powershell\20-iniciar-cluster.ps1
```

El script:
1. Verifica que los puertos 9092, 9093 y 9094 están disponibles
2. Detiene automáticamente el Nivel 1 si está corriendo
3. Ejecuta `docker-compose -f docker-compose-cluster.yml up -d`
4. Espera activamente hasta que los 3 brokers respondan
5. Verifica el quorum KRaft y el estado del controlador

**Tiempo esperado:** 60-120 segundos para que los 3 brokers completen el leader election.

### 2.2 Verificar el clúster

```powershell
.\scripts\powershell\21-verificar-cluster.ps1
```

Busca en la salida:
- Los 3 brokers con estado `running`
- El quorum KRaft con un `LeaderId` definido
- La línea `ClusterId: TzJkYzFlZDctMzJhNy00NTM`

---

## Sección 3: Compilación del código Java

### 3.1 Compilar las nuevas clases

```powershell
cd java
mvn clean package
cd ..
```

**JAR generado:** `java\target\kafka-lab-nivel-3-1.3.0.jar`

### 3.2 Clases disponibles en el JAR

```
com.nexus.kafka.nivel1.SimpleProducer       (del Nivel 1)
com.nexus.kafka.nivel1.SimpleConsumer       (del Nivel 1)
com.nexus.kafka.nivel2.KeyedProducer        (del Nivel 2)
com.nexus.kafka.nivel2.InstrumentedConsumer (del Nivel 2)
com.nexus.kafka.nivel2.BatchProducer        (del Nivel 2)
com.nexus.kafka.nivel2.PartitionAnalyzer    (del Nivel 2)
com.nexus.kafka.nivel3.ClusterAnalyzer      ← NUEVO
com.nexus.kafka.nivel3.LoadBalancedProducer ← NUEVO
com.nexus.kafka.nivel3.ThroughputBenchmark  ← NUEVO
```

### 3.3 Formato del comando Java

Siempre desde `kafka-lab-nivel-3\java\`:

```powershell
& "$env:JAVA_HOME\bin\java" -cp target\kafka-lab-nivel-3-1.3.0.jar com.nexus.kafka.nivel3.NombreClase [args]
```

---

## Sección 4: Experimento 05 - Distribución de leaders

Sigue `experimentos\exp-05-distribucion-leaders.md` paso a paso.

**Puntos clave:**
- Crea 3 topics: `transacciones-6p` (6 particiones), `transacciones-12p` (12), `transacciones-5p` (5)
- Observa la distribución perfecta con múltiplos de 3
- Observa la distribución imperfecta con 5 particiones

**Comando principal:**
```powershell
& "$env:JAVA_HOME\bin\java" -cp java\target\kafka-lab-nivel-3-1.3.0.jar `
    com.nexus.kafka.nivel3.ClusterAnalyzer transacciones-6p
```

---

## Sección 5: Experimento 06 - Carga balanceada

Sigue `experimentos\exp-06-carga-balanceada.md`.

**Antes de ejecutar el producer**, abre una nueva ventana y ejecuta:
```powershell
docker stats kafka-broker-1 kafka-broker-2 kafka-broker-3
```

Deja esta ventana visible. Verás los 3 brokers activos simultáneamente durante el envío.

---

## Sección 6: Experimento 07 - Benchmark de throughput

Sigue `experimentos\exp-07-throughput-multibroker.md`.

Ejecuta los 3 tests en orden y anota los resultados en la tabla del experimento.

Todos los resultados se guardan automáticamente en:
```
experimentos\resultados\metricas-throughput.txt
```

---

## Sección 7: Exploración adicional

### Ver el controlador KRaft activo

```powershell
docker exec kafka-broker-1 /opt/kafka/bin/kafka-metadata-quorum.sh `
    --bootstrap-server localhost:9092 describe --status
```

### Listar todos los brokers desde Java

```powershell
& "$env:JAVA_HOME\bin\java" -cp java\target\kafka-lab-nivel-3-1.3.0.jar `
    com.nexus.kafka.nivel3.ClusterAnalyzer
```

(Sin argumentos, analiza todos los topics y el cluster completo)

### Ver distribución de todos los topics

```powershell
.\scripts\powershell\22-describir-distribucion.ps1
```

### Seguir logs en tiempo real de un broker específico

```powershell
.\scripts\powershell\24-ver-logs-brokers.ps1 -Broker 2 -Seguir
```

---

## Sección 8: Limpieza

### Detener el clúster (conservar datos)

```powershell
.\scripts\powershell\23-detener-cluster.ps1
```

Los 3 volúmenes (`kafka-data-1`, `kafka-data-2`, `kafka-data-3`) conservan los datos.

### Eliminar todo (incluyendo datos)

```powershell
cd docker
docker-compose -f docker-compose-cluster.yml down -v
cd ..
```

---

## Sección 9: Próximos pasos — Nivel 4

En el **Nivel 4** aprenderás sobre **réplicas y tolerancia a fallos**:

- **Replication factor > 1**: cada partición tendrá múltiples copias en diferentes brokers
- **ISR (In-Sync Replicas)**: qué brokers tienen los datos actualizados
- **Leader failover**: si el broker líder falla, un follower se convierte en líder automáticamente
- **Simulación de fallos**: detendrás uno de los 3 brokers y observarás que el clúster sigue operando
- **min.insync.replicas**: el parámetro que controla el trade-off durabilidad vs disponibilidad
- **acks=all**: cuando usarlo y cuándo preferir acks=1

El clúster de 3 brokers que creaste en este nivel es exactamente la base sobre la que se construye el Nivel 4.
