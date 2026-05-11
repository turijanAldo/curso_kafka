# Experimento 05 - Distribución de Leaders entre Brokers

## Objetivo

Demostrar que Kafka distribuye automáticamente los leaders de particiones de forma balanceada entre todos los brokers disponibles en el clúster, maximizando el uso de recursos y evitando cuellos de botella.

## Hipótesis

Si creamos un topic con **6 particiones** en un clúster de **3 brokers**, Kafka asignará exactamente **2 particiones como leader** a cada broker (1/3 del total). Cuando el número de particiones no es múltiplo de 3, Kafka minimizará la diferencia entre el broker más cargado y el menos cargado.

## Concepto clave: ¿Qué es el leader de una partición?

El **leader** es el único broker responsable de todas las lecturas y escrituras de una partición en un momento dado. Cuando produces un mensaje, va al broker que es leader de la partición destino. Cuando consumes, también lees del leader.

Los **followers** (réplicas no líderes) solo existen en clústeres con `replication-factor > 1`. En este nivel todavía usamos `replication-factor=1`, así que cada partición tiene solo 1 réplica (el leader mismo).

## Pre-requisitos

- [ ] Clúster de 3 brokers iniciado (`20-iniciar-cluster.ps1`)
- [ ] Clúster verificado (`21-verificar-cluster.ps1`)
- [ ] Java compilado: `cd java && mvn clean package`
- [ ] `JAVA_HOME` configurado

---

## Procedimiento

### Paso 1 — Iniciar y verificar el clúster

```powershell
.\scripts\powershell\20-iniciar-cluster.ps1
.\scripts\powershell\21-verificar-cluster.ps1
```

Confirma que los 3 brokers aparecen en el quorum KRaft.

---

### Paso 2 — Crear topic con 6 particiones (múltiplo de 3)

```powershell
docker exec kafka-broker-1 /opt/kafka/bin/kafka-topics.sh `
    --bootstrap-server localhost:9092 `
    --create `
    --topic transacciones-6p `
    --partitions 6 `
    --replication-factor 1
```

**¿Por qué 6 particiones?** Es múltiplo de 3 (número de brokers), lo que permite una distribución perfecta de 2 particiones por broker.

**CMD:**
```cmd
docker exec kafka-broker-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic transacciones-6p --partitions 6 --replication-factor 1
```

---

### Paso 3 — Ver distribución con el script PS

```powershell
.\scripts\powershell\22-describir-distribucion.ps1 transacciones-6p
```

El script mostrará qué partición está en qué broker y una barra visual de balance.

---

### Paso 4 — Ver distribución con ClusterAnalyzer (Java)

Desde `kafka-lab-nivel-3\java\`:

```powershell
& "$env:JAVA_HOME\bin\java" -cp target\kafka-lab-nivel-3-1.3.0.jar `
    com.nexus.kafka.nivel3.ClusterAnalyzer transacciones-6p
```

**CMD:**
```cmd
"%JAVA_HOME%\bin\java" -cp target\kafka-lab-nivel-3-1.3.0.jar com.nexus.kafka.nivel3.ClusterAnalyzer transacciones-6p

#
& "$env:JAVA_HOME\bin\java" -cp "target\kafka-lab-nivel-3-1.3.0.jar" com.nexus.kafka.nivel3.ClusterAnalyzer transacciones-6p
```

---

### Paso 5 — Crear topic con 12 particiones

```powershell
docker exec kafka-broker-1 /opt/kafka/bin/kafka-topics.sh `
    --bootstrap-server localhost:9092 `
    --create `
    --topic transacciones-12p `
    --partitions 12 `
    --replication-factor 1
```

Analiza la distribución: cada broker debería tener exactamente **4 particiones** (12/3).

```powershell
& "$env:JAVA_HOME\bin\java" -cp target\kafka-lab-nivel-3-1.3.0.jar `
    com.nexus.kafka.nivel3.ClusterAnalyzer transacciones-12p

#
& "$env:JAVA_HOME\bin\java" -cp "target\kafka-lab-nivel-3-1.3.0.jar" com.nexus.kafka.nivel3.ClusterAnalyzer transacciones-12p
```

---

### Paso 6 — Crear topic con 5 particiones (NO múltiplo de 3)

```powershell
docker exec kafka-broker-1 /opt/kafka/bin/kafka-topics.sh `
    --bootstrap-server localhost:9092 `
    --create `
    --topic transacciones-5p `
    --partitions 5 `
    --replication-factor 1
```

**Observa:** La distribución no puede ser perfecta. Kafka hará su mejor esfuerzo: probablemente 2 brokers con 2 particiones y 1 broker con 1 partición.

```powershell
& "$env:JAVA_HOME\bin\java" -cp target\kafka-lab-nivel-3-1.3.0.jar `
    com.nexus.kafka.nivel3.ClusterAnalyzer transacciones-5p
```

---

## Resultados esperados

### Topic `transacciones-6p` (6 particiones, 3 brokers):

```
╔════════════════════════════════════════════════════════╗
║          ANÁLISIS DEL CLÚSTER KAFKA - NIVEL 3         ║
╚════════════════════════════════════════════════════════╝
  Cluster ID       : TzJkYzFlZDctMzJhNy00NTM
  Controlador KRaft: Broker X (localhost:909X)

  Brokers en el cluster:
    • Broker 1: localhost:9092
    • Broker 2: localhost:9093 ⭐ (Controlador activo)    <- puede variar
    • Broker 3: localhost:9094

  Topic: transacciones-6p
  Particiones: 6

  Distribucion de particiones:
    Partition  0 -> Leader: Broker 1  | Replicas: [1] | ISR: [1]
    Partition  1 -> Leader: Broker 2  | Replicas: [2] | ISR: [2]
    Partition  2 -> Leader: Broker 3  | Replicas: [3] | ISR: [3]
    Partition  3 -> Leader: Broker 1  | Replicas: [1] | ISR: [1]
    Partition  4 -> Leader: Broker 2  | Replicas: [2] | ISR: [2]
    Partition  5 -> Leader: Broker 3  | Replicas: [3] | ISR: [3]

  Estadisticas de distribucion de leaders:
    Broker 1: 2 particiones (33.33%)
    Broker 2: 2 particiones (33.33%)
    Broker 3: 2 particiones (33.33%)

  ✅ Distribucion balanceada correctamente
```

### Topic `transacciones-5p` (distribución imperfecta):

```
    Broker 1: 2 particiones (40.00%)
    Broker 2: 2 particiones (40.00%)
    Broker 3: 1 particiones (20.00%)
  ⚠️  Distribucion desbalanceada
```

---

## Análisis

Kafka usa un algoritmo de asignación **round-robin** para distribuir leaders: la partición 0 va al broker 0 (mod num_brokers), la partición 1 al broker 1, etc. Esto garantiza que con N particiones y B brokers, ningún broker tenga más de `ceil(N/B)` particiones.

Esta distribución es **crítica para el rendimiento** porque:
- Cada write va al broker leader de la partición destino
- Si todos los leaders estuvieran en 1 broker, ese sería el cuello de botella
- Con distribución balanceada, los 3 brokers procesan 1/3 de las escrituras cada uno

**Regla práctica:** Usa números de particiones que sean múltiplos del número de brokers para garantizar distribución perfecta.

---

## Conclusiones

- [ ] Confirmé que con 6 particiones/3 brokers la distribución es perfecta (2 cada uno)
- [ ] Confirmé que con 5 particiones la distribución es imperfecta pero minimizada
- [ ] Entiendo que la distribución de leaders determina dónde va cada write
- [ ] Entiendo por qué usar múltiplos del número de brokers es buena práctica

## Resultados reales

### `transacciones-6p`:
| Broker | Particiones asignadas | Cantidad |
|---|---|---|
| Broker 1 | | |
| Broker 2 | | |
| Broker 3 | | |

### `transacciones-5p`:
| Broker | Particiones asignadas | Cantidad |
|---|---|---|
| Broker 1 | | |
| Broker 2 | | |
| Broker 3 | | |
