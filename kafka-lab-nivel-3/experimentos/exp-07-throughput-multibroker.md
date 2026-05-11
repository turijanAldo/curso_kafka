# Experimento 07 - Throughput: Multi-Broker vs Una Partición

## Objetivo

Medir empíricamente la diferencia de throughput entre producir a un topic con 6 particiones (distribuido en 3 brokers) vs un topic con 1 partición (forzado a un solo broker).

## Hipótesis

El topic de 6 particiones permitirá mayor throughput que el de 1 partición porque la carga se distribuye entre 3 brokers que procesan en paralelo.

## Advertencia

Este es un benchmark **sintético en Docker en tu máquina local**. Los números absolutos no son representativos de producción (hardware dedicado + red Gigabit). Lo que sí es observable es la **mejora relativa** entre 1 partición y 6 particiones.

## Pre-requisitos

- [ ] Clúster de 3 brokers corriendo
- [ ] Topic `transacciones-6p` creado
- [ ] Java compilado (`mvn clean package`)
- [ ] `JAVA_HOME` configurado

---

## Procedimiento

### Test A — 6 particiones, 1 thread productor

Desde `kafka-lab-nivel-3\java\`:

```powershell
& "$env:JAVA_HOME\bin\java" -cp target\kafka-lab-nivel-3-1.3.0.jar `
    com.nexus.kafka.nivel3.ThroughputBenchmark `
    --topic transacciones-6p `
    --messages 10000 `
    --message-size 1024 `
    --threads 1 `
    --batch-size 100 `
    --linger-ms 10
```

**CMD:**
```cmd
"%JAVA_HOME%\bin\java" -cp target\kafka-lab-nivel-3-1.3.0.jar com.nexus.kafka.nivel3.ThroughputBenchmark --topic transacciones-6p --messages 10000 --message-size 1024 --threads 1 --batch-size 100 --linger-ms 10
```

Anota el **throughput (mensajes/seg)**.

---

### Test B — 6 particiones, 3 threads productores

```powershell
& "$env:JAVA_HOME\bin\java" -cp target\kafka-lab-nivel-3-1.3.0.jar `
    com.nexus.kafka.nivel3.ThroughputBenchmark `
    --topic transacciones-6p `
    --messages 10000 `
    --message-size 1024 `
    --threads 3 `
    --batch-size 100 `
    --linger-ms 10
```

Anota el throughput. Con 3 threads enviando a 3 brokers simultáneamente, el throughput debería ser mayor.

---

### Test C — Crear topic de 1 partición para comparación

```powershell
docker exec kafka-broker-1 /opt/kafka/bin/kafka-topics.sh `
    --bootstrap-server localhost:9092 `
    --create `
    --topic transacciones-1p-benchmark `
    --partitions 1 `
    --replication-factor 1
```

---

### Test D — 1 partición, 3 threads productores

```powershell
& "$env:JAVA_HOME\bin\java" -cp target\kafka-lab-nivel-3-1.3.0.jar `
    com.nexus.kafka.nivel3.ThroughputBenchmark `
    --topic transacciones-1p-benchmark `
    --messages 10000 `
    --message-size 1024 `
    --threads 3 `
    --batch-size 100 `
    --linger-ms 10
```

Con 1 sola partición, **todos los writes van al mismo broker** independientemente del número de threads. El throughput debería ser menor que el Test B.

---

## Resultados esperados

| Test | Topic | Particiones | Threads | Throughput esperado |
|---|---|---|---|---|
| A | transacciones-6p | 6 | 1 | Base (~500-2000 msg/s) |
| B | transacciones-6p | 6 | 3 | ~2-3x Test A |
| D | transacciones-1p-benchmark | 1 | 3 | < Test B |

### Output esperado del ThroughputBenchmark (Test B):

```
╔═══════════════════════════════════════════════════════╗
║         BENCHMARK DE THROUGHPUT - KAFKA CLUSTER       ║
╚═══════════════════════════════════════════════════════╝
Configuracion del test:
  • Topic          : transacciones-6p (3 brokers)
  • Mensajes totales: 10,000
  • Tamano mensaje  : 1024 bytes
  • Threads         : 3
  • Batch size      : 100
  • Linger ms       : 10 ms
─────────────────────────────────────────────────────────

✅ Benchmark completado

Resultados:
  Throughput:
    • Mensajes/segundo: 3,125
    • MB/segundo      : 3.05
    • Enviados OK      : 10,000
    • Errores          : 0

  Latencia (ms):
    • Promedio   : 24.5
    • Percentil 50: 22
    • Percentil 95: 45
    • Percentil 99: 67
    • Maxima       : 156

  Distribucion por particion:
    • Partition 0: 1,667 mensajes (16.67%)
    • Partition 1: 1,667 mensajes (16.67%)
    ...
```

---

## Análisis

### ¿Por qué 1 partición limita el throughput?

Con 1 partición, **todas las escrituras deben ir al mismo broker**. Aunque tengas 3 threads producers, todos compiten por enviar al mismo destino. El broker single-partition convierte el trabajo en secuencial en el nivel del log.

Con 6 particiones distribuidas en 3 brokers:
- Thread 1 envía a particiones 0,3 → Broker 1
- Thread 2 envía a particiones 1,4 → Broker 2
- Thread 3 envía a particiones 2,5 → Broker 3
- Los 3 brokers escriben a sus discos **en paralelo real**

### Factores que limitan la mejora en Docker local

- Los 3 brokers comparten el mismo CPU y disco de tu máquina
- La red entre contenedores es virtual (no Gigabit dedicado)
- Docker overhead afecta a los 3 brokers por igual

En producción con hardware dedicado por broker, la mejora sería mucho más pronunciada y consistente.

### Los percentiles de latencia

- **P50 (mediana):** El mensaje típico tarda X ms
- **P95:** El 95% de los mensajes llegan en menos de Y ms. Útil para SLAs
- **P99:** El 99% llegan en menos de Z ms. Los "outliers" que afectan experiencia de usuario
- **P99 >> P50:** Indica colas intermitentes o GC pauses en el broker

---

## Conclusiones

- [ ] Medí el throughput con 6 particiones y 3 threads
- [ ] Medí el throughput con 1 partición y 3 threads
- [ ] El topic de 6 particiones mostró mayor throughput
- [ ] Entiendo por qué 1 partición es un cuello de botella incluso con múltiples threads
- [ ] Entiendo que en producción la mejora sería más pronunciada

## Mis resultados reales

| Test | Throughput (msg/s) | P50 (ms) | P95 (ms) | P99 (ms) |
|---|---|---|---|---|
| A: 6p, 1 thread | | | | |
| B: 6p, 3 threads | | | | |
| D: 1p, 3 threads | | | | |

### Mejora observada (Test B / Test D): ___x
