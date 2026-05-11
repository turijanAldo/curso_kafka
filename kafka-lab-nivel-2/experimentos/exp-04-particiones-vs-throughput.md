# Experimento 04 - Particiones vs Throughput

## Objetivo

Medir cómo el número de particiones, combinado con el número de consumers en paralelo, afecta la velocidad total de procesamiento.

## Hipótesis

- `transacciones-1p` + 1 consumer → velocidad base
- `transacciones-4p` + 4 consumers → ~4x velocidad base
- `transacciones-8p` + 8 consumers → ~8x velocidad base (o hasta el límite del hardware)

## Pre-requisitos

- [ ] Clúster Kafka iniciado
- [ ] Los tres topics creados: `transacciones-1p`, `transacciones-4p`, `transacciones-8p`
- [ ] Java compilado (`mvn clean package`)
- [ ] `JAVA_HOME` configurado

---

## Procedimiento

### Fase 1 — Benchmark con 1 partición, 1 consumer

**Paso 1.1 — Llenar `transacciones-1p` con 1000 mensajes:**

Desde `kafka-lab-nivel-2\java\`:

```powershell
& "$env:JAVA_HOME\bin\java" -cp target\kafka-lab-nivel-2-1.0.0.jar `
    com.nexus.kafka.nivel2.BatchProducer transacciones-1p 1000 false
```

**CMD:**
```cmd
"%JAVA_HOME%\bin\java" -cp target\kafka-lab-nivel-2-1.0.0.jar com.nexus.kafka.nivel2.BatchProducer transacciones-1p 1000 false
```

Anota el throughput del producer.

**Paso 1.2 — Consumir con 1 consumer y medir tiempo:**

```powershell
$inicio = Get-Date
& "$env:JAVA_HOME\bin\java" -cp target\kafka-lab-nivel-2-1.0.0.jar `
    com.nexus.kafka.nivel2.InstrumentedConsumer transacciones-1p grupo-bench-1p consumer-bench
# Presiona Ctrl+C cuando veas que dejo de llegar mensajes nuevos
$fin = Get-Date
Write-Host "Tiempo: $(($fin - $inicio).TotalSeconds) segundos"
```

**CMD:**
```cmd
"%JAVA_HOME%\bin\java" -cp target\kafka-lab-nivel-2-1.0.0.jar com.nexus.kafka.nivel2.InstrumentedConsumer transacciones-1p grupo-bench-1p consumer-bench
```

**Paso 1.3 — Verificar con PartitionAnalyzer:**
```powershell
& "$env:JAVA_HOME\bin\java" -cp target\kafka-lab-nivel-2-1.0.0.jar `
    com.nexus.kafka.nivel2.PartitionAnalyzer transacciones-1p
```

---

### Fase 2 — Benchmark con 4 particiones, 4 consumers

**Paso 2.1 — Llenar `transacciones-4p` con 1000 mensajes:**

```powershell
& "$env:JAVA_HOME\bin\java" -cp target\kafka-lab-nivel-2-1.0.0.jar `
    com.nexus.kafka.nivel2.BatchProducer transacciones-4p 1000 false
```

**Paso 2.2 — Iniciar 4 consumers en paralelo** (usa el script 13):

```powershell
.\scripts\powershell\13-consumer-group-multiple.ps1
```

O manualmente en 4 terminales (group-id diferente para no mezclar con el experimento anterior):
```powershell
# Terminal 1
& "$env:JAVA_HOME\bin\java" -cp target\kafka-lab-nivel-2-1.0.0.jar `
    com.nexus.kafka.nivel2.InstrumentedConsumer transacciones-4p grupo-bench-4p consumer-1
# Terminal 2, 3, 4: igual cambiando consumer-2, consumer-3, consumer-4
```

**Paso 2.3 — Verificar distribución:**
```powershell
& "$env:JAVA_HOME\bin\java" -cp target\kafka-lab-nivel-2-1.0.0.jar `
    com.nexus.kafka.nivel2.PartitionAnalyzer transacciones-4p
```

---

### Fase 3 — Benchmark con 8 particiones, 8 consumers

**Paso 3.1 — Llenar `transacciones-8p` con 1000 mensajes:**

```powershell
& "$env:JAVA_HOME\bin\java" -cp target\kafka-lab-nivel-2-1.0.0.jar `
    com.nexus.kafka.nivel2.BatchProducer transacciones-8p 1000 false
```

**Paso 3.2 — Iniciar 8 consumers en paralelo** (abre 8 terminales):

```powershell
# Repetir para consumer-1 hasta consumer-8
& "$env:JAVA_HOME\bin\java" -cp target\kafka-lab-nivel-2-1.0.0.jar `
    com.nexus.kafka.nivel2.InstrumentedConsumer transacciones-8p grupo-bench-8p consumer-1
```

O usar el siguiente comando en PowerShell para abrir las 8 ventanas automaticamente:
```powershell
$jar = "target\kafka-lab-nivel-2-1.0.0.jar"
1..8 | ForEach-Object {
    $id = "consumer-$_"
    Start-Process powershell -ArgumentList "-NoExit", "-Command",
        "& '$env:JAVA_HOME\bin\java' -cp $jar com.nexus.kafka.nivel2.InstrumentedConsumer transacciones-8p grupo-bench-8p $id"
}
```

---

## Resultados esperados

### Throughput del BatchProducer (producción):

| Topic | Mensajes | Tiempo esperado | Throughput esperado |
|---|---|---|---|
| transacciones-1p | 1000 | ~1-2 seg | ~500-1000 msg/s |
| transacciones-4p | 1000 | ~1-2 seg | ~500-1000 msg/s |
| transacciones-8p | 1000 | ~1-2 seg | ~500-1000 msg/s |

*(El producer en este lab es el mismo, las particiones no afectan la velocidad de producción con 1 solo broker)*

### Throughput de consumo (procesamiento):

| Configuración | Mensajes | Consumers | Particiones por consumer | Throughput total estimado |
|---|---|---|---|---|
| 1p + 1c | 1000 | 1 | 1 | Base |
| 4p + 4c | 1000 | 4 | 1 | ~4x Base |
| 8p + 8c | 1000 | 8 | 1 | ~8x Base |

---

## Análisis

### ¿Por qué más particiones = más throughput?

Con **1 partición y 1 consumer**: el consumer lee los 1000 mensajes de forma secuencial. El proceso total tarda T segundos.

Con **4 particiones y 4 consumers**: cada consumer lee 250 mensajes de su partición de forma paralela. El proceso total tarda ~T/4 segundos.

Es como tener 4 cajas registradoras en un supermercado vs 1 sola. Los 1000 clientes se atienden 4 veces más rápido.

### ¿Cuándo más particiones ya no ayudan?

El paralelismo tiene límites:

1. **Límite del hardware**: si el procesador o la red están saturados, agregar particiones no mejora nada
2. **Límite del broker**: con 1 solo broker, todas las particiones compiten por el mismo disco y red
3. **Límite del consumer**: si el processing de cada mensaje es muy rápido (microsegundos), el overhead de Kafka domina
4. **Límite lógico**: más particiones que consumers activos → algunos consumers quedan idle
5. **Costo de operación**: muchas particiones aumentan la complejidad, memoria y latencia de rebalanceo

**Regla práctica en producción**: empieza con `num_particiones = num_consumers_esperados * 2` para tener margen de crecimiento.

---

## Conclusiones

- [ ] Medí el impacto de las particiones en el throughput de consumo
- [ ] Confirmé que más consumers + más particiones = mayor throughput total
- [ ] Entiendo que el producer no se ve afectado por el número de particiones (mismo broker)
- [ ] Entiendo los límites del paralelismo en Kafka

## Mis resultados reales

| Configuración | Tiempo de consumo | Mensajes/seg |
|---|---|---|
| 1p + 1c | | |
| 4p + 4c | | |
| 8p + 8c | | |
