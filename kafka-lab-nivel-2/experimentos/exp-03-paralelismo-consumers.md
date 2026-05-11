# Experimento 03 - Paralelismo con Consumer Groups

## Objetivo

Observar en tiempo real cómo Kafka redistribuye particiones entre consumers del mismo grupo cuando se agregan o quitan instancias.

## Hipótesis

Con un topic de **4 particiones** y un consumer group:
- 1 consumer → procesa las 4 particiones
- 2 consumers → 2 particiones cada uno
- 4 consumers → 1 partición cada uno
- 5 consumers → 1 queda **idle** (sin partición asignada)

## Pre-requisitos

- [ ] Clúster Kafka iniciado
- [ ] Topic `transacciones-4p` creado con 4 particiones
- [ ] Java compilado (`mvn clean package`)
- [ ] `JAVA_HOME` configurado

---

## Procedimiento

### Paso 1 — Llenar el topic con 100 mensajes (BatchProducer)

Desde `kafka-lab-nivel-2\java\`:

```powershell
& "$env:JAVA_HOME\bin\java" -cp target\kafka-lab-nivel-2-1.0.0.jar `
    com.nexus.kafka.nivel2.BatchProducer transacciones-4p 100 true
```

**CMD:**
```cmd
"%JAVA_HOME%\bin\java" -cp target\kafka-lab-nivel-2-1.0.0.jar com.nexus.kafka.nivel2.BatchProducer transacciones-4p 100 true
```

Verifica que se enviaron 100 mensajes distribuidos en las 4 particiones.

---

### Escenario A — 1 Consumer (procesa todas las particiones)

**Terminal 1:**
```powershell
& "$env:JAVA_HOME\bin\java" -cp target\kafka-lab-nivel-2-1.0.0.jar `
    com.nexus.kafka.nivel2.InstrumentedConsumer transacciones-4p grupo-exp03 consumer-A1
```

**Observa:**
```
🎯 [consumer-A1] ASIGNADO A: Partitions [0, 1, 2, 3]
```
Un solo consumer recibe **todas** las particiones. Puede procesar los mensajes de forma secuencial.

**Deja este consumer corriendo y abre una nueva terminal.**

---

### Escenario B — 2 Consumers (rebalanceo automático)

**Terminal 2 (mientras Terminal 1 sigue corriendo):**
```powershell
& "$env:JAVA_HOME\bin\java" -cp target\kafka-lab-nivel-2-1.0.0.jar `
    com.nexus.kafka.nivel2.InstrumentedConsumer transacciones-4p grupo-exp03 consumer-B2
```

**Observa en Terminal 1:**
```
⚠️  [consumer-A1] REVOCADAS: Partitions [0, 1, 2, 3]    <- Kafka le quita particiones
🎯 [consumer-A1] ASIGNADO A: Partitions [0, 1]           <- Le reasigna 2
```

**Observa en Terminal 2:**
```
🎯 [consumer-B2] ASIGNADO A: Partitions [2, 3]           <- Recibe las otras 2
```

---

### Escenario C — 4 Consumers (1 partición por consumer)

Abre dos terminales más:

**Terminal 3:**
```powershell
& "$env:JAVA_HOME\bin\java" -cp target\kafka-lab-nivel-2-1.0.0.jar `
    com.nexus.kafka.nivel2.InstrumentedConsumer transacciones-4p grupo-exp03 consumer-C3
```

**Terminal 4:**
```powershell
& "$env:JAVA_HOME\bin\java" -cp target\kafka-lab-nivel-2-1.0.0.jar `
    com.nexus.kafka.nivel2.InstrumentedConsumer transacciones-4p grupo-exp03 consumer-C4
```

**Resultado esperado** (un rebalanceo más):
```
🎯 [consumer-A1] ASIGNADO A: Partitions [0]
🎯 [consumer-B2] ASIGNADO A: Partitions [1]
🎯 [consumer-C3] ASIGNADO A: Partitions [2]
🎯 [consumer-C4] ASIGNADO A: Partitions [3]
```
Máximo paralelismo para este topic.

---

### Escenario D — 5 Consumers (uno queda idle)

**Terminal 5:**
```powershell
& "$env:JAVA_HOME\bin\java" -cp target\kafka-lab-nivel-2-1.0.0.jar `
    com.nexus.kafka.nivel2.InstrumentedConsumer transacciones-4p grupo-exp03 consumer-D5
```

**Observa en Terminal 5:**
```
🎯 [consumer-D5] ASIGNADO A: Partitions []    <- Lista VACIA = idle
```
No hay particiones disponibles para el 5to consumer. Está conectado al grupo pero no procesa nada.

---

### Verificar asignaciones desde el broker

En cualquier terminal:
```powershell
docker exec kafka-nivel1 /opt/kafka/bin/kafka-consumer-groups.sh `
    --bootstrap-server localhost:9092 `
    --describe `
    --group grupo-exp03
```

🎯 ¿Dónde ocurre el rebalanceo aquí?
java
```bash
consumer.subscribe(Collections.singletonList(topic));
```

Cuando haces subscribe(), el consumidor no se asigna manualmente a particiones. Es Kafka quien decide, mediante rebalanceo, qué particiones le tocan.
🔄 Ciclo completo de rebalanceo en tu código
1. Join Group (cuando inicias)
java
```bash
// El consumidor le dice al coordinador: "Quiero unirme al grupo X"
consumer.subscribe(...);  // Internamente envía JoinGroup request
// El coordinador decide: particiones [0, 1] para este consumidor
```

2. Heartbeats (durante la ejecución)
java
```bash
while (CORRIENDO.get()) {
    consumer.poll(Duration.ofMillis(1000));  // Internamente envía heartbeats
    // Si no hace poll en max.poll.interval.ms (5 min), el coordinador lo saca
}
```

3. Leave Group (al cerrar)
java
```bash
// Shutdown hook
Runtime.getRuntime().addShutdownHook(new Thread(() -> {
    CORRIENDO.set(false);  // Al salir del while, consumer.close() libera particiones
}));
```
⚙️ Configuraciones que afectan rebalanceos (en tu código)
```bash
java

// Tiempo máximo entre heartbeats (default 45 seg)
// Si el consumer no responde por más tiempo, el coordinador lo declara muerto
props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 30000); // 30 seg

// Tiempo máximo entre polls (default 5 minutos)
// Si no llamas a poll() en este tiempo, Kafka asume que está "atorado"
props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 300000); // 5 min

// Frecuencia de heartbeat (default 3 seg)
props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 3000);
```
---

## Resultados esperados (tabla de asignaciones)

| Escenario | consumer-A1 | consumer-B2 | consumer-C3 | consumer-C4 | consumer-D5 |
|---|---|---|---|---|---|
| A: 1 consumer  | [0,1,2,3] | — | — | — | — |
| B: 2 consumers | [0,1] | [2,3] | — | — | — |
| C: 4 consumers | [0] | [1] | [2] | [3] | — |
| D: 5 consumers | [0] | [1] | [2] | [3] | [] (idle) |

---

## Análisis

### La regla fundamental: 1 partición = máximo 1 consumer por grupo

Kafka garantiza que **dos consumers del mismo grupo nunca leen la misma partición al mismo tiempo**. Esto es lo que permite el procesamiento paralelo sin duplicados.

Si hubiera 2 consumers leyendo la misma partición → el mismo mensaje podría procesarse dos veces → inconsistencia de datos.

### ¿Qué pasa cuando un consumer muere?

Cierra una ventana (Ctrl+C). Observa en las otras:
1. Kafka detecta que el consumer dejó de enviar **heartbeats** (latido de vida)
2. Después del `session.timeout.ms` (30 segundos en nuestra config), lo declara muerto
3. Dispara un **rebalanceo**: el group coordinator redistribuye las particiones entre los consumers vivos
4. Los consumers que siguen activos reciben las particiones del muerto

Esto es la **alta disponibilidad** de los consumer groups en Kafka.

### ¿Por qué el 5to consumer queda idle?

Hay 4 particiones y 4 consumers vivos → todas las particiones están asignadas. El 5to consumer entra al grupo pero no hay partición disponible. Sin embargo, está listo para tomar una partición si algún consumer activo falla.

---

## Conclusiones

- [ ] Observé el rebalanceo automático al agregar consumers
- [ ] Confirmé que con N consumers y N particiones → 1 partición por consumer
- [ ] Entiendo por qué un consumer extra queda idle
- [ ] Entiendo la regla "1 partición = máximo 1 consumer por grupo"
- [ ] Vi que el rebalanceo ocurre también al quitar un consumer (simular fallo)

## Resultados reales (tabla)

| Escenario | Asignaciones observadas |
|---|---|
| 1 consumer  | |
| 2 consumers | |
| 4 consumers | |
| 5 consumers | |
