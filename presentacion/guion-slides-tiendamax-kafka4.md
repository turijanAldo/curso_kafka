# Guion de Slides — TiendaMax con Apache Kafka 4 (KRaft)

> **Audiencia:** Desarrolladores backend con experiencia básica en Java/Spring  
> **Duración total:** 2h 30 min (teoría + demos en vivo + hands-on)  
> **Imagen Docker:** `apache/kafka:4.2.0`  
> **Prerequisitos:** Docker Desktop, Java 17, Maven 3.9+, IDE

---

## Estructura general del deck

| Bloque | Tema | Slides | Duración |
|--------|------|--------|----------|
| 1 | El problema | 1–3 | 10 min |
| 2 | Kafka 4 KRaft | 4–7 | 15 min |
| 3 | Hola Kafka CLI | 8–10 | 15 min ★ DEMO |
| 4 | Infra & Topics | 11–12 | 10 min ★ DEMO |
| 5 | Producer Java | 13–15 | 15 min ★ DEMO |
| 6 | Consumer + Commit | 16–17 | 15 min ★ DEMO |
| 7 | Consumer Groups | 18 | 15 min ★ DEMO |
| 8 | Fallo y Rebalanceo | 19 | 15 min ★ DEMO |
| 9 | Lag y Reset | 20–21 | 10 min ★ DEMO |
| 10 | Cierre | 22 | 5 min |

---

## Bloque 1 — El problema (10 min)

### Slide 1 — Portada

**Título principal:**
> De API Síncrona a Streaming Real  
> El caso TiendaMax con Apache Kafka 4

**Subtítulo:**
> Por qué Kafka, cómo configurarlo desde cero, y código que funciona

**Elementos visuales:**
- Logo Apache Kafka a la derecha, fondo oscuro `#1a1a2e`
- Badge: `apache/kafka:4.2.0 · KRaft · Sin ZooKeeper`
- Nombre del instructor | Fecha

> 🎤 **Notas del instructor:**  
> Arranca de pie, no sentado. Di: *"Hoy vamos a romper una arquitectura REST, ver cómo falla en producción, y reconstruirla con Kafka 4. Cero diapositivas infinitas — código real que puedes llevarte a tu empresa esta tarde."*

---

### Slide 2 — El problema de TiendaMax

**Título:** `TiendaMax: cuando el REST síncrono duele`

**Diagrama a dibujar:**

```
Cliente HTTP
    │
    ▼  POST /pedidos
┌──────────────┐
│  API Pedidos │ ──────► Notificaciones  ❌ (CAÍDO)
└──────────────┘ ──────► Inventario      ✓
                 ──────► Factura         ✓
```

**Bullets (revelar uno a uno):**
- Si Notificaciones tarda 30s → el usuario espera 30s
- Si Notificaciones se cae → el pedido **falla** aunque todo lo demás esté OK
- Si el tráfico se triplica → los 3 servicios escalan juntos (caro)
- Sin registro de qué pasó y cuándo → debugging ciego

**❓ Pregunta al aula** *(pausa real, esperar respuestas):*
> "¿Qué pasa si Notificaciones se cae 5 minutos durante el Black Friday?"

> 🎤 **Notas del instructor:**  
> Espera 3-4 respuestas genuinas. Cuando alguien diga "se reintenta", pregunta: "¿dónde vive esa lógica de reintento? ¿y si la API también cae?". Deja que ELLOS lleguen a "necesitamos desacoplamiento".

---

### Slide 3 — Qué vamos a construir hoy

**Título:** `La arquitectura que queremos`

**Diagrama objetivo:**

```
Cliente HTTP
    │
    ▼  POST /pedidos
┌──────────────┐
│  API Pedidos │──► topic: pedidos-tiendamax ──► Inventario
└──────────────┘         (Kafka)               ──► Notificaciones
  "202 Accepted"                               ──► Factura
```

**Tres promesas:**

| | Promesa |
|-|---------|
| ✅ | Ningún pedido se pierde aunque un servicio caiga |
| ✅ | Cada servicio escala de forma independiente |
| ✅ | Trazabilidad completa: qué pasó, cuándo, en qué partición |

**Frase de cierre:**
> *"Lo vamos a construir hoy. Paso a paso. Con código que funciona."*

---

## Bloque 2 — Kafka 4 y KRaft (15 min)

### Slide 4 — Qué es Kafka en una frase

**Título:** `Kafka no es una cola — es un log`

**Cita central (letra grande):**
> "Un log distribuido, particionado y replicado al que muchos productores escriben y muchos consumidores leen **a su propio ritmo**."

**Comparación rápida:**

| Cola (RabbitMQ) | Log (Kafka) |
|-----------------|-------------|
| Mensaje = pop (desaparece al leer) | Mensaje = append (no se borra al leer) |
| 1 consumer group lee el mensaje | N consumer groups leen el **mismo** mensaje |
| Sin historial | Retention configurable (días / semanas) |
| Push al consumer | Consumer hace **pull** a su ritmo |

> 🎤 **Notas del instructor:**  
> Subraya *"a su propio ritmo"*. Es la diferencia más importante. Inventario puede ir a 10k msg/s, Factura a 100 msg/s — sin problema.

---

### Slide 5 — Topic, Partición y Offset

**Título:** `El modelo mental que necesitas`

**Diagrama:**

```
topic: pedidos-tiendamax (3 particiones)

Partición 0: [msg0][msg1][msg4][msg7]  ← offsets crecientes, inmutables
Partición 1: [msg2][msg5][msg8]
Partición 2: [msg3][msg6][msg9]

Producer envía con key=order_id → hash(key) % 3 → partición determinista
```

**Bullets clave:**
- Orden garantizado **solo dentro de una partición** (¡no entre particiones!)
- Los mensajes **no** se borran al leerlos (se borran por retención)
- Un consumer "avanza" por su propio offset — independiente de otros
- Más particiones = más paralelismo de lectura = más throughput

**Frase para recordar:**
> *"Orden por partición, no por topic. Repítelo hasta que duela."*

> 🎤 **Notas del instructor:**  
> Dibuja esto en la pizarra si puedes. Si solo se llevan UNA cosa del bloque 2, es esta.

---

### Slide 6 — Producer, Consumer y Consumer Group

**Título:** `¿Quién produce? ¿Quién consume?`

**Diagrama:**

```
┌─────────────┐     ┌────────────────────┐     ┌──────────────────────────┐
│  Producer   │────►│ pedidos-tiendamax  │────►│  inventario-group        │
│  (API Rest) │     │  P0  P1  P2        │     │  C1=P0, C2=P1, C3=P2    │
└─────────────┘     └────────────────────┘     └──────────────────────────┘
                             │                  ┌──────────────────────────┐
                             └─────────────────►│  notif-group             │
                                                │  C1=P0+P1+P2             │
                                                └──────────────────────────┘
```

**Reglas del consumer group:**
- 1 partición → máximo 1 consumer por grupo (no se comparte)
- Más consumers que particiones → los extras quedan en **standby**
- Distintos grupos → cada uno recibe **todos** los mensajes (broadcast efectivo)
- Kafka rastrea el offset por grupo: `inventario-group` y `notif-group` son independientes

---

### Slide 7 — Kafka 4.x y KRaft: el fin de ZooKeeper

**Título:** `Kafka 4.x: arquitectura simplificada con KRaft`

**Antes (Kafka < 3.0):**

```
┌──────────┐    ┌──────────────────────────────────┐
│ZooKeeper │◄───│ Broker 1, 2, 3 (meta en ZK)      │
│(cluster) │    │ Controller externo a los brokers  │
└──────────┘    └──────────────────────────────────┘
Problema: 2 sistemas, 2 configs, 2 monitoreos, 2 formas de fallar.
```

**Ahora (Kafka 4.x con KRaft):**

```
┌────────────────────────────────────────────────────┐
│  Broker/Controller (todo en uno — Raft consensus)  │
│  Un solo proceso, un solo log de metadatos         │
└────────────────────────────────────────────────────┘
```

**Beneficios de KRaft en Kafka 4:**

| | Beneficio |
|-|-----------|
| ✅ | Sin ZooKeeper — un proceso menos que operar |
| ✅ | Tiempo de arranque hasta 10x más rápido |
| ✅ | Clusters de hasta 1 millón de particiones (antes ~200k) |
| ✅ | Failover de controller en segundos (antes: minutos) |
| ✅ | Configuración más simple para desarrollo local |

**Imagen Docker para hoy:**
```
apache/kafka:4.2.0
```

> 🎤 **Notas del instructor:**  
> Si alguien trae un tutorial viejo con ZooKeeper, explica que en Kafka 4.x está completamente removido. KRaft es el único modo disponible.

---

## Bloque 3 — Paso 0: Hola Kafka sin código (15 min) ★ DEMO

### Slide 8 — Objetivo y comandos base

**Título:** `Paso 0: Ver Kafka en acción en 60 segundos`

**Subtítulo:** *Sin Java. Sin Maven. Solo el CLI que viene en la imagen.*

**Cheat sheet de comandos:**

| Comando | Para qué |
|---------|----------|
| `kafka-topics.sh` | Crear, listar, describir, borrar topics |
| `kafka-console-producer.sh` | Enviar mensajes a mano (stdin) |
| `kafka-console-consumer.sh` | Leer mensajes en tiempo real |
| `kafka-consumer-groups.sh` | Ver lag, listar grupos, resetear offsets |

**Ruta dentro del container:** `/opt/kafka/bin/`

> 🎤 **Notas del instructor:**  
> Abre 3 terminales conectadas al container y haz `export PATH` en todas ANTES de que entren los alumnos. Un `command not found` arruina el momento.

---

### Slide 9 — Demo en vivo *(pantalla compartida — cheat sheet)*

**Título:** `Comandos del Paso 0 — Demo en vivo`

> Este slide es referencia mientras compartes pantalla. Los alumnos siguen desde el lab HTML.

**Terminal 1 — entrar al container:**
```bash
docker exec -it kafka bash
export PATH=/opt/kafka/bin:$PATH
```

**Crear el topic de prueba:**
```bash
kafka-topics.sh --create \
  --topic hola-kafka \
  --partitions 1 \
  --replication-factor 1 \
  --bootstrap-server localhost:9092

# Verificar:
kafka-topics.sh --list --bootstrap-server localhost:9092
```

**Terminal 2 — Producer (escribir mensajes):**
```bash
kafka-console-producer.sh \
  --topic hola-kafka \
  --bootstrap-server localhost:9092
> Hola TiendaMax
> Primer pedido
> Apache Kafka 4 es genial
```

**Terminal 3 — Consumer (leer en tiempo real):**
```bash
# Con --from-beginning: lee TODO desde el offset 0
kafka-console-consumer.sh \
  --topic hola-kafka \
  --from-beginning \
  --bootstrap-server localhost:9092

# Sin --from-beginning: solo mensajes NUEVOS (a partir de ahora)
kafka-console-consumer.sh \
  --topic hola-kafka \
  --bootstrap-server localhost:9092
```

**Puntos clave a resaltar en vivo:**
1. Los mensajes aparecen en **tiempo real** en la Terminal 3
2. `--from-beginning` lee **todo el historial** (Kafka es un log, no una cola)
3. Si reinicias el consumer sin `--from-beginning`, empieza del final
4. Los mensajes **siguen en Kafka** aunque el consumer los leyó

---

### Slide 10 — Describe: entendiendo lo que creamos

**Título:** `kafka-topics.sh --describe`

**Comando:**
```bash
kafka-topics.sh \
  --describe \
  --topic hola-kafka \
  --bootstrap-server localhost:9092
```

**Salida esperada:**
```
Topic: hola-kafka   PartitionCount: 1   ReplicationFactor: 1   Configs: ...
  Partition: 0   Leader: 1   Replicas: 1   Isr: 1
```

**Interpretación:**

| Campo | Significado |
|-------|-------------|
| `Leader: 1` | El broker ID 1 sirve las lecturas y escrituras |
| `Replicas: 1` | Solo 1 réplica (dev). En prod mínimo 3 |
| `Isr: 1` | In-Sync Replicas. En prod debe igualar a Replicas |

> ⚠️ **Advertencia:** `ReplicationFactor: 1` = sin tolerancia a fallos. Solo para desarrollo local. En producción usar **RF=3** mínimo.

**Frase puente:**
> *"Ahora que entendemos el CLI, construyamos la infra real de TiendaMax."*

---

## Bloque 4 — Paso 1: Infraestructura con Docker (10 min) ★ DEMO

### Slide 11 — docker-compose.yml anotado (Kafka 4 KRaft)

**Título:** `El docker-compose.yml explicado línea a línea`

```yaml
services:
  kafka:
    image: apache/kafka:4.2.0          # ← Kafka 4, KRaft nativo
    container_name: kafka
    ports:
      - "9092:9092"                    # ← puerto del broker para apps Java
    environment:
      # ── IDENTIDAD ─────────────────────────────────────
      KAFKA_NODE_ID: 1                          # ID único del nodo
      KAFKA_PROCESS_ROLES: broker,controller    # este nodo es AMBOS (dev)

      # ── RED ───────────────────────────────────────────
      KAFKA_LISTENERS: PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT
      KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER

      # ── KRAFT CONSENSUS ───────────────────────────────
      KAFKA_CONTROLLER_QUORUM_VOTERS: 1@localhost:9093
      KAFKA_LOG_DIRS: /var/lib/kafka/data
      CLUSTER_ID: MkU3OEVBNTcwNTJENDM2Qk      # base64 UUID del cluster

      # ── CONFIGURACIÓN MÍNIMA PARA DEV ─────────────────
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1         # ⚠️ DEV ONLY
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1 # ⚠️ DEV ONLY
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1            # ⚠️ DEV ONLY

    healthcheck:
      test: ["CMD", "/opt/kafka/bin/kafka-broker-api-versions.sh",
             "--bootstrap-server", "localhost:9092"]
      interval: 10s
      timeout: 10s
      retries: 5
      start_period: 30s
```

**Variables críticas:**

| Variable | Por qué importa |
|----------|-----------------|
| `KAFKA_PROCESS_ROLES=broker,controller` | 1 solo proceso. En prod separar roles |
| `KAFKA_ADVERTISED_LISTENERS` | La dirección que los clientes Java usan |
| `KAFKA_CONTROLLER_QUORUM_VOTERS` | Quorum Raft. En prod: 3 nodos mínimo |
| `CLUSTER_ID` | UUID del cluster. Generarlo una vez y no cambiar |
| `OFFSETS_TOPIC_REPLICATION_FACTOR=1` | ⚠️ Solo dev. En prod usar 3 |

> 💡 **Generar CLUSTER_ID:**
> ```bash
> docker run --rm apache/kafka:4.2.0 \
>   /opt/kafka/bin/kafka-storage.sh random-uuid
> ```

---

### Slide 12 — Crear el topic de TiendaMax

**Título:** `Paso 1: el topic de producción`

```bash
docker exec -it kafka \
  /opt/kafka/bin/kafka-topics.sh --create \
  --topic pedidos-tiendamax \
  --partitions 3 \
  --replication-factor 1 \
  --config retention.ms=604800000 \
  --config max.message.bytes=1048576 \
  --bootstrap-server localhost:9092
```

**Decisiones de diseño:**

| Parámetro | Valor | Por qué |
|-----------|-------|---------|
| `--partitions 3` | 3 | 3 servicios consumidores = 3 en paralelo |
| `--replication-factor 1` | 1 | Solo para dev. Prod: **mínimo 3** |
| `retention.ms=604800000` | 7 días | Si Notif cae 3 días, al volver procesa sin pérdida |
| `max.message.bytes=1048576` | 1 MB | Suficiente para cualquier JSON de pedido |

**Verificar la creación:**
```bash
docker exec -it kafka \
  /opt/kafka/bin/kafka-topics.sh \
  --describe \
  --topic pedidos-tiendamax \
  --bootstrap-server localhost:9092
```

---

## Bloque 5 — Paso 2: Producer Java (15 min) ★ DEMO

### Slide 13 — Dependencia Maven y configuración mínima

**Título:** `Anatomía de un Producer Kafka en Java`

**Dependencia `pom.xml`:**
```xml
<dependency>
    <groupId>org.apache.kafka</groupId>
    <artifactId>kafka-clients</artifactId>
    <version>4.0.0</version>
</dependency>
```

**Configuración mínima:**
```java
Properties props = new Properties();
props.put("bootstrap.servers",    "localhost:9092");
props.put("key.serializer",       StringSerializer.class.getName());
props.put("value.serializer",     StringSerializer.class.getName());
props.put("acks",                 "all");
props.put("enable.idempotence",   "true");
props.put("retries",              "3");
props.put("linger.ms",            "5");     // batching: espera 5ms
props.put("batch.size",           "16384"); // 16KB por batch

KafkaProducer<String, String> producer = new KafkaProducer<>(props);
```

---

### Slide 14 — El "por qué" de `acks=all`

**Título:** `acks — La decisión más importante de un producer`

| acks | Quién confirma | Riesgo |
|------|----------------|--------|
| `0` | Nadie (fire & forget) | Pérdida silenciosa garantizada |
| `1` | Solo el broker leader | Si el leader cae antes de replicar → pérdida de datos |
| `all` | Todas las ISR (In-Sync Replicas) | Cero pérdida con RF≥3 y min.insync.replicas=2 |

**Configuración para producción sin pérdidas:**
```java
props.put("acks",               "all");
props.put("enable.idempotence", "true");        // evita duplicados en reintento
props.put("retries",            Integer.MAX_VALUE); // reintenta indefinidamente
```

```yaml
# En el broker (docker-compose):
KAFKA_MIN_INSYNC_REPLICAS: 2  # necesita 2 réplicas sincronizadas para confirmar
```

> *"En TiendaMax cada pedido = dinero real. `acks=all` siempre, sin excepción."*

---

### Slide 15 — La key del mensaje importa

**Título:** `La key del mensaje = decisión de particionamiento`

**Código del Producer de TiendaMax:**
```java
ProducerRecord<String, String> record = new ProducerRecord<>(
    "pedidos-tiendamax",                         // topic
    pedido.getOrderId(),                         // KEY ← determina la partición
    objectMapper.writeValueAsString(pedido)      // VALUE (JSON)
);

producer.send(record, (metadata, exception) -> {
    if (exception == null) {
        System.out.printf("✅ Pedido %s → P%d offset %d%n",
            pedido.getOrderId(),
            metadata.partition(),
            metadata.offset());
    } else {
        System.err.println("❌ Error: " + exception.getMessage());
    }
});
```

**Lógica de particionamiento:**
```
partición = hash(key) % numPartitions

"ORDER-001" → hash → Partición 0  (siempre la misma)
"ORDER-002" → hash → Partición 2  (siempre la misma)
"ORDER-003" → hash → Partición 1  (siempre la misma)
```

**¿Por qué key = `order_id`?**
- ✅ Todos los eventos de un pedido van a **la misma partición**
- ✅ Orden garantizado: `CREADO → PAGADO → ENVIADO`
- ✅ No importa cuánto escale el producer

**Anti-patrón ❌ NO hacer:**
```
key = cliente_id
Si "Ana" hace el 80% de pedidos → P0 recibe el 80% del tráfico
→ hotspot: un consumer saturado, los otros dos ociosos
```

**Demo:** correr `PedidoProducer.java` y mostrar distribución:
```bash
kafka-console-consumer.sh --topic pedidos-tiendamax \
  --partition 0 --from-beginning --bootstrap-server localhost:9092
```

---

## Bloque 6 — Paso 3: Consumer + Commit manual (15 min) ★ DEMO

### Slide 16 — El loop del Consumer

**Título:** `Cómo funciona un Consumer Kafka`

**Configuración:**
```java
Properties props = new Properties();
props.put("bootstrap.servers",   "localhost:9092");
props.put("group.id",            "inventario-group");
props.put("key.deserializer",    StringDeserializer.class.getName());
props.put("value.deserializer",  StringDeserializer.class.getName());
props.put("enable.auto.commit",  "false");   // commit manual = control total
props.put("auto.offset.reset",   "earliest"); // al arrancar: leer desde el inicio
props.put("max.poll.records",    "100");      // lotes de máximo 100 mensajes
props.put("session.timeout.ms",  "30000");   // 30s sin heartbeat = considerado muerto
```

**El loop principal:**
```java
consumer.subscribe(List.of("pedidos-tiendamax"));

while (running) {
    ConsumerRecords<String, String> records =
        consumer.poll(Duration.ofMillis(500));

    for (ConsumerRecord<String, String> record : records) {
        procesarPedido(record.value());   // lógica de negocio
        // si falla aquí, NO commitear → Kafka reentrega
    }

    consumer.commitSync();  // confirmar DESPUÉS de procesar TODO el lote
}
```

**Semánticas de entrega:**

| Semántica | Cuándo ocurre | Riesgo |
|-----------|---------------|--------|
| **At-most-once** | Commit **antes** de procesar | Pérdida: commit → crash → mensaje perdido |
| **At-least-once** | Commit **después** de procesar | Duplicado: fallo tras proceso → Kafka reenvía |
| **Exactly-once** | Transacciones Kafka | Tema avanzado — producer + consumer transaccional |

> Para TiendaMax usamos **at-least-once + idempotencia en el consumer**.

---

### Slide 17 — commitSync vs commitAsync

**Título:** `¿commitSync o commitAsync?`

| Característica | `commitSync()` | `commitAsync()` |
|----------------|----------------|-----------------|
| Bloquea thread | SÍ | NO |
| Reintentos automáticos | SÍ | NO reintenta |
| Si falla el commit | Lanza excepción | Callback de error |
| Throughput | Menor | Mayor |
| Cuándo usar | Datos críticos (pedidos) | Métricas, telemetría |

**Patrón híbrido (referencia avanzada):**
```java
// commitAsync durante el loop (rendimiento)
for (ConsumerRecord<String, String> r : records) {
    procesarPedido(r.value());
}
consumer.commitAsync();

// commitSync garantizado al cerrar (try-finally)
try {
    consumer.commitSync();
} finally {
    consumer.close();
}
```

**Demo:** arrancar el consumer y ver el lag bajar a 0:
```bash
kafka-consumer-groups.sh \
  --describe \
  --group inventario-group \
  --bootstrap-server localhost:9092
```

---

## Bloque 7 — Paso 4: Consumer Groups y paralelismo (15 min) ★ DEMO

### Slide 18 — Cómo se reparten las particiones

**Título:** `Consumer Group: escalado automático del procesamiento`

**Escenario 1 — 3 consumers, 3 particiones (ideal):**
```
inventario-group
  Consumer-1 → Partición 0
  Consumer-2 → Partición 1
  Consumer-3 → Partición 2
  Throughput: MÁXIMO (3x paralelismo)
```

**Escenario 2 — 4 consumers, 3 particiones:**
```
inventario-group
  Consumer-1 → Partición 0
  Consumer-2 → Partición 1
  Consumer-3 → Partición 2
  Consumer-4 → ∅ STANDBY (hot standby: activa si otro cae)
```

**Escenario 3 — 2 consumers, 3 particiones:**
```
inventario-group
  Consumer-1 → Partición 0 + Partición 1  ← carga doble
  Consumer-2 → Partición 2
```

> **Regla práctica:** *"Particiones = techo de paralelismo. No puedes procesar más rápido que lo que permiten tus particiones aunque pongas 100 consumers."*

**Demo en vivo:**
```bash
# Terminal 1
mvn exec:java -Dexec.mainClass="com.tiendamax.InventarioConsumer" -Dexec.args="consumer-1"

# Terminal 2
mvn exec:java -Dexec.mainClass="com.tiendamax.InventarioConsumer" -Dexec.args="consumer-2"

# Terminal 3
mvn exec:java -Dexec.mainClass="com.tiendamax.InventarioConsumer" -Dexec.args="consumer-3"

# Ver asignación:
kafka-consumer-groups.sh \
  --describe --group inventario-group \
  --bootstrap-server localhost:9092
```

---

## Bloque 8 — Paso 5: Fallo y Rebalanceo (15 min) ★ DEMO

### Slide 19 — Anatomía de un rebalanceo

**Título:** `¿Qué pasa cuando un consumer se cae?`

**Timeline:**

| Tiempo | Evento |
|--------|--------|
| `t = 0s` | Consumer-2 se cae (Ctrl+C o crash de JVM) 💥 |
| `t = 0–10s` | Consumer-1 y Consumer-3 siguen procesando |
| `t = 10s` | Group Coordinator detecta heartbeat perdido (`session.timeout.ms`) |
| `t = 10s` | Consumer-2 declarado "muerto" → dispara **REBALANCEO** |
| `t = 10–11s` | **PAUSE**: todos los consumers del grupo dejan de leer |
| `t = 11s` | **REASSIGN**: C1 → P0+P1 · C3 → P2 |
| `t = 12s` | **RESUME** desde el último offset commiteado → mensajes duplicados ⚠️ |

**Antes vs después:**

```
ANTES DEL FALLO:          DESPUÉS DEL REBALANCEO:
C1 → P0                   C1 → P0 + P1  (carga extra)
C2 → P1  ← MUERTO         (ausente)
C3 → P2                   C3 → P2
```

**Parámetros que controlan el rebalanceo:**
```java
props.put("session.timeout.ms",    "30000");  // 30s sin heartbeat = muerto
props.put("heartbeat.interval.ms", "3000");   // frecuencia de "ping"
props.put("max.poll.interval.ms",  "300000"); // tiempo máximo entre polls
```

**Rebalanceo cooperativo (Kafka 2.4+, recomendado):**
```java
props.put("partition.assignment.strategy",
          "org.apache.kafka.clients.consumer.CooperativeStickyAssignor");
// Solo reasigna las particiones necesarias, sin pausar todo el grupo
```

**Demo:** matar Consumer-2 con `Ctrl+C`. Observar en `--describe` cómo P1 migra a C1. Notar los **mensajes duplicados** en el log de C1.

---

## Bloque 9 — Paso 6: Lag y Reset de Offsets (10 min) ★ DEMO

### Slide 20 — Consumer Lag: la métrica #1

**Título:** `Consumer Lag: ¿qué tan atrasado estás?`

**Fórmula:**
```
LAG = LOG-END-OFFSET − CURRENT-OFFSET
```

**Comando:**
```bash
kafka-consumer-groups.sh \
  --describe \
  --group inventario-group \
  --bootstrap-server localhost:9092
```

**Salida de ejemplo:**
```
GROUP              TOPIC               PART  CURRENT  LOG-END  LAG   CONSUMER-ID
inventario-group   pedidos-tiendamax   0     1247     1247     0     consumer-1
inventario-group   pedidos-tiendamax   1     1198     1250     52    consumer-2  ← PROBLEMA
inventario-group   pedidos-tiendamax   2     1300     1300     0     consumer-3
```

**Umbrales de alerta en producción:**

| LAG | Estado | Acción |
|-----|--------|--------|
| `0` | ✅ Al día | — |
| `> 1000` | ⚠️ Investigar | ¿Consumer lento? ¿Bug? |
| `> 10000` | 🚨 Incidente | ¿Consumer caído? |
| `> 100000` | 🔴 P0 | Sistema de procesamiento colapsado |

---

### Slide 21 — Reset de Offsets: herramienta de emergencia

**Título:** `Reset de offsets — cuándo y cómo`

> ⚠️ **Regla de oro:** El consumer group **DEBE ESTAR DETENIDO** para resetear offsets. Si hay consumers activos, el reset falla o tiene comportamiento impredecible.

**Comandos de reset:**

```bash
# 1. Ver el estado ANTES de actuar (siempre primero)
kafka-consumer-groups.sh \
  --describe --group inventario-group \
  --bootstrap-server localhost:9092

# 2a. Dry-run (ver qué haría sin ejecutar)
kafka-consumer-groups.sh \
  --reset-offsets --to-earliest --dry-run \
  --group inventario-group --topic pedidos-tiendamax \
  --bootstrap-server localhost:9092

# 2b. Volver al inicio (reprocesar TODO)
kafka-consumer-groups.sh \
  --reset-offsets --to-earliest --execute \
  --group inventario-group --topic pedidos-tiendamax \
  --bootstrap-server localhost:9092

# 2c. Saltar al final (descartar backlog)
kafka-consumer-groups.sh \
  --reset-offsets --to-latest --execute \
  --group inventario-group --topic pedidos-tiendamax \
  --bootstrap-server localhost:9092

# 2d. Ir a un timestamp exacto (bug introducido a las 14:30)
kafka-consumer-groups.sh \
  --reset-offsets --to-datetime 2025-05-14T14:30:00.000 --execute \
  --group inventario-group --topic pedidos-tiendamax \
  --bootstrap-server localhost:9092

# 2e. Retroceder N mensajes en una partición (saltar mensajes corruptos)
kafka-consumer-groups.sh \
  --reset-offsets --shift-by -100 --execute \
  --group inventario-group --topic pedidos-tiendamax:1 \
  --bootstrap-server localhost:9092
```

**Cuándo usar cada flag:**

| Flag | Cuándo usarlo |
|------|---------------|
| `--to-earliest` | Reprocessing total (nuevo feature retroactivo) |
| `--to-latest` | Descartar backlog de bajo valor (métricas viejas) |
| `--to-datetime` | Bug en producción introducido en un momento exacto |
| `--shift-by -N` | Unos pocos mensajes corruptos a saltear |
| `--dry-run` | **SIEMPRE** primero para ver el efecto sin ejecutar |

**Demo:** generar lag (producer corriendo, consumer apagado). Ver con `--describe`. Resetear con `--to-earliest --dry-run` primero, luego `--execute`.

---

## Bloque 10 — Cierre (5 min)

### Slide 22 — Recap y próximos pasos

**Título:** `¿Qué construimos hoy?`

**Lo que cubriste:**

| | Tema |
|-|------|
| ✅ | El problema de la arquitectura síncrona de TiendaMax |
| ✅ | Kafka 4.x con KRaft — sin ZooKeeper, configuración simplificada |
| ✅ | `docker-compose.yml` con `apache/kafka:4.2.0` línea a línea |
| ✅ | Topics con particiones y retention (`pedidos-tiendamax`) |
| ✅ | Producer Java con `acks=all` e idempotencia habilitada |
| ✅ | Consumer con `commitSync` manual (at-least-once) |
| ✅ | Consumer Groups: paralelismo real con 3 consumers / 3 particiones |
| ✅ | Rebalanceo: Kafka se recupera de una caída sin perder mensajes |
| ✅ | Consumer Lag: la métrica #1 de producción |
| ✅ | Reset de offsets: herramienta de emergencia para bugs y reprocessing |

**Lo que sigue (próximos cursos):**

| | Tema |
|-|------|
| 🔜 | Cluster multi-broker: RF=3, `min.insync.replicas=2`, tolerancia real a fallos |
| 🔜 | Exactly-once: transacciones Kafka en producer y consumer |
| 🔜 | Schema Registry: Avro / Protobuf — contratos de datos entre equipos |
| 🔜 | Dead Letter Queue (DLQ): qué hacer con mensajes que no se pueden procesar |
| 🔜 | Kafka Streams: procesamiento de streams en Java |
| 🔜 | Kafka Connect: integración con DB, S3, Elasticsearch sin código |

**Recursos para llevarse a casa:**
- 📄 Lab HTML anotado: `kafka-lab-tiendamax.html`
- 📄 Guion completo: `kafka-tiendamax-guion.md`
- 🐳 Docker image: `apache/kafka:4.2.0`

**Frase de cierre:**
> *"TiendaMax ya no pierde pedidos cuando Notificaciones se cae. Kafka almacena. El consumer lee cuando puede. Sin pérdida de datos. Eso es lo que construiste hoy."*

---

## Notas generales para el instructor

### Pre-flight checklist

```
□ docker compose up -d corriendo y verificado
□ docker compose ps → kafka: healthy
□ topic pedidos-tiendamax ya creado
□ 4 terminales abiertas dentro del container con PATH configurado
□ IDE con proyecto Java compilado (mvn compile ejecutado)
□ kafka-lab-tiendamax.html abierto en tab separado
□ Resolución de pantalla: 1920x1080, fuente IDE en 20px+
```

### Estilo de la sesión

- **No leas las slides.** Son anclas visuales; el contenido lo cuentas tú.
- Cada demo: comparte pantalla + lab HTML visible al costado.
- **Pregunta detonadora** antes de cada demo: *"¿qué creen que va a pasar si...?"*
- Los alumnos **deben seguir el lab en paralelo**. No avances si alguien está roto.
- Si te atrasas: comprime el Bloque 2. **Nunca** comprimas las demos.

### Tiempos de Q&A

- 5 min después del Bloque 2 → dudas conceptuales sobre particiones
- 10 min después del Bloque 8 → la mayoría de preguntas reales aparecen aquí
- Q&A abierto en el cierre (no más de 15 min)

### Errores comunes a anticipar

| Error | Causa | Solución |
|-------|-------|----------|
| `command not found` en Windows PowerShell | `\` en multilínea no funciona en PS | Usar `` ` `` o comandos en una línea; mejor: ejecutar dentro del bash del container |
| `kafka-console-consumer.sh` se congela | No hay mensajes en el topic | Enviar algo con el producer en otra terminal |
| `topic already exists` | Topic de ejecución anterior | Usar `--if-not-exists` o ignorar el error |
| `Connection refused localhost:9092` | Container no corriendo | `docker compose up -d` y esperar 30s |
| `Ctrl+C` en producer → último mensaje no enviado | Cierre forzado | Usar `Ctrl+D` para cerrar limpiamente |
| `ClassNotFoundException: StringSerializer` | Dependencia faltante | Verificar `pom.xml` y hacer `mvn compile` |

---

## Apéndice — Referencia rápida de comandos Kafka 4

### Topics

```bash
# Crear
kafka-topics.sh --create --topic NOMBRE \
  --partitions 3 --replication-factor 1 \
  --bootstrap-server localhost:9092

# Listar
kafka-topics.sh --list --bootstrap-server localhost:9092

# Describir
kafka-topics.sh --describe --topic NOMBRE --bootstrap-server localhost:9092

# Borrar
kafka-topics.sh --delete --topic NOMBRE --bootstrap-server localhost:9092

# Aumentar particiones (solo se puede aumentar, no bajar)
kafka-topics.sh --alter --topic NOMBRE \
  --partitions 6 --bootstrap-server localhost:9092
```

### Producer

```bash
# Producer básico (stdin)
kafka-console-producer.sh \
  --topic NOMBRE --bootstrap-server localhost:9092

# Producer con keys (separador = :)
kafka-console-producer.sh \
  --topic NOMBRE \
  --property "key.separator=:" \
  --property "parse.key=true" \
  --bootstrap-server localhost:9092
# > ORDER-001:{"id":"ORDER-001","amount":150.00}
```

### Consumer

```bash
# Desde el final (solo mensajes nuevos)
kafka-console-consumer.sh \
  --topic NOMBRE --bootstrap-server localhost:9092

# Desde el inicio
kafka-console-consumer.sh \
  --topic NOMBRE --from-beginning --bootstrap-server localhost:9092

# Con group + desde inicio
kafka-console-consumer.sh \
  --topic NOMBRE --from-beginning \
  --group mi-grupo --bootstrap-server localhost:9092

# Solo una partición
kafka-console-consumer.sh \
  --topic NOMBRE --partition 0 --from-beginning \
  --bootstrap-server localhost:9092

# Mostrar keys + timestamps
kafka-console-consumer.sh \
  --topic NOMBRE --from-beginning \
  --property print.key=true \
  --property print.timestamp=true \
  --bootstrap-server localhost:9092
```

### Consumer Groups

```bash
# Listar grupos
kafka-consumer-groups.sh --list --bootstrap-server localhost:9092

# Ver lag detallado
kafka-consumer-groups.sh \
  --describe --group GRUPO --bootstrap-server localhost:9092

# Reset al inicio
kafka-consumer-groups.sh \
  --reset-offsets --to-earliest --execute \
  --group GRUPO --topic TOPIC --bootstrap-server localhost:9092

# Reset al final
kafka-consumer-groups.sh \
  --reset-offsets --to-latest --execute \
  --group GRUPO --topic TOPIC --bootstrap-server localhost:9092

# Dry-run (sin ejecutar)
kafka-consumer-groups.sh \
  --reset-offsets --to-earliest --dry-run \
  --group GRUPO --topic TOPIC --bootstrap-server localhost:9092
```

### Rendimiento

```bash
# Test throughput escritura
kafka-producer-perf-test.sh \
  --topic NOMBRE \
  --num-records 100000 \
  --record-size 1024 \
  --throughput -1 \
  --producer-props bootstrap.servers=localhost:9092

# Test throughput lectura
kafka-consumer-perf-test.sh \
  --topic NOMBRE \
  --messages 100000 \
  --bootstrap-server localhost:9092
```

### Metadata y KRaft

```bash
# Info del broker
kafka-broker-api-versions.sh --bootstrap-server localhost:9092

# Estado del quorum KRaft
kafka-metadata-quorum.sh \
  --bootstrap-server localhost:9092 status

# Logs del controller
docker logs kafka 2>&1 | grep -i "controller\|kraft\|leader"
```

---

> **Versión:** 1.0 · **Kafka:** 4.2.0 KRaft · **Slides:** 22 · **Demos:** 7  
> **Archivos relacionados:** `kafka-lab-tiendamax.html` · `kafka-tiendamax-guion.md`
