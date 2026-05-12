# 🎬 Guion de slides — Curso "Kafka con TiendaMax"

**Audiencia:** devs backend que no han tocado Kafka, o lo han usado solo como "cola de mensajes".
**Duración total:** ~2h 30min (90 min teoría + demos + 60 min hands-on guiado).
**Pre-requisito:** Docker corriendo, Java 17, IDE.
**Material de soporte:** [`kafka-lab-tiendamax.html`](kafka-lab-tiendamax.html) (lab anotado) + [`kafka-tiendamax-slides.html`](kafka-tiendamax-slides.html) (deck Reveal.js).

---

## Estructura general

| Bloque | Tema | Duración | Slides |
|--------|------|----------|--------|
| 1 | El problema | 10 min | 1-3 |
| 2 | Fundamentos mínimos | 15 min | 4-7 |
| 3 | Paso 0 — Hola Kafka (CLI) 🎥 | 15 min | 8-10 |
| 4 | Paso 1 — Infra & topic real | 10 min | 11-12 |
| 5 | Paso 2 — Producer Java 🎥 | 15 min | 13-15 |
| 6 | Paso 3 — Consumer + commit 🎥 | 15 min | 16-17 |
| 7 | Paso 4 — Consumer group 🎥 | 15 min | 18 |
| 8 | Paso 5 — Fallo y rebalanceo 🎥 | 15 min | 19 |
| 9 | Paso 6 — Lag y reset 🎥 | 10 min | 20-21 |
| 10 | Cierre | 5 min | 22 |

---

## Bloque 1 — El problema (10 min)

### Slide 1 — Portada
- **Título:** *De API síncrona a streaming: el caso TiendaMax*
- **Subtítulo:** "Por qué Kafka, paso a paso, con código real"
- Tu nombre / fecha.

**Notas del instructor:**
> Arranca con energía. No leas la portada. Di algo como "Hoy vamos a romper una arquitectura y reconstruirla con Kafka. Cero PowerPoint infinito, mucho código."

### Slide 2 — El problema (5 min)
- "TiendaMax procesa pedidos vía REST síncrona."
- Diagrama: `Cliente → API → [Inventario | Notif | Factura]` con una ❌ roja sobre una de las flechas.
- **Pregunta al aula:** "¿Qué pasa si Notificaciones se cae 5 minutos?"
- Cierre: **el pedido se pierde**.

**Notas del instructor:**
> Espera respuestas reales del aula antes de pasar. La mayoría dirá "se reintenta" o "se mete en una cola". Aquí entra el gancho: "¿y si esa cola tampoco existe?".

### Slide 3 — Qué queremos lograr (3 min)
- Ningún pedido se pierde aunque un servicio caiga.
- Cada servicio escala independiente.
- Trazabilidad en tiempo real.

**Frase de transición:** "Lo vamos a construir hoy. Sin slides infinitas — código real."

---

## Bloque 2 — Fundamentos mínimos (15 min)

### Slide 4 — Qué es Kafka en una frase
> "Un **log distribuido, particionado y replicado** al que muchos productores escriben y muchos consumidores leen a su propio ritmo."

**Notas del instructor:**
> Subraya **"a su propio ritmo"**. Es la diferencia con RabbitMQ/SQS. No es una cola — es un cuaderno que cada lector recorre donde quiere.

### Slide 5 — Topic, partición, offset
- Diagrama: 1 topic → 3 particiones → cada una con offsets 0,1,2,3…
- **Idea fuerte:** el orden solo se garantiza **dentro de una partición**.

**Notas del instructor:**
> Si solo se llevan UNA cosa del bloque 2, que sea esta. Repítelo: "orden por partición, no por topic".

### Slide 6 — Producer, Consumer, Consumer Group
- Producer escribe → topic → consumers del mismo group se reparten particiones.
- "Más consumers que particiones = los extras quedan en standby."

### Slide 7 — KRaft en una línea
- Kafka 4.x: adiós ZooKeeper.
- Hoy todo lo corres con `apache/kafka:4.2.0`, un solo contenedor.

**Notas del instructor:**
> Si alguien menciona ZooKeeper de un tutorial viejo, recuérdales que en 4.x ya no existe. KRaft = Raft consensus dentro del propio broker.

---

## Bloque 3 — Paso 0: Hola Kafka sin código (15 min) 🎥 DEMO

### Slide 8 — Objetivo del Paso 0
- "En 60 segundos verás un mensaje entrar y salir de Kafka. Sin Java. Sin Maven."
- Cheat sheet de los 4 comandos:

| Comando | Para qué |
|---------|----------|
| `kafka-topics.sh` | Crear, listar, describir, borrar topics |
| `kafka-console-producer.sh` | Enviar mensajes a mano |
| `kafka-console-consumer.sh` | Leer mensajes en tiempo real |
| `kafka-consumer-groups.sh` | Ver lag y resetear offsets |

### Slide 9 — Demo en vivo (no slide, pantalla compartida)

```bash
docker exec -it kafka bash
export PATH=/opt/kafka/bin:$PATH

# Crear topic desechable
kafka-topics.sh --create --topic hola-kafka \
  --partitions 1 --replication-factor 1 \
  --bootstrap-server localhost:9092

# Terminal A — producer
kafka-console-producer.sh --topic hola-kafka \
  --bootstrap-server localhost:9092

# Terminal B — consumer
kafka-console-consumer.sh --topic hola-kafka \
  --from-beginning --bootstrap-server localhost:9092
```

**Punto clave:** `--from-beginning` vs sin él. Demuestra ambos casos.

**Notas del instructor:**
> Antes de la demo, abre las 2 terminales y `export PATH` en ambas. Si tropiezas con `command not found` arruinas el momento.

### Slide 10 — Lectura del `--describe`
```
Topic: hola-kafka  PartitionCount: 1  ReplicationFactor: 1
  Topic: hola-kafka  Partition: 0  Leader: 1  Replicas: 1  Isr: 1
```
- Anota qué significa cada columna.
- **"Esto se va a volver importante en el Paso 5."**

---

## Bloque 4 — Paso 1: Infra & topic real (10 min)

### Slide 11 — `docker-compose.yml` anotado
Las 4 variables críticas:

| Variable | Por qué |
|----------|---------|
| `PROCESS_ROLES=broker,controller` | Este nodo es broker Y controller. En prod se separan |
| `ADVERTISED_LISTENERS` | Dirección que Kafka anuncia. En prod va el hostname público |
| `CONTROLLER_QUORUM_VOTERS` | Quién vota en elecciones Raft. En prod mínimo 3 |
| `OFFSETS_TOPIC_REPLICATION_FACTOR=1` | **DEV ONLY**. En prod = 3 |

### Slide 12 — Topic `pedidos-tiendamax`
```bash
kafka-topics.sh --create \
  --topic pedidos-tiendamax \
  --partitions 3 \
  --replication-factor 1 \
  --config retention.ms=604800000 \
  --bootstrap-server localhost:9092
```
- `--partitions 3` → "máximo 3 consumers en paralelo. Decidimos 3 porque tenemos 3 servicios."
- `--retention.ms=604800000` → "7 días. Inventario puede caer 3 días sin perder un pedido."

---

## Bloque 5 — Paso 2: Producer Java (15 min) 🎥 DEMO

### Slide 13 — Anatomía de un Producer
Configuración mínima:
- `bootstrap.servers`
- `key.serializer` / `value.serializer`
- `acks`

```java
Properties props = new Properties();
props.put("bootstrap.servers",  "localhost:9092");
props.put("key.serializer",     StringSerializer.class.getName());
props.put("value.serializer",   StringSerializer.class.getName());
props.put("acks",               "all");
props.put("enable.idempotence", "true");
```

### Slide 14 — El "porqué" de `acks=all`

| acks | Quién confirma | Riesgo |
|------|----------------|--------|
| `0` | Nadie (fire & forget) | Pérdida silenciosa |
| `1` | Solo el leader | Si el leader cae antes de replicar → pérdida |
| `all` | Todas las ISR | Cero pérdida (con RF≥3) |

> "En TiendaMax cada pedido = dinero. `acks=all`, sin excepciones."

### Slide 15 — La key importa
- Mismo `order_id` → siempre misma partición → orden garantizado del ciclo de vida del pedido.
- **Anti-patrón:** usar `cliente_id` → hotspot si "ana" hace el 80% de los pedidos.

**Demo:** correr `PedidoProducer.java`, mostrar distribución en las 3 particiones con `kafka-console-consumer.sh --partition N`.

---

## Bloque 6 — Paso 3: Consumer + commit manual (15 min) 🎥 DEMO

### Slide 16 — `poll()` en una imagen
Loop: `poll → procesar → commit`.

```java
while (running) {
    var records = consumer.poll(Duration.ofMillis(500));
    for (var r : records) {
        procesarPedido(r.value());
    }
    consumer.commitSync();
}
```

- Si commiteas **antes** de procesar = at-most-once (pierdes).
- Si commiteas **después** = at-least-once (puedes duplicar).

### Slide 17 — `commitSync` vs `commitAsync`

| | commitSync | commitAsync |
|---|---|---|
| Bloquea | Sí | No |
| Reintentos | Automáticos | NO reintenta |
| Throughput | Menor | Mayor |
| Cuándo usar | Datos críticos | Métricas, telemetría |

> "En este lab usamos `commitSync`. Lento pero seguro."

**Demo:** levantar el consumer, ver el lag bajar a 0 con `kafka-consumer-groups.sh --describe`.

---

## Bloque 7 — Paso 4: Consumer group y paralelismo (15 min) 🎥 DEMO

### Slide 18 — RangeAssignor
- 3 particiones, 3 consumers → 1:1.
- 3 particiones, 4 consumers → uno queda en standby.

**Demo:** abrir 3 terminales, levantar 3 consumers del mismo group, mostrar `--describe` con 3 owners distintos. Después levanta un 4° y muestra que queda idle.

---

## Bloque 8 — Paso 5: Fallo y rebalanceo (15 min) 🎥 DEMO

### Slide 19 — Anatomía de un rebalanceo
Timeline:
1. `t=0s` — Ctrl+C en consumer-B 💥
2. `t=10s` — group coordinator detecta heartbeat perdido (`session.timeout.ms`)
3. `t=11s` — PAUSE: todos paran de leer
4. `t=12s` — REASSIGN: P0,P2 → consumer-A · P1 → consumer-C
5. `t=13s` — RESUME desde último offset commiteado → **mensajes duplicados**

Mencionar Eager vs Cooperative, no profundizar.

**Demo:** matar 1 consumer con `Ctrl+C`. Ver cómo los otros 2 absorben sus particiones. Resaltar **mensajes duplicados** = at-least-once.

---

## Bloque 9 — Paso 6: Lag y reset de offsets (10 min) 🎥 DEMO

### Slide 20 — Consumer lag
> `LAG = LOG-END-OFFSET - CURRENT-OFFSET`

```
TOPIC               PARTITION  CURRENT  LOG-END  LAG
pedidos-tiendamax   0          1247     1247     0
pedidos-tiendamax   1          1198     1250     52  ← atrasado
pedidos-tiendamax   2          1300     1300     0
```

> "Es la métrica **#1** que vas a alertar en producción."

### Slide 21 — Reset de offsets

| Flag | Efecto | Cuándo |
|------|--------|--------|
| `--to-earliest` | Vuelve al offset 0 | Reprocessing total |
| `--to-latest` | Salta al final | Descartar backlog |
| `--to-datetime` | Va a un timestamp | Bug introducido a las 14:30 |
| `--shift-by N` | Avanza/retrocede N | Saltar N mensajes corruptos |

> **Regla de oro:** "El consumer group debe estar **DETENIDO** para resetear."

**Demo:** generar lag (producer corriendo, consumer apagado), `--describe`, luego `--reset-offsets --to-earliest --execute`.

---

## Bloque 10 — Cierre (5 min)

### Slide 22 — Recap y qué sigue

**✅ Cubierto:**
- Topics y particiones
- Producer con `acks=all`
- Consumer + commit manual
- Consumer groups y paralelismo
- Rebalanceo y at-least-once
- Lag y reset de offsets

**🔜 Siguiente curso:**
- Cluster **multi-broker** + replicación real (ISR, `min.insync.replicas`)
- **Idempotencia + transacciones** (exactly-once)
- **Schema Registry** (Avro / Protobuf)
- **DLQ** y manejo de errores
- Kafka Streams / Connect

Link al repo y al lab HTML para que repitan en casa.

---

## 📋 Notas generales para el instructor

### Antes de empezar
- [ ] `docker compose up -d` corriendo y verificado (`docker compose ps` → healthy).
- [ ] Topic `pedidos-tiendamax` ya creado (si vas a saltar el comando en vivo).
- [ ] 4 terminales abiertas dentro del container con `PATH` configurado.
- [ ] IDE con el proyecto Java abierto, listo para correr.
- [ ] [`kafka-lab-tiendamax.html`](kafka-lab-tiendamax.html) abierto en un tab separado.

### Estilo de la sesión
- **No leas las slides.** Son anclas visuales; el contenido lo cuentas tú.
- **Pasa cada demo desde el lab HTML abierto en pantalla** — los alumnos ven el slide + el código anotado al mismo tiempo.
- **Pregunta detonadora cada bloque:** ya te di una para el Slide 2. Repite el patrón antes de cada demo ("¿qué creen que va a pasar si…?").
- **No comprimas los demos.** Son el 70% del valor. Si te atrasas, comprime el Bloque 2 (fundamentos).

### Errores comunes a anticipar
- Alumnos en Windows: `\` en comandos multilínea no siempre funciona en PowerShell. Recomienda usar el bash del container.
- `kafka-console-consumer.sh` con `--from-beginning` en un topic gigante puede arrancar lento. Usa siempre el topic del lab, no uno con backlog.
- Si un alumno cierra el producer con Ctrl+C antes de Enter, el último mensaje no se envía. Aclara que `Ctrl+D` cierra limpio.

### Tiempos sugeridos para Q&A
- 5 min después del Bloque 2 (dudas conceptuales).
- 10 min después del Bloque 8 (la mayoría de las preguntas reales aparecen aquí).
- Q&A abierto en el cierre.
