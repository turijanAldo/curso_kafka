# Guion — Apache Kafka: Conceptos Básicos para Devs

> **Audiencia:** Desarrolladores (backend, fullstack, data engineers junior/mid)
> **Duración:** ~45–60 min
> **Template visual de referencia:** Slidego "Consultoría Tecnológica"
> **Paleta:** Azul marino `#0D1B2A`, acento cian `#00C2CB`, blanco `#FFFFFF`, gris claro `#E8EEF4`

---

## Distribución de slides por fase

| Fase | Bloque | Slides | Duración |
|------|--------|--------|----------|
| 1 | ¿Qué es y por qué existe Kafka? | 1 – 5 | ~12 min |
| 2 | Conceptos Core | 6 – 15 | ~28 min |
| 3 | Kafka en Acción | 16 – 20 | ~10 min |

---

## GUION PRINCIPAL NARRATIVO

Apache Kafka nació de una pregunta muy concreta que un equipo de LinkedIn se hizo en 2010: ¿cómo movemos cientos de millones de eventos por día entre decenas de sistemas sin que todo explote? La respuesta que encontraron — un log distribuido, persistente y de altísimo rendimiento — terminó convirtiéndose en la columna vertebral de datos de empresas como Netflix, Uber, Airbnb y miles de organizaciones en el mundo.

Antes de Kafka, el modelo dominante era el de sistemas síncronos punto a punto. Un servicio A llama al servicio B, espera la respuesta, y sigue. Funciona bien cuando tienes dos o tres servicios. Cuando tienes veinte, esa red de llamadas directas se convierte en una telaraña imposible de mantener: un servicio caído bloquea a todos los que dependen de él, escalar uno implica escalar los demás, y entender qué pasó hace dos horas requiere revisar logs de diez sistemas distintos.

Kafka rompe ese modelo. No es una base de datos, no es exactamente una cola de mensajes clásica. Es un **log distribuido**: una secuencia ordenada, inmutable y replicada de eventos a la que muchos productores pueden escribir y muchos consumidores pueden leer de forma completamente independiente, a su propio ritmo, incluso reprocesando eventos del pasado.

La unidad fundamental es el **topic**: un canal con nombre al que los productores envían mensajes. Cada topic se divide en **particiones**, que son el mecanismo por el cual Kafka escala horizontalmente y garantiza orden. Dentro de cada partición, cada mensaje tiene un **offset**: un número secuencial inmutable que permite a cualquier consumidor saber exactamente en qué punto del log está y volver a cualquier posición anterior.

Los **producers** escriben mensajes al topic eligiendo a qué partición van (por clave, por round-robin, o con lógica custom). Los **consumers** leen mensajes de las particiones a su propio ritmo, manteniendo su offset de forma independiente. Cuando varios consumers colaboran para procesar el mismo topic en paralelo, se organizan en un **consumer group**: Kafka le asigna particiones distintas a cada miembro del grupo, garantizando que cada mensaje lo procese exactamente un consumer del grupo.

Debajo de todo esto están los **brokers**: los servidores que almacenan las particiones. Un clúster tiene múltiples brokers, y cada partición tiene una réplica líder y réplicas follower distribuidas entre ellos. Si un broker cae, el sistema elige automáticamente un nuevo líder para las particiones afectadas. Desde Kafka 4.x, esto se coordina con **KRaft** — el protocolo de consenso interno de Kafka, que eliminó la dependencia de ZooKeeper y simplificó radicalmente la operación del clúster.

El resultado es un sistema capaz de manejar millones de eventos por segundo con latencias de milisegundos, retención configurable de días o semanas, y reprocessing de eventos históricos sin afectar a los productores. Por eso Kafka aparece en casos tan distintos como pipelines de datos en tiempo real, event sourcing en microservicios, integración de sistemas legacy, métricas y telemetría, y detección de fraude.

El punto de entrada más natural para un dev es simple: un topic, un producer que escribe, un consumer que lee. Desde ahí, el sistema crece en complejidad solo cuando el problema lo requiere.

---

## DISECCIÓN POR SLIDE

---

### FASE 1 — ¿QUÉ ES Y POR QUÉ EXISTE KAFKA?
> *"El problema antes de la solución."*
> Duración: ~12 min | Slides 1–5

---

#### Slide 1 — Portada

**Layout:** Portada de impacto — fondo oscuro `#0D1B2A`, título centrado, logo Kafka derecha.

**Título principal:**
> Apache Kafka
> Conceptos Básicos para Devs

**Subtítulo:**
> Del log distribuido a la arquitectura event-driven

**Elementos visuales:**
- Logo oficial Apache Kafka (blanco) alineado a la derecha
- Badge inferior: `Kafka 4.x · KRaft · Sin ZooKeeper`
- Línea decorativa cian horizontal bajo el título

**Notas del instructor:**
> No leas la portada. Empieza con una pregunta: *"¿Alguien aquí ha tenido un microservicio que se llama a sí mismo en cadena y uno solo revienta toda la operación?"* Espera respuestas. Eso es lo que vamos a resolver hoy.

---

#### Slide 2 — El mundo sin Kafka

**Layout:** Diagrama central + bullets laterales. Fondo oscuro.

**Título:** `El problema: sistemas síncronos acoplados`

**Diagrama:**
```
Cliente
   │
   ▼  POST /evento
┌─────────┐
│Servicio A│──────► Servicio B  ✓
└─────────┘──────► Servicio C  ❌ (CAÍDO)
           ──────► Servicio D  ✓
```

**Bullets (revelar uno a uno):**
- 1 servicio caído → toda la operación bloqueada
- Escalar A implica escalar B, C y D
- Sin historial de eventos → debugging ciego
- Cada integración nueva = código custom en A

**Pregunta al aula:**
> "¿Qué pasa si el Servicio C tarda 10 segundos en responder durante pico de tráfico?"

**Notas del instructor:**
> Deja que respondan. La respuesta correcta que buscas: *"el usuario espera 10 segundos aunque B y D estén instantáneos"*. El acoplamiento síncrono hace que el sistema sea tan lento como su eslabón más débil.

---

#### Slide 3 — La evolución: de colas a streams

**Layout:** Línea de tiempo horizontal. Tres columnas.

**Título:** `30 años de mensajería: hacia el streaming`

**Timeline:**

| Época | Paradigma | Ejemplo | Problema |
|-------|-----------|---------|----------|
| 1990s | Message Brokers | IBM MQ, ActiveMQ | Punto a punto, difícil de escalar |
| 2000s | ESB / SOA | Oracle Service Bus | Monolito de integración |
| 2011→ | Log distribuido | **Apache Kafka** | Escala, persistencia, replay |

**Frase de anclaje:**
> "Kafka no es una cola. Es un **cuaderno de registro inmutable** que todos pueden leer."

**Notas del instructor:**
> El insight clave: en una cola tradicional, cuando consumes el mensaje, desaparece. En Kafka, el mensaje queda. Otro consumer puede leerlo después. Puedes volver a procesarlo. Eso cambia todo.

---

#### Slide 4 — ¿Qué es Apache Kafka?

**Layout:** Definición destacada + 4 características en grid 2x2.

**Título:** `Kafka en una oración`

**Definición (bloque destacado cian):**
> "Un **log distribuido, particionado y replicado** al que muchos productores escriben y muchos consumidores leen a su propio ritmo."

**Grid 4 características:**

| | |
|---|---|
| **Alta disponibilidad** — Replicación entre brokers | **Alto rendimiento** — Millones de eventos/seg |
| **Persistencia** — Retención configurable (días, semanas) | **Replay** — Reprocesa eventos del pasado |

**Badge de datos:**
- `LinkedIn → 2011 → Apache Software Foundation`
- `Java/Scala · Open Source · CNCF Graduated`

**Notas del instructor:**
> Subraya "a su propio ritmo". Es la diferencia fundamental con RabbitMQ o SQS: el consumidor controla cuándo avanza. El broker no empuja mensajes — el consumer los jala.

---

#### Slide 5 — Historia y origen

**Layout:** Storytelling — icono timeline vertical izquierda, texto derecha.

**Título:** `De LinkedIn a la industria global`

**Hitos:**
- **2010** — Jay Kreps, Neha Narkhede y Jun Rao lo crean en LinkedIn para mover 1 billón de eventos/día
- **2011** — Donado a Apache Software Foundation
- **2014** — Confluent fundada por los creadores originales
- **2017** — Kafka Streams y Connect maduran
- **2022** — KRaft llega a producción (bye ZooKeeper)
- **2024** — Kafka 4.x: KRaft-only, mejoras de rendimiento masivas

**Dato de impacto (bloque):**
> `+80% de Fortune 100` usan Apache Kafka en producción

**Notas del instructor:**
> El contexto importa: Kafka no nació como producto comercial ni como proyecto académico. Nació para resolver un problema real de escala brutal en producción. Por eso es pragmático.

---

### FASE 2 — CONCEPTOS CORE
> *"Los bloques con los que está construido Kafka."*
> Duración: ~28 min | Slides 6–15

---

#### Slide 6 — Arquitectura general (vista de helicóptero)

**Layout:** Diagrama full-width. Fondo oscuro.

**Título:** `Kafka: vista de alto nivel`

**Diagrama:**
```
┌─────────────┐        ┌──────────────────────────────────┐        ┌─────────────────┐
│  Producers  │──────► │           Kafka Cluster           │──────► │    Consumers    │
│             │        │  ┌────────┐ ┌────────┐ ┌───────┐ │        │                 │
│ App A       │        │  │Broker 1│ │Broker 2│ │Broker3│ │        │ App X           │
│ App B       │        │  └────────┘ └────────┘ └───────┘ │        │ App Y           │
│ App C       │        │         Topics / Particiones       │        │ App Z           │
└─────────────┘        └──────────────────────────────────┘        └─────────────────┘
```

**Tres conceptos que se ampliarán:**
1. **Topic** — el canal con nombre
2. **Partición** — la unidad de paralelismo
3. **Offset** — la posición de cada mensaje

**Notas del instructor:**
> Este slide es el mapa del viaje. Vas a volver a él al inicio de cada bloque siguiente. Deja que se quede en la cabeza: producers a la izquierda, cluster en el medio, consumers a la derecha.

---

#### Slide 7 — El Log Distribuido: la idea central

**Layout:** Diagrama secuencial + texto explicativo.

**Título:** `La idea que lo cambia todo: el log`

**Diagrama — partición como log:**
```
Partición 0
┌──────┬──────┬──────┬──────┬──────┬──────┐
│  0   │  1   │  2   │  3   │  4   │  5   │ ◄── Escritura siempre al final
└──────┴──────┴──────┴──────┴──────┴──────┘
         ▲              ▲
   Consumer A       Consumer B
   (offset: 1)     (offset: 3)
```

**Propiedades del log:**
- **Inmutable** — los mensajes no se modifican ni se borran al leer
- **Ordenado** — los offsets son secuenciales dentro de la partición
- **Persistente** — retención configurable independientemente de si alguien leyó
- **Replicado** — cada partición vive en múltiples brokers

**Notas del instructor:**
> Esta es la idea central de todo Kafka. Si entienden esto, entienden todo lo demás. El consumer A puede estar en el offset 1 mientras el consumer B está en el 3 — son completamente independientes.

---

#### Slide 8 — Topics

**Layout:** Diagrama + definición + bullets.

**Título:** `Topic: el canal con nombre`

**Definición:**
> "Un topic es una categoría lógica a la que los producers publican mensajes y de la que los consumers leen."

**Diagrama:**
```
Producer ──► topic: "pedidos"      ──► Consumer(s)
Producer ──► topic: "pagos"        ──► Consumer(s)
Producer ──► topic: "inventario"   ──► Consumer(s)
```

**Características clave:**
- Nombre único en el clúster (`kebab-case` por convención: `user-events`, `order-created`)
- Configurable: número de particiones, factor de replicación, retención
- Un producer puede escribir a múltiples topics
- Un consumer puede leer de múltiples topics

**Analogía:**
> "Un topic es como un canal de YouTube: muchos pueden publicar en él y muchos pueden verlo, sin que unos afecten a los otros."

**Notas del instructor:**
> Nombrar topics bien importa en producción. Convención recomendada: `<dominio>.<entidad>.<evento>` — por ejemplo `ecommerce.order.created`.

---

#### Slide 9 — Particiones

**Layout:** Diagrama multi-partición + explicación de beneficios.

**Título:** `Particiones: cómo Kafka escala`

**Diagrama:**
```
topic: "pedidos"  (3 particiones)

Partición 0:  [0][1][2][3][4]...
Partición 1:  [0][1][2][3]...
Partición 2:  [0][1][2][3][4][5]...
```

**Puntos clave:**
- El **orden solo se garantiza dentro de una partición** (no entre particiones)
- Más particiones = más paralelismo de lectura/escritura
- El número de particiones define el máximo de consumers en paralelo en un grupo
- La clave del mensaje determina a qué partición va (misma clave → siempre misma partición)

**Regla de oro (bloque destacado):**
> "Si necesitas orden global → 1 partición. Si necesitas escala → más particiones, orden por clave."

**Notas del instructor:**
> El error más común de principiantes: esperar orden global. Kafka garantiza orden por partición. Si quieres que todos los pedidos del mismo cliente vayan en orden, usa el `clienteId` como clave — siempre irán a la misma partición.

---

#### Slide 10 — Offsets

**Layout:** Diagrama + tabla de conceptos.

**Título:** `Offset: el puntero de posición`

**Diagrama:**
```
Partición 0:
┌─────┬─────┬─────┬─────┬─────┬─────┐
│  0  │  1  │  2  │  3  │  4  │  5  │
└─────┴─────┴─────┴─────┴─────┴─────┘
                     ▲
             committed offset
             (Consumer procesó hasta aquí)
```

**Tipos de offset:**
| Offset | Descripción |
|--------|-------------|
| **Log-end offset** | Último mensaje escrito en la partición |
| **Current offset** | Posición actual del consumer |
| **Committed offset** | Última posición confirmada como procesada |
| **Consumer lag** | Log-end − Committed (mensajes pendientes) |

**Poder del offset:**
- Retroceder y reprocesar eventos históricos
- Múltiples consumers independientes con sus propios offsets
- Recovery automático: el consumer retoma desde el último committed

**Notas del instructor:**
> El lag es tu métrica de salud más importante. Un consumer con lag creciente significa que no puede procesar a la velocidad que el producer escribe. Alertar sobre lag, no sobre throughput.

---

#### Slide 11 — Producers

**Layout:** Diagrama de flujo + configuración clave.

**Título:** `Producer: quien escribe en Kafka`

**Diagrama:**
```
Producer
   │
   ├─ Serializa mensaje (Key + Value)
   ├─ Selecciona partición (por clave / round-robin / custom)
   ├─ Envía al broker líder de esa partición
   └─ Recibe ACK
```

**Configuraciones críticas:**
| Config | Valores | Efecto |
|--------|---------|--------|
| `acks` | `0`, `1`, `all` | Durabilidad del mensaje |
| `retries` | número | Reintentos ante fallo |
| `batch.size` | bytes | Agrupación para throughput |
| `linger.ms` | ms | Espera antes de enviar batch |

**Semánticas de entrega:**
- `acks=0` → Fire and forget (rápido, puede perder)
- `acks=1` → Líder confirma (balance)
- `acks=all` → Todas las réplicas confirman (seguro)

**Notas del instructor:**
> Para producción crítica: `acks=all` + `retries>0` + `enable.idempotence=true`. Eso te da exactly-once delivery del lado del producer.

---

#### Slide 12 — Consumers

**Layout:** Diagrama de flujo + modelo pull.

**Título:** `Consumer: quien lee de Kafka`

**Diagrama:**
```
Kafka Broker
    │
    │  ◄── Consumer hace PULL
    ▼
Consumer
   ├─ Deserializa mensaje
   ├─ Procesa lógica de negocio
   └─ Hace commit del offset
```

**Modelo Pull (diferencia clave):**
> "El consumer **jala** mensajes del broker. El broker **no empuja** al consumer."

**Ventajas del modelo pull:**
- El consumer controla su velocidad de consumo
- Sin riesgo de overwhelm (back-pressure natural)
- Puede hacer batch reads según su capacidad

**Config clave:**
- `auto.offset.reset`: `earliest` (desde el inicio) o `latest` (solo nuevos)
- `enable.auto.commit`: `true/false` (autocommit vs commit manual)
- `max.poll.records`: máximo de mensajes por poll

**Notas del instructor:**
> La diferencia pull vs push es profunda. RabbitMQ empuja. Kafka espera. Eso significa que si tu consumer se cae y vuelve, retoma exactamente donde estaba — sin perder nada, sin duplicar (si haces commit bien).

---

#### Slide 13 — Consumer Groups

**Layout:** Diagrama de distribución de particiones.

**Título:** `Consumer Groups: paralelismo de lectura`

**Diagrama:**
```
topic: "pedidos" — 4 particiones

Group A (3 consumers):
  Consumer 1 ──► Partición 0, Partición 1
  Consumer 2 ──► Partición 2
  Consumer 3 ──► Partición 3

Group B (1 consumer):
  Consumer B ──► Partición 0, 1, 2, 3  (todo el topic)
```

**Reglas del consumer group:**
- Cada partición es asignada a **exactamente 1 consumer** del grupo
- Consumers en distinto grupo reciben **todos** los mensajes (independientes)
- `consumers > partitions` → los extras quedan en standby
- Si un consumer cae → rebalanceo automático

**Caso de uso:**
> Group A = procesamiento en paralelo (microservicio escalado)
> Group B = analytics (consume todo independientemente)

**Notas del instructor:**
> Este mecanismo es lo que permite tener un microservicio con 4 instancias procesando en paralelo SIN duplicar trabajo. Kafka se encarga del reparto. Tú solo escalas las instancias.

---

#### Slide 14 — Brokers y Clúster

**Layout:** Diagrama de clúster + roles.

**Título:** `Brokers: los servidores de Kafka`

**Diagrama:**
```
Kafka Cluster
┌──────────────────────────────────────────┐
│  Broker 1 (Controller)                   │
│  ┌─────────────┐  ┌─────────────┐        │
│  │ topic-A P0  │  │ topic-B P1  │        │
│  │  (Líder)    │  │  (Follower) │        │
│  └─────────────┘  └─────────────┘        │
│                                          │
│  Broker 2          Broker 3              │
│  ┌─────────────┐  ┌─────────────┐        │
│  │ topic-A P0  │  │ topic-B P1  │        │
│  │  (Follower) │  │  (Líder)    │        │
│  └─────────────┘  └─────────────┘        │
└──────────────────────────────────────────┘
```

**Conceptos clave:**
- **Broker** — servidor que almacena particiones y atiende producers/consumers
- **Controller** — broker que coordina el clúster (elecciones de líderes)
- **Líder de partición** — recibe escrituras y lecturas para esa partición
- **Follower** — réplica pasiva, toma el liderazgo si el líder cae

**Notas del instructor:**
> Un clúster mínimo de producción: 3 brokers. Eso permite perder 1 y seguir con quórum. El factor de replicación 3 es el estándar de la industria.

---

#### Slide 15 — KRaft: Kafka sin ZooKeeper

**Layout:** Comparación antes/después.

**Título:** `KRaft — La arquitectura moderna de Kafka`

**Comparación:**

**Antes (Kafka < 3.x con ZooKeeper):**
```
Kafka Cluster + ZooKeeper Cluster
   ├─ 3+ Kafka brokers
   └─ 3+ ZooKeeper nodes (proceso separado)
       Dos sistemas que operar, monitorear y actualizar
```

**Ahora (Kafka 4.x con KRaft):**
```
Kafka Cluster (solo)
   └─ 3+ Kafka brokers (con KRaft controller integrado)
       Un solo sistema. Sin dependencias externas.
```

**¿Qué es KRaft?**
> Kafka Raft Metadata Protocol — consenso distribuido embebido en los propios brokers, usando el algoritmo Raft.

**Beneficios:**
- Arranque y shutdown 10x más rápido
- Soporte para millones de particiones por clúster
- Operación simplificada radicalmente
- `apache/kafka:4.2.0` — un solo contenedor para empezar

**Notas del instructor:**
> Si alguien tiene tutoriales viejos con ZooKeeper: obsoletos para Kafka 4.x. En el curso usamos `apache/kafka:4.2.0` y arrancamos con un solo `docker run`. Sin configuración extra.

---

### FASE 3 — KAFKA EN ACCIÓN
> *"¿Cuándo, dónde y cómo se usa en la industria?"*
> Duración: ~10 min | Slides 16–20

---

#### Slide 16 — Casos de uso reales

**Layout:** Grid 2x3 con iconos. Fondo claro con cards oscuras.

**Título:** `¿Dónde vive Kafka en producción?`

**6 casos de uso:**

| Caso | Descripción | Empresa ejemplo |
|------|-------------|-----------------|
| **Event Sourcing** | Los microservicios se comunican via eventos, no llamadas directas | Uber, Airbnb |
| **Pipelines de datos** | Mover datos en tiempo real entre sistemas (DB → Data Lake) | LinkedIn, Twitter |
| **Métricas y telemetría** | Ingestar logs, métricas, traces de miles de servicios | Netflix, Cloudflare |
| **Detección de fraude** | Analizar streams de transacciones en tiempo real | Visa, PayPal |
| **Procesamiento IoT** | Ingestar millones de eventos de sensores/dispositivos | Tesla, GE |
| **CDC (Change Data Capture)** | Capturar cambios en BD y propagarlos al ecosistema | Debezium + cualquier RDBMS |

**Notas del instructor:**
> Pregunta al aula cuál de estos casos se parece más a su empresa. Normalmente el de event sourcing o el de pipelines de datos resuena más en devs backend.

---

#### Slide 17 — Kafka vs otras herramientas

**Layout:** Tabla comparativa. Fondo oscuro.

**Título:** `¿Cuándo Kafka y cuándo no?`

| | **Kafka** | **RabbitMQ** | **Amazon SQS** | **Redis Pub/Sub** |
|---|---|---|---|---|
| **Modelo** | Log distribuido | Cola de mensajes | Cola gestionada | Pub/Sub en memoria |
| **Retención** | Configurable (días/semanas) | Hasta que se consume | Hasta 14 días | No (fire and forget) |
| **Replay** | ✅ Sí | ❌ No | ❌ No | ❌ No |
| **Throughput** | Millones/seg | Miles/seg | Miles/seg | Millones/seg (sin durabilidad) |
| **Complejidad** | Alta | Media | Baja | Baja |
| **Mejor para** | Streams, alta escala, replay | Workflows, tasks | Colas simples en AWS | Notificaciones en tiempo real |

**Cuándo NO usar Kafka:**
- Necesitas solo colas simples de trabajo (usa RabbitMQ o SQS)
- El sistema no justifica la complejidad operacional
- Mensajes <1000/día (overkill)

**Notas del instructor:**
> Kafka no es la solución a todo. Es una herramienta poderosa con complejidad operacional real. Si no necesitas replay, alta escala o múltiples consumers independientes, SQS o RabbitMQ pueden ser más simples.

---

#### Slide 18 — El ecosistema Kafka

**Layout:** Mapa de ecosistema. Diagrama en capas.

**Título:** `Kafka no está solo: el ecosistema`

**Diagrama en capas:**
```
┌─────────────────────────────────────────────────────┐
│                   APLICACIONES                       │
│     Microservicios · Data Pipelines · Analytics      │
└─────────────────────────────────────────────────────┘
┌───────────────────┐   ┌───────────────────────────┐
│  Kafka Connect    │   │    Kafka Streams /         │
│  (integración)    │   │    Apache Flink            │
│  Source/Sink      │   │    (procesamiento)         │
└───────────────────┘   └───────────────────────────┘
┌─────────────────────────────────────────────────────┐
│                 APACHE KAFKA CORE                    │
│         Brokers · Topics · KRaft                     │
└─────────────────────────────────────────────────────┘
┌─────────────────────────────────────────────────────┐
│              SCHEMA REGISTRY                         │
│           (Avro · Protobuf · JSON Schema)            │
└─────────────────────────────────────────────────────┘
```

**Componentes clave:**
- **Kafka Connect** — conectores listos para integrar 200+ fuentes/destinos (DB, S3, Elasticsearch...)
- **Kafka Streams** — librería Java para procesar streams directamente
- **Schema Registry** — validación y evolución de esquemas de mensajes
- **ksqlDB** — SQL sobre streams de Kafka

**Notas del instructor:**
> El núcleo es Kafka. El ecosistema multiplica el valor. En el 80% de proyectos reales verás Kafka + Connect + Schema Registry como mínimo.

---

#### Slide 19 — Por dónde empezar

**Layout:** Checklist / roadmap visual. 3 columnas: Día 1, Semana 1, Mes 1.

**Título:** `Tu ruta de aprendizaje`

**Día 1 — Manos en la masa:**
```
docker run -p 9092:9092 apache/kafka:4.2.0

# Crear topic
kafka-topics.sh --create --topic mi-primer-topic \
  --bootstrap-server localhost:9092

# Producir
kafka-console-producer.sh --topic mi-primer-topic \
  --bootstrap-server localhost:9092

# Consumir
kafka-console-consumer.sh --topic mi-primer-topic \
  --from-beginning --bootstrap-server localhost:9092
```

**Semana 1:**
- Producer y Consumer en Java/Python con cliente oficial
- Consumer Groups
- Manejo de offsets y commit manual

**Mes 1:**
- Kafka Connect (mover datos desde/hacia BD)
- Particionado estratégico
- Monitoreo: lag, throughput, latencia

**Notas del instructor:**
> El comando de docker es todo lo que necesitan para arrancar hoy. KRaft en un solo contenedor, sin ZooKeeper, sin configuración. Producción en 30 segundos.

---

#### Slide 20 — Cierre

**Layout:** Portada de cierre — fondo oscuro, resumen visual.

**Título:** `Lo que llevas hoy`

**Resumen de conceptos (6 bloques):**

```
┌─────────────┐  ┌─────────────┐  ┌─────────────┐
│    Topic    │  │ Partición   │  │   Offset    │
│  canal con  │  │  unidad de  │  │  posición   │
│   nombre    │  │ paralelismo │  │ del consumer│
└─────────────┘  └─────────────┘  └─────────────┘
┌─────────────┐  ┌─────────────┐  ┌─────────────┐
│  Producer   │  │  Consumer   │  │   KRaft     │
│  escribe al │  │   lee a su  │  │ sin ZooKeep │
│   broker    │  │ propio ritmo│  │     er      │
└─────────────┘  └─────────────┘  └─────────────┘
```

**Frase de cierre:**
> "Kafka no es una cola. Es la memoria de tu sistema distribuido."

**Próximos pasos (QR / link):**
- Documentación oficial: `kafka.apache.org`
- Imagen Docker: `apache/kafka:4.2.0`
- Quickstart: 5 comandos en terminal

**Notas del instructor:**
> Cierra con energía. Pregunta: "¿Quién tiene un sistema con el problema del Slide 2 en su empresa ahora mismo?" Eso conecta la teoría con su realidad y deja la conversación abierta.

---

## NOTAS DE PRODUCCIÓN (para armar las slides)

**Template Slidego "Consultoría Tecnológica" — aplicación:**
- Fondo principal: `#0D1B2A` (azul marino profundo)
- Acento principal: `#00C2CB` (cian tecnológico)
- Texto principal: `#FFFFFF`
- Texto secundario: `#B0BEC5`
- Cards / contenedores: `#1A2F45`
- Fuente títulos: **Poppins Bold** o equivalente sans-serif geométrico
- Fuente cuerpo: **Inter Regular**
- Iconografía: línea fina, estilo flat tech (Feather Icons / Phosphor)
- Separadores de sección: línea cian + número de fase en grande y translúcido

**Slides de sección (entre fases):**
Insertar 1 slide divisor antes de cada fase con:
- Número de fase grande (`01`, `02`, `03`) en cian translúcido al fondo
- Nombre de la fase centrado
- Subtítulo descriptor
