# Apache Kafka — Conceptos Básicos para Devs
## Guion de 20 Slides | Template: Slidego "Consultoría Tecnológica"

**Audiencia:** Desarrolladores backend, fullstack, data engineers  
**Template visual:** Slidego "Consultoría Tecnológica"  
**Paleta:** Azul marino #0D1B2A · Cian #00C2CB · Blanco #FFFFFF · Gris #E8EEF4  
**Tipografía:** Poppins Bold (títulos) · Inter Regular (cuerpo)

---

## FORMATO ESTÁNDAR POR SLIDE

Cada slide sigue esta estructura fija:
- **TIPO:** portada | sección | contenido | diagrama | comparación | cierre
- **TÍTULO:** máximo 8 palabras, impactante
- **MENSAJE CLAVE:** 1 frase que resume el slide
- **CONTENIDO:** bullets, tabla o diagrama
- **VISUAL:** descripción del elemento gráfico principal

---

---

## FASE 1 — ¿Qué es y por qué existe Apache Kafka?
### Slides 1 al 5

---

### SLIDE 1 — PORTADA

**TIPO:** Portada  
**TÍTULO:** Apache Kafka: Conceptos Básicos para Devs  
**MENSAJE CLAVE:** Del log distribuido a la arquitectura event-driven  

**CONTENIDO:**
- Subtítulo: "Del log distribuido a la arquitectura event-driven"
- Badge: Kafka 4.x · KRaft · Sin ZooKeeper
- Nombre del instructor | Fecha

**VISUAL:** Logo Apache Kafka centrado sobre fondo azul marino #0D1B2A. Línea decorativa cian horizontal bajo el título. Tipografía blanca. Imagen de fondo: red de nodos conectados, baja opacidad.

---

### SLIDE 2 — EL PROBLEMA: SISTEMAS ACOPLADOS

**TIPO:** Diagrama  
**TÍTULO:** Cuando el REST síncrono duele  
**MENSAJE CLAVE:** Un servicio caído bloquea toda la operación

**CONTENIDO:**
- Diagrama de arquitectura síncrona punto a punto:
  - Cliente → Servicio A → Servicio B ✓
  - Servicio A → Servicio C ❌ CAÍDO
  - Servicio A → Servicio D ✓
- Consecuencias clave:
  - 1 fallo = operación bloqueada completa
  - Escalar A obliga a escalar B, C y D
  - Sin trazabilidad de eventos históricos
  - Debugging ciego ante incidentes

**VISUAL:** Diagrama de flujo con flechas rojas sobre el servicio caído. Nodos en cards cian sobre fondo oscuro. Ícono de alerta sobre el nodo fallido. Estilo: dark tech diagram.

---

### SLIDE 3 — 30 AÑOS DE MENSAJERÍA

**TIPO:** Contenido — Timeline  
**TÍTULO:** La evolución hacia el streaming  
**MENSAJE CLAVE:** Kafka no es una cola, es un cuaderno de registro inmutable

**CONTENIDO:**
| Época | Paradigma | Herramienta | Limitación |
|-------|-----------|-------------|------------|
| 1990s | Message Broker | IBM MQ, ActiveMQ | Punto a punto, no escala |
| 2000s | ESB / SOA | Oracle Service Bus | Monolito de integración |
| 2011+ | Log distribuido | **Apache Kafka** | — La solución |

- Diferencia clave con colas tradicionales: en Kafka el mensaje no desaparece al ser leído
- Cualquier sistema puede leer el mismo evento de forma independiente
- Es posible releer eventos del pasado en cualquier momento

**VISUAL:** Línea de tiempo horizontal con 3 puntos de inflexión. El tercer punto (Kafka) destacado en cian con mayor tamaño. Iconos de cada herramienta alineados debajo de cada hito.

---

### SLIDE 4 — ¿QUÉ ES APACHE KAFKA?

**TIPO:** Contenido — Definición  
**TÍTULO:** Kafka en una oración  
**MENSAJE CLAVE:** Un log distribuido al que muchos escriben y muchos leen a su propio ritmo

**CONTENIDO:**
- **Definición central (bloque destacado):**
  "Un log distribuido, particionado y replicado al que muchos productores escriben y muchos consumidores leen de forma completamente independiente, a su propio ritmo."

- **4 pilares fundamentales:**
  - Alta disponibilidad: replicación automática entre brokers
  - Alto rendimiento: millones de eventos por segundo
  - Persistencia: retención configurable en días o semanas
  - Replay: reprocesa eventos históricos sin afectar productores

- **Origen:** LinkedIn 2011 → Apache Software Foundation → +80% Fortune 100

**VISUAL:** Grid 2×2 con los 4 pilares en cards con íconos. Bloque de definición con fondo cian y texto oscuro destacado arriba. Estilo: infographic tech card layout.

---

### SLIDE 5 — HISTORIA Y ORIGEN

**TIPO:** Contenido — Timeline vertical  
**TÍTULO:** De LinkedIn al estándar de la industria  
**MENSAJE CLAVE:** Kafka nació para resolver un problema real de escala en producción

**CONTENIDO:**
- **2010** — Jay Kreps, Neha Narkhede y Jun Rao crean Kafka en LinkedIn para mover 1 billón de eventos diarios
- **2011** — Donado a Apache Software Foundation. Nace como proyecto open source
- **2014** — Confluent fundada por los creadores originales
- **2017** — Kafka Streams y Kafka Connect maduran. El ecosistema se completa
- **2022** — KRaft llega a producción: adiós ZooKeeper
- **2024** — Kafka 4.x: arquitectura KRaft exclusiva, soporte para millones de particiones

- **Dato de impacto:** Más del 80% de las empresas del Fortune 100 usan Kafka en producción

**VISUAL:** Timeline vertical con íconos de año y descripción lateral. Último hito (2024) destacado en cian. Logos de LinkedIn y Apache en los hitos correspondientes.

---

---

## FASE 2 — Conceptos Core de Apache Kafka
### Slides 6 al 15

---

### SLIDE 6 — ARQUITECTURA GENERAL

**TIPO:** Diagrama — Vista de alto nivel  
**TÍTULO:** Kafka: la vista desde el helicóptero  
**MENSAJE CLAVE:** Producers a la izquierda, Cluster en el centro, Consumers a la derecha

**CONTENIDO:**
- Tres bloques principales:
  - **Producers:** aplicaciones que generan y publican eventos (App A, App B, App C)
  - **Kafka Cluster:** brokers que almacenan y sirven los eventos (Broker 1, Broker 2, Broker 3) con Topics y Particiones dentro
  - **Consumers:** aplicaciones que leen y procesan eventos (App X, App Y, App Z)

- Flujo unidireccional: Producers → Kafka Cluster → Consumers
- Producers y consumers completamente desacoplados: no se conocen entre sí
- Tres conceptos a desarrollar: Topic · Partición · Offset

**VISUAL:** Diagrama de arquitectura full-width en tres columnas. Flechas de izquierda a derecha. Cards de brokers apilados verticalmente en el centro. Estilo: arquitectura enterprise dark.

---

### SLIDE 7 — EL LOG DISTRIBUIDO

**TIPO:** Diagrama — Concepto central  
**TÍTULO:** La idea que cambia todo: el log  
**MENSAJE CLAVE:** El mensaje no desaparece al leerlo — cada consumer avanza a su ritmo

**CONTENIDO:**
- Representación de partición como log secuencial:
  - Posiciones 0 · 1 · 2 · 3 · 4 · 5 (offsets)
  - Consumer A en offset 1 leyendo de forma independiente
  - Consumer B en offset 3 leyendo de forma independiente
  - Escritura siempre al final (append-only)

- **4 propiedades del log:**
  - Inmutable: los mensajes no se modifican ni se borran al leer
  - Ordenado: offsets secuenciales y garantizados dentro de la partición
  - Persistente: retención independiente de quién leyó
  - Replicado: cada partición vive en múltiples brokers simultáneamente

**VISUAL:** Visualización de secuencia de bloques horizontales numerados (0-5) con dos punteros de flecha abajo en posiciones distintas, etiquetados Consumer A y Consumer B. Animación sugerida: bloques aparecen de izquierda a derecha.

---

### SLIDE 8 — TOPICS

**TIPO:** Diagrama  
**TÍTULO:** El canal con nombre: el Topic  
**MENSAJE CLAVE:** Un topic es un canal lógico al que todos pueden publicar y del que todos pueden leer

**CONTENIDO:**
- Definición: canal con nombre único al que producers publican y consumers leen
- Arquitectura multi-topic:
  - Producer → topic: order-created → Consumer(s)
  - Producer → topic: payment-processed → Consumer(s)
  - Producer → topic: inventory-updated → Consumer(s)

- Características clave:
  - Nombre único en el clúster (convención: kebab-case descriptivo)
  - Configurable: particiones, factor de replicación, política de retención
  - Un producer puede publicar en múltiples topics
  - Un consumer puede leer de múltiples topics simultáneamente

- **Convención de nombres recomendada:** dominio.entidad.evento → ecommerce.order.created

**VISUAL:** Diagrama de tres canales paralelos (topics) con flechas de producers a la izquierda y consumers a la derecha. Cada canal con color distinto (variaciones del cian). Labels de nombres de topics reales en fuente monospace.

---

### SLIDE 9 — PARTICIONES

**TIPO:** Diagrama  
**TÍTULO:** Particiones: cómo Kafka escala  
**MENSAJE CLAVE:** El orden solo se garantiza dentro de una partición, no entre ellas

**CONTENIDO:**
- Topic "pedidos" con 3 particiones:
  - Partición 0: mensajes con offsets 0, 1, 2, 3, 4
  - Partición 1: mensajes con offsets 0, 1, 2, 3
  - Partición 2: mensajes con offsets 0, 1, 2, 3, 4, 5

- **Beneficios de las particiones:**
  - Más particiones = más paralelismo de lectura y escritura
  - El número de particiones define el máximo de consumers en paralelo por grupo
  - La clave del mensaje determina siempre la misma partición destino

- **Regla de oro:** Si necesitas orden global → 1 partición. Si necesitas escala → más particiones, orden por clave.

- **Caso práctico:** usar clienteId como clave garantiza que todos los eventos del mismo cliente van a la misma partición, en orden

**VISUAL:** Diagrama de topic dividido en 3 particiones como barras horizontales paralelas. Bloques de mensajes coloreados dentro de cada partición. Flecha de "clave → partición" apuntando a la partición correcta.

---

### SLIDE 10 — OFFSETS

**TIPO:** Contenido — Diagrama + Tabla  
**TÍTULO:** El offset: la memoria del consumer  
**MENSAJE CLAVE:** El consumer lag es la métrica más importante de salud en Kafka

**CONTENIDO:**
- Representación visual de partición con offset marcado:
  - Bloques 0, 1, 2, 3, 4, 5 en secuencia
  - Puntero en posición 3 = committed offset del consumer

- **Tipos de offset:**

| Offset | Descripción |
|--------|-------------|
| Log-end offset | Último mensaje escrito en la partición |
| Current offset | Posición actual de lectura del consumer |
| Committed offset | Última posición confirmada como procesada |
| Consumer lag | Log-end − Committed = mensajes pendientes |

- **Por qué el lag importa:** un lag creciente indica que el consumer no procesa a la velocidad que el producer escribe

**VISUAL:** Representación de barra de progreso con "log-end" al final y "committed offset" como marcador intermedio. La diferencia entre ambos resaltada en rojo como "LAG". Tabla de tipos de offset al lado derecho.

---

### SLIDE 11 — PRODUCERS

**TIPO:** Diagrama + Tabla  
**TÍTULO:** Producer: quien escribe en Kafka  
**MENSAJE CLAVE:** La configuración de acks define el balance entre velocidad y durabilidad

**CONTENIDO:**
- **Flujo del producer:**
  1. Serializa el mensaje (Key + Value → bytes)
  2. Selecciona la partición destino (por clave, round-robin o lógica custom)
  3. Envía al broker líder de esa partición
  4. Recibe confirmación (ACK) según configuración

- **Configuración crítica — acks:**

| acks | Comportamiento | Uso recomendado |
|------|----------------|-----------------|
| 0 | Sin confirmación (fire and forget) | Métricas, logs no críticos |
| 1 | Líder confirma escritura | Balance velocidad/seguridad |
| all | Todas las réplicas confirman | Datos críticos de negocio |

- **Combinación para producción:** acks=all + retries>0 + enable.idempotence=true → exactly-once delivery

**VISUAL:** Diagrama de flujo vertical con 4 pasos numerados del producer al broker. Tabla de acks con código de colores: rojo (0), amarillo (1), verde (all). Estilo: flow diagram dark tech.

---

### SLIDE 12 — CONSUMERS

**TIPO:** Diagrama  
**TÍTULO:** Consumer: quien lee de Kafka  
**MENSAJE CLAVE:** El consumer jala mensajes del broker — el broker nunca empuja

**CONTENIDO:**
- **Modelo pull vs push:**
  - Pull (Kafka): el consumer solicita mensajes cuando está listo para procesarlos
  - Push (RabbitMQ/SQS): el broker empuja mensajes al consumer sin control de ritmo
  - Ventaja del pull: el consumer controla su velocidad, no hay riesgo de saturación

- **Ciclo de vida del consumer:**
  1. Hace poll al broker (solicita mensajes)
  2. Deserializa los mensajes recibidos
  3. Ejecuta la lógica de negocio
  4. Hace commit del offset (confirma procesamiento exitoso)
  5. Si cae antes del commit: retoma desde el último committed al reiniciar

- **Configuraciones clave:**
  - auto.offset.reset: earliest (desde el inicio) | latest (solo nuevos)
  - enable.auto.commit: false recomendado para control manual
  - max.poll.records: máximo de mensajes por ciclo de poll

**VISUAL:** Diagrama de ciclo con 4 pasos en círculo. Flecha de PULL desde el consumer hacia el broker (dirección inversa a lo intuitivo). Comparación pull vs push en cards lado a lado.

---

### SLIDE 13 — CONSUMER GROUPS

**TIPO:** Diagrama  
**TÍTULO:** Consumer Groups: paralelismo de lectura  
**MENSAJE CLAVE:** Kafka reparte las particiones automáticamente entre los consumers del grupo

**CONTENIDO:**
- **Escenario:** topic "pedidos" con 4 particiones

- **Group A — procesamiento paralelo (3 consumers):**
  - Consumer 1 procesa Partición 0 y Partición 1
  - Consumer 2 procesa Partición 2
  - Consumer 3 procesa Partición 3

- **Group B — analytics independiente (1 consumer):**
  - Consumer B procesa todas las particiones (0, 1, 2, 3)
  - No interfiere con Group A — son completamente independientes

- **Reglas del consumer group:**
  - Cada partición asignada a exactamente 1 consumer del grupo
  - consumers > particiones → los extras quedan en standby
  - Consumer caído → rebalanceo automático en segundos
  - Escalar = añadir más instancias del consumer al grupo

**VISUAL:** Diagrama que muestra el mismo topic dividido en 4 particiones distribuyéndose entre dos grupos distintos. Group A con 3 nodos, Group B con 1 nodo. Líneas de asignación claras con colores distintos por grupo.

---

### SLIDE 14 — BROKERS Y CLÚSTER

**TIPO:** Diagrama  
**TÍTULO:** Brokers: los servidores del clúster  
**MENSAJE CLAVE:** Con factor de replicación 3, el clúster tolera la pérdida de 1 broker sin perder datos

**CONTENIDO:**
- **Definiciones:**
  - Broker: servidor que almacena particiones y atiende producers y consumers
  - Controller: broker que coordina elecciones de líderes y estado del clúster
  - Líder de partición: recibe todas las escrituras y lecturas de esa partición
  - Follower: réplica pasiva que toma el liderazgo si el líder falla

- **Distribución de réplicas en clúster de 3 brokers:**
  - Broker 1: Partición A (Líder) + Partición B (Follower)
  - Broker 2: Partición A (Follower) + Partición C (Líder)
  - Broker 3: Partición B (Líder) + Partición C (Follower)

- **Estándares de producción:**
  - Mínimo 3 brokers en producción
  - Factor de replicación = 3 (estándar de la industria)
  - Si 1 broker cae: el sistema elige nuevo líder automáticamente en segundos

**VISUAL:** Diagrama de 3 servidores (brokers) con particiones distribuidas entre ellos. Líderes marcados con corona o estrella dorada, followers en gris. Flecha de "nuevo líder" aparece cuando un broker se desconecta.

---

### SLIDE 15 — KRAFT: KAFKA SIN ZOOKEEPER

**TIPO:** Contenido — Comparación antes/después  
**TÍTULO:** KRaft: la arquitectura moderna de Kafka  
**MENSAJE CLAVE:** Kafka 4.x = un solo sistema, cero dependencias externas

**CONTENIDO:**
- **Antes — Kafka con ZooKeeper (< 3.x):**
  - 3+ brokers Kafka + 3+ nodos ZooKeeper por separado
  - Dos sistemas que instalar, operar, monitorear y actualizar
  - Tiempo de arranque lento, límite de ~200K particiones por clúster

- **Ahora — Kafka con KRaft (4.x):**
  - Solo brokers Kafka con controlador KRaft integrado
  - Un solo sistema. Sin dependencias externas
  - Arranque 10x más rápido, soporte para millones de particiones

- **¿Qué es KRaft?**
  Kafka Raft Metadata Protocol: consenso distribuido basado en el algoritmo Raft, embebido directamente en los brokers de Kafka

- **Para desarrolladores:** un solo contenedor para empezar
  `docker run -p 9092:9092 apache/kafka:4.2.0`

**VISUAL:** Layout de comparación lado a lado (before/after). Lado izquierdo (Before): diagrama con Kafka Cluster + ZooKeeper Cluster separados, colores apagados. Lado derecho (After): solo Kafka Cluster, destacado en cian. Badge verde: "Sin ZooKeeper".

---

---

## FASE 3 — Kafka en Acción
### Slides 16 al 20

---

### SLIDE 16 — CASOS DE USO REALES

**TIPO:** Contenido — Grid de casos  
**TÍTULO:** ¿Dónde vive Kafka en producción?  
**MENSAJE CLAVE:** Kafka aparece en cualquier sistema que necesite eventos a escala

**CONTENIDO:**
- **6 casos de uso con empresa referencia:**

| Caso | Descripción | Empresa |
|------|-------------|---------|
| Event Sourcing | Microservicios se comunican por eventos, no llamadas directas | Uber, Airbnb |
| Pipelines de datos | Mover datos en tiempo real: DB → Data Lake → Analytics | LinkedIn |
| Métricas y telemetría | Centralizar logs y métricas de miles de servicios | Netflix |
| Detección de fraude | Analizar flujos de transacciones en tiempo real | Visa, PayPal |
| Procesamiento IoT | Ingestar millones de eventos de sensores y dispositivos | Tesla |
| Change Data Capture | Capturar cambios en BD y propagarlos al ecosistema | Debezium |

**VISUAL:** Grid 2×3 con cards de casos de uso. Cada card con ícono representativo, título del caso, descripción corta y logo de empresa. Fondo de cards #1A2F45, acento cian en el título de cada card.

---

### SLIDE 17 — KAFKA VS OTRAS HERRAMIENTAS

**TIPO:** Contenido — Tabla comparativa  
**TÍTULO:** ¿Cuándo Kafka y cuándo no?  
**MENSAJE CLAVE:** Kafka es la elección correcta cuando necesitas replay, escala y múltiples consumers independientes

**CONTENIDO:**
- **Tabla comparativa:**

| Criterio | Kafka | RabbitMQ | Amazon SQS | Redis Pub/Sub |
|----------|-------|----------|------------|---------------|
| Retención | Días/semanas configurable | Hasta consumir | Hasta 14 días | No (fire & forget) |
| Replay de eventos | ✅ Sí | ❌ No | ❌ No | ❌ No |
| Throughput | Millones/seg | Miles/seg | Miles/seg | Millones/seg* |
| Complejidad operacional | Alta | Media | Baja | Baja |
| Mejor para | Streams, alta escala | Workflows, tasks | Colas en AWS | Notificaciones RT |

*Sin durabilidad

- **Cuándo NO usar Kafka:** colas simples de trabajo, volumen menor a 10K eventos/día, equipos sin capacidad de operar el clúster

**VISUAL:** Tabla con código de colores: verde para ventajas de Kafka, neutro para las demás. Columna de Kafka destacada con borde cian. Header de tabla con fondo #0D1B2A y texto blanco.

---

### SLIDE 18 — EL ECOSISTEMA KAFKA

**TIPO:** Diagrama — Arquitectura en capas  
**TÍTULO:** Kafka no está solo: el ecosistema  
**MENSAJE CLAVE:** El núcleo es Kafka; el ecosistema multiplica su valor

**CONTENIDO:**
- **Arquitectura en 4 capas (de abajo hacia arriba):**

  - **Capa 1 — Schema Registry:** gestión y validación de esquemas de mensajes (Avro, Protobuf, JSON Schema)
  - **Capa 2 — Apache Kafka Core:** Brokers, Topics, KRaft — el corazón del sistema
  - **Capa 3 — Integración y Procesamiento:**
    - Kafka Connect: conectores para 200+ fuentes y destinos externos (DB, S3, Elasticsearch)
    - Kafka Streams / Apache Flink: procesamiento de streams en tiempo real
  - **Capa 4 — Aplicaciones:** Microservicios, Data Pipelines, Analytics, ksqlDB

- **Punto clave:** en el 80% de proyectos reales en producción se usa Kafka + Connect + Schema Registry como mínimo

**VISUAL:** Diagrama de arquitectura en capas horizontales apiladas. Cada capa con color de intensidad creciente. Iconos de tecnologías reconocibles (Confluent, Flink, Elasticsearch) en sus posiciones. Estilo: architecture blueprint dark.

---

### SLIDE 19 — RUTA DE APRENDIZAJE

**TIPO:** Contenido — Roadmap  
**TÍTULO:** Tu ruta de aprendizaje en 3 etapas  
**MENSAJE CLAVE:** En un solo comando de Docker puedes tener Kafka corriendo hoy

**CONTENIDO:**
- **Día 1 — Manos en la masa:**
  ```
  docker run -p 9092:9092 apache/kafka:4.2.0
  ```
  - Crear el primer topic
  - Publicar mensajes con kafka-console-producer
  - Leer mensajes con kafka-console-consumer
  - Objetivo: entender el ciclo completo producer → topic → consumer

- **Semana 1 — Código real:**
  - Producer y Consumer en Java o Python con el cliente oficial de Kafka
  - Consumer Groups: arrancar múltiples instancias y ver el rebalanceo
  - Commit manual del offset para control preciso del procesamiento

- **Mes 1 — Ecosistema:**
  - Kafka Connect: mover datos desde y hacia bases de datos
  - Monitoreo: consumer lag, throughput, latencia
  - Schema Registry: validar y evolucionar esquemas de mensajes

**VISUAL:** Roadmap horizontal con 3 etapas marcadas como Día 1, Semana 1, Mes 1. Barra de progreso debajo. Cada etapa con lista de checkboxes y ícono representativo. Bloque de código del comando Docker en estilo terminal dark.

---

### SLIDE 20 — CIERRE

**TIPO:** Cierre  
**TÍTULO:** Lo que llevas hoy  
**MENSAJE CLAVE:** Kafka no es una cola — es la memoria distribuida de tu sistema

**CONTENIDO:**
- **Los 6 conceptos fundamentales:**

| Concepto | Definición en una línea |
|----------|------------------------|
| Topic | Canal lógico con nombre — donde viven los eventos |
| Partición | Unidad de paralelismo — el orden vive aquí |
| Offset | Posición del consumer — su puntero en el log |
| Producer | Quien escribe — controla durabilidad con acks |
| Consumer | Quien lee a su propio ritmo — modelo pull |
| Consumer Group | Conjunto que se reparte particiones para escalar |

- **La frase que se llevan:**
  > "Kafka no es una cola. Es la memoria distribuida de tu sistema."

- **Próximos pasos:**
  - Imagen Docker: apache/kafka:4.2.0
  - Documentación oficial: kafka.apache.org
  - Ecosistema: Confluent Hub (conectores listos)

**VISUAL:** Grid 2×3 con los 6 conceptos en cards. Frase de cierre en bloque destacado cian centrado. Fondo: mismo que portada (#0D1B2A). Simetría visual con Slide 1. QR o URL de recursos al pie.

---

## NOTAS DE PRODUCCIÓN PARA SLIDEGO

**Slides de transición entre fases (insertar antes de cada fase):**
- Número de fase grande (01, 02, 03) en cian translúcido al fondo
- Nombre de la fase centrado en blanco, bold
- Subtítulo descriptor en gris claro

**Reglas de diseño consistentes en todos los slides:**
- Fondo: siempre #0D1B2A o variante oscura
- Títulos: Poppins Bold, blanco, alineado izquierda
- Acento decorativo: línea cian de 3px bajo cada título
- Diagramas: líneas cian sobre fondo #1A2F45
- Tablas: header #0D1B2A, filas alternadas #1A2F45/#243547, texto blanco
- Código: fondo #111827, texto verde claro, fuente monospace
- Iconografía: Phosphor Icons estilo "regular" — sin relleno, línea fina
