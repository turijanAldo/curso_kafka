# Lección 1 — Ciclo de vida de un pedido con Kafka

> **Nivel:** Básico  
> **Tiempo estimado:** 30 minutos  
> **Archivos involucrados:** `PedidoEstadoProducer.java` · `PedidoEstadoConsumer.java`  
> **Topic Kafka:** `pedidos-estados`

---

## ¿Qué vamos a aprender?

Al terminar esta lección serás capaz de:

1. Explicar por qué la **KEY** de un mensaje en Kafka determina en qué partición cae
2. Entender cómo Kafka **garantiza el orden** de eventos dentro de una partición
3. Construir un producer que emite **múltiples eventos del mismo objeto** a lo largo del tiempo
4. Construir un consumer que **reconstruye el estado actual** de un objeto leyendo su historial de eventos
5. Identificar el patrón **Event Sourcing** en su forma más simple

---

## El problema que resuelve esta lección

En el laboratorio anterior (v1.0) enviamos pedidos como eventos independientes. Cada pedido se publicaba una sola vez con estado `CREADO` y listo.

Pero en la realidad, un pedido **cambia de estado** varias veces:

```
CREADO  →  PAGADO  →  ENVIADO  →  ENTREGADO
```

¿Cómo garantizamos que un consumer que procesa pedidos de TiendaMax vea estos eventos **en el orden correcto**?

La respuesta está en cómo Kafka usa la **KEY** del mensaje.

---

## Conceptos clave

### 1. KEY = orderId → partición fija

Cuando el producer envía un mensaje con KEY, Kafka aplica una función hash (murmur2) sobre esa KEY y la usa para elegir la partición de destino:

```
particion = murmur2("ORD-001") % numero_de_particiones
```

El resultado es siempre el mismo para la misma KEY. Esto significa:

- Todos los eventos de `ORD-001` van a la **misma partición**, siempre
- Todos los eventos de `ORD-002` van a otra partición, siempre
- Esta asignación no cambia mientras el número de particiones no cambie

```
Producer envía eventos de 3 pedidos con KEY = orderId:

ORD-001  CREADO    → Partición 0
ORD-001  PAGADO    → Partición 0   <- mismo pedido, misma particion
ORD-001  ENVIADO   → Partición 0   <- mismo pedido, misma particion
ORD-001  ENTREGADO → Partición 0   <- mismo pedido, misma particion

ORD-002  CREADO    → Partición 1
ORD-002  PAGADO    → Partición 1   <- mismo pedido, misma particion

ORD-003  CREADO    → Partición 2
```

### 2. Orden garantizado dentro de una partición

Kafka garantiza que los mensajes dentro de **una misma partición** se entregan al consumer en el mismo orden en que fueron escritos.

Esto nos da una propiedad muy valiosa:

> Si todos los eventos de un pedido están en la misma partición, el consumer siempre los verá en orden cronológico.

Sin KEY (null) → Kafka hace round-robin → los eventos de un mismo pedido pueden caer en distintas particiones → el consumer puede ver `ENVIADO` antes que `PAGADO`.

Con KEY = orderId → todos los eventos del mismo pedido van a la misma partición → orden garantizado.

### 3. Event Sourcing básico

En lugar de guardar el "estado actual" del pedido en una base de datos, guardamos **todos los eventos que le ocurrieron**:

```
Topic pedidos-estados:
  offset 0  → ORD-001 CREADO
  offset 1  → ORD-002 CREADO
  offset 2  → ORD-001 PAGADO
  offset 3  → ORD-001 ENVIADO
  offset 4  → ORD-002 PAGADO
  offset 5  → ORD-001 ENTREGADO
```

Para conocer el estado actual de `ORD-001`, el consumer lee todos sus eventos en orden y se queda con el último: `ENTREGADO`.

El topic **es la fuente de verdad**. No necesitas una DB de estado separada para reconstruir lo que pasó.

---

## Prerrequisitos

Antes de ejecutar esta lección debes tener:

| Requisito | Verificación |
|-----------|--------------|
| Kafka corriendo en Docker | `docker ps` muestra el contenedor `kafka` |
| Puerto 9092 accesible | El contenedor expone `0.0.0.0:9092` |
| Java 17+ instalado | `java -version` |
| Maven instalado | `mvn -version` |
| Proyecto compilado | Ejecutar el paso de compilación abajo |

---

## Ejecución paso a paso

### Paso 1 — Crear el topic

El topic `pedidos-estados` es independiente del topic `pedidos-tiendamax` del laboratorio anterior. Lo creamos con 3 particiones para que haya una por cada pedido de ejemplo.

Entra al contenedor de Kafka:

```bash
docker exec -it kafka bash
```

Dentro del contenedor, crea el topic:

```bash
/opt/kafka/bin/kafka-topics.sh \
  --create \
  --topic pedidos-estados \
  --partitions 3 \
  --replication-factor 1 \
  --bootstrap-server localhost:9092
```

Verifica que se creó:

```bash
/opt/kafka/bin/kafka-topics.sh \
  --describe \
  --topic pedidos-estados \
  --bootstrap-server localhost:9092
```

Deberías ver algo como:

```
Topic: pedidos-estados  PartitionCount: 3  ReplicationFactor: 1
  Partition: 0  Leader: 1  Replicas: 1  Isr: 1
  Partition: 1  Leader: 1  Replicas: 1  Isr: 1
  Partition: 2  Leader: 1  Replicas: 1  Isr: 1
```

Sal del contenedor:

```bash
exit
```

---

### Paso 2 — Compilar el proyecto

Desde el directorio `kafka-week/tiendamax/`:

```bash
mvn compile -q
```

Sin errores = listo para ejecutar.

---

### Paso 3 — Lanzar el Consumer (Terminal A)

Abre la primera terminal y ejecuta el consumer. Es importante lanzarlo **antes que el producer** para que puedas observar los mensajes llegar en tiempo real.

```bash
mvn exec:java -Dexec.mainClass="com.tiendamax.PedidoEstadoConsumer"
mvn exec:java "-Dexec.mainClass=com.tiendamax.PedidoEstadoConsumer"
```

El consumer mostrará este mensaje y quedará esperando:

```
Escuchando [pedidos-estados] como grupo [grupo-ciclovida]
Reconstruyendo estado desde eventos...
```

---

### Paso 4 — Lanzar el Producer (Terminal B)

Abre una segunda terminal y ejecuta el producer:

```bash
mvn exec:java -Dexec.mainClass="com.tiendamax.PedidoEstadoProducer"
mvn exec:java "-Dexec.mainClass=com.tiendamax.PedidoEstadoProducer"
```

El producer emitirá los eventos con pausas de 800ms entre cada transición de estado para que puedas observarlos llegar de uno en uno.

---

## ¿Qué observar en la salida?

### Salida del Producer (Terminal B)

```
Emitiendo ciclo de vida para 3 pedidos...
KEY = orderId -> todos los eventos de un pedido van a la MISMA particion

-- Transicion: CREADO --
  OK ORD-001  CREADO       -> P0  offset=0
  OK ORD-002  CREADO       -> P1  offset=0
  OK ORD-003  CREADO       -> P2  offset=0

-- Transicion: PAGADO --
  OK ORD-001  PAGADO       -> P0  offset=1
  OK ORD-002  PAGADO       -> P1  offset=1
  OK ORD-003  PAGADO       -> P2  offset=1

-- Transicion: ENVIADO --
  OK ORD-001  ENVIADO      -> P0  offset=2
  OK ORD-002  ENVIADO      -> P1  offset=2

-- Transicion: ENTREGADO --
  OK ORD-001  ENTREGADO    -> P0  offset=3
```

**Qué observar:**  
- `ORD-001` siempre aparece en `P0`. En todos los eventos.  
- `ORD-002` siempre aparece en `P1`. En todos los eventos.  
- El offset **aumenta** dentro de cada partición. Eso es el orden cronológico en Kafka.  
- Los pedidos 2 y 3 no llegan al estado `ENTREGADO` porque el producer simula que aún están en tránsito (situación realista).

---

### Salida del Consumer (Terminal A)

```
-- Poll: 3 eventos recibidos --
  P0 offset=0   ORD-001  NINGUNO    -> CREADO
  P1 offset=0   ORD-002  NINGUNO    -> CREADO
  P2 offset=0   ORD-003  NINGUNO    -> CREADO

  Estado actual reconstruido desde eventos:
    ORD-001  CREADO       (fijo en P0)
    ORD-002  CREADO       (fijo en P1)
    ORD-003  CREADO       (fijo en P2)

-- Poll: 3 eventos recibidos --
  P0 offset=1   ORD-001  CREADO     -> PAGADO
  P1 offset=1   ORD-002  CREADO     -> PAGADO
  P2 offset=1   ORD-003  CREADO     -> PAGADO

  Estado actual reconstruido desde eventos:
    ORD-001  PAGADO       (fijo en P0)
    ORD-002  PAGADO       (fijo en P1)
    ORD-003  PAGADO       (fijo en P2)
```

**Qué observar:**  
- La columna del medio muestra la **transición**: de dónde viene y a dónde va.  
- El estado `NINGUNO` significa que es el primer evento que vemos de ese pedido.  
- La sección **"Estado actual reconstruido"** es la proyección: el resumen del estado más reciente de cada pedido, construido solo a partir de los eventos del topic.  
- `ORD-001` siempre está en `P0` y `ORD-002` siempre en `P1`. Eso confirma que la KEY determina la partición de forma consistente.

---

## Preguntas de reflexión

Estas preguntas te ayudan a verificar que el concepto quedó claro. Intenta responderlas antes de ver la respuesta.

**1. ¿Qué pasaría si enviáramos los eventos de ORD-001 sin KEY (null)?**

> Sin KEY Kafka distribuye los mensajes en round-robin entre las 3 particiones.
> El consumer podría recibir PAGADO antes que CREADO si caen en distintas particiones.
> El estado reconstruido sería incorrecto.

**2. ¿Podría el consumer ver el evento ENTREGADO antes que CREADO para el mismo pedido?**

> No, siempre que usen la misma KEY. Kafka garantiza orden dentro de una partición.
> CREADO tiene offset 0, ENTREGADO tiene offset 3. El consumer siempre lee de menor a mayor offset.

**3. Si agregamos una cuarta partición al topic, ¿ORD-001 seguirá yendo a P0?**

> No necesariamente. La formula es murmur2(KEY) % N. Si N cambia de 3 a 4, el resultado cambia.
> Esta es una razón importante para no modificar el número de particiones en un topic productivo.

**4. ¿Qué ventaja tiene reconstruir el estado desde eventos en lugar de guardar solo el estado actual?**

> Puedes ver todo el historial: cuándo se creó, cuándo se pagó, cuánto tardó en enviarse.
> Puedes "rebobinar" y recalcular el estado desde cero si hay un bug en la lógica de procesamiento.
> Múltiples servicios pueden leer el mismo topic y construir vistas distintas del mismo dato.

---

## Resumen

| Concepto | Lo que aprendiste |
|----------|-------------------|
| **KEY del mensaje** | Determina la partición de destino mediante hash. Misma KEY = misma partición, siempre. |
| **Orden por partición** | Kafka garantiza que los mensajes de una partición llegan al consumer en el orden en que se escribieron. |
| **Orden por entidad** | KEY = identificador de entidad (orderId) = todos los eventos de esa entidad en orden. |
| **Event Sourcing básico** | El topic guarda el historial completo. El consumer reconstruye el estado leyendo eventos en orden. |
| **Proyección** | El `Map<orderId, estado>` del consumer es una vista materializada del stream. |

---

## Comandos de referencia rápida

| Acción | Comando |
|--------|---------|
| Crear topic | `docker exec kafka /opt/kafka/bin/kafka-topics.sh --create --topic pedidos-estados --partitions 3 --replication-factor 1 --bootstrap-server localhost:9092` |
| Ver detalle del topic | `docker exec kafka /opt/kafka/bin/kafka-topics.sh --describe --topic pedidos-estados --bootstrap-server localhost:9092` |
| Compilar | `mvn compile -q` |
| Ejecutar producer | `mvn exec:java -Dexec.mainClass="com.tiendamax.PedidoEstadoProducer"` |
| Ejecutar consumer | `mvn exec:java -Dexec.mainClass="com.tiendamax.PedidoEstadoConsumer"` |
| Ver mensajes raw | `docker exec kafka /opt/kafka/bin/kafka-console-consumer.sh --topic pedidos-estados --from-beginning --bootstrap-server localhost:9092` |
| Eliminar topic (reset) | `docker exec kafka /opt/kafka/bin/kafka-topics.sh --delete --topic pedidos-estados --bootstrap-server localhost:9092` |

---

## Siguiente lección

**Lección 2 — Dead Letter Topic:** qué pasa cuando el consumer no puede procesar un mensaje y cómo evitar que ese error bloquee toda la cola.
