# Lección 2 — Dead Letter Topic (DLT)

> **Nivel:** Básico  
> **Tiempo estimado:** 30 minutos  
> **Archivos involucrados:** `PedidoDLTConsumer.java` · `PedidoEstadoProducer.java`  
> **Topics Kafka:** `pedidos-estados` (fuente) · `pedidos-estados-dlt` (destino de errores)

---

## ¿Qué vamos a aprender?

Al terminar esta lección serás capaz de:

1. Explicar qué es un Dead Letter Topic y por qué existe
2. Identificar las dos formas incorrectas de manejar errores en un consumer
3. Implementar el patrón DLT: un consumer que produce hacia otro topic cuando falla
4. Usar **headers de Kafka** para agregar metadata a un mensaje sin modificar su contenido
5. Inspeccionar mensajes fallidos con el consumer de consola

---

## El problema que resuelve esta lección

En la lección anterior, el consumer procesaba todos los pedidos correctamente. Pero en la realidad, algunas cosas fallan:

- El JSON llega corrupto
- La base de datos no responde
- Una regla de negocio rechaza el mensaje (ejemplo: pedido sin stock)
- El servicio externo regresa un error

Cuando eso pasa, el consumer tiene que tomar una decision. Hay dos caminos malos y uno bueno.

---

## Las dos formas incorrectas

### Forma incorrecta 1: ignorar el error y hacer commit

```
for (record : records) {
    try {
        procesarPedido(record);
    } catch (Exception e) {
        // ignorar
    }
}
consumer.commitSync(); // <- el mensaje se marca como procesado
```

**Consecuencia:** el mensaje se pierde para siempre. Nadie sabe que fallo. En produccion esto significa pedidos de clientes que desaparecen sin dejar rastro.

### Forma incorrecta 2: no hacer commit y reintentar indefinidamente

```
for (record : records) {
    while (true) {
        try {
            procesarPedido(record);
            break;
        } catch (Exception e) {
            Thread.sleep(1000); // reintentar
        }
    }
}
consumer.commitSync();
```

**Consecuencia:** si el mensaje es permanentemente malo (dato corrupto, logica de negocio que siempre falla), el consumer se queda atascado en ese mensaje para siempre. Los mensajes siguientes nunca se procesan. El lag del consumer grupo crece sin parar.

---

## La forma correcta: Dead Letter Topic

```
Topic principal               Topic DLT
pedidos-estados               pedidos-estados-dlt
      |                              ^
      v                              |
[Consumer DLT] ---- falla -------> [mensaje fallido + metadata del error]
      |
      v (commit y continua)
[siguiente mensaje]
```

Cuando un mensaje no se puede procesar:
1. Se publica en el topic DLT con informacion del error en los **headers**
2. Se hace `commitSync()` normalmente
3. El consumer continua con el siguiente mensaje sin bloquearse

El equipo puede inspeccionar el DLT, entender que paso, corregir el bug, y republicar los mensajes cuando el sistema este listo.

---

## Concepto nuevo: Headers de Kafka

Los mensajes de Kafka tienen tres partes:

```
ProducerRecord
  ├── KEY     : identificador del mensaje (orderId)
  ├── VALUE   : contenido del mensaje (JSON del pedido)
  └── HEADERS : pares clave-valor en bytes (metadata adicional)
```

Los headers son similares a los HTTP headers: agregan informacion sin modificar el cuerpo. Para los mensajes DLT los usamos para registrar:

| Header | Contenido |
|--------|-----------|
| `error-message` | Descripcion del error que causo el fallo |
| `original-topic` | De que topic vino el mensaje |
| `original-partition` | En que particion estaba |
| `original-offset` | Cual era su offset |
| `failed-at-ms` | Timestamp del fallo |

El VALUE del mensaje DLT es identico al original. Solo se agrega informacion, nunca se modifica.

---

## Regla de negocio simulada

Para que la demo sea predecible, el consumer falla con pedidos cuyo total supera $200 (requieren aprobacion manual):

| Pedido | Producto | Total | Resultado |
|--------|----------|-------|-----------|
| ORD-001 | Laptop Pro | $1299 | Falla → DLT |
| ORD-002 | Teclado Mec | $89 | OK |
| ORD-003 | Monitor 27 | $399 | Falla → DLT |

---

## Prerrequisitos

| Requisito | Verificacion |
|-----------|--------------|
| Leccion 1 completada | El topic `pedidos-estados` existe y tiene mensajes |
| Kafka corriendo en Docker | `docker ps` muestra el contenedor `kafka` |
| Proyecto compilado | `mvn compile -q` sin errores |

---

## Ejecucion paso a paso

### Paso 1 — Crear el topic DLT

El topic DLT es un topic normal de Kafka. La convencion de nombre es agregar el sufijo `-dlt` al topic original.

```bash
docker exec kafka /opt/kafka/bin/kafka-topics.sh \
  --create \
  --topic pedidos-estados-dlt \
  --partitions 3 \
  --replication-factor 1 \
  --bootstrap-server localhost:9092
```

Verifica que ambos topics existen:

```bash
docker exec kafka /opt/kafka/bin/kafka-topics.sh \
  --list \
  --bootstrap-server localhost:9092
```

Deberias ver:

```
pedidos-estados
pedidos-estados-dlt
```

---

### Paso 2 — Compilar

```bash
mvn compile -q
```

---

### Paso 3 — Lanzar el Consumer DLT (Terminal A)

```bash
mvn exec:java -Dexec.mainClass="com.tiendamax.PedidoDLTConsumer"
```

El consumer mostrara:

```
Consumer DLT escuchando [pedidos-estados]
Mensajes que fallen iran a [pedidos-estados-dlt]
Regla: pedidos con total > $200.0 requieren aprobacion manual
```

---

### Paso 4 — Lanzar el Producer (Terminal B)

Reutilizamos el producer de la leccion anterior:

```bash
mvn exec:java -Dexec.mainClass="com.tiendamax.PedidoEstadoProducer"
```

> Si el topic `pedidos-estados` ya tiene mensajes del ejercicio anterior,
> el consumer DLT los leera desde el principio porque usa un `group.id`
> distinto (`grupo-dlt`) con `auto.offset.reset=earliest`.

---

### Paso 5 — Inspeccionar el DLT (Terminal C)

Mientras el consumer DLT esta corriendo, abre una tercera terminal para ver que mensajes llegan al DLT:

```bash
docker exec -it kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --topic pedidos-estados-dlt \
  --from-beginning \
  --bootstrap-server localhost:9092
```

Veras el JSON original de los pedidos fallidos (ORD-001 y ORD-003).

---

## Que observar en la salida

### Salida del Consumer DLT (Terminal A)

```
-- Poll: 3 mensajes --
  DLT     P0 offset=0   ORD-001  -> [pedidos-estados-dlt] Requiere aprobacion manual: total $1299.0 supera el limite $200.0
  OK      P1 offset=0   ORD-002  $89.00    CREADO
  DLT     P2 offset=0   ORD-003  -> [pedidos-estados-dlt] Requiere aprobacion manual: total $399.0 supera el limite $200.0
  Commit realizado. Continuando con los siguientes mensajes.

-- Poll: 3 mensajes --
  DLT     P0 offset=1   ORD-001  -> [pedidos-estados-dlt] Requiere aprobacion manual: total $1299.0 supera el limite $200.0
  OK      P1 offset=1   ORD-002  $89.00    PAGADO
  DLT     P2 offset=1   ORD-003  -> [pedidos-estados-dlt] Requiere aprobacion manual: total $399.0 supera el limite $200.0
  Commit realizado. Continuando con los siguientes mensajes.
```

**Que observar:**

- `DLT` indica que el mensaje fue redirigido al Dead Letter Topic
- `OK` indica procesamiento exitoso
- El consumer **nunca se bloquea**: procesa lo que puede, redirige lo que no puede, hace commit y continua
- Cada estado del ciclo de vida (CREADO, PAGADO, etc.) genera un evento separado, y cada uno puede fallar o pasar independientemente

### Salida del Inspector DLT (Terminal C)

```
{"order_id":"ORD-001","clienteId":"cli-ana","producto":"Laptop Pro","total":1299.0,"estado":"CREADO","timestamp":1716210000000}
{"order_id":"ORD-003","clienteId":"cli-carlos","producto":"Monitor 27","total":399.0,"estado":"CREADO","timestamp":1716210000100}
{"order_id":"ORD-001","clienteId":"cli-ana","producto":"Laptop Pro","total":1299.0,"estado":"PAGADO","timestamp":1716210000800}
```

**Que observar:**

- El VALUE es el JSON original sin modificar
- Los headers (con el error y el origen) no se ven en el consumer de consola porque ese comando no los imprime por defecto
- Para ver los headers usa la opcion `--property print.headers=true` (ver referencia rapida abajo)

---

## Preguntas de reflexion

**1. ¿Por que hacemos `producer.flush()` antes de `consumer.commitSync()`?**

> Para garantizar que el mensaje llego al DLT antes de commitear el offset del consumer.
> Si commitamos primero y el flush falla, el mensaje se perdio: ya no lo reintentamos
> (el offset esta marcado como procesado) y tampoco llego al DLT.
> El orden correcto es: enviar al DLT -> confirmar entrega (flush) -> commit del consumer.

**2. ¿Por que usamos headers en lugar de modificar el JSON del VALUE?**

> Porque el VALUE del mensaje DLT debe ser identico al original.
> El equipo que va a revisar y relanzar los mensajes del DLT necesita el dato sin alteraciones.
> Si agregaramos campos al JSON estariamos mezclando el dato de negocio con metadata de infraestructura.
> Los headers son el lugar correcto para esa metadata.

**3. ¿Que pasa si el envio al DLT tambien falla?**

> En el codigo actual el error queda en stderr pero el consumer de todas formas hace commit.
> Esto significa que si el DLT no esta disponible, el mensaje se pierde.
> En produccion se agrega un retry con backoff exponencial para el envio al DLT,
> o se usa un circuit breaker que detiene el consumer si el DLT no responde.

**4. ¿Cuantas veces puede aparecer el mismo pedido en el DLT?**

> Tantas veces como eventos tenga en el topic fuente que fallen la validacion.
> ORD-001 con 4 estados (CREADO, PAGADO, ENVIADO, ENTREGADO) aparecera 4 veces en el DLT
> si todos fallan. En produccion esto es normal y deseable: el DLT es un historial completo
> de todos los intentos fallidos, no solo el primero.

---

## Resumen

| Concepto | Lo que aprendiste |
|----------|-------------------|
| **Dead Letter Topic** | Topic separado donde van los mensajes que el consumer no pudo procesar |
| **Sin bloqueo** | El consumer nunca se atasca en un mensaje malo: lo redirige y continua |
| **Headers de Kafka** | Metadata adjunta al mensaje (clave-valor en bytes) sin modificar el VALUE |
| **Orden de operaciones** | Enviar al DLT → flush → commitSync. Nunca al reves. |
| **Consumer-produce pattern** | Un mismo proceso puede ser consumer de un topic y producer de otro simultaneamente |

---

## Comandos de referencia rapida

| Accion | Comando |
|--------|---------|
| Crear topic DLT | `docker exec kafka /opt/kafka/bin/kafka-topics.sh --create --topic pedidos-estados-dlt --partitions 3 --replication-factor 1 --bootstrap-server localhost:9092` |
| Ver mensajes DLT | `docker exec kafka /opt/kafka/bin/kafka-console-consumer.sh --topic pedidos-estados-dlt --from-beginning --bootstrap-server localhost:9092` |
| Ver mensajes DLT con headers | `docker exec kafka /opt/kafka/bin/kafka-console-consumer.sh --topic pedidos-estados-dlt --from-beginning --property print.headers=true --bootstrap-server localhost:9092` |
| Ver lag del consumer | `docker exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --describe --group grupo-dlt --bootstrap-server localhost:9092` |
| Ejecutar consumer DLT | `mvn exec:java -Dexec.mainClass="com.tiendamax.PedidoDLTConsumer"` |
| Ejecutar producer | `mvn exec:java -Dexec.mainClass="com.tiendamax.PedidoEstadoProducer"` |

---

## Siguiente leccion

**Leccion 3 — Admin Client:** crear y configurar topics desde codigo Java en lugar de hacerlo manualmente con scripts de consola.
