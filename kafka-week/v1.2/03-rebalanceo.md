# Lección 3 — Consumer Groups y Rebalanceo

> **Nivel:** Básico  
> **Tiempo estimado:** 25 minutos  
> **Archivos involucrados:** `RebalanceoDemo.java`  
> **Topic Kafka:** `pedidos-estados` (ya creado en Lección 1)

---

## ¿Qué vamos a aprender?

Al terminar esta lección serás capaz de:

1. Explicar qué es un Consumer Group y para qué sirve
2. Entender cómo Kafka distribuye las particiones entre los consumers de un grupo
3. Observar en tiempo real qué es un rebalanceo y cuándo ocurre
4. Aplicar la regla fundamental: **una partición → un consumer activo por grupo**
5. Entender por qué agregar consumers no siempre aumenta la velocidad

---

## ¿Qué es un Consumer Group?

Un Consumer Group es un conjunto de consumers que cooperan para leer un mismo topic. Kafka divide el trabajo entre ellos automáticamente.

```
Topic: pedidos-estados  (3 particiones)

  P0 ──►  Consumer A  ┐
  P1 ──►  Consumer B  ├── grupo "grupo-rebalanceo"
  P2 ──►  Consumer C  ┘
```

Cada mensaje es procesado **por exactamente un consumer del grupo**. Esto permite escalar el procesamiento horizontalmente: si los mensajes llegan rápido, agregas más consumers al grupo.

---

## La regla fundamental

> **Una partición solo puede ser leída por UN consumer del grupo a la vez.**

Esto significa que el número de consumers útiles está limitado por el número de particiones:

| Consumers en el grupo | Particiones = 3 | Resultado |
|---|---|---|
| 1 consumer | P0, P1, P2 → Consumer 1 | Todo el trabajo en uno |
| 2 consumers | P0, P1 → C1 · P2 → C2 | Trabajo distribuido |
| 3 consumers | P0 → C1 · P1 → C2 · P2 → C3 | Máximo paralelismo |
| 4 consumers | P0 → C1 · P1 → C2 · P2 → C3 · **C4 inactivo** | C4 no recibe nada |

Agregar un 4° consumer **no ayuda** si solo hay 3 particiones.

---

## ¿Qué es el Rebalanceo?

Cuando el número de consumers en un grupo cambia, Kafka redistribuye las particiones. Ese proceso se llama **rebalanceo**.

El rebalanceo ocurre cuando:
- Un consumer nuevo se une al grupo
- Un consumer se desconecta (Ctrl+C, crash, timeout de sesión)
- Se agregan particiones al topic

Durante el rebalanceo, **ningún consumer puede leer mensajes**. Es un "stop-the-world" breve pero visible. Por eso en sistemas de alto rendimiento se diseña para minimizar los rebalanceos.

---

## Paso 1 — Verificar que el topic existe

El topic `pedidos-estados` ya fue creado en la Lección 1. Verificarlo:

```bash
docker exec kafka /opt/kafka/bin/kafka-topics.sh \
  --describe --topic pedidos-estados \
  --bootstrap-server localhost:9092
```

Debe mostrar **3 particiones**. Si no existe, crearlo:

```bash
docker exec kafka /opt/kafka/bin/kafka-topics.sh \
  --create --topic pedidos-estados \
  --partitions 3 --replication-factor 1 \
  --bootstrap-server localhost:9092
```

---

## Paso 2 — Compilar

```bash
mvn compile -q
```

---

## Paso 3 — El experimento

Necesitas **3 terminales** abiertas en la carpeta `tiendamax/`.

### Terminal 1 — Primera instancia

```bash
mvn exec:java -Dexec.mainClass="com.tiendamax.RebalanceoDemo"
```

**Salida esperada:**

```
Consumer iniciado. Grupo: [grupo-rebalanceo]
Esperando asignacion de particiones...

>>> REBALANCEO COMPLETADO <<<
    Particiones ASIGNADAS a este consumer:
      - pedidos-estados [P0]
      - pedidos-estados [P1]
      - pedidos-estados [P2]
```

El primer consumer recibe las **3 particiones** porque no hay nadie más en el grupo.

---

### Terminal 2 — Segunda instancia

```bash
mvn exec:java -Dexec.mainClass="com.tiendamax.RebalanceoDemo"
```

Observa **ambas terminales**. Las dos deben imprimir el rebalanceo:

**Terminal 1 (consumer existente):**
```
>>> REBALANCEO INICIADO <<<
    Particiones REVOCADAS (me las quitaron):
      - pedidos-estados [P0]
      - pedidos-estados [P1]
      - pedidos-estados [P2]

>>> REBALANCEO COMPLETADO <<<
    Particiones ASIGNADAS a este consumer:
      - pedidos-estados [P0]
      - pedidos-estados [P1]
```

**Terminal 2 (consumer nuevo):**
```
>>> REBALANCEO COMPLETADO <<<
    Particiones ASIGNADAS a este consumer:
      - pedidos-estados [P2]
```

Kafka revocó todas las particiones del Consumer 1 y las redistribuyó entre los dos.

---

### Terminal 3 — Tercera instancia

```bash
mvn exec:java -Dexec.mainClass="com.tiendamax.RebalanceoDemo"
```

Ahora hay 3 consumers y 3 particiones: **cada uno recibe exactamente 1 partición**.

---

### Lanzar una 4ª instancia (opcional)

```bash
mvn exec:java -Dexec.mainClass="com.tiendamax.RebalanceoDemo"
```

**Salida del 4° consumer:**
```
>>> REBALANCEO COMPLETADO <<<
    Sin particiones asignadas.
    Hay mas consumers que particiones en el grupo.
    Este consumer esta INACTIVO hasta que salga otro del grupo.
```

El 4° consumer queda en espera. Kafka lo mantiene en el grupo como "stand-by".

---

### Matar un consumer — Ctrl+C en Terminal 1

Presiona Ctrl+C en la Terminal 1. Las terminales restantes deben imprimir el rebalanceo automáticamente:

```
[SHUTDOWN] Cerrando consumer - esto disparara un rebalanceo en el grupo.

>>> REBALANCEO INICIADO <<<
    Particiones REVOCADAS (me las quitaron): ...

>>> REBALANCEO COMPLETADO <<<
    Particiones ASIGNADAS a este consumer:
      - pedidos-estados [P0]   <- absorbe la particion del consumer que salio
```

Si tenías el 4° consumer inactivo, ahora recibirá una partición.

---

## Paso 4 — Ver el estado del grupo desde CLI

Mientras las instancias están corriendo, en otra terminal:

```bash
# Ver todos los consumer groups
docker exec kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --list --bootstrap-server localhost:9092

# Ver detalle del grupo: qué consumer tiene cada partición y su lag
docker exec kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --describe --group grupo-rebalanceo \
  --bootstrap-server localhost:9092
```

La columna **LAG** muestra cuántos mensajes tiene pendientes cada consumer. Un lag alto indica que el consumer no está al día con el producer.

---

## ¿Qué hay dentro del código?

El punto clave es el `ConsumerRebalanceListener`:

```java
consumer.subscribe(List.of(TOPIC), new RebalanceoListener());
```

```java
class RebalanceoListener implements ConsumerRebalanceListener {

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> particiones) {
        // Kafka llama esto ANTES del rebalanceo.
        // Aquí se hacen commits de offsets pendientes para no perder progreso.
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> particiones) {
        // Kafka llama esto DESPUÉS del rebalanceo.
        // Aquí se puede inicializar estado local para las particiones nuevas.
    }
}
```

En producción, `onPartitionsRevoked` es donde se hace el `commitSync()` final antes de soltar las particiones.

---

## Resumen

| Concepto | Explicación |
|---|---|
| Consumer Group | Conjunto de consumers que reparten el trabajo de un topic |
| Partición | Unidad de paralelismo — solo un consumer activo por grupo |
| Rebalanceo | Redistribución de particiones cuando cambia el grupo |
| `onPartitionsRevoked` | Se ejecuta antes del rebalanceo — punto de commit |
| `onPartitionsAssigned` | Se ejecuta después del rebalanceo — punto de inicialización |
| LAG | Mensajes pendientes de procesar en una partición |

---

## Conexión con IBM MQ

El concepto equivalente en IBM MQ es el de **consumers competidores** (competing consumers): múltiples instancias leyendo de la misma cola, donde cada mensaje lo procesa exactamente una instancia.

La diferencia clave es que en Kafka el paralelismo está determinado por las **particiones** (debes definirlas al crear el topic), mientras que en IBM MQ cualquier número de consumers puede competir por los mensajes de una cola sin configuración previa.
