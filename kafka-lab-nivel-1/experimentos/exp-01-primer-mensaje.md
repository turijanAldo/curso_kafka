# Experimento 01 - Producir y Consumir Mi Primer Mensaje

## Objetivo

Validar que el clúster de Kafka está funcional enviando y recibiendo un mensaje simple usando el producer y consumer Java del laboratorio.

## Hipótesis

Si el clúster está configurado correctamente, podré enviar un mensaje con `SimpleProducer` y recibirlo inmediatamente con `SimpleConsumer`. El mensaje aparecerá en el consumer con metadatos de partición, offset y timestamp.

## Pre-requisitos

- [ ] Clúster Kafka iniciado (`01-iniciar-kafka.ps1` o `.bat`)
- [ ] Clúster verificado (`02-verificar-cluster.ps1` o `.bat`)
- [ ] Maven instalado (`mvn --version` funciona en terminal)
- [ ] Java 17+ instalado (`java --version` funciona en terminal)

---

## Procedimiento paso a paso

### Paso 1 — Crear el topic `primer-topic`

Ejecuta este comando para crear el topic dentro del contenedor:

```powershell
docker exec kafka-nivel1 /opt/kafka/bin/kafka-topics.sh `
    --bootstrap-server localhost:9092 `
    --create `
    --topic primer-topic `
    --partitions 1 `
    --replication-factor 1
```

**CMD equivalente:**
```cmd
docker exec kafka-nivel1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic primer-topic --partitions 1 --replication-factor 1
```

**Explicación de parámetros:**
| Parámetro | Valor | Explicación |
|---|---|---|
| `--bootstrap-server` | `localhost:9092` | Punto de entrada al clúster |
| `--create` | — | Acción: crear topic |
| `--topic` | `primer-topic` | Nombre del topic |
| `--partitions` | `1` | Número de particiones (1 es suficiente para aprender) |
| `--replication-factor` | `1` | Réplicas por partición (máximo 1 con un solo broker) |

**Output esperado:**
```
Created topic primer-topic.
```

---

### Paso 2 — Verificar que el topic se creó

```powershell
docker exec kafka-nivel1 /opt/kafka/bin/kafka-topics.sh `
    --bootstrap-server localhost:9092 `
    --describe `
    --topic primer-topic
```

**Output esperado:**
```
Topic: primer-topic  TopicId: <uuid>  PartitionCount: 1  ReplicationFactor: 1  Configs: ...
  Topic: primer-topic  Partition: 0  Leader: 1  Replicas: 1  Isr: 1
```

---

### Paso 3 — Compilar el código Java con Maven

Desde la carpeta raíz del laboratorio (`kafka-lab-nivel-1/`):

```powershell
cd java
mvn clean package -q
```

**Verificar que compiló correctamente:**
```powershell
ls target\kafka-lab-nivel-1-1.0.0.jar
```

Deberías ver el archivo JAR (~15-20 MB, incluye todas las dependencias).

---

### Paso 4 — Abrir dos terminales

Abre dos ventanas de **PowerShell** o **CMD** y navega en ambas a `kafka-lab-nivel-1\java\`:

```powershell
cd C:\ruta\a\tu\kafka-lab-nivel-1\java
```

---

### Paso 5 — Iniciar el Consumer (Terminal 1)

En la **primera terminal**, ejecuta el SimpleConsumer:

```powershell
java -cp target\kafka-lab-nivel-1-1.0.0.jar com.nexus.kafka.nivel1.SimpleConsumer primer-topic grupo-prueba
```

**Output esperado inmediatamente:**
```
🔵 Consumer iniciado - Topic: primer-topic, Group: grupo-prueba
   Escuchando mensajes... (Ctrl+C para detener)
```

El consumer queda en espera. **No cierres esta terminal.**

---

### Paso 6 — Enviar un mensaje con el Producer (Terminal 2)

En la **segunda terminal**, ejecuta el SimpleProducer:

```powershell
java -cp target\kafka-lab-nivel-1-1.0.0.jar com.nexus.kafka.nivel1.SimpleProducer primer-topic "Hola Kafka desde Nivel 1"

# en caso de falla
& "$env:JAVA_HOME\bin\java" -cp ".\target\kafka-lab-nivel-1-1.0.0.jar" com.nexus.kafka.nivel1.SimpleProducer primer-topic "Hola Kafka desde Nivel 1 mensaje 2"
```

**Output esperado:**
```
Enviando mensaje al topic [primer-topic]: Hola Kafka desde Nivel 1
✅ Mensaje enviado exitosamente - Topic: primer-topic, Partition: 0, Offset: 0, Timestamp: 2026-04-28 10:30:15.123
```

---

### Paso 7 — Observar ambas terminales

Vuelve a la **Terminal 1** (el consumer). Deberías ver:

```
📨 Mensaje recibido - Partition: 0, Offset: 0, Key: null, Value: Hola Kafka desde Nivel 1, Timestamp: 2026-04-28 10:30:15.123
```

---

## Resultados esperados

### Terminal del Producer (Terminal 2):
```
Enviando mensaje al topic [primer-topic]: Hola Kafka desde Nivel 1
✅ Mensaje enviado exitosamente - Topic: primer-topic, Partition: 0, Offset: 0, Timestamp: 2026-04-28 HH:MM:SS.mmm
```

### Terminal del Consumer (Terminal 1):
```
🔵 Consumer iniciado - Topic: primer-topic, Group: grupo-prueba
   Escuchando mensajes... (Ctrl+C para detener)
📨 Mensaje recibido - Partition: 0, Offset: 0, Key: null, Value: Hola Kafka desde Nivel 1, Timestamp: 2026-04-28 HH:MM:SS.mmm
```

---

## Análisis

### ¿Por qué el mensaje llegó del producer al consumer?

El flujo fue:
1. **Producer** → establece conexión TCP con el broker en `localhost:9092`
2. **Producer** → serializa el mensaje (String → bytes) y lo empaqueta en un `ProducerRecord`
3. **Producer** → envía el record al broker con `acks=all`, esperando confirmación
4. **Broker** → recibe el mensaje, lo escribe en el segmento de log de `primer-topic/partición 0`
5. **Broker** → responde al producer con el offset asignado
6. **Consumer** → en su siguiente ciclo de `poll()`, solicita mensajes nuevos al broker
7. **Broker** → entrega el mensaje al consumer
8. **Consumer** → deserializa los bytes a String e imprime los metadatos

### Significado de cada campo de metadata

| Campo | Valor típico | Significado |
|---|---|---|
| **Partition** | `0` | La partición física donde se almacenó el mensaje. Con 1 partición, siempre es 0. |
| **Offset** | `0` | Posición del mensaje dentro de la partición. El primer mensaje tiene offset 0. Es inmutable y creciente. |
| **Key** | `null` | Clave del mensaje. `null` = sin clave. Con clave, Kafka garantiza que mensajes con la misma clave van a la misma partición. |
| **Timestamp** | fecha/hora | Momento en que el producer registró el envío (epoch milliseconds). |

### El rol del broker

El broker actuó como intermediario desacoplado: el producer y el consumer nunca se comunicaron directamente. Esto es el pilar de Kafka — el broker persiste los mensajes en disco (el log), permitiendo que consumers lean en cualquier momento futuro, a su propio ritmo, incluso si el producer ya terminó.

---

## Conclusiones

Con este experimento validamos:

1. **El clúster KRaft está funcional**: el broker puede recibir y entregar mensajes sin ZooKeeper.
2. **El flujo básico de Kafka funciona**: producer → broker → consumer.
3. **Los mensajes persisten**: el offset `0` puede volver a leerse iniciando un consumer con `auto.offset.reset=earliest` en un nuevo grupo.
4. **Los metadatos son ricos**: cada mensaje lleva información de ubicación (topic, partition, offset) y tiempo (timestamp) que permite trazabilidad completa.

---

## Espacio para resultados

Copia aquí tu output real al completar el experimento:

### Output real del Producer:
```
(pegar aquí)
```

### Output real del Consumer:
```
(pegar aquí)
```

### Observaciones personales:
```
(notas, preguntas, diferencias con lo esperado)
```
