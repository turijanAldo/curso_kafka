# Tutorial: Kafka con Docker en modo KRaft

> **Para quién es este tutorial:** Si nunca has usado Kafka y quieres levantarlo localmente en minutos con Docker, este tutorial es para ti. No necesitas instalar Java ni Kafka directamente — Docker lo hace todo.

---

## ¿Qué vamos a construir?

Un broker Kafka corriendo en tu máquina local dentro de un contenedor Docker, usando el modo moderno **KRaft** (sin ZooKeeper). Al final vas a poder:

- Crear topics
- Enviar mensajes como producer
- Recibirlos en tiempo real como consumer

```
Tu terminal (Producer)
       |
       v
  [ Kafka Broker ]  <-- Docker container
       |
       v
Tu terminal (Consumer)
```

---

## Prerrequisitos

Antes de empezar, asegúrate de tener instalado:

| Herramienta | Versión mínima | Verificar con |
|-------------|----------------|---------------|
| Docker Desktop | 24+ | `docker --version` |
| Docker Compose | v2+ | `docker compose version` |

> Si `docker compose version` no funciona, prueba `docker-compose --version` (versión antigua con guión).

---

## Estructura de archivos

```
kafka-week/
├── infra/
│   └── docker-compose.yml    ← configuración del broker
├── notas.md
├── TUTORIAL.md               ← este archivo
└── FUNCIONALIDAD.md
```

---

## Paso 1: Entrar al directorio de infraestructura

```bash
cd kafka-week/infra
```

---

## Paso 2: Levantar Kafka con Docker

```bash
docker compose up -d
```

La bandera `-d` significa "detached" — el contenedor corre en segundo plano.

**Qué verás:**
```
[+] Running 2/2
 ✔ Volume "infra_kafka-data"  Created
 ✔ Container kafka            Started
```

> Docker descargará la imagen `apache/kafka:4.2.0` la primera vez (~500 MB). Las siguientes veces usa la copia local y arranca en segundos.

---

## Paso 3: Verificar que Kafka arrancó bien

```bash
docker compose logs kafka
```

Busca esta línea en los logs:

```
[KafkaServer id=1] started (kafka.server.KafkaServer)
```

Si ves esa línea, Kafka está listo para recibir conexiones. Si no aparece aún, espera 10-15 segundos y repite el comando.

**Tip:** Para seguir los logs en tiempo real:
```bash
docker compose logs -f kafka
```
Presiona `Ctrl+C` para salir.

---

## Paso 4: Entrar al contenedor de Kafka

Los scripts de administración de Kafka viven dentro del contenedor. Necesitamos entrar para usarlos:

```bash
docker exec -it kafka bash
```

Ahora estás dentro del contenedor. El prompt cambiará a algo como:
```
bash-5.1$
```

> Todos los comandos de los pasos 5-8 se ejecutan **dentro** del contenedor.

---

## Paso 5: Crear un topic

Un **topic** es como un canal o buzón donde los mensajes se guardan. Vamos a crear uno llamado `test-topic` con 3 particiones:

```bash
kafka-topics.sh --create \
  --topic test-topic \
  --partitions 3 \
  --replication-factor 1 \
  --bootstrap-server localhost:9092
```

**Resultado esperado:**
```
Created topic test-topic.
```

**¿Qué significan los parámetros?**

| Parámetro | Valor | Significado |
|-----------|-------|-------------|
| `--topic` | `test-topic` | Nombre del topic |
| `--partitions` | `3` | Divide el topic en 3 partes para paralelismo |
| `--replication-factor` | `1` | Sin réplicas (solo tenemos 1 broker) |
| `--bootstrap-server` | `localhost:9092` | Dirección del broker al que nos conectamos |

---

## Paso 6: Listar los topics existentes

```bash
kafka-topics.sh --list --bootstrap-server localhost:9092
```

**Resultado:**
```
test-topic
```

---

## Paso 7: Abrir el Producer (Terminal 1)

Abre una **nueva terminal** (o tab) y entra al contenedor:

```bash
docker exec -it kafka bash
```

Luego lanza el producer:

```bash
kafka-console-producer.sh \
  --topic test-topic \
  --bootstrap-server localhost:9092
```

Verás el cursor `>` esperando que escribas. Cada línea que presiones Enter será un mensaje enviado a Kafka.

---

## Paso 8: Abrir el Consumer (Terminal 2)

Abre otra **nueva terminal** y entra al contenedor:

```bash
docker exec -it kafka bash
```

Luego lanza el consumer:

```bash
kafka-console-consumer.sh \
  --topic test-topic \
  --from-beginning \
  --bootstrap-server localhost:9092
```

> `--from-beginning` hace que el consumer lea desde el primer mensaje del topic, no solo los nuevos.

---

## Paso 9: Enviar mensajes y observarlos

En la **Terminal 1 (producer)**, escribe estos 5 mensajes, presionando Enter después de cada uno:

```
>Hola Kafka!
>Este es mi primer mensaje
>Aprendiendo Kafka con Docker
>KRaft mode sin ZooKeeper
>Quinto mensaje exitoso!
```

En la **Terminal 2 (consumer)** verás los mensajes aparecer casi instantáneamente:

```
Hola Kafka!
Este es mi primer mensaje
Aprendiendo Kafka con Docker
KRaft mode sin ZooKeeper
Quinto mensaje exitoso!
```

**Felicidades!** Acabas de enviar y recibir tus primeros mensajes con Kafka.

---

## Paso 10: Explorar más

### Ver detalles de un topic
```bash
kafka-topics.sh --describe --topic test-topic --bootstrap-server localhost:9092
```

Verás algo como:
```
Topic: test-topic  PartitionCount: 3  ReplicationFactor: 1
  Partition: 0  Leader: 1  Replicas: 1  Isr: 1
  Partition: 1  Leader: 1  Replicas: 1  Isr: 1
  Partition: 2  Leader: 1  Replicas: 1  Isr: 1
```

### Consumer con grupo de consumidores
```bash
kafka-console-consumer.sh \
  --topic test-topic \
  --group mi-grupo \
  --bootstrap-server localhost:9092
```

Con un grupo, Kafka recuerda hasta dónde leyó el consumer. Si lo reinicias, continúa desde donde se quedó.

---

## Paso 11: Detener el entorno

Sal del contenedor con `exit`, y desde el directorio `infra/`:

```bash
docker compose down
```

Esto detiene y elimina el contenedor pero **conserva los datos** en el volumen Docker.

Para borrar todo (incluyendo mensajes guardados):
```bash
docker compose down -v
```

---

## Resumen de comandos

| Acción | Comando |
|--------|---------|
| Levantar Kafka | `docker compose up -d` |
| Ver logs | `docker compose logs kafka` |
| Entrar al container | `docker exec -it kafka bash` |
| Crear topic | `kafka-topics.sh --create --topic NAME --partitions 3 --replication-factor 1 --bootstrap-server localhost:9092` |
| Listar topics | `kafka-topics.sh --list --bootstrap-server localhost:9092` |
| Abrir producer | `kafka-console-producer.sh --topic NAME --bootstrap-server localhost:9092` |
| Abrir consumer | `kafka-console-consumer.sh --topic NAME --from-beginning --bootstrap-server localhost:9092` |
| Detener Kafka | `docker compose down` |

---

## Errores comunes

### "Connection refused" al conectar al broker
- Espera 20-30 segundos después de `docker compose up -d` y reintenta
- Verifica con `docker compose logs kafka` que el broker arrancó

### El consumer no recibe mensajes
- Asegúrate de que producer y consumer usan el mismo `--topic` y `--bootstrap-server`
- Verifica que el producer está activo (cursor `>` visible)

### "Topic already exists" al crear el topic
- Normal si ya lo creaste antes. Puedes ignorar el error o usar `--if-not-exists`

### El contenedor no arranca
- Verifica que el puerto 9092 no está ocupado: `netstat -an | grep 9092`
- Revisa los logs: `docker compose logs kafka`
