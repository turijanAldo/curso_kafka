# Notas — Semana Kafka con Docker

## Entorno
- Imagen: `apache/kafka:4.2.0`
- Modo: KRaft (sin ZooKeeper)
- Broker: 1 nodo en `localhost:9092`

---

## 1. Levantar el entorno

```bash
cd kafka-week/infra
docker compose up -d
```

**Resultado esperado:**
```
[+] Running 2/2
 ✔ Volume "infra_kafka-data"  Created
 ✔ Container kafka            Started
```

---

## 2. Verificar que Kafka arrancó correctamente

```bash
docker compose logs kafka | grep "Kafka Server started"
```

powershell w10

```bash
docker-compose logs kafka | sls "Kafka Server started"
```

**Línea que confirma el arranque:**
```
[KafkaServer id=1] started (kafka.server.KafkaServer)
```
> En Kafka 4.x con KRaft, el log relevante es `started` en el `KafkaServer`.  
> Esto indica que el broker está listo para recibir conexiones.

---

## 3. Entrar al contenedor

```bash
docker exec -it kafka bash
```

---

## 4. Crear un topic

```bash
kafka-topics.sh --create \
  --topic test-topic \
  --partitions 3 \
  --replication-factor 1 \
  --bootstrap-server localhost:9092
```

si da error el comando anterior:
```bash
/opt/kafka/bin/kafka-topics.sh --create --topic test-topic --partitions 3 --replication-factor 1 --bootstrap-server localhost:9092
```


**Resultado:**
```
Created topic test-topic.
```

**¿Por qué 3 particiones?**  
Permite que hasta 3 consumers en el mismo grupo lean en paralelo, uno por partición. Es la base del escalado horizontal en Kafka.

**¿Por qué replication-factor 1?**  
Con un solo broker no podemos replicar a otros nodos. En producción se usa 3 o más.

---

## 5. Listar topics

```bash
kafka-topics.sh --list --bootstrap-server localhost:9092
```

listar topics:

```bash
/opt/kafka/bin/kafka-topics.sh --list --bootstrap-server localhost:9092
```


**Resultado:**
```
test-topic
```

---

## 6. Producer (Terminal 1)

```bash
kafka-console-producer.sh --topic test-topic --bootstrap-server localhost:9092
```

```bash
/opt/kafka/bin/kafka-console-producer.sh --topic test-topic --bootstrap-server localhost:9092
```


Mensajes enviados:
```
>Hola Kafka!
>Este es mi primer mensaje
>Aprendiendo Kafka con Docker
>KRaft mode sin ZooKeeper
>Quinto mensaje exitoso!
```

---

## 7. Consumer (Terminal 2)

```bash
kafka-console-consumer.sh \
  --topic test-topic \
  --from-beginning \
  --bootstrap-server localhost:9092
```

```bash
/opt/kafka/bin/kafka-console-consumer.sh --topic test-topic --from-beginning --bootstrap-server localhost:9092
```


**Mensajes recibidos:**
```
Hola Kafka!
Este es mi primer mensaje
Aprendiendo Kafka con Docker
KRaft mode sin ZooKeeper
Quinto mensaje exitoso!
```

> **Nota:** El orden de los mensajes puede variar ligeramente si van a distintas particiones, pero todos llegan al consumer.

**¿Qué significa `--from-beginning`?**  
El consumer lee desde el offset 0 de cada partición, es decir, desde el primer mensaje que fue enviado al topic, no solo los nuevos.

---

## 8. Observaciones

- Los mensajes aparecen en el consumer **casi en tiempo real** (< 1 segundo de latencia en local)
- `--from-beginning` es útil para ver el historial completo del topic
- Al cerrar y reabrir el consumer sin `--from-beginning`, solo recibirá mensajes **nuevos**
- Kafka persiste los mensajes en disco; si se detiene y reinicia el broker, los mensajes siguen ahí

---

## 9. Detener el entorno

```bash
docker compose down
```

Para destruir también los volúmenes (y borrar los mensajes):
```bash
docker compose down -v
```
