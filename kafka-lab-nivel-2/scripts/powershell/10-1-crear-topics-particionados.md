# Guía de configuración de Topics en Kafka

## 🚀 1. Verifica que Kafka esté corriendo
```bash
docker inspect kafka-nivel1 --format "{{.State.Status}}"
```

👉 Debe salir: running
📋 2. Ver todos los topics (por si ya existen)
bash

```bash
docker exec kafka-nivel1 /opt/kafka/bin/kafka-topics.sh \
--bootstrap-server localhost:9092 \
--list
```

🧹 3. (Opcional) Eliminar topics si ya existen

    Nota: Hazlo solo si quieres empezar limpio

```bash
docker exec kafka-nivel1 /opt/kafka/bin/kafka-topics.sh \
--bootstrap-server localhost:9092 \
--delete \
--topic transacciones-1p

docker exec kafka-nivel1 /opt/kafka/bin/kafka-topics.sh \
--bootstrap-server localhost:9092 \
--delete \
--topic transacciones-4p

docker exec kafka-nivel1 /opt/kafka/bin/kafka-topics.sh \
--bootstrap-server localhost:9092 \
--delete \
--topic transacciones-8p
```

🏗️ 4. Crear los topics
🔹 Topic con 1 partición (máximo orden)


```bash
docker exec kafka-nivel1 /opt/kafka/bin/kafka-topics.sh \
--bootstrap-server localhost:9092 \
--create \
--topic transacciones-1p \
--partitions 1 \
--replication-factor 1
```

🔹 Topic con 4 particiones (balance)
bash

```bash
docker exec kafka-nivel1 /opt/kafka/bin/kafka-topics.sh \
--bootstrap-server localhost:9092 \
--create \
--topic transacciones-4p \
--partitions 4 \
--replication-factor 1
```

🔹 Topic con 8 particiones (máximo paralelismo)
bash

```bash
docker exec kafka-nivel1 /opt/kafka/bin/kafka-topics.sh \
--bootstrap-server localhost:9092 \
--create \
--topic transacciones-8p \
--partitions 8 \
--replication-factor 1
```

🔍 5. Ver detalle de cada topic
```bash
docker exec kafka-nivel1 /opt/kafka/bin/kafka-topics.sh \
--bootstrap-server localhost:9092 \
--describe \
--topic transacciones-1p
```

(Repite para los otros)
📊 6. Ver todos los topics al final

```bash
docker exec kafka-nivel1 /opt/kafka/bin/kafka-topics.sh \
--bootstrap-server localhost:9092 \
--list
```

🧠 Lo que debes observar (esto es lo importante)
```bash
    transacciones-1p → solo 1 consumer puede procesar

    transacciones-4p → hasta 4 consumers en paralelo

    transacciones-8p → hasta 8 consumers
```


