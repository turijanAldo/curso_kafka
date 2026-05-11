# 🧪 Kafka Nivel 2 — Descripción de Particiones

Este laboratorio te permite inspeccionar cómo Kafka organiza internamente
los topics en particiones, así como sus offsets y configuración.

---

## 🧠 Objetivo

Aprender a interpretar:
```bash
- Particiones
- Leader
- Replicas
- ISR (In-Sync Replicas)
- Offsets (earliest / latest)
```
---

## 🚀 1. Verificar que Kafka esté corriendo

```bash
docker inspect kafka-nivel1 --format "{{.State.Status}}"
```
✅ Debe devolver:

running
🔍 2. Describir estructura de particiones
```bash

Ejecuta esto para cada topic:

🔹 transacciones-1p
docker exec kafka-nivel1 /opt/kafka/bin/kafka-topics.sh \
--bootstrap-server localhost:9092 \
--describe \
--topic transacciones-1p
🔹 transacciones-4p
docker exec kafka-nivel1 /opt/kafka/bin/kafka-topics.sh \
--bootstrap-server localhost:9092 \
--describe \
--topic transacciones-4p
🔹 transacciones-8p
docker exec kafka-nivel1 /opt/kafka/bin/kafka-topics.sh \
--bootstrap-server localhost:9092 \
--describe \
--topic transacciones-8p
⚙️ 3. Ver configuración del topic
docker exec kafka-nivel1 /opt/kafka/bin/kafka-configs.sh \
--bootstrap-server localhost:9092 \
--describe \
--entity-type topics \
--entity-name transacciones-4p
```
👉 Puedes cambiar el nombre del topic para inspeccionar otros.

📊 4. Ver offsets por partición
```bash
docker exec kafka-nivel1 /opt/kafka/bin/kafka-run-class.sh kafka.tools.GetOffsetShell \
--bootstrap-server localhost:9092 \
--topic transacciones-4p \
--time -1
```
🔎 --time -1 = latest offset (último mensaje)

🧠 Cómo interpretar la salida
📌 Campos importantes
Partition
Número de partición (empieza en 0)
Leader
Broker que maneja esa partición
Replicas
Brokers que tienen copia
Isr (In-Sync Replicas)
Réplicas actualizadas (sin retraso)
⚠️ Importante en tu laboratorio

Como solo tienes 1 broker:

Leader = 1
Replicas = 1
Isr = 1

👉 Siempre serán iguales

🏭 En producción (lo que cambiaría)

Con múltiples brokers verías algo como:

Partition: 0
Leader: 2
Replicas: 1,2,3
Isr: 2,3

👉 Aquí ya puedes analizar tolerancia a fallos

🔥 Qué debes observar
transacciones-1p
Solo 1 partición → sin paralelismo
transacciones-4p
4 particiones → distribución de carga
transacciones-8p
Más particiones → más throughput potencial
🧪 Siguiente paso recomendado

👉 Producir mensajes y ver cómo cambian los offsets:

Enviar eventos
Volver a ejecutar GetOffsetShell
Observar crecimiento por partición
🧠 Insight clave

Kafka no escala por CPU… escala por particiones

Más particiones = más consumidores trabajando en paralelo