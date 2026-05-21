# Contexto del proyecto — kafka-week v1.2

## Estado actual

| Leccion | Tema | Codigo | Documento | Estado |
|---------|------|--------|-----------|--------|
| 1 | Ciclo de vida del pedido (Event Sourcing basico) | `PedidoEstadoProducer.java` `PedidoEstadoConsumer.java` | `01-ciclo-vida-pedido.md` | Completa |
| 2 | Dead Letter Topic (DLT) | `PedidoDLTConsumer.java` | `02-dead-letter-topic.md` | Completa |
| 3 | Consumer Groups con rebalanceo visible | `RebalanceoDemo.java` | `03-rebalanceo.md` | Completa |

---

## Estructura de archivos del proyecto

```
kafka-week/
├── infra/
│   └── docker-compose.yml          <- Kafka 4.2.0 KRaft, puerto 9092, volumen kafka-data
├── tiendamax/
│   ├── pom.xml                     <- Java 17, kafka-clients 3.7.1, jackson-databind, slf4j-simple
│   └── src/main/java/com/tiendamax/
│       ├── model/
│       │   └── Pedido.java         <- Modelo: orderId, clienteId, producto, total, estado, timestamp
│       ├── PedidoProducer.java     <- v1.0: producer original (broker remoto 209.2.15.10:9092)
│       ├── PedidoConsumer.java     <- v1.0: consumer con commit manual, 3 servicios
│       ├── PedidoEstadoProducer.java  <- v1.2 L1: ciclo de vida CREADO->PAGADO->ENVIADO->ENTREGADO
│       ├── PedidoEstadoConsumer.java  <- v1.2 L1: reconstruye estado actual desde eventos
│       └── PedidoDLTConsumer.java     <- v1.2 L2: consumer con DLT, headers de error
├── v1.2/
│   ├── CONTEXT.md                  <- este archivo
│   ├── 01-ciclo-vida-pedido.md
│   └── 02-dead-letter-topic.md
├── FUNCIONALIDAD.md                <- teoria general de Kafka (v1.0)
├── TUTORIAL.md                     <- tutorial paso a paso (v1.0)
└── notas.md
```

---

## Topics Kafka usados

| Topic | Particiones | Usado en | Notas |
|-------|-------------|----------|-------|
| `test-topic` | 3 | TUTORIAL.md | Tutorial basico de consola |
| `pedidos-tiendamax` | 3 | v1.0 | Producer apunta a broker remoto 209.2.15.10:9092 |
| `pedidos-estados` | 3 | v1.2 L1 y L2 | Consumer groups: grupo-ciclovida, grupo-dlt |
| `pedidos-estados-dlt` | 3 | v1.2 L2 | Mensajes fallidos con headers de error |
| `pedidos-estados` | 3 | v1.2 L3 | Reutilizado para demo de rebalanceo |

---

## Decisiones tomadas

- **Sin alterar v1.0**: todos los archivos originales (`PedidoProducer`, `PedidoConsumer`, `Pedido`) no se modificaron
- **Mismo proyecto Maven**: las clases v1.2 viven en el mismo `pom.xml` y package `com.tiendamax`
- **Sin acentos ni caracteres especiales** en mensajes de consola (System.out/err) para evitar problemas de encoding en Windows
- **Broker local**: las clases v1.2 apuntan a `localhost:9092` (Docker local), no al broker remoto de v1.0
- **Consumer groups separados**: cada feature usa su propio `group.id` para no interferir entre lecciones

---

## Leccion 3 — Consumer Groups con rebalanceo visible

**Codigo:** `RebalanceoDemo.java`

**Objetivo de aprendizaje:**
- Ver en tiempo real como Kafka redistribuye particiones cuando entra o sale un consumer
- Entender que es un `ConsumerRebalanceListener`
- Observar el periodo de rebalanceo donde ningun consumer lee (stop-the-world)
- Entender por que el numero de consumers activos en un grupo no puede superar el numero de particiones

**Instruccion para el alumno:**
1. Lanzar instancia 1 -> se le asignan las 3 particiones
2. Lanzar instancia 2 -> rebalanceo: cada una recibe ~1-2 particiones
3. Lanzar instancia 3 -> rebalanceo: una particion por consumer
4. Lanzar instancia 4 -> rebalanceo: la cuarta instancia queda sin particion (inactiva)
5. Matar instancia 1 con Ctrl+C -> rebalanceo: sus particiones se redistribuyen

---

## Comandos utiles de referencia

```bash
# Ver todos los topics
docker exec kafka /opt/kafka/bin/kafka-topics.sh --list --bootstrap-server localhost:9092

# Ver detalle de un topic
docker exec kafka /opt/kafka/bin/kafka-topics.sh --describe --topic pedidos-estados --bootstrap-server localhost:9092

# Ver estado de un consumer group
docker exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --describe --group grupo-dlt --bootstrap-server localhost:9092

# Ver todos los consumer groups
docker exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --list --bootstrap-server localhost:9092

# Borrar un topic (para resetear entre pruebas)
docker exec kafka /opt/kafka/bin/kafka-topics.sh --delete --topic pedidos-estados --bootstrap-server localhost:9092

# Compilar
mvn compile -q

# Ejecutar cualquier clase
mvn exec:java -Dexec.mainClass="com.tiendamax.NombreDeLaClase"
```
