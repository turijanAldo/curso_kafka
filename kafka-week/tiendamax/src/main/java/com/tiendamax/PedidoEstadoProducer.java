package com.tiendamax;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.tiendamax.model.Pedido;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.List;
import java.util.Properties;

/**
 * V1.2 - Feature 1: Ciclo de vida del pedido (Event Sourcing basico)
 *
 * QUE DEMUESTRA:
 *   Un mismo orderId emite multiples eventos de estado a lo largo del tiempo:
 *   CREADO -> PAGADO -> ENVIADO -> ENTREGADO
 *
 *   KEY = orderId -> murmur2(orderId) % partitions = particion fija
 *   Todos los eventos de ORD-001 siempre caen en la misma particion.
 *   La misma particion = orden de escritura garantizado.
 *   El consumer leera CREADO antes que PAGADO, siempre.
 *
 * POR QUE IMPORTA LA KEY:
 *   Sin key (null) -> round-robin entre particiones -> los eventos de un mismo
 *   pedido pueden llegar desordenados al consumer (ENVIADO antes que PAGADO).
 *   Con key=orderId -> siempre a la misma particion -> orden garantizado.
 *
 * TOPIC: pedidos-estados (distinto al de v1.0 para no interferir)
 *
 * CREAR EL TOPIC PRIMERO (desde dentro del contenedor):
 *   kafka-topics.sh --create --topic pedidos-estados
 *     --partitions 3 --replication-factor 1
 *     --bootstrap-server localhost:9092
 *
 * EJECUTAR:
 *   mvn exec:java -Dexec.mainClass="com.tiendamax.PedidoEstadoProducer"
 *
 * LUEGO: ejecutar PedidoEstadoConsumer en otra terminal para ver el estado reconstruido.
 */
public class PedidoEstadoProducer {

    private static final String TOPIC  = "pedidos-estados";
    private static final String BROKER = "localhost:9092";

    private static final ObjectMapper MAPPER = new ObjectMapper();

    // Ciclo de vida completo de un pedido en TiendaMax
    private static final String[] CICLO = {"CREADO", "PAGADO", "ENVIADO", "ENTREGADO"};

    public static void main(String[] args) throws Exception {

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKER);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,   StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        List<Pedido> pedidos = List.of(
            new Pedido("ORD-001", "cli-ana",    "Laptop Pro",   1299.00),
            new Pedido("ORD-002", "cli-bob",    "Teclado Mec",    89.00),
            new Pedido("ORD-003", "cli-carlos", "Monitor 27",    399.00)
        );

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {

            System.out.println("Emitiendo ciclo de vida para " + pedidos.size() + " pedidos...");
            System.out.println("KEY = orderId -> todos los eventos de un pedido van a la MISMA particion");
            System.out.println();

            // Cada iteracion representa un momento en el tiempo.
            // Los pedidos progresan a distintas velocidades (realista).
            for (int estadoIdx = 0; estadoIdx < CICLO.length; estadoIdx++) {
                String estado = CICLO[estadoIdx];

                System.out.println("-- Transicion: " + estado + " --");

                // No todos los pedidos avanzan al mismo tiempo
                int cantidadQueAvanzan = pedidos.size() - estadoIdx / 2;

                for (int i = 0; i < cantidadQueAvanzan && i < pedidos.size(); i++) {
                    Pedido pedido = pedidos.get(i);
                    pedido.setEstado(estado);
                    pedido.setTimestamp(System.currentTimeMillis());

                    String json = MAPPER.writeValueAsString(pedido);

                    // ProducerRecord(topic, KEY, value)
                    // La KEY garantiza que ORD-001 siempre va a la misma particion.
                    // Sin KEY -> Kafka haria round-robin -> el orden inter-particion no esta garantizado.
                    ProducerRecord<String, String> record =
                        new ProducerRecord<>(TOPIC, pedido.getOrderId(), json);

                    producer.send(record, (meta, ex) -> {
                        if (ex == null) {
                            System.out.printf("  OK %-8s %-12s -> P%d  offset=%d%n",
                                pedido.getOrderId(), estado,
                                meta.partition(), meta.offset());
                        } else {
                            System.err.printf("  ERROR %s: %s%n",
                                pedido.getOrderId(), ex.getMessage());
                        }
                    });
                }

                producer.flush();
                Thread.sleep(800);
                System.out.println();
            }

            System.out.println("------------------------------------------------------");
            System.out.println("Observa en el consumer que los eventos de cada");
            System.out.println("orderId siempre estan en la MISMA particion y en orden.");
        }
    }
}
