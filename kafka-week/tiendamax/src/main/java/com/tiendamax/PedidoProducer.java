package com.tiendamax;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.tiendamax.model.Pedido;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * PASO 2 — Producer de pedidos para TiendaMax.
 *
 * QUÉ RESUELVE:
 *   Antes: REST API → servicio inventario (si cae → pedido perdido)
 *   Ahora: REST API → Kafka (si el servicio cae → pedido guardado, se procesa al reiniciar)
 *
 * CONCEPTOS APLICADOS:
 *   - acks=all          → durabilidad garantizada (Módulo 3)
 *   - enable.idempotence→ sin duplicados en retry (Módulo 3 → PID)
 *   - key = orderId     → orden garantizado por pedido (Módulo 2)
 *   - flush() + close() → ningún mensaje pendiente al cerrar (Módulo 4)
 */
public class PedidoProducer {

    private static final String TOPIC  = "pedidos-tiendamax";
    private static final String BROKER = "209.2.15.10:9092";

    // ObjectMapper es thread-safe y costoso de crear → static
    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static void main(String[] args) throws Exception {

        // ── 1. Configuración del KafkaProducer ───────────────────────────────
        Properties props = new Properties();

        // Punto de entrada al clúster. En prod → "broker1:9092,broker2:9092,broker3:9092"
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKER);

        // acks=all: el broker líder espera que TODAS las ISR repliquen antes de responder.
        // Sin esto, si el líder cae justo después de escribir (antes de replicar), el pedido se pierde.
        props.put(ProducerConfig.ACKS_CONFIG, "all");

        // Activa el productor idempotente: Kafka asigna un PID + sequence number a cada mensaje.
        // Si la red falla y el producer reintenta, Kafka detecta el duplicado y lo descarta.
        // REQUIERE acks=all y retries > 0.
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");

        // Reintentos automáticos ante fallos de red. Con idempotencia son seguros (sin duplicados).
        props.put(ProducerConfig.RETRIES_CONFIG, 3);

        // Espera hasta 5ms para acumular mensajes en un batch antes de enviar.
        // Mejora el throughput al reducir el número de requests de red.
        props.put(ProducerConfig.LINGER_MS_CONFIG, 5);

        // El producer solo maneja bytes. StringSerializer convierte String → byte[].
        // La KEY (orderId) se serializa igual que el VALUE.
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,   StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // ── 2. Pedidos de muestra (simula lo que vendría de la API REST) ──────
        Pedido[] pedidos = {
            new Pedido("ORD-001", "cli-ana",    "Laptop Pro",    1299.00),
            new Pedido("ORD-002", "cli-bob",    "Teclado Mec.",    89.00),
            new Pedido("ORD-003", "cli-ana",    "Mouse Inalámb.",  45.00),
            new Pedido("ORD-004", "cli-carlos", "Monitor 27\"",   399.00),
            new Pedido("ORD-005", "cli-bob",    "Webcam 4K",      129.00),
            new Pedido("ORD-006", "cli-diana",  "Audífonos BT",    79.00),
            new Pedido("ORD-007", "cli-ana",    "SSD 1TB NVMe",   159.00),
            new Pedido("ORD-008", "cli-carlos", "RAM 32GB DDR5",   89.00),
            new Pedido("ORD-009", "cli-diana",  "Mousepad XL",     19.00),
            new Pedido("ORD-010", "cli-bob",    "Hub USB-C 7p",    35.00),
        };

        // ── 3. Enviar cada pedido ─────────────────────────────────────────────
        // try-with-resources → garantiza producer.close() aunque haya excepción
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {

            System.out.println("🚀 Iniciando envío de " + pedidos.length + " pedidos...\n");

            for (Pedido pedido : pedidos) {

                // Serializar el objeto Pedido a JSON String
                String jsonValue = MAPPER.writeValueAsString(pedido);

                // ProducerRecord(topic, key, value)
                // KEY = orderId → murmur2("ORD-001") % 3 = partición fija
                // Todos los eventos futuros de ORD-001 (PAGADO, ENVIADO...) irán a la misma partición.
                ProducerRecord<String, String> record =
                    new ProducerRecord<>(TOPIC, pedido.getOrderId(), jsonValue);

                // send() es ASÍNCRONO. El callback se ejecuta cuando el broker confirma.
                // metadata → información de dónde quedó el mensaje
                // ex       → null si fue exitoso, Exception si falló
                producer.send(record, (metadata, ex) -> {
                    if (ex == null) {
                        System.out.printf("  ✅ %-8s → P-%d  offset=%-3d  key=%s%n",
                            pedido.getOrderId(),
                            metadata.partition(),
                            metadata.offset(),
                            pedido.getOrderId());
                    } else {
                        System.err.printf("  ❌ Error enviando %s: %s%n",
                            pedido.getOrderId(), ex.getMessage());
                    }
                });

                // Pausa para que el log sea legible en el tutorial
                Thread.sleep(150);
            }

            // flush() BLOQUEA hasta que todos los mensajes pendientes en el RecordAccumulator
            // fueron enviados y recibieron ACK. Sin esto, algunos mensajes podrían perderse
            // al llegar al close() antes de que el sender thread los despache.
            producer.flush();

            System.out.println("\n📊 Todos los pedidos enviados con acks=all.");
            System.out.println("   Verifica la distribución en particiones:");
            System.out.println("   docker exec -it kafka /opt/kafka/bin/kafka-run-class.sh \\");
            System.out.println("     kafka.tools.GetOffsetShell --broker-list localhost:9092 \\");
            System.out.println("     --topic pedidos-tiendamax");
        }
        // El try-with-resources llama automáticamente a producer.close() aquí
    }
}
