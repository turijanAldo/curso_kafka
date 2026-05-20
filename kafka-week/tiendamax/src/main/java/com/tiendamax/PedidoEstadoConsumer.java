package com.tiendamax;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.tiendamax.model.Pedido;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * V1.2 - Feature 1: Consumer que reconstruye el estado actual de cada pedido.
 *
 * QUE DEMUESTRA:
 *   El topic es la fuente de verdad (event sourcing basico).
 *   Leyendo los eventos en orden reconstruimos el estado actual de cada pedido
 *   sin necesitar una base de datos de estado separada.
 *
 *   Gracias a que KEY = orderId en el producer, el consumer siempre recibe
 *   los eventos de ORD-001 en orden: CREADO -> PAGADO -> ENVIADO -> ENTREGADO.
 *   Nunca veras ENTREGADO antes de CREADO dentro de una particion.
 *
 * PATRON CLAVE:
 *   Map<orderId, estadoActual> actua como una "proyeccion" del stream de eventos.
 *   Cada evento actualiza el estado. Al final tienes la vista mas reciente.
 *   Esto es la base de CQRS + Event Sourcing.
 *
 * EJECUTAR (terminal separada, mientras el producer corre o despues):
 *   mvn exec:java -Dexec.mainClass="com.tiendamax.PedidoEstadoConsumer"
 */
public class PedidoEstadoConsumer {

    private static final String TOPIC    = "pedidos-estados";
    private static final String BROKER   = "localhost:9092";
    // Grupo distinto al de v1.0 -> offset independiente, lee desde el principio
    private static final String GROUP_ID = "grupo-ciclovida";

    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static void main(String[] args) {

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKER);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        // earliest: la primera vez que corre este grupo lee desde offset 0
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,   StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        // Proyeccion del stream: orderId -> estado mas reciente
        // Esta es la vista materializada construida a partir de los eventos.
        Map<String, String> estadoActual = new HashMap<>();

        // Para rastrear la particion de cada orderId (confirma que KEY = misma particion)
        Map<String, Integer> particionPorOrder = new HashMap<>();

        Thread mainThread = Thread.currentThread();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println();
            System.out.println("Cerrando consumer...");
            mainThread.interrupt();
        }));

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {

            consumer.subscribe(List.of(TOPIC));

            System.out.println("Escuchando [" + TOPIC + "] como grupo [" + GROUP_ID + "]");
            System.out.println("Reconstruyendo estado desde eventos...");
            System.out.println();

            while (!Thread.currentThread().isInterrupted()) {

                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

                if (records.isEmpty()) continue;

                System.out.println("-- Poll: " + records.count() + " eventos recibidos --");

                for (ConsumerRecord<String, String> record : records) {
                    try {
                        Pedido pedido = MAPPER.readValue(record.value(), Pedido.class);

                        String orderId    = record.key();
                        int    particion  = record.partition();
                        long   offset     = record.offset();
                        String estadoPrev = estadoActual.getOrDefault(orderId, "NINGUNO");

                        // Actualizar la proyeccion con el nuevo estado
                        estadoActual.put(orderId, pedido.getEstado());

                        // Registrar en que particion vive este orderId
                        particionPorOrder.put(orderId, particion);

                        System.out.printf("  P%d offset=%-3d  %-8s  %-10s -> %s%n",
                            particion, offset,
                            orderId,
                            estadoPrev, pedido.getEstado());

                    } catch (Exception e) {
                        System.err.printf("  Error parseando offset=%d: %s%n",
                            record.offset(), e.getMessage());
                    }
                }

                consumer.commitSync();

                System.out.println();
                imprimirEstadoActual(estadoActual, particionPorOrder);
            }

        } catch (Exception e) {
            if (!Thread.currentThread().isInterrupted()) {
                System.err.println("Error inesperado: " + e.getMessage());
            }
        }

        System.out.println("Consumer cerrado.");
    }

    /**
     * Muestra el estado actual reconstruido desde los eventos.
     * Equivalente a un SELECT * FROM pedidos en una DB tradicional,
     * pero derivado unicamente del stream de eventos de Kafka.
     */
    private static void imprimirEstadoActual(Map<String, String> estados,
                                              Map<String, Integer> particiones) {
        System.out.println("  Estado actual reconstruido desde eventos:");
        estados.entrySet().stream()
            .sorted(Map.Entry.comparingByKey())
            .forEach(e -> {
                int p = particiones.getOrDefault(e.getKey(), -1);
                System.out.printf("    %-8s  %-12s  (fijo en P%d)%n",
                    e.getKey(), e.getValue(), p);
            });
        System.out.println();
    }
}
