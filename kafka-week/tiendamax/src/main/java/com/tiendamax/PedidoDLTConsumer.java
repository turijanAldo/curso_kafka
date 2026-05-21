package com.tiendamax;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.tiendamax.model.Pedido;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Properties;

/**
 * V1.2 - Feature 2: Dead Letter Topic (DLT)
 *
 * QUE DEMUESTRA:
 *   Cuando un consumer no puede procesar un mensaje (excepcion, validacion fallida,
 *   dato corrupto), tiene dos opciones malas y una buena:
 *
 *   MALA 1 - Ignorar el error y hacer commit:
 *     El mensaje se pierde para siempre. Nadie sabe que fallo.
 *
 *   MALA 2 - Reintentar infinitamente sin commit:
 *     El consumer se queda bloqueado en ese mensaje. Los mensajes siguientes
 *     nunca se procesan. El lag del consumer crece hasta el infinito.
 *
 *   BUENA - Dead Letter Topic:
 *     El mensaje fallido se publica en un topic separado (DLT) con metadata
 *     del error. Se hace commit y se continua procesando. El equipo puede
 *     inspeccionar el DLT, corregir el problema y replay los mensajes.
 *
 * REGLA DE NEGOCIO QUE FALLA (simulada):
 *   Pedidos con total > 200 requieren aprobacion manual.
 *   El consumer lanza excepcion para esos pedidos -> van al DLT.
 *   ORD-001 ($1299) y ORD-003 ($399) fallaran.
 *   ORD-002 ($89) se procesara correctamente.
 *
 * HEADERS EN KAFKA:
 *   Los mensajes de Kafka pueden llevar headers (clave-valor en bytes),
 *   similares a los HTTP headers. Los usamos para registrar metadata del error
 *   sin modificar el value original del mensaje fallido.
 *
 * TOPICS NECESARIOS:
 *   - pedidos-estados      (fuente, ya creado en Leccion 1)
 *   - pedidos-estados-dlt  (destino de mensajes fallidos, crear en Paso 1)
 *
 * EJECUTAR:
 *   Terminal A: mvn exec:java -Dexec.mainClass="com.tiendamax.PedidoDLTConsumer"
 *   Terminal B: mvn exec:java -Dexec.mainClass="com.tiendamax.PedidoEstadoProducer"
 *   Terminal C (inspeccionar DLT):
 *     docker exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
 *       --topic pedidos-estados-dlt --from-beginning \
 *       --bootstrap-server localhost:9092
 */
public class PedidoDLTConsumer {

    private static final String TOPIC     = "pedidos-estados";
    private static final String DLT_TOPIC = "pedidos-estados-dlt";
    private static final String BROKER    = "localhost:9092";
    private static final String GROUP_ID  = "grupo-dlt";

    // Limite de aprobacion automatica (pedidos mayores van al DLT)
    private static final double LIMITE_AUTO_APROBACION = 200.0;

    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static void main(String[] args) {

        // ── Consumer: lee de pedidos-estados ────────────────────────────────
        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKER);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,   StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        // ── Producer: escribe en pedidos-estados-dlt ─────────────────────────
        // El mismo proceso actua como consumer Y producer al mismo tiempo.
        // Esto es el patron "consumer-transform-produce" (base de Kafka Streams).
        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKER);
        producerProps.put(ProducerConfig.ACKS_CONFIG, "all");
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,   StringSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        Thread mainThread = Thread.currentThread();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println();
            System.out.println("Cerrando consumer DLT...");
            mainThread.interrupt();
        }));

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps);
             KafkaProducer<String, String> dltProducer = new KafkaProducer<>(producerProps)) {

            consumer.subscribe(List.of(TOPIC));

            System.out.println("Consumer DLT escuchando [" + TOPIC + "]");
            System.out.println("Mensajes que fallen iran a [" + DLT_TOPIC + "]");
            System.out.println("Regla: pedidos con total > $" + LIMITE_AUTO_APROBACION + " requieren aprobacion manual");
            System.out.println();

            while (!Thread.currentThread().isInterrupted()) {

                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                if (records.isEmpty()) continue;

                System.out.println("-- Poll: " + records.count() + " mensajes --");

                for (ConsumerRecord<String, String> record : records) {
                    try {
                        Pedido pedido = MAPPER.readValue(record.value(), Pedido.class);

                        // Intentar procesar el pedido. Lanza excepcion si falla la validacion.
                        procesarPedido(pedido);

                        System.out.printf("  OK      P%d offset=%-3d  %-8s  $%-8.2f  %s%n",
                            record.partition(), record.offset(),
                            record.key(), pedido.getTotal(), pedido.getEstado());

                    } catch (Exception e) {
                        // El procesamiento fallo. En lugar de perder el mensaje o bloquearnos,
                        // lo enviamos al DLT con metadata del error en los headers.
                        enviarAlDLT(dltProducer, record, e.getMessage());

                        System.out.printf("  DLT     P%d offset=%-3d  %-8s  -> [%s] %s%n",
                            record.partition(), record.offset(),
                            record.key(), DLT_TOPIC, e.getMessage());
                    }
                }

                // Commit DESPUES de procesar todo el batch.
                // Los mensajes exitosos se procesan, los fallidos van al DLT.
                // Ninguno bloquea al siguiente.
                consumer.commitSync();
                System.out.println("  Commit realizado. Continuando con los siguientes mensajes.");
                System.out.println();
            }

        } catch (Exception e) {
            if (!Thread.currentThread().isInterrupted()) {
                System.err.println("Error inesperado: " + e.getMessage());
            }
        }

        System.out.println("Consumer cerrado.");
    }

    /**
     * Simula la logica de negocio que valida el pedido.
     * En produccion aqui iria: llamada a DB, validacion de stock, verificacion de fraude, etc.
     * Lanza excepcion si la validacion falla (el pedido ira al DLT).
     */
    private static void procesarPedido(Pedido pedido) throws Exception {
        if (pedido.getTotal() > LIMITE_AUTO_APROBACION) {
            throw new Exception("Requiere aprobacion manual: total $" + pedido.getTotal() + " supera el limite $" + LIMITE_AUTO_APROBACION);
        }
        // Simular tiempo de procesamiento
        Thread.sleep(50);
    }

    /**
     * Publica el mensaje fallido en el DLT con headers que describen el error.
     *
     * Headers que agregamos:
     *   error-message      : descripcion del error
     *   original-topic     : de donde vino el mensaje
     *   original-partition : en que particion estaba
     *   original-offset    : cual era su offset
     *   failed-at-ms       : timestamp del fallo en epoch ms
     *
     * El VALUE del mensaje DLT es identico al original.
     * El KEY es identico al original.
     * Solo se agrega informacion, nunca se modifica el contenido original.
     */
    private static void enviarAlDLT(KafkaProducer<String, String> producer,
                                     ConsumerRecord<String, String> original,
                                     String errorMessage) {

        ProducerRecord<String, String> dltRecord =
            new ProducerRecord<>(DLT_TOPIC, original.key(), original.value());

        // Agregar metadata del error como headers (no modifica el value original)
        dltRecord.headers()
            .add("error-message",       errorMessage.getBytes(StandardCharsets.UTF_8))
            .add("original-topic",      original.topic().getBytes(StandardCharsets.UTF_8))
            .add("original-partition",  String.valueOf(original.partition()).getBytes(StandardCharsets.UTF_8))
            .add("original-offset",     String.valueOf(original.offset()).getBytes(StandardCharsets.UTF_8))
            .add("failed-at-ms",        String.valueOf(System.currentTimeMillis()).getBytes(StandardCharsets.UTF_8));

        producer.send(dltRecord, (meta, ex) -> {
            if (ex != null) {
                // Si falla el envio al DLT, al menos queda en stderr
                System.err.println("  ERROR enviando al DLT: " + ex.getMessage());
            }
        });

        // flush() para garantizar que el mensaje llego al DLT antes de hacer commitSync()
        producer.flush();
    }
}
