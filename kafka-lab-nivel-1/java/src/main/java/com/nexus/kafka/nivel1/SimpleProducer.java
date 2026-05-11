package com.nexus.kafka.nivel1;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * Producer simple para el Laboratorio Kafka - Nivel 1.
 * Uso: java -cp target/kafka-lab-nivel-1-1.0.0.jar com.nexus.kafka.nivel1.SimpleProducer <topic> <mensaje>
 */
public class SimpleProducer {

    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final DateTimeFormatter FORMATTER =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS")
                             .withZone(ZoneId.systemDefault());

    public static void main(String[] args) {
        if (args.length < 2) {
            System.err.println("Uso: SimpleProducer <topic> <mensaje>");
            System.err.println("Ejemplo: SimpleProducer primer-topic \"Hola Kafka desde Nivel 1\"");
            System.exit(1);
        }

        String topic   = args[0];
        String mensaje = args[1];

        Properties props = buildProducerProperties();

        // try-with-resources garantiza que el producer se cierre siempre,
        // liberando conexiones y flusheando mensajes pendientes
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {

            System.out.println("Enviando mensaje al topic [" + topic + "]: " + mensaje);

            // ProducerRecord: (topic, key, value)
            // key=null -> Kafka distribuye el mensaje entre particiones en round-robin
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, null, mensaje);

            // send() es asincrono; .get() lo vuelve sincrono y lanza excepcion si falla
            RecordMetadata metadata = producer.send(record).get();

            String timestamp = FORMATTER.format(Instant.ofEpochMilli(metadata.timestamp()));

            System.out.println("✅ Mensaje enviado exitosamente"
                    + " - Topic: "     + metadata.topic()
                    + ", Partition: "  + metadata.partition()
                    + ", Offset: "     + metadata.offset()
                    + ", Timestamp: "  + timestamp);

        } catch (InterruptedException e) {
            System.err.println("❌ El hilo fue interrumpido mientras esperaba confirmacion: " + e.getMessage());
            Thread.currentThread().interrupt();
            System.exit(1);
        } catch (ExecutionException e) {
            System.err.println("❌ Error al enviar el mensaje: " + e.getCause().getMessage());
            System.err.println("   Verifica que Kafka este corriendo en " + BOOTSTRAP_SERVERS);
            System.exit(1);
        }
    }

    private static Properties buildProducerProperties() {
        Properties props = new Properties();

        // Direccion del broker Kafka
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);

        // Serializadores: convierten String Java a bytes para la red
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,   StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // acks=all: el broker lider espera que TODOS los replicas in-sync confirmen
        // antes de responder. Maxima durabilidad (mas lento que acks=1 o acks=0)
        props.put(ProducerConfig.ACKS_CONFIG, "all");

        // retries: cuantas veces reintentar si hay un error de red transitorio
        props.put(ProducerConfig.RETRIES_CONFIG, 3);

        // linger.ms=0: enviar inmediatamente sin esperar acumular mensajes en batch
        props.put(ProducerConfig.LINGER_MS_CONFIG, 0);

        // ID del cliente (aparece en logs del broker, util para debugging)
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "kafka-lab-nivel1-producer");

        return props;
    }
}
