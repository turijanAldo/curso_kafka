package com.nexus.kafka.nivel1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Consumer simple para el Laboratorio Kafka - Nivel 1.
 * Uso: java -cp target/kafka-lab-nivel-1-1.0.0.jar com.nexus.kafka.nivel1.SimpleConsumer <topic> <groupId>
 */
public class SimpleConsumer {

    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final DateTimeFormatter FORMATTER =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS")
                             .withZone(ZoneId.systemDefault());

    // Flag para shutdown limpio desde el hook de Ctrl+C
    private static final AtomicBoolean CORRIENDO = new AtomicBoolean(true);

    public static void main(String[] args) {
        if (args.length < 2) {
            System.err.println("Uso: SimpleConsumer <topic> <groupId>");
            System.err.println("Ejemplo: SimpleConsumer primer-topic grupo-prueba");
            System.exit(1);
        }

        String topic   = args[0];
        String groupId = args[1];

        // Shutdown hook: se ejecuta cuando el usuario presiona Ctrl+C
        // Pone CORRIENDO en false para que el loop de poll termine limpiamente
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("\n🔴 Señal de cierre recibida. Cerrando consumer...");
            CORRIENDO.set(false);
        }));

        Properties props = buildConsumerProperties(groupId);

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {

            // Suscribirse al topic (la asignacion de particiones es dinamica por el grupo)
            consumer.subscribe(Collections.singletonList(topic));

            System.out.println("🔵 Consumer iniciado - Topic: " + topic + ", Group: " + groupId);
            System.out.println("   Escuchando mensajes... (Ctrl+C para detener)");

            while (CORRIENDO.get()) {
                // poll(): solicitar mensajes al broker con un timeout de 1 segundo
                // Si no hay mensajes en 1s, devuelve coleccion vacia y vuelve a intentar
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

                for (ConsumerRecord<String, String> record : records) {
                    String timestamp = FORMATTER.format(Instant.ofEpochMilli(record.timestamp()));

                    System.out.println("📨 Mensaje recibido"
                            + " - Partition: " + record.partition()
                            + ", Offset: "     + record.offset()
                            + ", Key: "        + record.key()
                            + ", Value: "      + record.value()
                            + ", Timestamp: "  + timestamp);
                }
            }

        } catch (Exception e) {
            System.err.println("❌ Error en el consumer: " + e.getMessage());
            System.err.println("   Verifica que Kafka este corriendo en " + BOOTSTRAP_SERVERS);
            System.exit(1);
        }

        System.out.println("✅ Consumer cerrado limpiamente.");
    }

    private static Properties buildConsumerProperties(String groupId) {
        Properties props = new Properties();

        // Direccion del broker Kafka
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);

        // ID del grupo de consumidores
        // Los consumers del mismo grupo se reparten las particiones del topic
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);

        // Deserializadores: convierten bytes de la red a String Java
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,   StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        // earliest: leer desde el inicio del topic si no hay offset guardado para este grupo
        // (util para no perderse mensajes enviados antes de que el consumer iniciara)
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // enable.auto.commit=true: Kafka guarda automaticamente el offset cada 5 segundos
        // En produccion se puede cambiar a false para control manual
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "5000");

        // ID del cliente (aparece en logs del broker)
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "kafka-lab-nivel1-consumer");

        return props;
    }
}
