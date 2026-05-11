package com.nexus.kafka.nivel2;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Utils;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * Producer con clave (key) para el Laboratorio Kafka - Nivel 2.
 *
 * Demuestra como Kafka usa la clave para determinar la particion destino.
 * El algoritmo de particionamiento por defecto:
 *   particion = hash(key) % numero_de_particiones
 *
 * Uso:
 *   "%JAVA_HOME%\bin\java" -cp target\kafka-lab-nivel-2-1.0.0.jar
 *       com.nexus.kafka.nivel2.KeyedProducer <topic> <clave> <valor>
 *
 * Ejemplo:
 *   "%JAVA_HOME%\bin\java" -cp target\kafka-lab-nivel-2-1.0.0.jar
 *       com.nexus.kafka.nivel2.KeyedProducer transacciones-4p user-123 "{\"monto\":100}"
 */
public class KeyedProducer {

    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final DateTimeFormatter FORMATTER =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS")
                             .withZone(ZoneId.systemDefault());

    public static void main(String[] args) {
        if (args.length < 3) {
            System.err.println("Uso: KeyedProducer <topic> <clave> <valor>");
            System.err.println("Ejemplo: KeyedProducer transacciones-4p user-123 \"{\\\"monto\\\":100}\"");
            System.exit(1);
        }

        String topic = args[0];
        String clave = args[1];
        String valor = args[2];

        Properties props = buildProducerProperties();

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {

            // ── Calcular particion teorica ANTES de enviar ──────────
            // Kafka usa murmur2 hash de la clave serializada
            // Esta es la misma logica que usa DefaultPartitioner internamente
            List<PartitionInfo> particiones = producer.partitionsFor(topic);
            int numParticiones = particiones.size();

            // Serializar la clave a bytes (como lo haria el StringSerializer)
            byte[] claveBytes = clave.getBytes(java.nio.charset.StandardCharsets.UTF_8);

            // Calcular hash usando el mismo algoritmo que Kafka (murmur2)
            int hashClave = Utils.murmur2(claveBytes);

            // toPositive convierte a positivo sin perder el rango (evita el problema de Integer.MIN_VALUE)
            int particionTeorica = Utils.toPositive(hashClave) % numParticiones;

            // ── Mostrar calculo antes del envio ─────────────────────
            System.out.println("🔑 Clave del mensaje  : " + clave);
            System.out.println("📊 Hash murmur2       : " + hashClave);
            System.out.println("📐 Particion teorica  : " + hashClave + " % " + numParticiones + " = " + particionTeorica);
            System.out.println("📤 Enviando mensaje...");

            // ── Enviar el record con clave ───────────────────────────
            // Al especificar una clave, Kafka garantiza que todos los mensajes
            // con la misma clave lleguen SIEMPRE a la misma particion
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, clave, valor);
            RecordMetadata metadata = producer.send(record).get();

            // ── Mostrar resultado post-envio ─────────────────────────
            String timestamp = FORMATTER.format(Instant.ofEpochMilli(metadata.timestamp()));
            boolean coincide = metadata.partition() == particionTeorica;
            String verificacion = coincide ? "✓ (coincide con calculo teorico)" : "✗ (diferente al calculo)";

            System.out.println("✅ Mensaje enviado:");
            System.out.println("   Topic     : " + metadata.topic());
            System.out.println("   Partition : " + metadata.partition() + " " + verificacion);
            System.out.println("   Offset    : " + metadata.offset());
            System.out.println("   Timestamp : " + timestamp);
            System.out.println("   Key       : " + clave);
            System.out.println("   Value     : " + valor);

        } catch (InterruptedException e) {
            System.err.println("❌ Hilo interrumpido: " + e.getMessage());
            Thread.currentThread().interrupt();
            System.exit(1);
        } catch (ExecutionException e) {
            System.err.println("❌ Error al enviar: " + e.getCause().getMessage());
            System.err.println("   Verifica que Kafka este corriendo en " + BOOTSTRAP_SERVERS);
            System.exit(1);
        }
    }

    private static Properties buildProducerProperties() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,   StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // acks=all: maxima durabilidad, el broker confirma solo cuando todos los ISR reciben el mensaje
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 3);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "kafka-lab-nivel2-keyed-producer");
        return props;
    }
}
