package com.nexus.kafka.nivel2;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

/**
 * Consumer instrumentado para el Laboratorio Kafka - Nivel 2.
 *
 * Muestra claramente:
 *   - Que particiones le fueron asignadas por el coordinator del grupo
 *   - Cuando ocurre un rebalanceo (asignacion/revocacion de particiones)
 *   - Cuantos mensajes ha procesado por particion
 *
 * Uso:
 *   "%JAVA_HOME%\bin\java" -cp target\kafka-lab-nivel-2-1.0.0.jar
 *       com.nexus.kafka.nivel2.InstrumentedConsumer <topic> <groupId> <consumerId>
 *
 * Ejemplo:
 *   "%JAVA_HOME%\bin\java" -cp target\kafka-lab-nivel-2-1.0.0.jar
 *       com.nexus.kafka.nivel2.InstrumentedConsumer transacciones-4p grupo-nivel-2 consumer-1
 */
public class InstrumentedConsumer {

    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final DateTimeFormatter FORMATTER =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS")
                             .withZone(ZoneId.systemDefault());

    private static final AtomicBoolean CORRIENDO = new AtomicBoolean(true);

    // Contador de mensajes procesados por particion
    private static final Map<Integer, Long> contadorPorParticion = new HashMap<>();

    public static void main(String[] args) {
        if (args.length < 3) {
            System.err.println("Uso: InstrumentedConsumer <topic> <groupId> <consumerId>");
            System.exit(1);
        }

        String topic      = args[0];
        String groupId    = args[1];
        String consumerId = args[2];

        // Shutdown hook: manejo limpio de Ctrl+C
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("\n🔴 [" + consumerId + "] Cierre solicitado. Terminando...");
            CORRIENDO.set(false);
        }));

        Properties props = buildConsumerProperties(groupId, consumerId);

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {

            // ── ConsumerRebalanceListener: detecta cambios en asignacion ──
            // Se llama automaticamente por Kafka cuando el grupo hace rebalanceo
            consumer.subscribe(Collections.singletonList(topic), new ConsumerRebalanceListener() {

                @Override
                public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                    // Se llama ANTES del rebalanceo, con las particiones que se van a quitar
                    if (!partitions.isEmpty()) {
                        String lista = partitions.stream()
                                .map(p -> String.valueOf(p.partition()))
                                .collect(Collectors.joining(", "));
                        System.out.println("⚠️  [" + consumerId + "] REVOCADAS: Partitions [" + lista + "]");
                        System.out.println("    El grupo esta haciendo rebalanceo...");
                    }
                }

                @Override
                public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                    // Se llama DESPUES del rebalanceo, con las particiones ahora asignadas a este consumer
                    String lista = partitions.stream()
                            .map(p -> String.valueOf(p.partition()))
                            .collect(Collectors.joining(", "));
                    System.out.println("🎯 [" + consumerId + "] ASIGNADO A: Partitions [" + lista + "]");
                    System.out.println("    Este consumer leera SOLO estas particiones.");

                    // Inicializar contadores para las particiones asignadas
                    partitions.forEach(p -> contadorPorParticion.putIfAbsent(p.partition(), 0L));
                }
            });

            System.out.println("🔵 [" + consumerId + "] Consumer iniciado");
            System.out.println("   Topic  : " + topic);
            System.out.println("   Group  : " + groupId);
            System.out.println("   ID     : " + consumerId);
            System.out.println("   Esperando asignacion de particiones... (Ctrl+C para detener)");

            // ── Loop principal de consumo ────────────────────────────
            while (CORRIENDO.get()) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

                for (ConsumerRecord<String, String> record : records) {
                    String timestamp = FORMATTER.format(Instant.ofEpochMilli(record.timestamp()));

                    // Incrementar contador de la particion
                    contadorPorParticion.merge(record.partition(), 1L, Long::sum);
                    long totalEnParticion = contadorPorParticion.get(record.partition());

                    System.out.printf("📨 [%s] Part=%d | Offset=%d | Key=%-12s | Value=%s | %s | (msg #%d en esta particion)%n",
                            consumerId,
                            record.partition(),
                            record.offset(),
                            record.key(),
                            record.value(),
                            timestamp,
                            totalEnParticion);
                }
            }

            // ── Mostrar resumen al cerrar ────────────────────────────
            System.out.println("\n📈 [" + consumerId + "] Resumen de mensajes procesados:");
            contadorPorParticion.entrySet().stream()
                    .sorted(Map.Entry.comparingByKey())
                    .forEach(e -> System.out.println("   Partition " + e.getKey() + ": " + e.getValue() + " mensajes"));

        } catch (Exception e) {
            System.err.println("❌ [" + consumerId + "] Error: " + e.getMessage());
            System.exit(1);
        }

        System.out.println("✅ [" + consumerId + "] Consumer cerrado limpiamente.");
    }

    private static Properties buildConsumerProperties(String groupId, String consumerId) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,   StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        // earliest: leer desde el inicio si no hay offset guardado para este grupo
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        // session.timeout: si el consumer no envia heartbeat en 30s, el broker lo considera muerto
        // y hace rebalanceo (redistribuye sus particiones a los consumers vivos)
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
        // ID unico para este consumer dentro del grupo (aparece en kafka-consumer-groups --describe)
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "kafka-lab-nivel2-" + consumerId);
        return props;
    }
}
