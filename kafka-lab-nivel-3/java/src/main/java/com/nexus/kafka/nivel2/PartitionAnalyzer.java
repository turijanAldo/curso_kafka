package com.nexus.kafka.nivel2;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * Analizador de particiones para el Laboratorio Kafka - Nivel 2.
 *
 * Conecta al cluster via AdminClient, obtiene informacion de cada particion
 * del topic especificado y muestra una tabla con:
 *   - Earliest offset (primer mensaje disponible)
 *   - Latest offset   (proximo offset disponible, equivale a total de mensajes)
 *   - Cantidad de mensajes en cada particion
 *   - Si la distribucion es balanceada o desigual
 *
 * Uso:
 *   "%JAVA_HOME%\bin\java" -cp target\kafka-lab-nivel-2-1.0.0.jar
 *       com.nexus.kafka.nivel2.PartitionAnalyzer <topic>
 *
 * Ejemplo:
 *   "%JAVA_HOME%\bin\java" -cp target\kafka-lab-nivel-2-1.0.0.jar
 *       com.nexus.kafka.nivel2.PartitionAnalyzer transacciones-4p
 */
public class PartitionAnalyzer {

    private static final String BOOTSTRAP_SERVERS = "localhost:9092";

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        if (args.length < 1) {
            System.err.println("Uso: PartitionAnalyzer <topic>");
            System.exit(1);
        }

        String topic = args[0];

        System.out.println("📊 Analizando topic: " + topic);
        System.out.println("   Conectando a " + BOOTSTRAP_SERVERS + "...");

        Properties adminProps = new Properties();
        adminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);

        Properties consumerProps = new Properties();
        consumerProps.put("bootstrap.servers", BOOTSTRAP_SERVERS);
        consumerProps.put("key.deserializer",   StringDeserializer.class.getName());
        consumerProps.put("value.deserializer", StringDeserializer.class.getName());
        consumerProps.put("group.id", "partition-analyzer-internal");

        // ── Usar AdminClient para obtener numero de particiones ───
        try (AdminClient admin = AdminClient.create(adminProps)) {

            DescribeTopicsResult result = admin.describeTopics(Collections.singletonList(topic));
            TopicDescription desc = result.topicNameValues().get(topic).get();

            int numParticiones = desc.partitions().size();

            // ── Usar KafkaConsumer para obtener offsets ────────────
            // KafkaConsumer expone beginningOffsets() y endOffsets()
            // que devuelven el primer y ultimo offset de cada particion
            try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps)) {

                // Construir lista de TopicPartition para consultar
                List<TopicPartition> particiones = new ArrayList<>();
                for (int i = 0; i < numParticiones; i++) {
                    particiones.add(new TopicPartition(topic, i));
                }

                // Obtener offsets extremos de todas las particiones en una sola llamada
                Map<TopicPartition, Long> earliestOffsets = consumer.beginningOffsets(particiones);
                Map<TopicPartition, Long> latestOffsets   = consumer.endOffsets(particiones);

                // ── Calcular estadisticas ─────────────────────────
                Map<Integer, Long> mensajesPorParticion = new HashMap<>();
                long totalMensajes = 0;
                long maxMensajes   = 0;
                long minMensajes   = Long.MAX_VALUE;

                for (TopicPartition tp : particiones) {
                    long earliest = earliestOffsets.getOrDefault(tp, 0L);
                    long latest   = latestOffsets.getOrDefault(tp, 0L);
                    // El numero de mensajes es la diferencia entre latest y earliest
                    // (los mensajes eliminados por retencion no se cuentan)
                    long mensajes = latest - earliest;
                    mensajesPorParticion.put(tp.partition(), mensajes);
                    totalMensajes += mensajes;
                    maxMensajes = Math.max(maxMensajes, mensajes);
                    minMensajes = Math.min(minMensajes, mensajes);
                }

                // ── Imprimir tabla ────────────────────────────────
                System.out.println();
                String sep = "+-----------+-------------+--------------+----------+";
                System.out.println(sep);
                System.out.printf("| %-9s | %-11s | %-12s | %-8s |%n",
                        "Partition", "First Offset", "Last Offset", "Mensajes");
                System.out.println(sep.replace("-", "="));

                for (int i = 0; i < numParticiones; i++) {
                    TopicPartition tp = new TopicPartition(topic, i);
                    long earliest = earliestOffsets.getOrDefault(tp, 0L);
                    long latest   = latestOffsets.getOrDefault(tp, 0L);
                    long mensajes = mensajesPorParticion.get(i);

                    System.out.printf("| %-9d | %-11d | %-12d | %-8d |%n",
                            i, earliest, latest, mensajes);
                }
                System.out.println(sep);

                // ── Resumen de distribucion ───────────────────────
                System.out.println();
                System.out.println("   Total de mensajes: " + totalMensajes);

                if (totalMensajes == 0) {
                    System.out.println("   Distribucion     : Topic vacio");
                } else {
                    // Calcular desviacion: si max/min es mayor que 1.5x, esta desbalanceado
                    boolean balanceado = minMensajes > 0 && ((double) maxMensajes / minMensajes) <= 1.5;
                    String etiqueta = balanceado ? "Balanceada ✓" : "Desbalanceada ⚠️";
                    System.out.println("   Distribucion     : " + etiqueta);

                    if (!balanceado) {
                        System.out.println("   Consejo: usa claves hash para mejor distribucion,");
                        System.out.println("            o envia mensajes sin clave para round-robin.");
                    }
                }
                System.out.println();

                // ── Descripcion del leader y replicas ─────────────
                System.out.println("Informacion del broker por particion:");
                desc.partitions().forEach(p ->
                    System.out.printf("   Partition %d -> Leader: broker %d | ISR: %s%n",
                            p.partition(),
                            p.leader() != null ? p.leader().id() : -1,
                            p.isr().stream().map(n -> String.valueOf(n.id()))
                                   .reduce((a, b) -> a + "," + b).orElse("none"))
                );
            }
        }
    }
}
