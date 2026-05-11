package com.nexus.kafka.nivel3;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartitionInfo;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

/**
 * Analizador programatico del cluster Kafka - Nivel 3.
 *
 * Usa AdminClient para obtener informacion sobre brokers, topics y
 * distribucion de particiones. Muestra el reporte en consola.
 *
 * Uso:
 *   "%JAVA_HOME%\bin\java" -cp target\kafka-lab-nivel-3-1.3.0.jar
 *       com.nexus.kafka.nivel3.ClusterAnalyzer
 *
 *   "%JAVA_HOME%\bin\java" -cp target\kafka-lab-nivel-3-1.3.0.jar
 *       com.nexus.kafka.nivel3.ClusterAnalyzer transacciones-6p
 */
public class ClusterAnalyzer {

    // Especificar los 3 brokers es mejor practica:
    // si uno esta caido, el cliente puede conectarse por los otros dos
    private static final String BOOTSTRAP_SERVERS = "localhost:9092,localhost:9093,localhost:9094";

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        String topicEspecifico = args.length > 0 ? args[0] : null;

        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "10000");
        props.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, "15000");

        System.out.println();
        System.out.println("╔════════════════════════════════════════════════════════╗");
        System.out.println("║          ANÁLISIS DEL CLÚSTER KAFKA - NIVEL 3         ║");
        System.out.println("╚════════════════════════════════════════════════════════╝");
        System.out.println("  Conectando a: " + BOOTSTRAP_SERVERS);

        try (AdminClient admin = AdminClient.create(props)) {
            describeCluster(admin);

            if (topicEspecifico != null) {
                analyzeTopicDistribution(admin, topicEspecifico);
            } else {
                // Analizar todos los topics de usuario
                List<String> topics = admin.listTopics().names().get()
                        .stream()
                        .filter(t -> !t.startsWith("__"))
                        .sorted()
                        .collect(Collectors.toList());

                if (topics.isEmpty()) {
                    System.out.println("\n  [ INFO ] No hay topics de usuario. Crealos con exp-05.");
                } else {
                    for (String t : topics) {
                        analyzeTopicDistribution(admin, t);
                    }
                }
            }
        } catch (Exception e) {
            System.err.println("\n❌ Error conectando al cluster: " + e.getMessage());
            System.err.println("   Verifica que el cluster este corriendo: 20-iniciar-cluster.ps1");
        }
    }

    /**
     * Describe el cluster: ID, controlador activo y lista de brokers.
     */
    private static void describeCluster(AdminClient admin) throws ExecutionException, InterruptedException {
        DescribeClusterResult clusterResult = admin.describeCluster();

        String clusterId    = clusterResult.clusterId().get();
        Node   controlador  = clusterResult.controller().get();
        var    nodos        = clusterResult.nodes().get();

        System.out.println();
        System.out.println("  Cluster ID       : " + clusterId);
        System.out.println("  Controlador KRaft: Broker " + controlador.id()
                + " (" + controlador.host() + ":" + controlador.port() + ")");
        System.out.println();
        System.out.println("  Brokers en el cluster:");

        for (Node nodo : nodos.stream().sorted((a, b) -> Integer.compare(a.id(), b.id())).collect(Collectors.toList())) {
            boolean esControlador = nodo.id() == controlador.id();
            String marca = esControlador ? " ⭐ (Controlador activo)" : "";
            System.out.println("    • Broker " + nodo.id() + ": " + nodo.host() + ":" + nodo.port() + marca);
        }
    }

    /**
     * Analiza la distribucion de particiones de un topic especifico.
     * Muestra leader, replicas, ISR y estadisticas de balance.
     */
    private static void analyzeTopicDistribution(AdminClient admin, String topic)
            throws ExecutionException, InterruptedException {

        System.out.println();
        System.out.println("─────────────────────────────────────────────────────────");
        System.out.println("  Topic: " + topic);

        DescribeTopicsResult result = admin.describeTopics(Collections.singletonList(topic));
        TopicDescription desc;

        try {
            desc = result.topicNameValues().get(topic).get();
        } catch (ExecutionException e) {
            System.out.println("  ❌ Topic '" + topic + "' no encontrado o error al acceder.");
            return;
        }

        List<TopicPartitionInfo> particiones = desc.partitions();
        System.out.println("  Particiones: " + particiones.size());
        System.out.println();
        System.out.println("  Distribucion de particiones:");

        // Contar cuantas particiones tiene cada broker como leader
        Map<Integer, Integer> leaderCount = new HashMap<>();

        for (TopicPartitionInfo p : particiones) {
            Node leader    = p.leader();
            int  leaderId  = leader != null ? leader.id() : -1;
            String replicas = p.replicas().stream()
                    .map(n -> String.valueOf(n.id()))
                    .collect(Collectors.joining(","));
            String isr = p.isr().stream()
                    .map(n -> String.valueOf(n.id()))
                    .collect(Collectors.joining(","));

            System.out.printf("    Partition %2d -> Leader: Broker %-2d | Replicas: [%s] | ISR: [%s]%n",
                    p.partition(), leaderId, replicas, isr);

            leaderCount.merge(leaderId, 1, Integer::sum);
        }

        // ── Estadisticas de balance ──────────────────────────
        int total = particiones.size();
        System.out.println();
        System.out.println("  Estadisticas de distribucion de leaders:");

        List<Map.Entry<Integer, Integer>> entries = new ArrayList<>(leaderCount.entrySet());
        entries.sort(Map.Entry.comparingByKey());

        int maxParts = 0, minParts = Integer.MAX_VALUE;
        for (Map.Entry<Integer, Integer> e : entries) {
            int cnt = e.getValue();
            double pct = (cnt / (double) total) * 100;
            System.out.printf("    Broker %-2d: %d particiones (%.2f%%)%n", e.getKey(), cnt, pct);
            maxParts = Math.max(maxParts, cnt);
            minParts = Math.min(minParts, cnt);
        }

        System.out.println();
        boolean balanceado = minParts > 0 && ((double) maxParts / minParts) <= 1.5;
        if (balanceado) {
            System.out.println("  ✅ Distribucion balanceada correctamente");
        } else {
            System.out.println("  ⚠️  Distribucion desbalanceada (max=" + maxParts + ", min=" + minParts + ")");
            System.out.println("     Considera usar un numero de particiones multiplo de 3");
        }
    }
}
