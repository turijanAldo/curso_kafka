package com.nexus.kafka.nivel4;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartitionInfo;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

/**
 * ReplicaAnalyzer - Nivel 4
 *
 * Análisis programático del estado de replicación e ISR del clúster.
 * Conecta al clúster via AdminClient y examina la distribución de réplicas
 * por topic y partición, identificando particiones degradadas o en riesgo.
 *
 * Uso:
 *   java -cp target/kafka-lab-nivel-4-1.4.0.jar com.nexus.kafka.nivel4.ReplicaAnalyzer
 *   java -cp target/kafka-lab-nivel-4-1.4.0.jar com.nexus.kafka.nivel4.ReplicaAnalyzer transacciones-rf3
 */
public class ReplicaAnalyzer {

    // Bootstrap servers: los 3 brokers del clúster
    private static final String BOOTSTRAP_SERVERS = "localhost:9092,localhost:9093,localhost:9094";
    private static final int    TIMEOUT_MS         = 10_000;

    // ─── Clases internas para reportes estructurados ───────────────────────

    /** Estado de una partición individual */
    static class PartitionReplicationState {
        String topic;
        int    partition;
        int    leaderId;
        List<Integer> replicas;   // réplicas configuradas
        List<Integer> isr;        // réplicas actualmente in-sync
        List<Integer> outOfSync;  // diferencia: replicas - isr

        boolean isFullyReplicated() { return outOfSync.isEmpty(); }
        boolean isCritical()        { return isr.size() <= 1; }

        String getEstado() {
            if (isFullyReplicated()) return "COMPLETAMENTE REPLICADO";
            if (isCritical())        return "CRITICO (solo leader in-sync)";
            return "PARCIALMENTE REPLICADO";
        }
    }

    /** Reporte completo de un topic */
    static class TopicReplicationReport {
        String topicName;
        List<PartitionReplicationState> partitions = new ArrayList<>();

        int countHealthy()   { return (int) partitions.stream().filter(PartitionReplicationState::isFullyReplicated).count(); }
        int countDegraded()  { return (int) partitions.stream().filter(p -> !p.isFullyReplicated() && !p.isCritical()).count(); }
        int countCritical()  { return (int) partitions.stream().filter(PartitionReplicationState::isCritical).count(); }
    }

    /** Reporte global del clúster */
    static class ClusterReplicationReport {
        List<TopicReplicationReport> topics = new ArrayList<>();
        int totalPartitions = 0;
        int fullyReplicated = 0;
        int partiallyReplicated = 0;
        int critical = 0;
        Map<Integer, Integer> replicasByBroker = new TreeMap<>(); // brokerID -> count
        Map<Integer, Integer> isrByBroker      = new TreeMap<>();
    }

    // ─── Método principal de análisis ──────────────────────────────────────

    public static ClusterReplicationReport analyzeReplicationState(
            AdminClient admin, String topicFilter) throws ExecutionException, InterruptedException {

        ClusterReplicationReport report = new ClusterReplicationReport();

        // Obtener lista de topics a analizar
        List<String> topicsToAnalyze;
        if (topicFilter != null && !topicFilter.isBlank()) {
            topicsToAnalyze = List.of(topicFilter);
        } else {
            topicsToAnalyze = admin.listTopics().names().get().stream()
                    .filter(t -> !t.startsWith("__"))
                    .sorted()
                    .collect(Collectors.toList());
        }

        if (topicsToAnalyze.isEmpty()) {
            System.out.println("  [!] No hay topics para analizar.");
            return report;
        }

        // Describir todos los topics de una vez
        Map<String, TopicDescription> descriptions =
                admin.describeTopics(topicsToAnalyze).allTopicNames().get();

        for (String topicName : topicsToAnalyze) {
            TopicDescription desc = descriptions.get(topicName);
            if (desc == null) continue;

            TopicReplicationReport topicReport = new TopicReplicationReport();
            topicReport.topicName = topicName;

            for (TopicPartitionInfo tpi : desc.partitions()) {
                PartitionReplicationState state = new PartitionReplicationState();
                state.topic     = topicName;
                state.partition = tpi.partition();
                state.leaderId  = tpi.leader() != null ? tpi.leader().id() : -1;

                // Lista de IDs de todas las réplicas configuradas
                state.replicas = tpi.replicas().stream()
                        .map(Node::id)
                        .collect(Collectors.toList());

                // Lista de IDs actualmente in-sync
                state.isr = tpi.isr().stream()
                        .map(Node::id)
                        .collect(Collectors.toList());

                // Réplicas fuera de sync = réplicas configuradas que NO están en ISR
                state.outOfSync = state.replicas.stream()
                        .filter(r -> !state.isr.contains(r))
                        .collect(Collectors.toList());

                topicReport.partitions.add(state);

                // Contabilizar por broker
                for (int brokerId : state.replicas) {
                    report.replicasByBroker.merge(brokerId, 1, Integer::sum);
                }
                for (int brokerId : state.isr) {
                    report.isrByBroker.merge(brokerId, 1, Integer::sum);
                }

                // Contadores globales
                report.totalPartitions++;
                if (state.isFullyReplicated()) report.fullyReplicated++;
                else if (state.isCritical())   report.critical++;
                else                           report.partiallyReplicated++;
            }

            report.topics.add(topicReport);
        }

        return report;
    }

    // ─── Visualización en consola ──────────────────────────────────────────

    static void printReport(ClusterReplicationReport report) {
        System.out.println();
        System.out.println("╔════════════════════════════════════════════════════════╗");
        System.out.println("║     ANALISIS DE REPLICACION - CLUSTER KAFKA            ║");
        System.out.println("╚════════════════════════════════════════════════════════╝");

        // Resumen global
        System.out.println();
        System.out.println("  Resumen global:");
        System.out.printf("  • Total de particiones      : %d%n", report.totalPartitions);

        double pctOk   = report.totalPartitions > 0
                ? report.fullyReplicated * 100.0 / report.totalPartitions : 0;
        double pctPar  = report.totalPartitions > 0
                ? report.partiallyReplicated * 100.0 / report.totalPartitions : 0;
        double pctCrit = report.totalPartitions > 0
                ? report.critical * 100.0 / report.totalPartitions : 0;

        System.out.printf("  • Completamente replicadas  : %d (%.1f%%)%n", report.fullyReplicated,   pctOk);
        System.out.printf("  • Parcialmente replicadas   : %d (%.1f%%)%n", report.partiallyReplicated, pctPar);
        System.out.printf("  • Riesgo critico            : %d (%.1f%%)%n", report.critical,           pctCrit);

        if (!report.replicasByBroker.isEmpty()) {
            System.out.println();
            System.out.println("  Replicas por broker:");
            for (Map.Entry<Integer, Integer> e : report.replicasByBroker.entrySet()) {
                int brokerId = e.getKey();
                int total    = e.getValue();
                int inSync   = report.isrByBroker.getOrDefault(brokerId, 0);
                System.out.printf("    Broker %d: %d replicas totales, %d in-sync%n",
                        brokerId, total, inSync);
            }
        }

        System.out.println();
        System.out.println("─".repeat(58));

        // Detalle por topic
        for (TopicReplicationReport tr : report.topics) {
            int rf = tr.partitions.isEmpty() ? 0 : tr.partitions.get(0).replicas.size();
            System.out.printf("%nTopic: %s (%d particiones, RF=%d)%n",
                    tr.topicName, tr.partitions.size(), rf);

            for (PartitionReplicationState p : tr.partitions) {
                String icono = p.isFullyReplicated() ? "✅" : (p.isCritical() ? "🔴" : "⚠️ ");
                System.out.printf("  %s Partition %d%n", icono, p.partition);
                System.out.printf("     Leader  : Broker %d%n", p.leaderId);
                System.out.printf("     Replicas: %s%n", p.replicas);
                System.out.printf("     ISR     : %s%n", p.isr);
                if (!p.outOfSync.isEmpty()) {
                    System.out.printf("     Fuera de sync: %s%n", p.outOfSync);
                }
                System.out.printf("     Estado  : %s%n", p.getEstado());
            }
        }

        // Conclusión
        System.out.println();
        System.out.println("─".repeat(58));
        if (report.critical == 0 && report.partiallyReplicated == 0) {
            System.out.println("  ✅ CLUSTER COMPLETAMENTE SALUDABLE");
        } else if (report.critical > 0) {
            System.out.printf("  🔴 ALERTA: %d particion(es) en riesgo critico%n", report.critical);
            System.out.println("     Si el leader de esas particiones falla, quedaran inaccesibles.");
        } else {
            System.out.printf("  ⚠️  ADVERTENCIA: %d particion(es) parcialmente replicadas%n",
                    report.partiallyReplicated);
        }
        System.out.println();
    }

    // ─── Comparación de dos estados ────────────────────────────────────────

    /**
     * Compara dos reportes y genera un resumen de qué cambió.
     * Útil para análisis post-mortem de un incidente.
     */
    public static void compareStates(ClusterReplicationReport before, ClusterReplicationReport after) {
        System.out.println();
        System.out.println("  COMPARACION DE ESTADOS:");
        System.out.println("  ─────────────────────────────────────────────────");
        System.out.printf("  Particiones saludables: %d → %d%n",
                before.fullyReplicated, after.fullyReplicated);
        System.out.printf("  Particiones degradadas: %d → %d%n",
                before.partiallyReplicated, after.partiallyReplicated);
        System.out.printf("  Particiones criticas  : %d → %d%n",
                before.critical, after.critical);

        // Detectar cambios de leader por partición
        Map<String, Integer> beforeLeaders = new HashMap<>();
        for (TopicReplicationReport tr : before.topics) {
            for (PartitionReplicationState p : tr.partitions) {
                beforeLeaders.put(tr.topicName + "-" + p.partition, p.leaderId);
            }
        }

        System.out.println("  Cambios de leader detectados:");
        boolean hayCambios = false;
        for (TopicReplicationReport tr : after.topics) {
            for (PartitionReplicationState p : tr.partitions) {
                String key = tr.topicName + "-" + p.partition;
                Integer prevLeader = beforeLeaders.get(key);
                if (prevLeader != null && prevLeader != p.leaderId) {
                    System.out.printf("    %s P%d: Broker%d → Broker%d (FAILOVER)%n",
                            tr.topicName, p.partition, prevLeader, p.leaderId);
                    hayCambios = true;
                }
            }
        }
        if (!hayCambios) {
            System.out.println("    Ninguno (leaders estables)");
        }
    }

    // ─── main ──────────────────────────────────────────────────────────────

    public static void main(String[] args) {
        String topicFiltro = args.length > 0 ? args[0] : null;

        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, TIMEOUT_MS);
        props.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, TIMEOUT_MS);

        System.out.println();
        System.out.println("  Conectando al cluster...");
        System.out.printf("  Bootstrap servers: %s%n", BOOTSTRAP_SERVERS);
        if (topicFiltro != null) {
            System.out.printf("  Filtrando topic  : %s%n", topicFiltro);
        }

        try (AdminClient admin = AdminClient.create(props)) {

            ClusterReplicationReport report = analyzeReplicationState(admin, topicFiltro);
            printReport(report);

        } catch (ExecutionException e) {
            System.err.println();
            System.err.println("  [ERROR] No se pudo conectar al cluster: " + e.getCause().getMessage());
            System.err.println("  Verifica que el cluster esta corriendo con:");
            System.err.println("    docker ps | findstr kafka");
            System.exit(1);
        } catch (Exception e) {
            System.err.println("  [ERROR] " + e.getMessage());
            System.exit(1);
        }
    }
}
