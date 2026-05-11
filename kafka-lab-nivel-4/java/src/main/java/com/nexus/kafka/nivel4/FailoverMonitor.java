package com.nexus.kafka.nivel4;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartitionInfo;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

/**
 * FailoverMonitor - Nivel 4
 *
 * Monitorea el clúster en tiempo real y detecta eventos de failover,
 * cambios en el ISR y entrada/salida de brokers del clúster.
 *
 * Ideal para ejecutar en una ventana separada mientras realizas
 * experimentos de fallo/recuperación de brokers.
 *
 * Uso:
 *   java -cp target/... com.nexus.kafka.nivel4.FailoverMonitor
 *   java -cp target/... com.nexus.kafka.nivel4.FailoverMonitor transacciones-rf3
 *   java -cp target/... com.nexus.kafka.nivel4.FailoverMonitor transacciones-rf3 critical-data
 */
public class FailoverMonitor {

    private static final String BOOTSTRAP     = "localhost:9092,localhost:9093,localhost:9094";
    private static final int    POLL_INTERVAL = 2_000;  // ms entre checks
    private static final long   FAILOVER_WARN = 5_000;  // ms: alerta si failover tarda más
    private static final String LOG_FILE      = "..\\experimentos\\resultados\\failover-monitor.log";

    private static final SimpleDateFormat SDF = new SimpleDateFormat("HH:mm:ss.SSS");

    // Estado anterior del clúster (para detectar cambios)
    private final Map<String, Integer>      prevLeaders    = new ConcurrentHashMap<>();
    private final Map<String, List<Integer>> prevISR       = new ConcurrentHashMap<>();
    private final Set<Integer>               prevBrokers   = ConcurrentHashMap.newKeySet();

    // Estadísticas de failover
    private final List<Long>  tiemposFailover  = new ArrayList<>();
    private long              ultimaBajadaBroker = -1;
    private int               brokerCaido        = -1;

    private PrintWriter logWriter;

    // ─── Punto de entrada ─────────────────────────────────────────────────────

    public static void main(String[] args) throws Exception {
        List<String> topicsFiltro = args.length > 0 ? Arrays.asList(args) : null;

        System.out.println();
        System.out.println("╔════════════════════════════════════════════════════════╗");
        System.out.println("║        FAILOVER MONITOR - NIVEL 4                      ║");
        System.out.println("╚════════════════════════════════════════════════════════╝");
        System.out.println();
        System.out.println("  Detecta en tiempo real:");
        System.out.println("    • Brokers que se caen o vuelven al cluster");
        System.out.println("    • Cambios de leader de particion (failover)");
        System.out.println("    • Cambios en el ISR (replicas que se sinc/desinc)");
        System.out.println();
        if (topicsFiltro != null) {
            System.out.println("  Topics monitoreados: " + topicsFiltro);
        } else {
            System.out.println("  Monitoreando TODOS los topics.");
        }
        System.out.println("  Intervalo de polling: " + POLL_INTERVAL + "ms");
        System.out.println("  Presiona Ctrl+C para detener.");
        System.out.println();

        new FailoverMonitor().run(topicsFiltro);
    }

    // ─── Loop principal ────────────────────────────────────────────────────────

    private void run(List<String> topicsFiltro) throws Exception {

        // Abrir archivo de log
        try {
            logWriter = new PrintWriter(new FileWriter(LOG_FILE, true));
        } catch (IOException e) {
            System.err.println("  [!] No se pudo abrir el log: " + e.getMessage());
        }

        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG,      BOOTSTRAP);
        props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG,     5_000);
        props.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 5_000);

        String ts = timestamp();
        log(ts, "Monitor iniciado");
        System.out.printf("  [%s] Monitor iniciado, rastreando el cluster...%n", ts);

        try (AdminClient admin = AdminClient.create(props)) {

            // Capturar estado inicial
            captureInitialState(admin, topicsFiltro);

            // Loop de monitoreo
            while (true) {
                Thread.sleep(POLL_INTERVAL);
                try {
                    checkClusterState(admin, topicsFiltro);
                } catch (Exception e) {
                    // El clúster puede estar parcialmente caído - ignorar y reintentar
                    System.err.printf("  [%s] [!] Error consultando cluster: %s%n",
                            timestamp(), e.getClass().getSimpleName());
                }
            }

        } finally {
            if (logWriter != null) logWriter.close();
        }
    }

    // ─── Captura estado inicial ────────────────────────────────────────────────

    private void captureInitialState(AdminClient admin, List<String> topicsFiltro)
            throws ExecutionException, InterruptedException {

        String ts = timestamp();

        // Brokers
        Collection<Node> brokers = admin.describeCluster().nodes().get();
        for (Node n : brokers) { prevBrokers.add(n.id()); }

        // Leaders e ISR
        List<String> topics = resolveTopics(admin, topicsFiltro);
        Map<String, TopicDescription> descs = admin.describeTopics(topics).allTopicNames().get();
        for (TopicDescription td : descs.values()) {
            for (TopicPartitionInfo tpi : td.partitions()) {
                String key = td.name() + "-" + tpi.partition();
                prevLeaders.put(key, tpi.leader() != null ? tpi.leader().id() : -1);
                prevISR.put(key, tpi.isr().stream().map(Node::id).collect(Collectors.toList()));
            }
        }

        System.out.printf("  [%s] ✅ Estado inicial capturado%n", ts);
        System.out.printf("       • %d brokers activos: %s%n", prevBrokers.size(), prevBrokers);
        System.out.printf("       • %d particiones monitoreadas%n", prevLeaders.size());
        System.out.println();
        log(ts, "Estado inicial: brokers=" + prevBrokers + " particiones=" + prevLeaders.size());
    }

    // ─── Verificar estado actual vs anterior ──────────────────────────────────

    private void checkClusterState(AdminClient admin, List<String> topicsFiltro)
            throws ExecutionException, InterruptedException {

        String ts = timestamp();

        // ── 1. Detectar cambios en brokers ──────────────────────────────────
        Collection<Node> brokersActuales = admin.describeCluster().nodes().get();
        Set<Integer> idsActuales = brokersActuales.stream()
                .map(Node::id).collect(Collectors.toSet());

        // Brokers que cayeron
        for (int prev : prevBrokers) {
            if (!idsActuales.contains(prev)) {
                System.out.printf("  [%s] ⚠️  BROKER CAIDO DETECTADO%n", ts);
                System.out.printf("       • Broker %d ya no responde%n", prev);
                log(ts, "BROKER CAIDO: " + prev);
                ultimaBajadaBroker = System.currentTimeMillis();
                brokerCaido = prev;

                // Contar impacto
                long impactadas = prevLeaders.entrySet().stream()
                        .filter(e -> e.getValue() == prev).count();
                System.out.printf("       • %d particiones tenian su leader en Broker %d%n", impactadas, prev);
            }
        }

        // Brokers que volvieron
        for (int actual : idsActuales) {
            if (!prevBrokers.contains(actual)) {
                System.out.printf("  [%s] 🔄 BROKER RECUPERADO%n", ts);
                System.out.printf("       • Broker %d ha regresado al cluster%n", actual);
                System.out.printf("       • Comenzara a re-sincronizar sus replicas%n");
                log(ts, "BROKER RECUPERADO: " + actual);
                if (actual == brokerCaido) {
                    brokerCaido = -1;
                }
            }
        }
        prevBrokers.clear();
        prevBrokers.addAll(idsActuales);

        // ── 2. Detectar cambios de leader e ISR ─────────────────────────────
        List<String> topics = resolveTopics(admin, topicsFiltro);
        if (topics.isEmpty()) return;

        Map<String, TopicDescription> descs;
        try {
            descs = admin.describeTopics(topics).allTopicNames().get();
        } catch (Exception e) {
            return; // El clúster puede estar en transición
        }

        for (TopicDescription td : descs.values()) {
            for (TopicPartitionInfo tpi : td.partitions()) {
                String key        = td.name() + "-" + tpi.partition();
                int    leaderAct  = tpi.leader() != null ? tpi.leader().id() : -1;
                List<Integer> isrAct = tpi.isr().stream()
                        .map(Node::id).collect(Collectors.toList());

                // Detectar cambio de leader (failover)
                Integer leaderPrev = prevLeaders.get(key);
                if (leaderPrev != null && leaderPrev != leaderAct && leaderAct >= 0) {
                    long elapsed = ultimaBajadaBroker > 0
                            ? System.currentTimeMillis() - ultimaBajadaBroker : -1;

                    System.out.printf("  [%s] 🔄 FAILOVER DETECTADO%n", ts);
                    System.out.printf("       • Topic: %s, Partition: %d%n", td.name(), tpi.partition());
                    System.out.printf("       • Leader: Broker %d → Broker %d%n", leaderPrev, leaderAct);
                    if (elapsed >= 0) {
                        System.out.printf("       • Tiempo de failover: %d ms%n", elapsed);
                        tiemposFailover.add(elapsed);

                        if (elapsed > FAILOVER_WARN) {
                            System.out.printf("       ⚠️  Failover lento (>%ds) - posible problema de configuracion%n",
                                    FAILOVER_WARN / 1000);
                        }
                    }

                    log(ts, String.format("FAILOVER T=%s P%d: Broker%d->Broker%d (%dms)",
                            td.name(), tpi.partition(), leaderPrev, leaderAct, elapsed));
                }

                // Detectar cambios en ISR
                List<Integer> isrPrev = prevISR.get(key);
                if (isrPrev != null) {
                    List<Integer> removidos = new ArrayList<>(isrPrev);
                    removidos.removeAll(isrAct);
                    List<Integer> agregados = new ArrayList<>(isrAct);
                    agregados.removeAll(isrPrev);

                    if (!removidos.isEmpty()) {
                        System.out.printf("  [%s] 📉 REPLICAS REMOVIDAS DEL ISR%n", ts);
                        System.out.printf("       • Topic: %s, Partition: %d%n", td.name(), tpi.partition());
                        System.out.printf("       • ISR antes: %s%n", isrPrev);
                        System.out.printf("       • ISR ahora: %s%n", isrAct);
                        System.out.printf("       • Replicas removidas: Broker(s) %s%n", removidos);
                        log(ts, "ISR REDUCE " + key + ": " + isrPrev + "->" + isrAct);
                    }

                    if (!agregados.isEmpty()) {
                        System.out.printf("  [%s] 📈 REPLICAS AGREGADAS AL ISR (RE-SINC)%n", ts);
                        System.out.printf("       • Topic: %s, Partition: %d%n", td.name(), tpi.partition());
                        System.out.printf("       • Broker(s) %s sincronizados%n", agregados);
                        log(ts, "ISR CRECE " + key + ": " + isrPrev + "->" + isrAct);
                    }
                }

                // Actualizar estado previo
                prevLeaders.put(key, leaderAct);
                prevISR.put(key, isrAct);
            }
        }

        // ── 3. Mostrar resumen de failovers si los hay ──────────────────────
        if (!tiemposFailover.isEmpty() && tiemposFailover.size() % 4 == 0) {
            long promedio = (long) tiemposFailover.stream().mapToLong(x -> x).average().orElse(0);
            long maximo   = tiemposFailover.stream().mapToLong(x -> x).max().orElse(0);
            System.out.printf("  [%s] ⏱️  Estadisticas de failover: %d eventos, prom=%dms, max=%dms%n",
                    ts, tiemposFailover.size(), promedio, maximo);
        }
    }

    // ─── Utilidades ───────────────────────────────────────────────────────────

    private List<String> resolveTopics(AdminClient admin, List<String> filtro)
            throws ExecutionException, InterruptedException {
        if (filtro != null) return filtro;
        return admin.listTopics().names().get().stream()
                .filter(t -> !t.startsWith("__"))
                .sorted()
                .collect(Collectors.toList());
    }

    private static String timestamp() {
        return SDF.format(new Date());
    }

    private void log(String ts, String mensaje) {
        if (logWriter != null) {
            logWriter.printf("[%s] %s%n", ts, mensaje);
            logWriter.flush();
        }
    }
}
