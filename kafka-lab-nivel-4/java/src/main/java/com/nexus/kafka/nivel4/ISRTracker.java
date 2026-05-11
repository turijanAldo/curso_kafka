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
 * ISRTracker - Nivel 4
 *
 * Rastreo histórico del estado del ISR en el tiempo.
 * Genera snapshots periódicos y análisis de tendencias.
 *
 * Modos de operación:
 *   - Snapshot único: toma una foto del estado actual y la guarda
 *   - Tracking continuo: snapshots periódicos para análisis histórico
 *
 * Uso:
 *   java -cp target/... com.nexus.kafka.nivel4.ISRTracker
 *   java -cp target/... com.nexus.kafka.nivel4.ISRTracker transacciones-rf3
 *   java -cp target/... com.nexus.kafka.nivel4.ISRTracker transacciones-rf3 --track
 *   java -cp target/... com.nexus.kafka.nivel4.ISRTracker transacciones-rf3 --track --interval 10
 */
public class ISRTracker {

    private static final String BOOTSTRAP = "localhost:9092,localhost:9093,localhost:9094";
    private static final String CSV_FILE  = "..\\experimentos\\resultados\\isr-historico.csv";
    private static final SimpleDateFormat SDF     = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    private static final SimpleDateFormat SDF_SHORT = new SimpleDateFormat("HH:mm:ss");

    // ─── Estructura de datos ──────────────────────────────────────────────────

    record PartitionISRState(
            String topic,
            int    partition,
            int    leaderId,
            List<Integer> replicas,
            List<Integer> isr,
            String timestamp
    ) {
        double isrPct() {
            return replicas.isEmpty() ? 100.0 : isr.size() * 100.0 / replicas.size();
        }
        boolean isFullyReplicated() {
            return new HashSet<>(replicas).equals(new HashSet<>(isr));
        }
        List<Integer> outOfSync() {
            return replicas.stream().filter(r -> !isr.contains(r)).collect(Collectors.toList());
        }
    }

    record Snapshot(List<PartitionISRState> partitions, String timestamp) {
        long healthy()   { return partitions.stream().filter(PartitionISRState::isFullyReplicated).count(); }
        long degraded()  { return partitions.stream().filter(p -> !p.isFullyReplicated()).count(); }
        double healthPct() {
            return partitions.isEmpty() ? 100.0 : healthy() * 100.0 / partitions.size();
        }
    }

    // ─── main ─────────────────────────────────────────────────────────────────

    public static void main(String[] args) throws Exception {

        // Parsear argumentos
        String topicFiltro = null;
        boolean modoTrack  = false;
        int intervalo      = 30; // segundos por defecto

        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--track"    -> modoTrack = true;
                case "--interval" -> { if (i + 1 < args.length) intervalo = Integer.parseInt(args[++i]); }
                default           -> { if (!args[i].startsWith("--")) topicFiltro = args[i]; }
            }
        }

        System.out.println();
        System.out.println("╔════════════════════════════════════════════════════════╗");
        System.out.println("║         ISR TRACKER - NIVEL 4                          ║");
        System.out.println("╚════════════════════════════════════════════════════════╝");
        System.out.printf("  Modo       : %s%n", modoTrack ? "TRACKING continuo" : "Snapshot unico");
        if (topicFiltro != null) System.out.printf("  Topic      : %s%n", topicFiltro);
        if (modoTrack)           System.out.printf("  Intervalo  : %d segundos%n", intervalo);
        System.out.printf("  Historial  : %s%n", CSV_FILE);
        System.out.println();

        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG,      BOOTSTRAP);
        props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG,     8_000);
        props.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 8_000);

        // Control de Ctrl+C
        boolean[] corriendo = {true};
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            corriendo[0] = false;
            System.out.println("\n  [Ctrl+C] Deteniendo tracker...");
        }));

        List<Snapshot> historial = new ArrayList<>();

        try (AdminClient admin = AdminClient.create(props)) {

            if (!modoTrack) {
                // ── Snapshot único ────────────────────────────────────────────
                Snapshot snap = takeSnapshot(admin, topicFiltro);
                historial.add(snap);
                printSnapshot(snap);
                saveToCSV(snap);
                System.out.printf("  Snapshot guardado en: %s%n%n", CSV_FILE);

            } else {
                // ── Modo tracking continuo ────────────────────────────────────
                System.out.println("  Iniciando tracking continuo. Ctrl+C para detener.");
                System.out.println("  ─────────────────────────────────────────────────────");
                System.out.println();

                Snapshot prevSnap = null;
                int numSnap = 0;

                while (corriendo[0]) {
                    Snapshot snap = takeSnapshot(admin, topicFiltro);
                    historial.add(snap);
                    saveToCSV(snap);
                    numSnap++;

                    // Imprimir snapshot compacto
                    String ts = SDF_SHORT.format(new Date());
                    System.out.printf("  [%s] Snapshot #%d | Total: %d | Saludables: %d (%.1f%%) | Degradadas: %d%n",
                            ts, numSnap,
                            snap.partitions().size(),
                            snap.healthy(), snap.healthPct(),
                            snap.degraded());

                    // Detectar cambios respecto al snapshot anterior
                    if (prevSnap != null) {
                        detectChanges(prevSnap, snap);
                    }

                    // Cada 10 snapshots mostrar análisis de tendencia
                    if (historial.size() >= 3 && historial.size() % 10 == 0) {
                        printTrendAnalysis(historial);
                    }

                    // Alertas
                    checkAlerts(snap);

                    prevSnap = snap;

                    // Esperar intervalo
                    for (int i = 0; i < intervalo * 10 && corriendo[0]; i++) {
                        Thread.sleep(100);
                    }
                }

                // Resumen final al terminar
                System.out.println();
                System.out.println("  ═══════════════════════════════════════════════════");
                System.out.println("  RESUMEN DE LA SESION DE TRACKING");
                System.out.println("  ═══════════════════════════════════════════════════");
                System.out.printf("  Snapshots tomados : %d%n", historial.size());
                if (!historial.isEmpty()) {
                    printTrendAnalysis(historial);
                }
            }
        } catch (Exception e) {
            System.err.println("  [ERROR] " + e.getMessage());
            System.exit(1);
        }
    }

    // ─── Tomar snapshot ───────────────────────────────────────────────────────

    private static Snapshot takeSnapshot(AdminClient admin, String topicFiltro)
            throws ExecutionException, InterruptedException {

        List<String> topics;
        if (topicFiltro != null) {
            topics = List.of(topicFiltro);
        } else {
            topics = admin.listTopics().names().get().stream()
                    .filter(t -> !t.startsWith("__"))
                    .sorted()
                    .collect(Collectors.toList());
        }

        String ts = SDF.format(new Date());
        List<PartitionISRState> states = new ArrayList<>();

        if (topics.isEmpty()) return new Snapshot(states, ts);

        Map<String, TopicDescription> descs = admin.describeTopics(topics).allTopicNames().get();

        for (TopicDescription td : descs.values()) {
            for (TopicPartitionInfo tpi : td.partitions()) {
                List<Integer> replicas = tpi.replicas().stream().map(Node::id).collect(Collectors.toList());
                List<Integer> isr      = tpi.isr().stream().map(Node::id).collect(Collectors.toList());
                int leader = tpi.leader() != null ? tpi.leader().id() : -1;

                states.add(new PartitionISRState(
                        td.name(), tpi.partition(), leader, replicas, isr, ts));
            }
        }

        return new Snapshot(states, ts);
    }

    // ─── Imprimir snapshot detallado ─────────────────────────────────────────

    private static void printSnapshot(Snapshot snap) {
        System.out.printf("  Timestamp: %s%n", snap.timestamp());
        System.out.printf("  Total particiones: %d | Saludables: %d | Degradadas: %d%n",
                snap.partitions().size(), snap.healthy(), snap.degraded());
        System.out.println();

        // Agrupar por topic
        Map<String, List<PartitionISRState>> porTopic = new LinkedHashMap<>();
        for (PartitionISRState p : snap.partitions()) {
            porTopic.computeIfAbsent(p.topic(), k -> new ArrayList<>()).add(p);
        }

        for (Map.Entry<String, List<PartitionISRState>> e : porTopic.entrySet()) {
            System.out.printf("  Topic: %s%n", e.getKey());
            for (PartitionISRState p : e.getValue()) {
                String icono = p.isFullyReplicated() ? "✅" : (p.isr().size() <= 1 ? "🔴" : "⚠️ ");
                System.out.printf("    %s P%d | Leader: Broker%d | Replicas: %s | ISR: %s",
                        icono, p.partition(), p.leaderId(), p.replicas(), p.isr());
                if (!p.outOfSync().isEmpty()) {
                    System.out.printf(" | FUERA SYNC: %s", p.outOfSync());
                }
                System.out.println();
            }

            // Barra visual de salud del topic
            long topicOk  = e.getValue().stream().filter(PartitionISRState::isFullyReplicated).count();
            int  total    = e.getValue().size();
            int  barLen   = 20;
            int  llenos   = (int)(topicOk * barLen / total);
            System.out.printf("    Salud: [%s%s] %d/%d%n",
                    "█".repeat(llenos), "░".repeat(barLen - llenos), topicOk, total);
            System.out.println();
        }
    }

    // ─── Detectar cambios entre snapshots ────────────────────────────────────

    private static void detectChanges(Snapshot prev, Snapshot curr) {
        Map<String, PartitionISRState> prevMap = new HashMap<>();
        for (PartitionISRState p : prev.partitions()) {
            prevMap.put(p.topic() + "-" + p.partition(), p);
        }

        for (PartitionISRState p : curr.partitions()) {
            String key = p.topic() + "-" + p.partition();
            PartitionISRState prevP = prevMap.get(key);
            if (prevP == null) continue;

            // Cambio de leader
            if (prevP.leaderId() != p.leaderId() && p.leaderId() >= 0) {
                System.out.printf("    🔄 FAILOVER: %s P%d  Broker%d → Broker%d%n",
                        p.topic(), p.partition(), prevP.leaderId(), p.leaderId());
            }

            // ISR se redujo
            List<Integer> perdidos = new ArrayList<>(prevP.isr());
            perdidos.removeAll(p.isr());
            if (!perdidos.isEmpty()) {
                System.out.printf("    📉 ISR REDUCIDO: %s P%d | Broker(s) %s salieron%n",
                        p.topic(), p.partition(), perdidos);
            }

            // ISR creció (re-sincronización)
            List<Integer> ganados = new ArrayList<>(p.isr());
            ganados.removeAll(prevP.isr());
            if (!ganados.isEmpty()) {
                System.out.printf("    📈 RE-SINC: %s P%d | Broker(s) %s volvieron al ISR%n",
                        p.topic(), p.partition(), ganados);
            }
        }
    }

    // ─── Alertas ──────────────────────────────────────────────────────────────

    private static void checkAlerts(Snapshot snap) {
        long criticas = snap.partitions().stream().filter(p -> p.isr().size() <= 1).count();
        if (criticas > 0) {
            System.out.printf("    🔴 ALERTA: %d particion(es) solo con leader in-sync (riesgo maximo)%n",
                    criticas);
        }

        // Detectar particiones que oscilan (fuera de sync pero leader aun disponible)
        long degradadas = snap.degraded();
        if (degradadas > 0) {
            System.out.printf("    ⚠️  %d particion(es) under-replicated%n", degradadas);
        }
    }

    // ─── Análisis de tendencia ────────────────────────────────────────────────

    private static void printTrendAnalysis(List<Snapshot> historial) {
        System.out.println();
        System.out.println("  ── Tendencia de salud del cluster ──");

        int mostrar = Math.min(20, historial.size());
        List<Snapshot> recientes = historial.subList(historial.size() - mostrar, historial.size());

        // Gráfica ASCII de salud en el tiempo
        int alturaGraf = 5;
        System.out.println("  100% |");
        for (int fila = alturaGraf; fila >= 0; fila--) {
            double umbral = fila * 100.0 / alturaGraf;
            System.out.printf("  %3.0f%% |", umbral);
            for (Snapshot s : recientes) {
                System.out.print(s.healthPct() >= umbral ? "█" : " ");
            }
            System.out.println();
        }
        System.out.print("       +");
        System.out.println("-".repeat(mostrar));
        System.out.println("        " + recientes.get(0).timestamp().substring(11, 16)
                + " ... " + recientes.get(recientes.size()-1).timestamp().substring(11, 16));
        System.out.println();
    }

    // ─── Guardar en CSV ───────────────────────────────────────────────────────

    private static void saveToCSV(Snapshot snap) {
        try {
            File f = new File(CSV_FILE);
            boolean nuevo = !f.exists() || f.length() == 0;
            try (PrintWriter pw = new PrintWriter(new FileWriter(CSV_FILE, true))) {
                if (nuevo) {
                    pw.println("Timestamp,Topic,Partition,Leader,Replicas,ISR,IsrPct,FullyReplicated");
                }
                for (PartitionISRState p : snap.partitions()) {
                    pw.printf("%s,%s,%d,%d,\"%s\",\"%s\",%.1f,%s%n",
                            snap.timestamp(),
                            p.topic(), p.partition(), p.leaderId(),
                            p.replicas().toString().replaceAll("[\\[\\] ]",""),
                            p.isr().toString().replaceAll("[\\[\\] ]",""),
                            p.isrPct(),
                            p.isFullyReplicated());
                }
            }
        } catch (IOException e) {
            // No crítico
        }
    }
}
