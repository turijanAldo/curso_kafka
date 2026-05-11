package com.nexus.kafka.nivel3;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.FileWriter;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Benchmark de throughput multi-thread para el Laboratorio Kafka - Nivel 3.
 *
 * Mide mensajes/segundo, MB/segundo y percentiles de latencia (P50, P95, P99).
 * Usa multiples threads productores en paralelo para saturar el cluster.
 *
 * Argumentos (todos opcionales con defaults):
 *   --topic         nombre del topic         (default: transacciones-6p)
 *   --messages      total de mensajes        (default: 10000)
 *   --message-size  tamano en bytes          (default: 1024)
 *   --threads       threads productores      (default: 1)
 *   --batch-size    tamano del batch Kafka   (default: 100)
 *   --linger-ms     espera para batching     (default: 10)
 *
 * Uso:
 *   "%JAVA_HOME%\bin\java" -cp target\kafka-lab-nivel-3-1.3.0.jar
 *       com.nexus.kafka.nivel3.ThroughputBenchmark
 *       --topic transacciones-6p --messages 10000 --threads 3
 */
public class ThroughputBenchmark {

    private static final String BOOTSTRAP_SERVERS = "localhost:9092,localhost:9093,localhost:9094";

    public static void main(String[] args) throws InterruptedException, IOException {
        // ── Parsear argumentos de linea de comandos ──────────────
        Map<String, String> params = parseArgs(args);

        String topic       = params.getOrDefault("topic",        "transacciones-6p");
        int    mensajes    = Integer.parseInt(params.getOrDefault("messages",      "10000"));
        int    msgSizeB    = Integer.parseInt(params.getOrDefault("message-size",  "1024"));
        int    numThreads  = Integer.parseInt(params.getOrDefault("threads",       "1"));
        int    batchSize   = Integer.parseInt(params.getOrDefault("batch-size",    "100"));
        int    lingerMs    = Integer.parseInt(params.getOrDefault("linger-ms",     "10"));

        // ── Encabezado del benchmark ─────────────────────────────
        System.out.println();
        System.out.println("╔═══════════════════════════════════════════════════════╗");
        System.out.println("║         BENCHMARK DE THROUGHPUT - KAFKA CLUSTER       ║");
        System.out.println("╚═══════════════════════════════════════════════════════╝");
        System.out.println("Configuracion del test:");
        System.out.println("  • Topic          : " + topic + " (3 brokers)");
        System.out.printf ("  • Mensajes totales: %,d%n", mensajes);
        System.out.println("  • Tamano mensaje  : " + msgSizeB + " bytes");
        System.out.println("  • Threads         : " + numThreads);
        System.out.println("  • Batch size      : " + batchSize);
        System.out.println("  • Linger ms       : " + lingerMs + " ms");
        System.out.println("─────────────────────────────────────────────────────────");
        System.out.println();

        // ── Estructuras de datos compartidas entre threads ───────
        // Lista thread-safe de latencias (en ms) de cada mensaje enviado
        List<Long> latencias = Collections.synchronizedList(new ArrayList<>(mensajes));
        // Contadores atomicos para throughput
        AtomicLong totalEnviados = new AtomicLong(0);
        AtomicLong totalErrores  = new AtomicLong(0);
        // Contadores por particion
        Map<Integer, AtomicLong> porParticion = Collections.synchronizedMap(new HashMap<>());

        CountDownLatch latch = new CountDownLatch(mensajes);
        String payloadBase = "X".repeat(Math.max(1, msgSizeB - 50)); // Cuerpo del mensaje

        Properties props = buildProducerProperties(batchSize, lingerMs);

        // ── Lanzar N threads productores ─────────────────────────
        ExecutorService pool = Executors.newFixedThreadPool(numThreads);
        int mensajesPorThread = mensajes / numThreads;
        int resto = mensajes % numThreads;

        System.out.println("⏱️  Ejecutando benchmark...");
        long tiempoInicio = System.currentTimeMillis();

        // KafkaProducer es thread-safe: se puede compartir entre threads
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {

            for (int t = 0; t < numThreads; t++) {
                final int threadId  = t;
                final int cantidad  = mensajesPorThread + (t == 0 ? resto : 0);

                pool.submit(() -> {
                    for (int i = 0; i < cantidad; i++) {
                        String clave = "bench-t" + threadId + "-" + i;
                        String valor = String.format("{\"t\":%d,\"i\":%d,\"p\":\"%s\"}", threadId, i, payloadBase);

                        long tsAntes = System.currentTimeMillis();
                        ProducerRecord<String, String> record = new ProducerRecord<>(topic, clave, valor);

                        producer.send(record, (RecordMetadata metadata, Exception ex) -> {
                            long latencia = System.currentTimeMillis() - tsAntes;
                            if (ex == null) {
                                totalEnviados.incrementAndGet();
                                latencias.add(latencia);
                                porParticion.computeIfAbsent(metadata.partition(), k -> new AtomicLong(0))
                                            .incrementAndGet();
                            } else {
                                totalErrores.incrementAndGet();
                            }
                            latch.countDown();

                            // Progreso cada 500 mensajes
                            long procesados = mensajes - latch.getCount();
                            if (procesados % 500 == 0) {
                                int pct = (int)((procesados / (double) mensajes) * 100);
                                int ll  = pct / 5;
                                System.out.printf("\r  [%s%s] %3d%% (%,d/%,d) ERR:%d",
                                    "█".repeat(ll), "░".repeat(20-ll), pct, procesados, mensajes,
                                    totalErrores.get());
                                System.out.flush();
                            }
                        });
                    }
                });
            }

            pool.shutdown();
            producer.flush();
            boolean terminado = latch.await(120, TimeUnit.SECONDS);
            System.out.println();
            if (!terminado) {
                System.out.println("⚠️  Timeout! No todos los mensajes fueron confirmados.");
            }
        }

        long tiempoTotal = System.currentTimeMillis() - tiempoInicio;

        // ── Calcular metricas ────────────────────────────────────
        double duracionSeg  = tiempoTotal / 1000.0;
        long   enviados     = totalEnviados.get();
        double msgPorSeg    = enviados / duracionSeg;
        double mbPorSeg     = (enviados * msgSizeB / 1024.0 / 1024.0) / duracionSeg;

        // Calcular percentiles de latencia
        List<Long> latOrdenadas = new ArrayList<>(latencias);
        Collections.sort(latOrdenadas);
        long latP50  = percentil(latOrdenadas, 50);
        long latP95  = percentil(latOrdenadas, 95);
        long latP99  = percentil(latOrdenadas, 99);
        long latMax  = latOrdenadas.isEmpty() ? 0 : latOrdenadas.get(latOrdenadas.size()-1);
        double latAvg = latOrdenadas.isEmpty() ? 0 :
                latOrdenadas.stream().mapToLong(Long::longValue).average().orElse(0);

        // ── Imprimir reporte ─────────────────────────────────────
        System.out.println();
        System.out.println("✅ Benchmark completado");
        System.out.println();
        System.out.println("Resultados:");
        System.out.println("  Throughput:");
        System.out.printf("    • Mensajes/segundo: %,.0f%n", msgPorSeg);
        System.out.printf("    • MB/segundo      : %.2f%n", mbPorSeg);
        System.out.printf("    • Enviados OK      : %,d%n", enviados);
        System.out.printf("    • Errores          : %,d%n", totalErrores.get());
        System.out.println();
        System.out.println("  Latencia (ms):");
        System.out.printf("    • Promedio   : %.1f%n", latAvg);
        System.out.printf("    • Percentil 50 (mediana): %d%n", latP50);
        System.out.printf("    • Percentil 95: %d%n", latP95);
        System.out.printf("    • Percentil 99: %d%n", latP99);
        System.out.printf("    • Maxima       : %d%n", latMax);
        System.out.println();
        System.out.println("  Distribucion por particion:");
        porParticion.entrySet().stream().sorted(Map.Entry.comparingByKey()).forEach(e ->
            System.out.printf("    • Partition %d: %,d mensajes (%.2f%%)%n",
                e.getKey(), e.getValue().get(), (e.getValue().get() / (double) enviados) * 100)
        );

        // ── Guardar CSV ──────────────────────────────────────────
        String csvFile = "../../experimentos/resultados/metricas-throughput.txt";
        String ts = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
        try (FileWriter fw = new FileWriter(csvFile, true)) {
            fw.write(String.format("[%s] topic=%s msgs=%d size=%dB threads=%d | " +
                    "tps=%.0f mb/s=%.2f lat_avg=%.1f p50=%d p95=%d p99=%d max=%d%n",
                    ts, topic, mensajes, msgSizeB, numThreads,
                    msgPorSeg, mbPorSeg, latAvg, latP50, latP95, latP99, latMax));
        } catch (IOException e) {
            System.err.println("[ WARN ] No se pudo guardar el CSV: " + e.getMessage());
        }
        System.out.println("\n  Resultados guardados en: experimentos/resultados/metricas-throughput.txt");
    }

    /** Calcula el percentil P de una lista ya ordenada. */
    private static long percentil(List<Long> ordenada, int p) {
        if (ordenada.isEmpty()) return 0;
        int idx = (int) Math.ceil(p / 100.0 * ordenada.size()) - 1;
        return ordenada.get(Math.max(0, Math.min(idx, ordenada.size() - 1)));
    }

    /** Parsea argumentos --clave valor en un Map<String,String>. */
    private static Map<String, String> parseArgs(String[] args) {
        Map<String, String> m = new HashMap<>();
        for (int i = 0; i < args.length - 1; i += 2) {
            String key = args[i].replaceAll("^--", "");
            m.put(key, args[i + 1]);
        }
        return m;
    }

    private static Properties buildProducerProperties(int batchSize, int lingerMs) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,   StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "1");
        props.put(ProducerConfig.LINGER_MS_CONFIG, lingerMs);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, batchSize * 1024);  // batchSize en KB
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432L);       // 32 MB buffer
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");        // compresion para mayor throughput
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "kafka-lab-nivel3-throughput-benchmark");
        return props;
    }
}
