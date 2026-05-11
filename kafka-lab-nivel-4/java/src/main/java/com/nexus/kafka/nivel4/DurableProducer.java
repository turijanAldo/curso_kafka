package com.nexus.kafka.nivel4;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.*;

/**
 * DurableProducer - Nivel 4
 *
 * Demuestra empíricamente cómo diferentes niveles de acks afectan
 * el throughput y las garantías de durabilidad.
 *
 * Uso:
 *   java -cp target/... com.nexus.kafka.nivel4.DurableProducer <topic> <count> <acks>
 *
 * Ejemplos:
 *   DurableProducer critical-data 5000 0       (sin confirmacion - max throughput)
 *   DurableProducer critical-data 5000 1       (solo leader confirma)
 *   DurableProducer critical-data 5000 all     (leader + ISR confirman)
 *   DurableProducer critical-data continuous all (modo continuo - Ctrl+C para detener)
 */
public class DurableProducer {

    private static final String BOOTSTRAP = "localhost:9092,localhost:9093,localhost:9094";

    public static void main(String[] args) throws Exception {

        if (args.length < 3) {
            System.out.println();
            System.out.println("  Uso: DurableProducer <topic> <count|continuous> <acks>");
            System.out.println("  Ejemplos:");
            System.out.println("    DurableProducer critical-data 5000 0");
            System.out.println("    DurableProducer critical-data 5000 1");
            System.out.println("    DurableProducer critical-data 5000 all");
            System.out.println("    DurableProducer critical-data continuous all");
            System.exit(1);
        }

        String topic    = args[0];
        String countArg = args[1];
        String acksArg  = args[2];

        boolean modoContinu = countArg.equalsIgnoreCase("continuous");
        int     totalMsgs   = modoContinu ? Integer.MAX_VALUE : Integer.parseInt(countArg);

        // Normalizar acks
        String acksVal = acksArg.equals("-1") ? "all" : acksArg;
        if (!acksVal.equals("0") && !acksVal.equals("1") && !acksVal.equals("all")) {
            System.err.println("  [ERROR] acks debe ser: 0, 1 o all");
            System.exit(1);
        }

        // ─── Mostrar configuración y garantías ──────────────────────────────
        System.out.println();
        System.out.println("╔════════════════════════════════════════════════════════╗");
        System.out.println("║          DURABLE PRODUCER - NIVEL 4                    ║");
        System.out.println("╚════════════════════════════════════════════════════════╝");
        System.out.println();
        System.out.println("  Configuracion del producer:");
        System.out.printf("  • Topic           : %s%n", topic);
        System.out.printf("  • Mensajes        : %s%n", modoContinu ? "continuo (Ctrl+C)" : totalMsgs);
        System.out.printf("  • Nivel de ACKs   : %s%n", acksVal);
        System.out.println();
        System.out.println("  Garantias con acks=" + acksVal + ":");
        switch (acksVal) {
            case "0":
                System.out.println("    • El producer NO espera confirmacion de ningun broker");
                System.out.println("    • Maximo throughput, minima latencia");
                System.out.println("    • RIESGO: mensajes pueden perderse silenciosamente");
                System.out.println("    • Caso de uso: telemetria de alta frecuencia, logs desechables");
                break;
            case "1":
                System.out.println("    • El producer espera confirmacion SOLO del leader");
                System.out.println("    • Balance entre velocidad y durabilidad");
                System.out.println("    • Si el leader falla justo despues de confirmar,");
                System.out.println("      el mensaje podria perderse antes de replicarse");
                System.out.println("    • Caso de uso: logs importantes, metricas de negocio");
                break;
            case "all":
                System.out.println("    • El producer espera confirmacion del leader + TODOS los ISR");
                System.out.println("    • Maxima durabilidad - mensajes confirmados sobreviven fallos");
                System.out.println("    • Requiere que min.insync.replicas replicas esten disponibles");
                System.out.println("    • Caso de uso: transacciones financieras, datos de auditoria");
                break;
        }
        System.out.println();

        // ─── Configurar KafkaProducer ────────────────────────────────────────
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,      BOOTSTRAP);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,   StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG,                   acksVal);
        props.put(ProducerConfig.RETRIES_CONFIG,                3);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG,             16384);
        props.put(ProducerConfig.LINGER_MS_CONFIG,              5);
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,     true);
        // Con idempotencia, max.in.flight debe ser <= 5
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5);

        // Métricas acumuladas
        AtomicLong  enviados    = new AtomicLong(0);
        AtomicLong  errores     = new AtomicLong(0);
        List<Long>  latencias   = new CopyOnWriteArrayList<>();
        long        tiempoInicio = System.currentTimeMillis();

        // Para modo continuo: detectar Ctrl+C
        AtomicBoolean corriendo = new AtomicBoolean(true);
        if (modoContinu) {
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                corriendo.set(false);
                System.out.println("\n\n  [Ctrl+C detectado - deteniendo producer...]");
            }));
        }

        System.out.println("  Enviando mensajes...");
        System.out.println("  ──────────────────────────────────────────────────────────");

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {

            int progressInterval = Math.max(totalMsgs / 20, 100);
            CountDownLatch latch = new CountDownLatch(0); // reset en bucle

            for (int i = 0; i < totalMsgs && (modoContinu ? corriendo.get() : true); i++) {

                String clave  = "tx-" + String.format("%06d", i % 1000);
                String valor  = String.format(
                    "{\"id\":%d,\"ts\":%d,\"acks\":\"%s\",\"data\":\"%-100s\"}",
                    i, System.currentTimeMillis(), acksVal, "x".repeat(100));

                ProducerRecord<String, String> record = new ProducerRecord<>(topic, clave, valor);
                long tEnvio = System.currentTimeMillis();

                producer.send(record, (metadata, exception) -> {
                    if (exception == null) {
                        enviados.incrementAndGet();
                        latencias.add(System.currentTimeMillis() - tEnvio);
                    } else {
                        errores.incrementAndGet();
                        if (errores.get() <= 5) {
                            System.err.printf("  [ERROR] %s%n", exception.getMessage());
                        }
                    }
                });

                // Mostrar progreso
                if (!modoContinu && (i + 1) % progressInterval == 0) {
                    int pct = (int) ((i + 1) * 100.0 / totalMsgs);
                    int llenos = pct / 5;
                    String barra = "█".repeat(llenos) + "░".repeat(20 - llenos);
                    long elapsed = System.currentTimeMillis() - tiempoInicio;
                    long tps     = elapsed > 0 ? (i + 1) * 1000L / elapsed : 0;
                    System.out.printf("\r  [%s] %3d%% (%,d/%,d msgs)  ~%,d msg/s  Errores: %d",
                            barra, pct, i + 1, totalMsgs, tps, errores.get());
                } else if (modoContinu && enviados.get() % 500 == 0 && enviados.get() > 0) {
                    System.out.printf("\r  Enviados: %,d  Errores: %d", enviados.get(), errores.get());
                }
            }

            // Flush para asegurar que todos los mensajes pendientes se envíen
            producer.flush();
        }

        System.out.println();

        // ─── Reporte final ────────────────────────────────────────────────────
        long tiempoTotal = System.currentTimeMillis() - tiempoInicio;
        printReport(topic, acksVal, enviados.get(), errores.get(), latencias, tiempoTotal);

        // ─── Verificación: contar mensajes reales en el topic ─────────────────
        System.out.println("  Verificacion: contando mensajes reales en el topic...");
        long mensajesEnTopic = contarMensajesEnTopic(topic);
        System.out.printf("  Mensajes confirmados por producer : %,d%n", enviados.get());
        System.out.printf("  Mensajes encontrados en el topic  : %,d%n", mensajesEnTopic);
        if (enviados.get() > 0 && mensajesEnTopic >= enviados.get()) {
            System.out.println("  ✅ Todos los mensajes confirmados estan en el topic");
        } else if (acksArg.equals("0")) {
            System.out.println("  ⚠  Con acks=0 los conteos pueden diferir (no hay garantia)");
        }
        System.out.println();
    }

    // ─── Reporte de métricas ──────────────────────────────────────────────────

    private static void printReport(String topic, String acks, long enviados, long errores,
                                    List<Long> latencias, long tiempoMs) {
        System.out.println();
        System.out.println("  ════════════════════════════════════════════════════════════");
        System.out.println("  RESULTADOS DEL BENCHMARK DE DURABILIDAD");
        System.out.println("  ════════════════════════════════════════════════════════════");
        System.out.printf("  Topic          : %s%n", topic);
        System.out.printf("  Nivel acks     : %s%n", acks);
        System.out.printf("  Tiempo total   : %.2f seg%n", tiempoMs / 1000.0);
        System.out.println();

        long tps = tiempoMs > 0 ? enviados * 1000L / tiempoMs : 0;
        System.out.println("  Throughput:");
        System.out.printf("    • Mensajes/segundo : %,d%n", tps);
        System.out.printf("    • Enviados OK      : %,d%n", enviados);
        System.out.printf("    • Errores          : %,d%n", errores);
        System.out.println();

        if (!latencias.isEmpty()) {
            Collections.sort(latencias);
            long p50  = latencias.get((int) (latencias.size() * 0.50));
            long p95  = latencias.get((int) (latencias.size() * 0.95));
            long p99  = latencias.get((int) (latencias.size() * 0.99));
            long pmax = latencias.get(latencias.size() - 1);
            double avg = latencias.stream().mapToLong(x -> x).average().orElse(0);

            System.out.println("  Latencia (ms):");
            System.out.printf("    • Promedio    : %.1f%n", avg);
            System.out.printf("    • Percentil 50: %d%n", p50);
            System.out.printf("    • Percentil 95: %d%n", p95);
            System.out.printf("    • Percentil 99: %d%n", p99);
            System.out.printf("    • Maxima      : %d%n", pmax);
        }
        System.out.println();

        // Guardar resultado en CSV
        String archivoCSV = "..\\experimentos\\resultados\\comparacion-acks.txt";
        try {
            java.io.File f = new java.io.File(archivoCSV);
            boolean escribirCabecera = !f.exists() || f.length() == 0;
            try (java.io.PrintWriter pw = new java.io.PrintWriter(
                    new java.io.FileWriter(archivoCSV, true))) {
                if (escribirCabecera) {
                    pw.println("Timestamp,Topic,Acks,Mensajes,Errores,TPS,TiempoMs,P50ms,P95ms,P99ms");
                }
                long p50  = latencias.isEmpty() ? 0 : latencias.get((int)(latencias.size()*0.50));
                long p95  = latencias.isEmpty() ? 0 : latencias.get((int)(latencias.size()*0.95));
                long p99  = latencias.isEmpty() ? 0 : latencias.get((int)(latencias.size()*0.99));
                pw.printf("%s,%s,%s,%d,%d,%d,%d,%d,%d,%d%n",
                        new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()),
                        topic, acks, enviados, errores, tps, tiempoMs, p50, p95, p99);
                System.out.printf("  Resultado guardado en: %s%n", archivoCSV);
            }
        } catch (Exception e) {
            // No crítico si no puede guardar
        }
    }

    // ─── Verificación: consumir y contar mensajes ─────────────────────────────

    private static long contarMensajesEnTopic(String topic) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,  BOOTSTRAP);
        props.put(ConsumerConfig.GROUP_ID_CONFIG,           "durable-verifier-" + System.currentTimeMillis());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,  "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,   StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        long conteo = 0;
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(List.of(topic));
            // Leer hasta que no haya más mensajes (timeout = 3 segundos)
            long ultimoRegistro = System.currentTimeMillis();
            while (System.currentTimeMillis() - ultimoRegistro < 3000) {
                var records = consumer.poll(Duration.ofMillis(500));
                if (!records.isEmpty()) {
                    conteo += records.count();
                    ultimoRegistro = System.currentTimeMillis();
                }
            }
        } catch (Exception e) {
            return -1; // No crítico
        }
        return conteo;
    }
}
