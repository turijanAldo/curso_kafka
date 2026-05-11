package com.nexus.kafka.nivel3;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * Producer de carga balanceada para el Laboratorio Kafka - Nivel 3.
 *
 * Demuestra como el trabajo de procesar writes se distribuye automaticamente
 * entre los 3 brokers del cluster. Cada broker es leader de un subconjunto de
 * particiones, y las escrituras a esas particiones van a ese broker.
 *
 * Estrategias de keys disponibles:
 *   hash       -> claves hash uniformes: garantiza distribucion perfecta
 *   secuencial -> key-001, key-002... distribucion basada en murmur2
 *   random     -> claves UUID aleatorias: distribucion estadisticamente uniforme
 *
 * Uso:
 *   "%JAVA_HOME%\bin\java" -cp target\kafka-lab-nivel-3-1.3.0.jar
 *       com.nexus.kafka.nivel3.LoadBalancedProducer <topic> <cantidad> <estrategia>
 *
 * Ejemplo:
 *   "%JAVA_HOME%\bin\java" -cp target\kafka-lab-nivel-3-1.3.0.jar
 *       com.nexus.kafka.nivel3.LoadBalancedProducer transacciones-6p 600 hash
 */
public class LoadBalancedProducer {

    // Apuntar a los 3 brokers: si uno falla, el cliente reconecta automaticamente
    private static final String BOOTSTRAP_SERVERS = "localhost:9092,localhost:9093,localhost:9094";
    private static final Random RNG = new Random();

    public static void main(String[] args) throws InterruptedException {
        if (args.length < 3) {
            System.err.println("Uso: LoadBalancedProducer <topic> <cantidad> <estrategia: hash|secuencial|random>");
            System.exit(1);
        }

        String  topic      = args[0];
        int     cantidad   = Integer.parseInt(args[1]);
        String  estrategia = args[2].toLowerCase();

        System.out.println("\n🚀 LoadBalancedProducer iniciado");

        Properties props = buildProducerProperties();

        // Contadores por particion y por broker
        Map<Integer, AtomicLong> porParticion = new ConcurrentHashMap<>();
        Map<Integer, AtomicLong> porBroker    = new ConcurrentHashMap<>();
        CountDownLatch latch = new CountDownLatch(cantidad);

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {

            // ── Mostrar informacion pre-envio del topic ──────────
            List<PartitionInfo> metadatos = producer.partitionsFor(topic);
            int numParticiones = metadatos.size();

            System.out.println("📊 Analizando topic: " + topic);
            System.out.println("   Particiones: " + numParticiones);
            System.out.println("   Distribucion de leaders:");

            // Mapa particion -> broker leader (para el reporte final)
            Map<Integer, Integer> particionABroker = new HashMap<>();
            metadatos.forEach(p -> {
                int leaderId = p.leader() != null ? p.leader().id() : -1;
                particionABroker.put(p.partition(), leaderId);
                System.out.printf("     Partition %d -> Broker %d%n", p.partition(), leaderId);
                porParticion.put(p.partition(), new AtomicLong(0));
                porBroker.computeIfAbsent(leaderId, k -> new AtomicLong(0));
            });

            // ── Enviar mensajes ──────────────────────────────────
            System.out.printf("%n📤 Enviando %d mensajes con estrategia: %s%n", cantidad, estrategia);
            long inicio = System.currentTimeMillis();

            for (int i = 0; i < cantidad; i++) {
                String clave = generarClave(estrategia, i, numParticiones);
                String valor = String.format(
                    "{\"msgId\":%d,\"key\":\"%s\",\"estrategia\":\"%s\",\"valor\":%.2f}",
                    i, clave, estrategia, i * 1.5
                );

                final int msgNum = i;
                ProducerRecord<String, String> record = new ProducerRecord<>(topic, clave, valor);

                producer.send(record, (metadata, ex) -> {
                    if (ex == null) {
                        porParticion.get(metadata.partition()).incrementAndGet();
                        int broker = particionABroker.getOrDefault(metadata.partition(), -1);
                        porBroker.computeIfAbsent(broker, k -> new AtomicLong(0)).incrementAndGet();
                    }
                    latch.countDown();

                    // Barra de progreso cada 50 mensajes
                    long procesados = cantidad - latch.getCount();
                    if (procesados % 50 == 0 || procesados == cantidad) {
                        int pct   = (int) ((procesados / (double) cantidad) * 100);
                        int llenas = pct / 5;
                        System.out.printf("\r  [%s%s] %3d%% (%d/%d)",
                            "█".repeat(llenas), "░".repeat(20 - llenas), pct, procesados, cantidad);
                        System.out.flush();
                    }
                });
            }

            producer.flush();
            latch.await();
            System.out.println();

            long duracionMs = System.currentTimeMillis() - inicio;
            double duracionSeg = duracionMs / 1000.0;
            double throughput  = cantidad / duracionSeg;

            // ── Reporte final ────────────────────────────────────
            System.out.printf("%n✅ Envio completado en %.2f segundos%n", duracionSeg);
            System.out.printf("   Throughput: %.1f mensajes/segundo%n%n", throughput);

            System.out.println("📊 Distribucion final por particion:");
            porParticion.entrySet().stream()
                    .sorted(Map.Entry.comparingByKey())
                    .forEach(e -> {
                        int    part   = e.getKey();
                        long   cnt    = e.getValue().get();
                        double pct    = (cnt / (double) cantidad) * 100;
                        int    broker = particionABroker.getOrDefault(part, -1);
                        System.out.printf("   Partition %d: %d mensajes (%.2f%%) -> Broker %d%n",
                                part, cnt, pct, broker);
                    });

            System.out.println("\n📈 Carga por broker:");
            porBroker.entrySet().stream()
                    .sorted(Map.Entry.comparingByKey())
                    .forEach(e -> {
                        double pct = (e.getValue().get() / (double) cantidad) * 100;
                        System.out.printf("   Broker %d: %d mensajes (%.2f%%)%n",
                                e.getKey(), e.getValue().get(), pct);
                    });

            // Verificar si la carga esta balanceada
            long maxBroker = porBroker.values().stream().mapToLong(AtomicLong::get).max().orElse(0);
            long minBroker = porBroker.values().stream().mapToLong(AtomicLong::get).min().orElse(0);
            boolean balanceado = minBroker > 0 && ((double) maxBroker / minBroker) <= 1.5;
            System.out.println(balanceado
                    ? "\n✅ Carga perfectamente balanceada entre brokers"
                    : "\n⚠️  Carga desbalanceada. Prueba con estrategia 'hash'.");
        }
    }

    /**
     * Genera una clave de mensaje segun la estrategia elegida.
     *
     * @param estrategia "hash", "secuencial" o "random"
     * @param indice     numero de mensaje actual (0-based)
     * @param numParts   numero de particiones del topic
     */
    private static String generarClave(String estrategia, int indice, int numParts) {
        return switch (estrategia) {
            // hash: la clave esta disenada para que el hash module caiga en cada particion uniformemente
            case "hash" -> "key-part-" + (indice % numParts);
            // secuencial: key-000001, key-000002, etc.
            case "secuencial" -> String.format("key-%06d", indice + 1);
            // random: clave aleatoria basada en UUID recortado
            default -> "rnd-" + Long.toHexString(RNG.nextLong() & 0xFFFFFFFFL);
        };
    }

    private static Properties buildProducerProperties() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,   StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "1");         // acks=1 para mayor velocidad en el demo
        props.put(ProducerConfig.LINGER_MS_CONFIG, 5);       // pequeño linger para batching eficiente
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "kafka-lab-nivel3-load-balanced-producer");
        return props;
    }
}
