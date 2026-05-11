package com.nexus.kafka.nivel2;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Producer de alto volumen para el Laboratorio Kafka - Nivel 2.
 *
 * Envia N mensajes de forma asincrona y al final reporta:
 *   - Tiempo total y throughput (mensajes/segundo)
 *   - Distribucion de mensajes por particion
 *
 * Uso:
 *   "%JAVA_HOME%\bin\java" -cp target\kafka-lab-nivel-2-1.0.0.jar
 *       com.nexus.kafka.nivel2.BatchProducer <topic> <cantidad> [usarClaves=true|false]
 *
 * Ejemplo (1000 mensajes con claves):
 *   "%JAVA_HOME%\bin\java" -cp target\kafka-lab-nivel-2-1.0.0.jar
 *       com.nexus.kafka.nivel2.BatchProducer transacciones-4p 1000 true
 *
 * Ejemplo (1000 mensajes sin clave, distribucion round-robin):
 *   "%JAVA_HOME%\bin\java" -cp target\kafka-lab-nivel-2-1.0.0.jar
 *       com.nexus.kafka.nivel2.BatchProducer transacciones-4p 1000 false
 */
public class BatchProducer {

    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    // Cuantos usuarios distintos simular (para generar claves variadas)
    private static final int NUM_USUARIOS = 50;

    public static void main(String[] args) throws InterruptedException {
        if (args.length < 2) {
            System.err.println("Uso: BatchProducer <topic> <cantidad> [usarClaves=true|false]");
            System.exit(1);
        }

        String topic     = args[0];
        int    cantidad  = Integer.parseInt(args[1]);
        boolean usarClaves = args.length >= 3 && Boolean.parseBoolean(args[2]);

        System.out.println("📊 BatchProducer - Iniciando envio");
        System.out.println("   Topic         : " + topic);
        System.out.println("   Mensajes      : " + cantidad);
        System.out.println("   Usar claves   : " + usarClaves);
        System.out.println();

        Properties props = buildProducerProperties();

        // Contadores atomicos para uso en callbacks concurrentes
        AtomicInteger  enviados   = new AtomicInteger(0);
        AtomicInteger  errores    = new AtomicInteger(0);
        // Distribucion por particion: clave=particion, valor=cantidad de mensajes
        Map<Integer, AtomicLong> porParticion = new HashMap<>();

        // CountDownLatch: esperar a que todos los callbacks se ejecuten
        CountDownLatch latch = new CountDownLatch(cantidad);

        long inicio = System.currentTimeMillis();

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            System.out.println("⏱️  Enviando mensajes...");

            for (int i = 0; i < cantidad; i++) {
                // Generar clave y valor del mensaje
                String clave = usarClaves ? "user-" + String.format("%03d", (i % NUM_USUARIOS) + 1) : null;
                String valor = String.format(
                    "{\"msgId\": %d, \"userId\": \"%s\", \"monto\": %.2f, \"tipo\": \"pago\"}",
                    i, clave != null ? clave : "anonymous", (i % 100) * 10.5
                );

                ProducerRecord<String, String> record = new ProducerRecord<>(topic, clave, valor);

                // send() asincrono con Callback: se ejecuta cuando el broker confirma (o rechaza)
                // Esto es mucho mas eficiente que .get() bloqueante para alto volumen
                final int msgNum = i;
                producer.send(record, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception exception) {
                        if (exception != null) {
                            errores.incrementAndGet();
                            System.err.println("❌ Error en mensaje " + msgNum + ": " + exception.getMessage());
                        } else {
                            enviados.incrementAndGet();
                            // Registrar en que particion cayo este mensaje
                            porParticion
                                .computeIfAbsent(metadata.partition(), k -> new AtomicLong(0))
                                .incrementAndGet();
                        }
                        latch.countDown();

                        // Mostrar barra de progreso cada 100 mensajes
                        int total = enviados.get() + errores.get();
                        if (total % 100 == 0 || total == cantidad) {
                            int pct      = (int) ((total / (double) cantidad) * 100);
                            int llenas   = pct / 5;
                            int vacias   = 20 - llenas;
                            System.out.printf("\r  [%s%s] %3d%% (%d/%d)",
                                "=".repeat(llenas), ".".repeat(vacias), pct, total, cantidad);
                            System.out.flush();
                        }
                    }
                });

                // linger.ms=5 agrupa mensajes en batch; aqui simplemente continuamos
            }

            // flush() garantiza que todos los mensajes en buffer se envien antes de cerrar
            producer.flush();
        }

        // Esperar a que todos los callbacks terminen
        latch.await();
        System.out.println();  // Nueva linea tras la barra de progreso

        long duracionMs = System.currentTimeMillis() - inicio;
        double duracionSeg = duracionMs / 1000.0;
        double throughput  = enviados.get() / duracionSeg;

        // ── Resumen final ────────────────────────────────────────
        System.out.println();
        System.out.println("✅ Envio completado:");
        System.out.printf("   Tiempo total : %.2f segundos%n", duracionSeg);
        System.out.printf("   Throughput   : %.0f mensajes/segundo%n", throughput);
        System.out.printf("   Enviados OK  : %d%n", enviados.get());
        System.out.printf("   Errores      : %d%n", errores.get());
        System.out.println();
        System.out.println("📈 Distribucion por particion:");

        long total = enviados.get();
        porParticion.entrySet().stream()
                .sorted(Map.Entry.comparingByKey())
                .forEach(e -> {
                    long count = e.getValue().get();
                    double pct = total > 0 ? (count / (double) total) * 100 : 0;
                    System.out.printf("   Partition %d : %d mensajes (%.1f%%)%n",
                            e.getKey(), count, pct);
                });
    }

    private static Properties buildProducerProperties() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,   StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "1");         // acks=1 para mayor throughput en batch
        props.put(ProducerConfig.RETRIES_CONFIG, 3);
        // linger.ms=5: esperar hasta 5ms para agrupar mensajes en un batch mas grande
        // Mejora throughput a costa de latencia minima
        props.put(ProducerConfig.LINGER_MS_CONFIG, 5);
        // batch.size: tamano maximo del batch en bytes (16KB)
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "kafka-lab-nivel2-batch-producer");
        return props;
    }
}
