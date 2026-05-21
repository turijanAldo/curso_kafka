package com.tiendamax;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Properties;

/**
 * V1.2 - Leccion 3: Consumer Groups y Rebalanceo
 *
 * QUE DEMUESTRA:
 *   Kafka distribuye las particiones de un topic entre los consumers de un mismo grupo.
 *   Cuando entra o sale un consumer, Kafka redistribuye las particiones automaticamente.
 *   Ese proceso se llama REBALANCEO.
 *
 *   Este programa hace visible ese proceso: imprime exactamente que particiones
 *   se le quitan (onPartitionsRevoked) y cuales se le asignan (onPartitionsAssigned).
 *
 * REGLA FUNDAMENTAL:
 *   Una particion solo puede ser leida por UN consumer del grupo a la vez.
 *   Si hay mas consumers que particiones, los consumers extras quedan inactivos.
 *
 *   Con 3 particiones en pedidos-estados:
 *     1 consumer  -> recibe las 3 particiones
 *     2 consumers -> ~2 y ~1 particion (Kafka decide la distribucion)
 *     3 consumers -> 1 particion cada uno
 *     4 consumers -> 3 activos + 1 inactivo sin particion
 *
 * COMO EJECUTAR EL EXPERIMENTO:
 *   Abrir 3 terminales separadas. En cada una ejecutar:
 *     mvn exec:java -Dexec.mainClass="com.tiendamax.RebalanceoDemo"
 *
 *   Observar como cada vez que se lanza una nueva instancia, TODAS las instancias
 *   existentes imprimen el rebalanceo y reciben nuevas particiones.
 *
 *   Luego matar una instancia con Ctrl+C y observar como las restantes
 *   absorben sus particiones.
 */
public class RebalanceoDemo {

    private static final String TOPIC    = "pedidos-estados";
    private static final String BROKER   = "localhost:9092";
    private static final String GROUP_ID = "grupo-rebalanceo";

    public static void main(String[] args) {

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKER);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,   StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        // heartbeat.interval.ms: cada cuanto el consumer avisa al broker que sigue vivo.
        // session.timeout.ms: si el broker no recibe heartbeat en este tiempo, declara
        //   al consumer muerto y dispara un rebalanceo. Valores bajos = rebalanceo rapido
        //   pero mas sensible a pauses de GC. Reducimos aqui para ver el efecto rapido.
        props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, "1000");
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG,    "6000");

        Thread mainThread = Thread.currentThread();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println();
            System.out.println("[SHUTDOWN] Cerrando consumer - esto disparara un rebalanceo en el grupo.");
            mainThread.interrupt();
        }));

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {

            // Al suscribirse pasamos un ConsumerRebalanceListener.
            // Kafka lo llama automaticamente cada vez que hay un rebalanceo.
            consumer.subscribe(List.of(TOPIC), new RebalanceoListener());

            System.out.println("Consumer iniciado. Grupo: [" + GROUP_ID + "]");
            System.out.println("Esperando asignacion de particiones...");
            System.out.println("(Lanza otra instancia de este programa para ver el rebalanceo)");
            System.out.println();

            while (!Thread.currentThread().isInterrupted()) {

                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

                if (!records.isEmpty()) {
                    System.out.println("  Mensajes recibidos: " + records.count());
                    records.forEach(r ->
                        System.out.printf("    P%d offset=%-4d  key=%-8s  value=%s%n",
                            r.partition(), r.offset(), r.key(),
                            r.value().length() > 60 ? r.value().substring(0, 60) + "..." : r.value())
                    );
                }
            }

        } catch (Exception e) {
            if (!Thread.currentThread().isInterrupted()) {
                System.err.println("Error: " + e.getMessage());
            }
        }

        System.out.println("Consumer cerrado.");
    }

    // ────────────────────────────────────────────────────────────────────────

    /**
     * Escucha los eventos de rebalanceo de Kafka.
     *
     * onPartitionsRevoked:  se llama ANTES del rebalanceo.
     *   Kafka avisa "te voy a quitar estas particiones".
     *   Aqui se deben hacer commits pendientes para no perder progreso.
     *
     * onPartitionsAssigned: se llama DESPUES del rebalanceo.
     *   Kafka avisa "estas son tus particiones nuevas".
     *   Aqui se puede inicializar estado local para esas particiones.
     */
    static class RebalanceoListener implements ConsumerRebalanceListener {

        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> particiones) {
            if (particiones.isEmpty()) return;

            System.out.println();
            System.out.println(">>> REBALANCEO INICIADO <<<");
            System.out.println("    Particiones REVOCADAS (me las quitaron):");
            particiones.stream()
                .sorted((a, b) -> Integer.compare(a.partition(), b.partition()))
                .forEach(p -> System.out.println("      - " + p.topic() + " [P" + p.partition() + "]"));
            System.out.println("    (Kafka redistribuira estas particiones entre los consumers activos)");
        }

        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> particiones) {
            System.out.println();
            if (particiones.isEmpty()) {
                System.out.println(">>> REBALANCEO COMPLETADO <<<");
                System.out.println("    Sin particiones asignadas.");
                System.out.println("    Hay mas consumers que particiones en el grupo.");
                System.out.println("    Este consumer esta INACTIVO hasta que salga otro del grupo.");
            } else {
                System.out.println(">>> REBALANCEO COMPLETADO <<<");
                System.out.println("    Particiones ASIGNADAS a este consumer:");
                particiones.stream()
                    .sorted((a, b) -> Integer.compare(a.partition(), b.partition()))
                    .forEach(p -> System.out.println("      - " + p.topic() + " [P" + p.partition() + "]"));
            }
            System.out.println();
        }
    }
}
