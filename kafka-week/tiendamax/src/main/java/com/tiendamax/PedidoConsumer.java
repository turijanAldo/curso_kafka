package com.tiendamax;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.tiendamax.model.Pedido;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

/**
 * PASO 3 — Consumer de pedidos con commit manual.
 *
 * QUÉ RESUELVE:
 *   Un consumer que lee pedidos de Kafka y los "procesa" (simula inventario/notificaciones/factura).
 *   Si el servicio cae ANTES de hacer commit, los pedidos se releen al reiniciar → at-least-once.
 *   Si cayera DESPUÉS del commit → ya confirmamos → no se reprocesa → at-most-once.
 *   La diferencia la controla cuándo hacemos commitSync().
 *
 * CONCEPTOS APLICADOS:
 *   - group.id            → identifica el Consumer Group (Módulo 4)
 *   - auto.offset.reset   → qué hacer si no hay committed offset (Módulo 3)
 *   - enable.auto.commit  → FALSO → nosotros controlamos cuándo commitear (Módulo 3)
 *   - commitSync()        → commit manual DESPUÉS de procesar (at-least-once)
 *   - poll loop           → el patrón fundamental de cualquier consumer Kafka
 *
 * CÓMO EJECUTAR (Paso 4 — en 3 terminales distintas):
 *   mvn exec:java -Dexec.mainClass="com.tiendamax.PedidoConsumer" -Dexec.args="inventario"
 *   mvn exec:java -Dexec.mainClass="com.tiendamax.PedidoConsumer" -Dexec.args="notificaciones"
 *   mvn exec:java -Dexec.mainClass="com.tiendamax.PedidoConsumer" -Dexec.args="facturacion"
 *   (los 3 usan el mismo group.id → RangeAssignor distribuye las 3 particiones entre ellos)
 */
public class PedidoConsumer {

    private static final String TOPIC    = "pedidos-tiendamax";
    private static final String BROKER   = "localhost:9092";
    private static final String GROUP_ID = "grupo-tiendamax";

    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static void main(String[] args) throws Exception {

        // Nombre del servicio (pasado como argumento: inventario | notificaciones | facturacion)
        String servicio = args.length > 0 ? args[0] : "generico";

        // ── 1. Configuración del KafkaConsumer ───────────────────────────────
        Properties props = new Properties();

        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKER);

        // group.id: todos los consumers con el mismo ID forman UN Consumer Group.
        // Kafka distribuye las particiones entre ellos (RangeAssignor por defecto).
        // Si lanzas 3 instancias con el mismo group.id → 1 partición por consumer (óptimo).
        props.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);

        // Si no existe un committed offset para este grupo (primera vez o después de reset):
        // "earliest" → lee desde el offset 0 (from-beginning)
        // "latest"   → solo mensajes nuevos (ignora mensajes anteriores)
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // CRÍTICO: desactivar el auto-commit.
        // Si auto-commit=true, Kafka commitea cada N segundos independientemente de si
        // procesaste bien el mensaje. Un crash entre el auto-commit y el procesamiento
        // real → mensaje perdido silenciosamente.
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        // Cuántos mensajes máximo recibe el consumer en cada poll().
        // Con 10 mensajes de prueba, 10 es suficiente para verlos todos en un solo poll.
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 10);

        // Cada cuánto el consumer envía un heartbeat al Group Coordinator para
        // indicar que sigue vivo. Si no llega heartbeat en session.timeout.ms → rebalanceo.
        props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 3000);
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG,    10000);

        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,   StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        // ── 2. Poll loop ──────────────────────────────────────────────────────
        // Registrar shutdown hook para cerrar limpiamente con Ctrl+C
        Thread mainThread = Thread.currentThread();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.printf("\n[%s] 🛑 Señal de cierre recibida. Esperando que el poll termine...%n", servicio);
            mainThread.interrupt();
        }));

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {

            // Subscribirse al topic. Si hay rebalanceo, Kafka asigna particiones automáticamente.
            consumer.subscribe(List.of(TOPIC));

            System.out.printf("[%s] 👂 Escuchando topic '%s' en grupo '%s'...%n",
                servicio, TOPIC, GROUP_ID);
            System.out.printf("[%s]    Particiones asignadas: se muestran tras el primer poll()%n%n", servicio);

            while (!Thread.currentThread().isInterrupted()) {

                // poll(Duration) bloquea hasta que llegan mensajes o se agota el timeout.
                // El consumer envía heartbeats internamente durante el poll.
                // Si tu lógica de procesamiento tarda más que max.poll.interval.ms
                // (default 5 min), Kafka asume que el consumer murió → rebalanceo.
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

                if (records.isEmpty()) {
                    System.out.printf("[%s] ⏳ Esperando mensajes...%n", servicio);
                    continue;
                }

                System.out.printf("[%s] 📦 Recibidos %d mensajes en este poll():%n",
                    servicio, records.count());

                // ── 3. Procesar cada registro ─────────────────────────────────
                for (ConsumerRecord<String, String> record : records) {

                    try {
                        Pedido pedido = MAPPER.readValue(record.value(), Pedido.class);

                        // Simular el trabajo del servicio (inventario/notificaciones/factura)
                        procesarPedido(servicio, pedido, record);

                    } catch (Exception e) {
                        System.err.printf("[%s] ❌ Error procesando offset=%d: %s%n",
                            servicio, record.offset(), e.getMessage());
                        // En producción aquí va: dead-letter topic, retry logic, alertas
                    }
                }

                // ── 4. Commit MANUAL después de procesar TODOS los registros del poll ──
                // commitSync() bloquea hasta que el broker confirma el commit en __consumer_offsets.
                // Si el proceso cae ANTES de llegar aquí → relee los mensajes (at-least-once).
                // Si cae DESPUÉS → mensajes ya confirmados → no se reprocesa (correcto).
                consumer.commitSync();
                System.out.printf("[%s] ✔  Commit realizado. Offsets guardados en __consumer_offsets.%n%n", servicio);
            }

        } catch (Exception e) {
            if (!Thread.currentThread().isInterrupted()) {
                System.err.println("Error inesperado: " + e.getMessage());
            }
        }

        System.out.printf("[%s] 👋 Consumer cerrado limpiamente.%n", servicio);
    }

    /**
     * Simula el procesamiento del pedido según el servicio.
     * En producción aquí va: actualizar DB, enviar email, llamar a API de facturación, etc.
     */
    private static void procesarPedido(String servicio, Pedido pedido,
                                        ConsumerRecord<String, String> record) throws Exception {
        // Simular latencia de procesamiento (50-150ms)
        Thread.sleep(50 + (long)(Math.random() * 100));

        String accion = switch (servicio) {
            case "inventario"      -> "📦 Reservando stock para: " + pedido.getProducto();
            case "notificaciones"  -> "📧 Enviando email a:       " + pedido.getClienteId();
            case "facturacion"     -> "🧾 Generando factura:      $" + pedido.getTotal();
            default                -> "⚙️  Procesando:             " + pedido.getOrderId();
        };

        System.out.printf("  [%s] %s  |  P-%d offset=%-3d  key=%s%n",
            servicio, accion,
            record.partition(), record.offset(),
            record.key());
    }
}
