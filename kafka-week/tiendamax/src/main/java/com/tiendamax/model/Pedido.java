package com.tiendamax.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Modelo de un pedido de TiendaMax.
 * Se serializa a JSON antes de publicarse en Kafka.
 *
 * KEY en Kafka  = orderId  → garantiza orden de eventos por pedido
 * VALUE en Kafka = JSON de este objeto
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class Pedido {

    @JsonProperty("order_id")
    private String orderId;    // ORD-001, ORD-002...  ← será la KEY de Kafka


    private String clienteId;  // cli-ana, cli-bob...
    private String producto;   // nombre del producto
    private double total;      // monto en MXN
    private String estado;     // CREADO | PAGADO | ENVIADO | ENTREGADO
    private long   timestamp;  // epoch ms al momento de crear el pedido

    // Constructor vacío requerido por Jackson para deserializar
    public Pedido() {}

    public Pedido(String orderId, String clienteId, String producto, double total) {
        this.orderId   = orderId;
        this.clienteId = clienteId;
        this.producto  = producto;
        this.total     = total;
        this.estado    = "CREADO";
        this.timestamp = System.currentTimeMillis();
    }

    // ── Getters ──────────────────────────────────────────────────────────────
    public String getOrderId()   { return orderId; }
    public String getClienteId() { return clienteId; }
    public String getProducto()  { return producto; }
    public double getTotal()     { return total; }
    public String getEstado()    { return estado; }
    public long   getTimestamp() { return timestamp; }

    // ── Setters ──────────────────────────────────────────────────────────────
    public void setOrderId(String orderId)     { this.orderId   = orderId; }
    public void setClienteId(String clienteId) { this.clienteId = clienteId; }
    public void setProducto(String producto)   { this.producto  = producto; }
    public void setTotal(double total)         { this.total     = total; }
    public void setEstado(String estado)       { this.estado    = estado; }
    public void setTimestamp(long timestamp)   { this.timestamp = timestamp; }

    @Override
    public String toString() {
        return String.format("Pedido{orderId='%s', cliente='%s', producto='%s', total=%.2f, estado='%s'}",
                orderId, clienteId, producto, total, estado);
    }
}
