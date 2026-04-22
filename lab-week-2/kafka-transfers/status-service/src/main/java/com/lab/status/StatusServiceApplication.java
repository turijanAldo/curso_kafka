package com.lab.status;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Punto de entrada del microservicio status-service.
 *
 * Este servicio tiene dos roles:
 *   1. Consumer Kafka: escucha todos los eventos del Saga y actualiza
 *      el campo status en la tabla transactions
 *   2. REST API: expone GET /transfers/{id}/status para consultar
 *      el estado actual de una transferencia
 *
 * Al arrancar:
 *   1. MySQL connection pool inicializado
 *   2. Flyway ejecuta V1__create_status_schema.sql
 *      → crea transactions IF NOT EXISTS (probablemente ya existe de transfer-api)
 *   3. Hibernate valida entidades
 *   4. Tomcat arranca en puerto 8083
 *   5. Kafka consumer group status-group se registra en los topics del Saga
 */
@SpringBootApplication
public class StatusServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(StatusServiceApplication.class, args);
    }
}
