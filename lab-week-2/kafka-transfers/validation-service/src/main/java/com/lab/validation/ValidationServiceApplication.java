package com.lab.validation;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Punto de entrada del microservicio validation-service.
 *
 * Este servicio NO expone endpoints HTTP (no tiene spring-boot-starter-web).
 * Spring Boot arranca de todas formas porque tiene spring-kafka y spring-data-jpa.
 * El proceso queda vivo escuchando el topic transfer.requested indefinidamente.
 *
 * Secuencia de arranque:
 *   1. MySQL connection pool inicializado
 *   2. Flyway ejecuta V1__create_validation_schema.sql (si no fue ejecutado antes)
 *   3. Hibernate valida las entidades contra el schema (ddl-auto: validate)
 *   4. Kafka consumer se registra en el grupo validation-group
 *   5. Kafka consumer empieza a hacer polling en transfer.requested
 */
@SpringBootApplication
public class ValidationServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(ValidationServiceApplication.class, args);
    }
}
