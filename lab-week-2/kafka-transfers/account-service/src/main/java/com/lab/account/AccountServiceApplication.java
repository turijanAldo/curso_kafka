package com.lab.account;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Punto de entrada del microservicio account-service.
 *
 * Este es el servicio más complejo del Saga. Al arrancar:
 *   1. MySQL connection pool inicializado
 *   2. Flyway ejecuta V1__create_account_schema.sql
 *      → crea tabla accounts (con campo version para Optimistic Locking)
 *      → crea tabla processed_events (con UNIQUE constraint para idempotencia)
 *   3. Hibernate valida entidades contra el schema
 *   4. Dos consumer groups de Kafka se registran:
 *      → account-debit-group  en transfer.validated
 *      → account-credit-group en transfer.debited
 *
 * El flag app.simulate-credit-failure (en application.yml) controla
 * si CreditService simula un fallo para demostrar el rollback del Saga.
 */
@SpringBootApplication
public class AccountServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(AccountServiceApplication.class, args);
    }
}
