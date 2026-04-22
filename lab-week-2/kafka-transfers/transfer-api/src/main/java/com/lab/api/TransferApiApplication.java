package com.lab.api;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Punto de entrada del microservicio transfer-api.
 *
 * @SpringBootApplication es una anotación compuesta que activa tres cosas:
 *
 *   @Configuration       → esta clase puede declarar @Bean s
 *   @EnableAutoConfiguration → Spring Boot configura automáticamente todo lo que
 *                              encuentre en el classpath (Tomcat, Hibernate,
 *                              Kafka, Flyway, etc.) usando los valores de
 *                              application.yml
 *   @ComponentScan       → escanea todos los @Component, @Service, @Repository,
 *                          @Controller en el paquete com.lab.api y subpaquetes
 *
 * Por eso esta clase DEBE estar en el paquete raíz (com.lab.api) y no
 * en un subpaquete. Si estuviera en com.lab.api.config, el @ComponentScan
 * solo escanaría com.lab.api.config y no encontraría los controllers ni servicios.
 */
@SpringBootApplication
public class TransferApiApplication {

    public static void main(String[] args) {
        SpringApplication.run(TransferApiApplication.class, args);
    }
}
