package com.lab.api.cache;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.lab.api.dto.TransferResponse;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.serializer.Jackson2JsonRedisSerializer;
import org.springframework.data.redis.serializer.StringRedisSerializer;

/**
 * Configuración del RedisTemplate para TransferResponse.
 *
 * ────────────────────────────────────────────────────────────────────────────
 * ¿Por qué configurar el RedisTemplate manualmente?
 * ────────────────────────────────────────────────────────────────────────────
 *
 * Spring Boot auto-configura un RedisTemplate<Object, Object> con serialización
 * Java (JdkSerializationRedisSerializer). Eso tiene dos problemas:
 *
 *   1. Las keys se serializan como bytes Java, no como Strings legibles.
 *      En redis-cli verías: \xac\xed\x00\x05t\x00\x11transfer:f47a...
 *      Con StringRedisSerializer verías: transfer:f47a... (legible)
 *
 *   2. Los values se serializan como bytes Java, no como JSON.
 *      Los objetos deben ser Serializable. Versionar entre releases es frágil.
 *      Con Jackson2JsonRedisSerializer, el value es JSON legible y versionable.
 *
 * ────────────────────────────────────────────────────────────────────────────
 * ¿Por qué Jackson2JsonRedisSerializer y no GenericJackson2JsonRedisSerializer?
 * ────────────────────────────────────────────────────────────────────────────
 *
 * GenericJackson2JsonRedisSerializer incluye el nombre de clase completo en el
 * JSON (ej: "@class": "com.lab.api.dto.TransferResponse"). Esto es útil cuando
 * hay muchos tipos distintos en la misma caché, pero añade ruido al JSON y
 * crea acoplamiento al nombre del paquete.
 *
 * Jackson2JsonRedisSerializer está tipado a TransferResponse → serializa y
 * deserializa directamente sin metadatos de clase. El RedisTemplate sabe el
 * tipo en tiempo de compilación → más limpio y eficiente.
 *
 * ────────────────────────────────────────────────────────────────────────────
 * JavaTimeModule
 * ────────────────────────────────────────────────────────────────────────────
 *
 * TransferResponse tiene campos Instant (createdAt, updatedAt). Sin JavaTimeModule,
 * Jackson no sabe serializar java.time.Instant y lanza:
 *   InvalidDefinitionException: Java 8 date/time type not supported by default
 *
 * WRITE_DATES_AS_TIMESTAMPS = false → serializa Instant como ISO-8601 string
 * ("2024-01-15T10:23:41.123Z") en lugar de array de números ([2024, 1, 15, ...]).
 * Más legible en Redis y más fácil de depurar con redis-cli.
 */
@Configuration
public class RedisConfig {

    @Bean
    public RedisTemplate<String, TransferResponse> redisTemplate(RedisConnectionFactory connectionFactory) {

        // Jackson ObjectMapper con soporte para java.time.*
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.registerModule(new JavaTimeModule());
        objectMapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

        // Serializer de value: TransferResponse ↔ JSON
        Jackson2JsonRedisSerializer<TransferResponse> valueSerializer =
                new Jackson2JsonRedisSerializer<>(objectMapper, TransferResponse.class);

        RedisTemplate<String, TransferResponse> template = new RedisTemplate<>();
        template.setConnectionFactory(connectionFactory);

        // Keys como Strings UTF-8 legibles (ej: "transfer:f47a-...")
        template.setKeySerializer(new StringRedisSerializer());
        template.setHashKeySerializer(new StringRedisSerializer());

        // Values como JSON
        template.setValueSerializer(valueSerializer);
        template.setHashValueSerializer(valueSerializer);

        template.afterPropertiesSet();
        return template;
    }
}
