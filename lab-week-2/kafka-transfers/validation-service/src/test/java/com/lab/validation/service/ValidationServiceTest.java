package com.lab.validation.service;

import com.lab.common.event.TransferRequestedEvent;
import com.lab.validation.producer.ValidationEventProducer;
import com.lab.validation.repository.AccountRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

/**
 * Tests unitarios de ValidationService.
 *
 * CAPA: Unitaria (Pirámide base)
 * FRAMEWORK: JUnit 5 + Mockito
 * SCOPE: Solo la lógica de ValidationService, sin DB ni Kafka reales.
 *
 * Por qué tests unitarios aquí y no de integración:
 *   1. ValidationService tiene 4 ramas de decisión claras → perfectas para unit tests
 *   2. Toda la I/O (DB, Kafka) está detrás de interfaces → fácilmente mockeable
 *   3. Los tests corren en <100ms (sin Docker, sin red, sin Spring context)
 *   4. Feedback inmediato durante el desarrollo
 *
 * Estructura: @Nested para agrupar por regla de negocio.
 * Cada clase interna representa un escenario de validación.
 *
 * PATRÓN: Arrange → Act → Assert (AAA)
 *   - Arrange: configurar mocks y evento de entrada
 *   - Act:     llamar al método bajo prueba
 *   - Assert:  verificar que el producer publicó el evento correcto
 */
@ExtendWith(MockitoExtension.class)  // Activa Mockito sin Spring Context
@DisplayName("ValidationService — Tests unitarios de reglas de negocio")
class ValidationServiceTest {

    // ── Colaboradores mockeados ──────────────────────────────────────────────
    // Mockito crea implementaciones falsas de estas interfaces/clases.
    // NO hay conexión real a MySQL ni a Kafka.

    @Mock
    private AccountRepository accountRepository;

    @Mock
    private ValidationEventProducer producer;

    // ── Sistema bajo prueba (SUT) ────────────────────────────────────────────
    // @InjectMocks crea una instancia real de ValidationService e inyecta
    // los mocks declarados arriba como si fueran los beans reales de Spring.
    @InjectMocks
    private ValidationService validationService;

    // ── Evento base (reutilizado en todos los tests) ─────────────────────────
    private TransferRequestedEvent baseEvent;

    @BeforeEach
    void setUp() {
        // Un evento válido que pasaría todas las reglas con los mocks correctos.
        // Cada test puede sobreescribir los campos que necesite probar.
        baseEvent = TransferRequestedEvent.builder()
                .transactionId(UUID.randomUUID().toString())
                .fromAccount("ACC-001")
                .toAccount("ACC-002")
                .amount(new BigDecimal("100.00"))
                .timestamp(Instant.now())
                .build();
    }

    // ════════════════════════════════════════════════════════════════════════
    // REGLA 1: Cuenta origen ≠ cuenta destino
    // ════════════════════════════════════════════════════════════════════════

    @Nested
    @DisplayName("Regla 1 — Cuentas distintas (sin DB)")
    class Regla1CuentasDistintas {

        @Test
        @DisplayName("FAILED cuando fromAccount == toAccount")
        void debePublicarFailed_cuandoCuentasIguales() {
            // Arrange: modificar el evento base para que from == to
            TransferRequestedEvent evento = TransferRequestedEvent.builder()
                    .transactionId(UUID.randomUUID().toString())
                    .fromAccount("ACC-001")
                    .toAccount("ACC-001")  // ← misma cuenta que from
                    .amount(new BigDecimal("50.00"))
                    .timestamp(Instant.now())
                    .build();

            // Act
            validationService.validate(evento);

            // Assert: debe publicar FAILED con el mensaje de error correcto
            verify(producer).publishFailed(
                    eq(evento),
                    contains("origen y destino no pueden ser la misma"),
                    eq("validation-service")
            );

            // Assert negativo: NO debe publicar VALIDATED si falla la regla 1
            verify(producer, never()).publishValidated(any());

            // Assert: la verificación de DB no ocurrió (fail fast en memoria)
            verifyNoInteractions(accountRepository);
        }
    }

    // ════════════════════════════════════════════════════════════════════════
    // REGLA 2: Monto > 0
    // ════════════════════════════════════════════════════════════════════════

    @Nested
    @DisplayName("Regla 2 — Monto positivo (sin DB)")
    class Regla2MontoPositivo {

        @Test
        @DisplayName("FAILED cuando amount es cero")
        void debePublicarFailed_cuandoMontoCero() {
            // Arrange
            TransferRequestedEvent evento = TransferRequestedEvent.builder()
                    .transactionId(UUID.randomUUID().toString())
                    .fromAccount("ACC-001")
                    .toAccount("ACC-002")
                    .amount(BigDecimal.ZERO)  // ← monto inválido
                    .timestamp(Instant.now())
                    .build();

            // Act
            validationService.validate(evento);

            // Assert
            verify(producer).publishFailed(
                    eq(evento),
                    contains("mayor que cero"),
                    eq("validation-service")
            );
            verify(producer, never()).publishValidated(any());
            verifyNoInteractions(accountRepository);
        }

        @Test
        @DisplayName("FAILED cuando amount es negativo")
        void debePublicarFailed_cuandoMontoNegativo() {
            // Arrange
            TransferRequestedEvent evento = TransferRequestedEvent.builder()
                    .transactionId(UUID.randomUUID().toString())
                    .fromAccount("ACC-001")
                    .toAccount("ACC-002")
                    .amount(new BigDecimal("-50.00"))  // ← negativo
                    .timestamp(Instant.now())
                    .build();

            // Act
            validationService.validate(evento);

            // Assert
            verify(producer).publishFailed(
                    eq(evento),
                    contains("mayor que cero"),
                    eq("validation-service")
            );
            verifyNoInteractions(accountRepository);
        }

        @Test
        @DisplayName("FAILED cuando amount es null")
        void debePublicarFailed_cuandoMontoNull() {
            // Arrange
            TransferRequestedEvent evento = TransferRequestedEvent.builder()
                    .transactionId(UUID.randomUUID().toString())
                    .fromAccount("ACC-001")
                    .toAccount("ACC-002")
                    .amount(null)  // ← null
                    .timestamp(Instant.now())
                    .build();

            // Act
            validationService.validate(evento);

            // Assert
            verify(producer).publishFailed(
                    eq(evento),
                    contains("mayor que cero"),
                    eq("validation-service")
            );
            verifyNoInteractions(accountRepository);
        }
    }

    // ════════════════════════════════════════════════════════════════════════
    // REGLA 3: Cuenta origen existe en DB
    // ════════════════════════════════════════════════════════════════════════

    @Nested
    @DisplayName("Regla 3 — Cuenta origen existe (1 query DB)")
    class Regla3CuentaOrigenExiste {

        @Test
        @DisplayName("FAILED cuando cuenta origen no existe")
        void debePublicarFailed_cuandoCuentaOrigenNoExiste() {
            // Arrange: mock devuelve false para fromAccount
            when(accountRepository.existsById("ACC-999")).thenReturn(false);

            TransferRequestedEvent evento = TransferRequestedEvent.builder()
                    .transactionId(UUID.randomUUID().toString())
                    .fromAccount("ACC-999")  // ← no existe
                    .toAccount("ACC-002")
                    .amount(new BigDecimal("100.00"))
                    .timestamp(Instant.now())
                    .build();

            // Act
            validationService.validate(evento);

            // Assert: publica FAILED con referencia a la cuenta inexistente
            verify(producer).publishFailed(
                    eq(evento),
                    contains("ACC-999"),
                    eq("validation-service")
            );
            verify(producer, never()).publishValidated(any());

            // Assert: solo consulta fromAccount (fail fast — no consulta toAccount)
            verify(accountRepository).existsById("ACC-999");
            verify(accountRepository, never()).existsById("ACC-002");
        }
    }

    // ════════════════════════════════════════════════════════════════════════
    // REGLA 4: Cuenta destino existe en DB
    // ════════════════════════════════════════════════════════════════════════

    @Nested
    @DisplayName("Regla 4 — Cuenta destino existe (2 queries DB)")
    class Regla4CuentaDestinoExiste {

        @Test
        @DisplayName("FAILED cuando cuenta destino no existe")
        void debePublicarFailed_cuandoCuentaDestinoNoExiste() {
            // Arrange: fromAccount existe, toAccount no existe
            when(accountRepository.existsById("ACC-001")).thenReturn(true);
            when(accountRepository.existsById("ACC-888")).thenReturn(false);

            TransferRequestedEvent evento = TransferRequestedEvent.builder()
                    .transactionId(UUID.randomUUID().toString())
                    .fromAccount("ACC-001")
                    .toAccount("ACC-888")  // ← no existe
                    .amount(new BigDecimal("100.00"))
                    .timestamp(Instant.now())
                    .build();

            // Act
            validationService.validate(evento);

            // Assert
            verify(producer).publishFailed(
                    eq(evento),
                    contains("ACC-888"),
                    eq("validation-service")
            );
            verify(producer, never()).publishValidated(any());
        }
    }

    // ════════════════════════════════════════════════════════════════════════
    // CAMINO FELIZ: Todas las reglas pasan
    // ════════════════════════════════════════════════════════════════════════

    @Nested
    @DisplayName("Happy path — todas las reglas pasan")
    class HappyPath {

        @Test
        @DisplayName("VALIDATED cuando la transferencia es completamente válida")
        void debePublicarValidated_cuandoTodasLasReglasApasan() {
            // Arrange: ambas cuentas existen
            when(accountRepository.existsById("ACC-001")).thenReturn(true);
            when(accountRepository.existsById("ACC-002")).thenReturn(true);

            // Act
            validationService.validate(baseEvent);

            // Assert: publica VALIDATED (el camino feliz del Saga continúa)
            verify(producer).publishValidated(eq(baseEvent));

            // Assert negativo: NO publica FAILED
            verify(producer, never()).publishFailed(any(), anyString(), anyString());

            // Assert: consultó ambas cuentas en la DB
            verify(accountRepository).existsById("ACC-001");
            verify(accountRepository).existsById("ACC-002");
        }

        @Test
        @DisplayName("VALIDATED con monto mínimo (0.01)")
        void debePublicarValidated_conMontoMinimo() {
            // Arrange
            when(accountRepository.existsById(anyString())).thenReturn(true);

            TransferRequestedEvent evento = TransferRequestedEvent.builder()
                    .transactionId(UUID.randomUUID().toString())
                    .fromAccount("ACC-001")
                    .toAccount("ACC-002")
                    .amount(new BigDecimal("0.01"))  // ← monto mínimo válido
                    .timestamp(Instant.now())
                    .build();

            // Act
            validationService.validate(evento);

            // Assert
            verify(producer).publishValidated(eq(evento));
        }
    }

    // ════════════════════════════════════════════════════════════════════════
    // ORDEN DE EVALUACIÓN: fail-fast entre reglas
    // ════════════════════════════════════════════════════════════════════════

    @Nested
    @DisplayName("Orden fail-fast — no evalúa reglas siguientes tras primer fallo")
    class OrdenFailFast {

        @Test
        @DisplayName("Regla 1 falla → no consulta DB ni evalúa monto")
        void regla1Falla_noConsultaDB() {
            // Arrange: cuentas iguales (regla 1 falla) pero también monto negativo
            // Verificamos que solo se detecta el primer error (regla 1)
            TransferRequestedEvent evento = TransferRequestedEvent.builder()
                    .transactionId(UUID.randomUUID().toString())
                    .fromAccount("ACC-001")
                    .toAccount("ACC-001")    // regla 1 falla
                    .amount(new BigDecimal("-100.00"))  // regla 2 también falla
                    .timestamp(Instant.now())
                    .build();

            // Act
            validationService.validate(evento);

            // Assert: solo 1 publishFailed (el de regla 1, no de regla 2)
            verify(producer, times(1)).publishFailed(any(), anyString(), anyString());

            // El mensaje de error contiene la descripción de regla 1
            verify(producer).publishFailed(
                    eq(evento),
                    contains("origen y destino no pueden ser la misma"),
                    anyString()
            );

            // No se consulta la DB para nada
            verifyNoInteractions(accountRepository);
        }
    }
}
