package com.lab.account.service;

import com.lab.account.entity.Account;
import com.lab.account.repository.AccountRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.math.BigDecimal;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

/**
 * Tests unitarios de DebitService.
 *
 * CAPA: Unitaria (Pirámide base)
 * FRAMEWORK: JUnit 5 + Mockito + AssertJ
 * SCOPE: Lógica pura de débito sin DB real ni transacciones JPA reales.
 *
 * Nota importante sobre @Transactional en tests:
 *   DebitService usa @Transactional pero en este test unitario esa anotación
 *   NO tiene efecto porque no hay un ApplicationContext de Spring.
 *   Mockito simplemente llama al método sin proxy de Spring.
 *   Esto es correcto para tests unitarios — la transaccionalidad
 *   se prueba en los integration tests (DebitConsumerIT).
 *
 * AssertJ vs JUnit Assertions:
 *   assertThat(...).isEqualTo(...)     → AssertJ (mensajes de error descriptivos)
 *   assertEquals(expected, actual)     → JUnit 5 (verboso, sin fluent API)
 *   Preferimos AssertJ para mejor legibilidad.
 *
 * assertThatThrownBy:
 *   Verifica que un bloque de código lanza la excepción esperada.
 *   Es la forma fluent de JUnit's assertThrows.
 */
@ExtendWith(MockitoExtension.class)
@DisplayName("DebitService — Tests unitarios de lógica de débito")
class DebitServiceTest {

    @Mock
    private AccountRepository accountRepository;

    @InjectMocks
    private DebitService debitService;

    // ── Cuenta de prueba ─────────────────────────────────────────────────────
    private Account cuentaAna;

    @BeforeEach
    void setUp() {
        // Crear una cuenta en memoria (sin persistir en DB)
        // No hay @Entity magic aquí — es un POJO puro.
        cuentaAna = new Account();
        cuentaAna.setId("ACC-001");
        cuentaAna.setOwnerName("Ana García");
        cuentaAna.setBalance(new BigDecimal("1000.00"));
        cuentaAna.setVersion(0L);
    }

    // ════════════════════════════════════════════════════════════════════════
    // CAMINO FELIZ: Débito exitoso
    // ════════════════════════════════════════════════════════════════════════

    @Nested
    @DisplayName("Débito exitoso")
    class DebitoExitoso {

        @Test
        @DisplayName("Reduce el balance correctamente y retorna la cuenta actualizada")
        void debeReducirBalance_yRetornarCuentaActualizada() {
            // Arrange
            BigDecimal monto = new BigDecimal("300.00");
            BigDecimal balanceEsperado = new BigDecimal("700.00");

            // El repositorio retorna la cuenta, el save retorna la cuenta modificada
            when(accountRepository.findById("ACC-001")).thenReturn(Optional.of(cuentaAna));
            when(accountRepository.save(any(Account.class))).thenAnswer(inv -> inv.getArgument(0));

            // Act
            Account resultado = debitService.debit("ACC-001", monto);

            // Assert: el balance fue reducido
            assertThat(resultado.getBalance())
                    .as("El balance debe ser 1000 - 300 = 700")
                    .isEqualByComparingTo(balanceEsperado);

            // Assert: se llamó a save() exactamente una vez con el balance correcto
            verify(accountRepository, times(1)).save(argThat(cuenta ->
                    cuenta.getBalance().compareTo(balanceEsperado) == 0
            ));
        }

        @Test
        @DisplayName("Permite débito por el saldo exacto (balance = amount)")
        void debePermitirDebito_cuandoMontoIgualAlBalance() {
            // Arrange: el cliente saca TODO el dinero
            BigDecimal montoTotal = new BigDecimal("1000.00");
            when(accountRepository.findById("ACC-001")).thenReturn(Optional.of(cuentaAna));
            when(accountRepository.save(any(Account.class))).thenAnswer(inv -> inv.getArgument(0));

            // Act
            Account resultado = debitService.debit("ACC-001", montoTotal);

            // Assert: balance queda en cero
            assertThat(resultado.getBalance())
                    .as("Balance debe quedar en 0.00 cuando se saca todo")
                    .isEqualByComparingTo(BigDecimal.ZERO);
        }

        @Test
        @DisplayName("El account modificado pasado a save() tiene el balance reducido")
        void elObjetoGuardado_tieneElNuevoBalance() {
            // Este test verifica el mecanismo interno: modificamos el objeto Account
            // en memoria ANTES de llamar a save() — JPA necesita eso para el
            // UPDATE accounts SET balance=? WHERE id=? AND version=?

            BigDecimal monto = new BigDecimal("150.00");
            when(accountRepository.findById("ACC-001")).thenReturn(Optional.of(cuentaAna));
            when(accountRepository.save(any(Account.class))).thenAnswer(inv -> inv.getArgument(0));

            debitService.debit("ACC-001", monto);

            // Capturar el argumento pasado a save()
            verify(accountRepository).save(argThat(cuenta -> {
                BigDecimal esperado = new BigDecimal("850.00");
                return cuenta.getBalance().compareTo(esperado) == 0
                        && "ACC-001".equals(cuenta.getId());
            }));
        }
    }

    // ════════════════════════════════════════════════════════════════════════
    // SALDO INSUFICIENTE
    // ════════════════════════════════════════════════════════════════════════

    @Nested
    @DisplayName("Saldo insuficiente")
    class SaldoInsuficiente {

        @Test
        @DisplayName("Lanza InsufficientFundsException cuando el monto supera el balance")
        void debeLanzarExcepcion_cuandoMontoMayorAlBalance() {
            // Arrange: intentar debitar más de lo disponible
            BigDecimal montoExcesivo = new BigDecimal("1500.00"); // Ana tiene 1000
            when(accountRepository.findById("ACC-001")).thenReturn(Optional.of(cuentaAna));

            // Act & Assert: assertThatThrownBy ejecuta el bloque y verifica la excepción
            assertThatThrownBy(() -> debitService.debit("ACC-001", montoExcesivo))
                    .isInstanceOf(DebitService.InsufficientFundsException.class)
                    .hasMessageContaining("ACC-001")
                    .hasMessageContaining("1000")   // balance disponible en el mensaje
                    .hasMessageContaining("1500");  // monto solicitado en el mensaje

            // Assert: save() nunca fue llamado (no se modificó el balance)
            verify(accountRepository, never()).save(any());
        }

        @Test
        @DisplayName("Lanza InsufficientFundsException cuando el balance es 0")
        void debeLanzarExcepcion_cuandoBalanceCero() {
            // Arrange: cuenta sin dinero
            cuentaAna.setBalance(BigDecimal.ZERO);
            when(accountRepository.findById("ACC-001")).thenReturn(Optional.of(cuentaAna));

            // Act & Assert
            assertThatThrownBy(() -> debitService.debit("ACC-001", new BigDecimal("1.00")))
                    .isInstanceOf(DebitService.InsufficientFundsException.class);

            verify(accountRepository, never()).save(any());
        }
    }

    // ════════════════════════════════════════════════════════════════════════
    // CUENTA NO ENCONTRADA
    // ════════════════════════════════════════════════════════════════════════

    @Nested
    @DisplayName("Cuenta no encontrada")
    class CuentaNoEncontrada {

        @Test
        @DisplayName("Lanza AccountNotFoundException cuando la cuenta no existe")
        void debeLanzarExcepcion_cuandoCuentaNoExiste() {
            // Arrange: el repositorio no encuentra la cuenta
            when(accountRepository.findById("ACC-999")).thenReturn(Optional.empty());

            // Act & Assert
            assertThatThrownBy(() -> debitService.debit("ACC-999", new BigDecimal("100.00")))
                    .isInstanceOf(DebitService.AccountNotFoundException.class)
                    .hasMessageContaining("ACC-999");

            verify(accountRepository, never()).save(any());
        }

        @Test
        @DisplayName("No llama a save() cuando la cuenta no existe")
        void noLlamaASave_cuandoCuentaNoExiste() {
            when(accountRepository.findById(anyString())).thenReturn(Optional.empty());

            try {
                debitService.debit("ACC-XYZ", new BigDecimal("50.00"));
            } catch (DebitService.AccountNotFoundException ignored) {
                // La excepción es esperada
            }

            // Verificar que no hubo intento de guardar (crítico: evitar escritura
            // en DB cuando los datos son inválidos)
            verify(accountRepository, never()).save(any());
        }
    }
}
