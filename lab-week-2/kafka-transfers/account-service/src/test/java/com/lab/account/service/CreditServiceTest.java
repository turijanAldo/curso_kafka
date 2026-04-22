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
import org.springframework.test.util.ReflectionTestUtils;

import java.math.BigDecimal;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

/**
 * Tests unitarios de CreditService.
 *
 * CAPA: Unitaria (Pirámide base)
 *
 * PROBLEMA ESPECIAL: CreditService tiene un campo @Value:
 *   @Value("${app.simulate-credit-failure:false}")
 *   private boolean simulateCreditFailure;
 *
 * Con @ExtendWith(MockitoExtension.class) no hay Spring Context, entonces
 * @Value NO se inyecta automáticamente. El campo queda en su valor por defecto.
 *
 * SOLUCIÓN: ReflectionTestUtils.setField()
 *   Spring Test proporciona ReflectionTestUtils para inyectar valores en campos
 *   privados sin modificar la visibilidad del campo en producción.
 *   Es la herramienta correcta para probar código que usa @Value en tests unitarios.
 *
 * Alternativa: usar @SpringBootTest con propiedades de test, pero eso carga
 * el contexto completo (más lento) cuando solo queremos probar la lógica pura.
 */
@ExtendWith(MockitoExtension.class)
@DisplayName("CreditService — Tests unitarios de lógica de crédito")
class CreditServiceTest {

    @Mock
    private AccountRepository accountRepository;

    @InjectMocks
    private CreditService creditService;

    // ── Cuenta de prueba (destino del crédito) ───────────────────────────────
    private Account cuentaBob;

    @BeforeEach
    void setUp() {
        cuentaBob = new Account();
        cuentaBob.setId("ACC-002");
        cuentaBob.setOwnerName("Bob Martínez");
        cuentaBob.setBalance(new BigDecimal("500.00"));
        cuentaBob.setVersion(0L);

        // Por defecto: simulateCreditFailure = false (comportamiento normal)
        // Cada test puede sobreescribirlo con ReflectionTestUtils.setField()
        ReflectionTestUtils.setField(creditService, "simulateCreditFailure", false);
    }

    // ════════════════════════════════════════════════════════════════════════
    // CAMINO FELIZ: Crédito exitoso
    // ════════════════════════════════════════════════════════════════════════

    @Nested
    @DisplayName("Crédito exitoso")
    class CreditoExitoso {

        @Test
        @DisplayName("Incrementa el balance correctamente")
        void debeIncrementarBalance_exitosamente() {
            // Arrange
            BigDecimal monto = new BigDecimal("200.00");
            BigDecimal balanceEsperado = new BigDecimal("700.00");

            when(accountRepository.findById("ACC-002")).thenReturn(Optional.of(cuentaBob));
            when(accountRepository.save(any(Account.class))).thenAnswer(inv -> inv.getArgument(0));

            // Act
            Account resultado = creditService.credit("ACC-002", monto);

            // Assert: Bob ahora tiene 500 + 200 = 700
            assertThat(resultado.getBalance())
                    .as("El balance debe ser 500 + 200 = 700")
                    .isEqualByComparingTo(balanceEsperado);

            // Assert: se llamó a findById y a save
            verify(accountRepository).findById("ACC-002");
            verify(accountRepository).save(any(Account.class));
        }

        @Test
        @DisplayName("Permite acreditar un monto de 0.01 (mínimo)")
        void debePermitirCredito_conMontoMinimo() {
            when(accountRepository.findById("ACC-002")).thenReturn(Optional.of(cuentaBob));
            when(accountRepository.save(any(Account.class))).thenAnswer(inv -> inv.getArgument(0));

            Account resultado = creditService.credit("ACC-002", new BigDecimal("0.01"));

            assertThat(resultado.getBalance())
                    .isEqualByComparingTo(new BigDecimal("500.01"));
        }

        @Test
        @DisplayName("El objeto guardado en DB tiene el nuevo balance")
        void elObjetoGuardado_tieneNuevoBalance() {
            BigDecimal monto = new BigDecimal("300.00");
            when(accountRepository.findById("ACC-002")).thenReturn(Optional.of(cuentaBob));
            when(accountRepository.save(any(Account.class))).thenAnswer(inv -> inv.getArgument(0));

            creditService.credit("ACC-002", monto);

            // Verificar el objeto pasado a save tiene el balance correcto
            verify(accountRepository).save(argThat(cuenta ->
                    cuenta.getBalance().compareTo(new BigDecimal("800.00")) == 0
            ));
        }
    }

    // ════════════════════════════════════════════════════════════════════════
    // FALLO SIMULADO: simulate-credit-failure = true
    // ════════════════════════════════════════════════════════════════════════

    @Nested
    @DisplayName("Fallo simulado (simulate-credit-failure=true)")
    class FalloSimulado {

        @Test
        @DisplayName("Lanza SimulatedCreditFailureException cuando el flag está activo")
        void debeLanzarExcepcion_cuandoSimulateCreditFailureActivo() {
            // Arrange: activar el flag de simulación de fallo
            // ReflectionTestUtils.setField(objeto, "nombreCampo", valor)
            // Inyecta el valor en el campo privado sin modificar el código fuente.
            ReflectionTestUtils.setField(creditService, "simulateCreditFailure", true);

            // Act & Assert: la excepción se lanza ANTES de consultar la DB
            assertThatThrownBy(() -> creditService.credit("ACC-002", new BigDecimal("100.00")))
                    .isInstanceOf(CreditService.SimulatedCreditFailureException.class)
                    .hasMessageContaining("simulate-credit-failure");

            // Assert CRÍTICO: NO se consultó la DB cuando el flag está activo.
            // El fallo es "antes de" tocar la cuenta — coherente con que
            // el Saga necesita compensar el débito ya aplicado.
            verifyNoInteractions(accountRepository);
        }

        @Test
        @DisplayName("Con flag activo, save() nunca es llamado")
        void conFlagActivo_noLlamaASave() {
            ReflectionTestUtils.setField(creditService, "simulateCreditFailure", true);

            try {
                creditService.credit("ACC-002", new BigDecimal("100.00"));
            } catch (CreditService.SimulatedCreditFailureException ignored) {
                // Excepción esperada
            }

            // Nunca debería escribir en la DB si el crédito falla
            verify(accountRepository, never()).save(any());
            verify(accountRepository, never()).findById(any());
        }

        @Test
        @DisplayName("Con flag desactivado, el crédito se aplica normalmente")
        void conFlagDesactivado_creditoNormal() {
            // El setUp ya pone simulateCreditFailure = false
            when(accountRepository.findById("ACC-002")).thenReturn(Optional.of(cuentaBob));
            when(accountRepository.save(any(Account.class))).thenAnswer(inv -> inv.getArgument(0));

            // No debe lanzar excepción
            Account resultado = creditService.credit("ACC-002", new BigDecimal("100.00"));

            assertThat(resultado.getBalance()).isEqualByComparingTo(new BigDecimal("600.00"));
        }
    }

    // ════════════════════════════════════════════════════════════════════════
    // CUENTA NO ENCONTRADA
    // ════════════════════════════════════════════════════════════════════════

    @Nested
    @DisplayName("Cuenta destino no encontrada")
    class CuentaNoEncontrada {

        @Test
        @DisplayName("Lanza AccountNotFoundException cuando la cuenta destino no existe")
        void debeLanzarExcepcion_cuandoCuentaNoExiste() {
            // Arrange: la cuenta destino no existe (fue cerrada entre la validación y el crédito)
            when(accountRepository.findById("ACC-CERRADA")).thenReturn(Optional.empty());

            // Act & Assert
            assertThatThrownBy(() -> creditService.credit("ACC-CERRADA", new BigDecimal("100.00")))
                    .isInstanceOf(CreditService.AccountNotFoundException.class)
                    .hasMessageContaining("ACC-CERRADA");

            verify(accountRepository, never()).save(any());
        }

        @Test
        @DisplayName("La excepción es CreditService.AccountNotFoundException (no la de DebitService)")
        void laExcepcionEsDeCreditService_noDeDebitService() {
            // Este test verifica que CreditService tiene su PROPIA AccountNotFoundException
            // y no usa la de DebitService. Esto fue un bug que se corrigió.
            when(accountRepository.findById(anyString())).thenReturn(Optional.empty());

            assertThatThrownBy(() -> creditService.credit("ACC-X", new BigDecimal("1.00")))
                    .isInstanceOf(CreditService.AccountNotFoundException.class)
                    .isNotInstanceOf(DebitService.AccountNotFoundException.class);
        }
    }
}
