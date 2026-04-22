package com.lab.common.enums;

/**
 * Estados posibles de una transferencia dentro del Saga.
 *
 * Máquina de estados:
 *
 *   PROCESSING ──► VALIDATED ──► DEBITED ──► CREDITED ──► COMPLETED
 *       │               │            │
 *       └───────────────┴────────────┴──► FAILED ──► ROLLED_BACK
 *
 * Transiciones permitidas:
 *   PROCESSING  → VALIDATED   (validation-service aprobó)
 *   PROCESSING  → FAILED      (validation-service rechazó)
 *   VALIDATED   → DEBITED     (account-service débito exitoso)
 *   VALIDATED   → FAILED      (account-service sin fondos)
 *   DEBITED     → CREDITED    (account-service crédito exitoso)
 *   DEBITED     → FAILED      (account-service crédito falló — inicia compensación)
 *   CREDITED    → COMPLETED   (status-service confirma fin feliz)
 *   FAILED      → ROLLED_BACK (account-service revirtió el débito)
 */
public enum TransferStatus {

    /** La transferencia fue iniciada, esperando validación. */
    PROCESSING,

    /** Validación de fondos y cuentas aprobada. */
    VALIDATED,

    /** El débito en la cuenta origen fue aplicado. */
    DEBITED,

    /** El crédito en la cuenta destino fue aplicado. */
    CREDITED,

    /** Transferencia finalizada exitosamente. Estado terminal. */
    COMPLETED,

    /** Ocurrió un error en cualquier paso. Puede derivar a ROLLED_BACK. */
    FAILED,

    /** El débito previo fue revertido como compensación. Estado terminal. */
    ROLLED_BACK
}
