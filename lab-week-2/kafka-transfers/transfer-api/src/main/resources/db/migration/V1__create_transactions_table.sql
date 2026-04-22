-- ============================================================
-- Migración V1 — transfer-api
-- Crea la tabla transactions
--
-- Propietario: transfer-api (único servicio que INSERTA filas)
-- Lectores: transfer-api (GET /transfers/{id}), status-service
--
-- Flyway ejecuta este script UNA sola vez al arrancar el servicio.
-- Si la tabla ya existe (creada por init-db.sql), IF NOT EXISTS
-- la ignora silenciosamente. Esto hace la migración idempotente.
-- ============================================================

CREATE TABLE IF NOT EXISTS transactions (

    -- UUID generado por transfer-api al recibir el POST /transfers.
    -- Es el identificador de toda la saga de esa transferencia.
    id              VARCHAR(36)    NOT NULL,

    -- Cuenta que envía el dinero (ej: "ACC-001")
    from_account    VARCHAR(36)    NOT NULL,

    -- Cuenta que recibe el dinero (ej: "ACC-002")
    to_account      VARCHAR(36)    NOT NULL,

    -- Monto de la transferencia. DECIMAL para precisión monetaria exacta.
    amount          DECIMAL(15,2)  NOT NULL,

    -- Estado actual de la saga para esta transferencia.
    -- Valores posibles (enum TransferStatus):
    --   PROCESSING → VALIDATED → DEBITED → CREDITED → COMPLETED
    --   PROCESSING → FAILED
    --   DEBITED    → FAILED    → ROLLED_BACK
    status          VARCHAR(30)    NOT NULL DEFAULT 'PROCESSING',

    -- Motivo del fallo, si aplica. NULL en transferencias exitosas.
    failure_reason  VARCHAR(255)   NULL,

    -- Timestamps de auditoría
    created_at      DATETIME       NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at      DATETIME       NOT NULL DEFAULT CURRENT_TIMESTAMP
                                            ON UPDATE CURRENT_TIMESTAMP,

    -- Clave primaria
    CONSTRAINT pk_transactions PRIMARY KEY (id),

    -- Índice por status: status-service y el endpoint GET /transfers/{id}
    -- hacen queries frecuentes filtrando por status.
    -- Sin índice → full table scan en cada consulta.
    INDEX idx_transactions_status      (status),

    -- Índice por from_account: para listar el historial de transferencias
    -- de una cuenta origen (útil para dashboards de auditoría).
    INDEX idx_transactions_from_account (from_account),

    -- Restricción de dominio: solo estados válidos del enum.
    -- Si un bug en Java intenta guardar "DONE" o "done", la DB lo rechaza.
    CONSTRAINT chk_transaction_status CHECK (
        status IN (
            'PROCESSING', 'VALIDATED', 'DEBITED',
            'CREDITED', 'COMPLETED', 'FAILED', 'ROLLED_BACK'
        )
    )

) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4
  COLLATE = utf8mb4_unicode_ci;
