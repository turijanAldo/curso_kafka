-- ============================================================
-- Migración V1 — status-service
--
-- status-service necesita:
--   LEER y ESCRIBIR → transactions (actualiza el status en cada evento)
--
-- La tabla transactions fue CREADA por transfer-api
-- (V1__create_transactions_table.sql). Esta migración la crea
-- con IF NOT EXISTS como medida defensiva.
--
-- status-service NO tiene tablas propias. Solo actualiza la tabla
-- transactions que pertenece a transfer-api. En un diseño con
-- bases de datos separadas por servicio, status-service tendría
-- su propia tabla de estado y transfer-api consultaría al status-service
-- via HTTP o via eventos. Para simplificar el lab, comparten la DB.
-- ============================================================

-- Asegurar que transactions existe (propietario: transfer-api)
CREATE TABLE IF NOT EXISTS transactions (
    id              VARCHAR(36)    NOT NULL,
    from_account    VARCHAR(36)    NOT NULL,
    to_account      VARCHAR(36)    NOT NULL,
    amount          DECIMAL(15,2)  NOT NULL,
    status          VARCHAR(30)    NOT NULL DEFAULT 'PROCESSING',
    failure_reason  VARCHAR(255)   NULL,
    created_at      DATETIME       NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at      DATETIME       NOT NULL DEFAULT CURRENT_TIMESTAMP
                                            ON UPDATE CURRENT_TIMESTAMP,

    CONSTRAINT pk_transactions_status PRIMARY KEY (id),
    INDEX idx_transactions_status_svc (status),

    CONSTRAINT chk_transaction_status_svc CHECK (
        status IN (
            'PROCESSING', 'VALIDATED', 'DEBITED',
            'CREDITED', 'COMPLETED', 'FAILED', 'ROLLED_BACK'
        )
    )

) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_unicode_ci;
