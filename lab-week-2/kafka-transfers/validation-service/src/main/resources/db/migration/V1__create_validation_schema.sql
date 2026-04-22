-- ============================================================
-- Migración V1 — validation-service
--
-- validation-service necesita:
--   LEER  → accounts (verificar que existen y tienen saldo)
--   ESCRIBIR → processed_events (idempotencia)
--
-- Las tablas accounts y processed_events fueron CREADAS por
-- account-service (V1__create_account_schema.sql). Esta migración
-- las crea con IF NOT EXISTS como medida de seguridad.
--
-- ¿Por qué crear tablas que ya crea otro servicio?
-- En este lab todos los servicios comparten la misma base de datos.
-- En producción real cada servicio tendría su propia DB. Como medida
-- defensiva, cada servicio crea con IF NOT EXISTS las tablas que necesita,
-- sin depender del orden de arranque de otros servicios.
-- ============================================================

-- Asegurar que accounts existe (propietario: account-service)
CREATE TABLE IF NOT EXISTS accounts (
    id          VARCHAR(36)   NOT NULL,
    owner_name  VARCHAR(100)  NOT NULL,
    balance     DECIMAL(15,2) NOT NULL DEFAULT 0.00,
    version     BIGINT        NOT NULL DEFAULT 0,
    created_at  DATETIME      NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at  DATETIME      NOT NULL DEFAULT CURRENT_TIMESTAMP
                                       ON UPDATE CURRENT_TIMESTAMP,

    CONSTRAINT pk_accounts_val PRIMARY KEY (id),
    CONSTRAINT chk_balance_val CHECK (balance >= 0)

) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_unicode_ci;


-- Asegurar que processed_events existe (propietario: account-service)
CREATE TABLE IF NOT EXISTS processed_events (
    id            BIGINT        NOT NULL AUTO_INCREMENT,
    event_key     VARCHAR(200)  NOT NULL,
    service_name  VARCHAR(50)   NOT NULL,
    processed_at  DATETIME      NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT pk_processed_events_val PRIMARY KEY (id),
    CONSTRAINT uq_processed_event_val  UNIQUE (event_key, service_name)

) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_unicode_ci;
