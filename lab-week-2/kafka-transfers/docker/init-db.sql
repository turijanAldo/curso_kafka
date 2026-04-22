-- ================================================================
-- INIT-DB.SQL — Script de inicialización de la base de datos
--
-- Ejecutado automáticamente por MySQL al crear el contenedor
-- por primera vez (docker-entrypoint-initdb.d/).
--
-- IMPORTANTE: Flyway maneja las migraciones de cada microservicio
-- en tiempo de arranque. Este script solo crea las tablas base
-- y siembra datos de prueba.
-- ================================================================

USE transfers_db;

-- ================================================================
-- TABLA: accounts
-- Cuentas bancarias de los clientes.
-- Flyway la creará en account-service; la creamos aquí solo para
-- poder sembrar datos antes de que levante Spring Boot.
-- ================================================================
CREATE TABLE IF NOT EXISTS accounts (
    id            VARCHAR(36)    NOT NULL PRIMARY KEY,       -- UUID
    owner_name    VARCHAR(100)   NOT NULL,                   -- Nombre del titular
    balance       DECIMAL(15,2)  NOT NULL DEFAULT 0.00,      -- Saldo actual
    version       BIGINT         NOT NULL DEFAULT 0,         -- Optimistic locking
    created_at    DATETIME       NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at    DATETIME       NOT NULL DEFAULT CURRENT_TIMESTAMP
                                          ON UPDATE CURRENT_TIMESTAMP,

    CONSTRAINT chk_balance CHECK (balance >= 0)             -- Saldo nunca negativo
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ================================================================
-- TABLA: transactions
-- Registro de cada transferencia iniciada.
-- ================================================================
CREATE TABLE IF NOT EXISTS transactions (
    id              VARCHAR(36)   NOT NULL PRIMARY KEY,      -- UUID (transactionId)
    from_account    VARCHAR(36)   NOT NULL,
    to_account      VARCHAR(36)   NOT NULL,
    amount          DECIMAL(15,2) NOT NULL,
    status          VARCHAR(30)   NOT NULL DEFAULT 'PROCESSING',
    failure_reason  VARCHAR(255)  NULL,
    created_at      DATETIME      NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at      DATETIME      NOT NULL DEFAULT CURRENT_TIMESTAMP
                                          ON UPDATE CURRENT_TIMESTAMP,

    INDEX idx_tx_status     (status),
    INDEX idx_tx_from_acct  (from_account),

    -- Estados válidos según la Saga
    CONSTRAINT chk_status CHECK (
        status IN ('PROCESSING','VALIDATED','DEBITED','CREDITED',
                   'COMPLETED','FAILED','ROLLED_BACK')
    )
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ================================================================
-- TABLA: processed_events
-- Registro de idempotencia — Capa 2 de protección.
-- Guarda cada (event_key) ya procesado para evitar duplicados.
-- La UNIQUE KEY es la Capa 3 (fuerza idempotencia a nivel DB).
--
-- event_key = transactionId + ":" + eventType
-- Ejemplo:   "f47a-...:TRANSFER_DEBITED"
-- ================================================================
CREATE TABLE IF NOT EXISTS processed_events (
    id            BIGINT        NOT NULL AUTO_INCREMENT PRIMARY KEY,
    event_key     VARCHAR(200)  NOT NULL,                    -- FK lógica
    service_name  VARCHAR(50)   NOT NULL,                    -- Quién lo procesó
    processed_at  DATETIME      NOT NULL DEFAULT CURRENT_TIMESTAMP,

    -- CAPA 3: La DB rechaza duplicados aunque 2 threads pasen la Capa 2 al mismo tiempo
    CONSTRAINT uq_event_key UNIQUE (event_key, service_name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ================================================================
-- DATOS SEMILLA — Cuentas de prueba
-- ================================================================
INSERT INTO accounts (id, owner_name, balance, version) VALUES
    ('ACC-001', 'Ana García',     1000.00, 0),
    ('ACC-002', 'Bob Martínez',    500.00, 0),
    ('ACC-003', 'Carlos López',   2500.00, 0);

-- ================================================================
-- VERIFICACIÓN (comentada — útil para debug manual)
-- ================================================================
-- SELECT * FROM accounts;
-- SELECT * FROM transactions;
-- SELECT * FROM processed_events;
