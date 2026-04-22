-- ============================================================
-- Migración V1 — account-service
-- Crea las tablas accounts y processed_events
--
-- account-service es el propietario lógico de ambas tablas:
--   accounts         → es quien debita y acredita saldos
--   processed_events → es quien más necesita idempotencia
--                      (tanto validation-service como account-service
--                      escriben en ella, pero account-service la crea)
-- ============================================================

-- ── Tabla: accounts ─────────────────────────────────────────────────────
-- Almacena el saldo de cada cuenta bancaria del sistema.
-- Esta tabla es el recurso más sensible: modificaciones concurrentes
-- sin control pueden causar pérdida de dinero.

CREATE TABLE IF NOT EXISTS accounts (

    -- Identificador único de la cuenta (ej: "ACC-001").
    -- Usamos VARCHAR en lugar de BIGINT autoincremental porque
    -- los IDs de cuentas bancarias reales son alfanuméricos y
    -- provienen de sistemas externos (core bancario).
    id              VARCHAR(36)    NOT NULL,

    -- Nombre completo del titular de la cuenta.
    owner_name      VARCHAR(100)   NOT NULL,

    -- Saldo actual en la cuenta. DECIMAL(15,2):
    --   15 dígitos en total, 2 decimales.
    --   Permite montos hasta 9,999,999,999,999.99
    --   NUNCA usar FLOAT o DOUBLE para dinero.
    balance         DECIMAL(15,2)  NOT NULL DEFAULT 0.00,

    -- Campo de Optimistic Locking para JPA @Version.
    -- Cómo funciona:
    --   1. El servicio lee: SELECT * FROM accounts WHERE id='ACC-001'
    --      → obtiene balance=1000, version=5
    --   2. Aplica el débito: nuevo balance = 800
    --   3. Actualiza: UPDATE accounts SET balance=800, version=6
    --                 WHERE id='ACC-001' AND version=5
    --   Si entre los pasos 1 y 3 otro thread ya actualizó la fila
    --   (version ya es 6), el WHERE no encuentra la fila → 0 rows updated
    --   → JPA lanza OptimisticLockException → la transacción se reintenta.
    --   Esto previene que dos débitos simultáneos corrompan el saldo.
    version         BIGINT         NOT NULL DEFAULT 0,

    -- Timestamps de auditoría
    created_at      DATETIME       NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at      DATETIME       NOT NULL DEFAULT CURRENT_TIMESTAMP
                                            ON UPDATE CURRENT_TIMESTAMP,

    CONSTRAINT pk_accounts PRIMARY KEY (id),

    -- PROTECCIÓN CRÍTICA A NIVEL DB:
    -- Aunque la lógica Java valide que el saldo sea suficiente antes del débito,
    -- esta restricción es la última línea de defensa. Si un bug logra llegar
    -- aquí con balance negativo, MySQL rechaza el UPDATE con un error claro.
    CONSTRAINT chk_balance_non_negative CHECK (balance >= 0)

) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4
  COLLATE = utf8mb4_unicode_ci;


-- ── Tabla: processed_events ──────────────────────────────────────────────
-- TABLA DE IDEMPOTENCIA — el mecanismo más importante del sistema.
--
-- Propósito: registrar cada evento Kafka que ya fue procesado exitosamente.
-- Antes de ejecutar cualquier operación (débito, crédito, validación),
-- el servicio intenta insertar un registro aquí. Si la inserción falla
-- por clave duplicada → el evento ya fue procesado → ignorar.
--
-- ¿Por qué la DB y no un mapa en memoria?
-- Si el servicio se reinicia, un mapa en memoria se pierde. La DB persiste.
-- Además, la DB resuelve race conditions: si dos instancias del mismo
-- servicio reciben el mismo mensaje simultáneamente, solo una podrá
-- insertar el registro (la otra recibirá DuplicateKeyException).

CREATE TABLE IF NOT EXISTS processed_events (

    id            BIGINT        NOT NULL AUTO_INCREMENT,

    -- Clave única del evento ya procesado.
    -- Formato: transactionId + ":" + tipoEvento
    -- Ejemplo: "f47ac10b-58cc-4372-a567-0e02b2c3d479:TRANSFER_DEBITED"
    --
    -- ¿Por qué incluir el tipo de evento y no solo el transactionId?
    -- Porque la misma transacción genera MÚLTIPLES eventos distintos:
    --   "tx-abc:TRANSFER_VALIDATED"  → procesado por validation-service
    --   "tx-abc:TRANSFER_DEBITED"    → procesado por account-service (débito)
    --   "tx-abc:TRANSFER_CREDITED"   → procesado por account-service (crédito)
    -- Si solo usaras el transactionId, el segundo evento sería bloqueado
    -- por la clave del primero aunque sean operaciones completamente distintas.
    event_key     VARCHAR(200)  NOT NULL,

    -- Qué microservicio procesó este evento.
    -- La combinación (event_key + service_name) es lo que es único.
    -- Ejemplo: el mismo event_key "tx-abc:TRANSFER_DEBITED" puede existir
    -- en validation-service Y en account-service sin conflicto porque
    -- service_name es distinto.
    service_name  VARCHAR(50)   NOT NULL,

    processed_at  DATETIME      NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT pk_processed_events PRIMARY KEY (id),

    -- CAPA 3 DE IDEMPOTENCIA — la garantía final.
    -- Esta restricción UNIQUE es lo que hace el sistema verdaderamente
    -- seguro ante condiciones de carrera.
    --
    -- Escenario sin esta restricción:
    --   Thread A: SELECT → no existe → va a insertar
    --   Thread B: SELECT → no existe → va a insertar
    --   Thread A: INSERT → éxito, procesa el evento
    --   Thread B: INSERT → éxito (no hay restricción!) → procesa el evento OTRA VEZ
    --   RESULTADO: El débito se aplica dos veces. Ana pierde dinero.
    --
    -- Escenario con esta restricción:
    --   Thread A: INSERT → éxito, procesa el evento
    --   Thread B: INSERT → DuplicateKeyException → captura la excepción, ignora el evento
    --   RESULTADO: El débito se aplica exactamente una vez.
    CONSTRAINT uq_processed_event UNIQUE (event_key, service_name)

) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4
  COLLATE = utf8mb4_unicode_ci;
