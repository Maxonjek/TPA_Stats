CREATE TABLE telemetry_signals
(
    id                 BIGINT UNSIGNED PRIMARY KEY,

    code               VARCHAR(128) NOT NULL UNIQUE,

    keba_path          VARCHAR(512) NOT NULL,

    name               VARCHAR(255) NOT NULL,

    data_type          VARCHAR(32)  NOT NULL,

    unit               VARCHAR(32),

    category           VARCHAR(32)  NOT NULL,

    collection_class   VARCHAR(32)  NOT NULL,

    sample_interval_ms INT,

    history_policy     VARCHAR(32),

    enabled            BOOLEAN      NOT NULL DEFAULT TRUE
);