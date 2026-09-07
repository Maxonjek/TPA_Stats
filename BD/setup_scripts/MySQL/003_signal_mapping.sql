CREATE TABLE machine_signal_mapping
(
    machine_id     BIGINT UNSIGNED NOT NULL,
    signal_id      BIGINT UNSIGNED NOT NULL,

    source_path    VARCHAR(512)    NOT NULL,

    enabled        BOOLEAN         NOT NULL DEFAULT TRUE,

    transform_code VARCHAR(64),

    PRIMARY KEY (machine_id, signal_id)
);