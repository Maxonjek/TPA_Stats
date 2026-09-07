CREATE TABLE machines
(
    id                BIGINT UNSIGNED PRIMARY KEY,

    machine_code      VARCHAR(64)     NOT NULL UNIQUE,

    name              VARCHAR(255)    NOT NULL,

    machine_type_id   BIGINT UNSIGNED NOT NULL,

    serial_number     VARCHAR(128),

    manufacturer      VARCHAR(128),
    model             VARCHAR(128),

    location_id       BIGINT UNSIGNED,

    timezone          VARCHAR(64)     NOT NULL DEFAULT 'UTC',

    commissioned_at   DATETIME(3),
    decommissioned_at DATETIME(3),

    created_at        DATETIME(3)     NOT NULL,
    updated_at        DATETIME(3)     NOT NULL
);