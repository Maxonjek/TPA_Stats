CREATE TABLE machine_parameter_history
(
    id             BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    machine_id     BIGINT UNSIGNED NOT NULL,

    parameter_code VARCHAR(128)    NOT NULL,

    value_string   VARCHAR(1024),
    value_numeric  DECIMAL(20, 8),
    value_bool     BOOLEAN,

    valid_from     DATETIME(3)     NOT NULL,
    valid_to       DATETIME(3),

    source         VARCHAR(32)     NOT NULL,

    PRIMARY KEY (id),
    INDEX ix_machine_parameter (
                                machine_id,
                                parameter_code,
                                valid_from
        )
);