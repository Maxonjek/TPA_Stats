CREATE TABLE production_cycles
(
    machine_id               UInt64,
    cycle_id                 UInt64,
    started_at               DateTime64(3, 'UTC'),
    finished_at              DateTime64(3, 'UTC'),

    cycle_time_ms            UInt32,
    calculated_cycle_time_ms Nullable(UInt32),
    machine_cycle_time_ms    Nullable(UInt32),

    production_counter       UInt64,
    good_parts               UInt32,
    reject_parts             UInt32,

    cycle_quality            Int16,
    rejects_in_series        UInt32,

    cooling_time_ms          Nullable(UInt32),
    injection_time_ms        Nullable(UInt32),
    holding_time_ms          Nullable(UInt32),
    plasticizing_time_ms     Nullable(UInt32),
    decompression_time_ms    Nullable(UInt32),

    mold_open_time_ms        Nullable(UInt32),
    mold_close_time_ms       Nullable(UInt32),

    ejector_forward_time_ms  Nullable(UInt32),
    ejector_backward_time_ms Nullable(UInt32),

    mold_id                  Nullable(String),
    article_id               Nullable(String),
    material                 Nullable(String),
    production_dataset       Nullable(String),

    production_order_id      Nullable(UInt64)
)
    ENGINE = MergeTree
        PARTITION BY toYYYYMM(started_at)
        ORDER BY (machine_id, started_at);