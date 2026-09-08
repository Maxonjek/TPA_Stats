CREATE TABLE IF NOT EXISTS machine_state_intervals
(
    machine_id   UInt64,

    state        LowCardinality(String),

    started_at   DateTime64(3, 'UTC'),
    finished_at  DateTime64(3, 'UTC'),

    duration_ms  UInt64,

    reason_group Nullable(Int32),
    reason_code  Nullable(Int32)
)
    ENGINE = MergeTree
        PARTITION BY toYYYYMM(started_at)
        ORDER BY (machine_id, started_at);