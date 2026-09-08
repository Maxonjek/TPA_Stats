CREATE TABLE IF NOT EXISTS machine_alarms
(
    machine_id  UInt64,

    alarm_code  LowCardinality(String),
    source      LowCardinality(String),

    started_at  DateTime64(3, 'UTC'),
    finished_at Nullable(DateTime64(3, 'UTC')),

    duration_ms Nullable(UInt64),

    severity    LowCardinality(String)
)
    ENGINE = MergeTree
        PARTITION BY toYYYYMM(started_at)
        ORDER BY (machine_id, started_at);