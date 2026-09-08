CREATE TABLE IF NOT EXISTS cycle_features
(
    machine_id             UInt64,
    cycle_id               UInt64,
    started_at             DateTime64(3, 'UTC'),

    avg_injection_pressure Float32,
    max_injection_pressure Float32,

    avg_hydraulic_pressure Float32,
    max_hydraulic_pressure Float32,

    avg_screw_rpm          Float32,
    max_screw_rpm          Float32,

    avg_power_kw           Float32,
    energy_kwh             Float32
)
    ENGINE = MergeTree
        PARTITION BY toYYYYMM(started_at)
        ORDER BY (machine_id, started_at, cycle_id);