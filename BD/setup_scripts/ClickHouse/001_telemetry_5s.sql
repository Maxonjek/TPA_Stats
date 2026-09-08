CREATE TABLE IF NOT EXISTS telemetry_5s
(
    ts                   DateTime64(3, 'UTC'),
    machine_id           UInt64,
    cycle_id             Nullable(UInt64),
    -- Injection
    injection_pressure   Nullable(Float32),
    hydraulic_pressure   Nullable(Float32),
    injection_flow_pct   Nullable(Float32),
    screw_rpm            Nullable(Float32),
    screw_position_mm    Nullable(Float32),
    back_pressure_bar    Nullable(Float32),
    cavity_pressure_bar  Nullable(Float32),
    cushion              Nullable(Float32),
    -- Mold
    mold_position_mm     Nullable(Float32),
    clamp_force_kn       Nullable(Float32),
    toggle_position_mm   Nullable(Float32),
    -- Nozzle / ejector
    nozzle_position_mm   Nullable(Float32),
    ejector_position_mm  Nullable(Float32),
    ejector_pressure     Nullable(Float32),
    ejector_velocity_pct Nullable(Float32),
    -- Temperatures
    oil_temperature_c    Nullable(Float32),
    pump_temperature_c   Nullable(Float32),
    hopper_temperature_c Nullable(Float32),
    cpu_temperature_c    Nullable(Float32),

    zone1_temperature_c  Nullable(Float32),
    zone2_temperature_c  Nullable(Float32),
    zone3_temperature_c  Nullable(Float32),
    zone4_temperature_c  Nullable(Float32),
    zone5_temperature_c  Nullable(Float32),
    zone6_temperature_c  Nullable(Float32),

    -- Energy
    power_kw             Nullable(Float32),
    current_a            Nullable(Float32),
    current_b            Nullable(Float32),
    current_c            Nullable(Float32)
)
    ENGINE = MergeTree
        PARTITION BY toYYYYMM(ts)
        ORDER BY (machine_id, ts);
