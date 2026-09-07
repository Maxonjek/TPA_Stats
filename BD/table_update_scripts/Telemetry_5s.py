from dataclasses import dataclass
from typing import Optional
from datetime import datetime

@dataclass(slots=True)
class Telemetry5sCommand:
    ts: datetime
    machine_id: int

    cycle_id: Optional[int] = None

    # Injection
    injection_pressure: Optional[float] = None
    hydraulic_pressure: Optional[float] = None
    injection_flow_pct: Optional[float] = None
    screw_rpm: Optional[float] = None
    screw_position_mm: Optional[float] = None
    back_pressure_bar: Optional[float] = None
    cavity_pressure_bar: Optional[float] = None
    cushion: Optional[float] = None

    # Mold
    mold_position_mm: Optional[float] = None
    clamp_force_kn: Optional[float] = None
    toggle_position_mm: Optional[float] = None

    # Nozzle / ejector
    nozzle_position_mm: Optional[float] = None
    ejector_position_mm: Optional[float] = None
    ejector_pressure: Optional[float] = None
    ejector_velocity_pct: Optional[float] = None

    # Temperatures
    oil_temperature_c: Optional[float] = None
    pump_temperature_c: Optional[float] = None
    hopper_temperature_c: Optional[float] = None
    cpu_temperature_c: Optional[float] = None

    zone1_temperature_c: Optional[float] = None
    zone2_temperature_c: Optional[float] = None
    zone3_temperature_c: Optional[float] = None
    zone4_temperature_c: Optional[float] = None
    zone5_temperature_c: Optional[float] = None
    zone6_temperature_c: Optional[float] = None

    # Energy
    power_kw: Optional[float] = None
    current_a: Optional[float] = None
    current_b: Optional[float] = None
    current_c: Optional[float] = None

class TelemetryRepository:
    INSERT_SQL = """INSERT INTO telemetry_5s (ts,
                                              machine_id,
                                              cycle_id,
                                              injection_pressure,
                                              hydraulic_pressure,
                                              injection_flow_pct,
                                              screw_rpm,
                                              screw_position_mm,
                                              back_pressure_bar,
                                              cavity_pressure_bar,
                                              cushion,
                                              mold_position_mm,
                                              clamp_force_kn,
                                              toggle_position_mm,
                                              nozzle_position_mm,
                                              ejector_position_mm,
                                              ejector_pressure,
                                              ejector_velocity_pct,
                                              oil_temperature_c,
                                              pump_temperature_c,
                                              hopper_temperature_c,
                                              cpu_temperature_c,
                                              zone1_temperature_c,
                                              zone2_temperature_c,
                                              zone3_temperature_c,
                                              zone4_temperature_c,
                                              zone5_temperature_c,
                                              zone6_temperature_c,
                                              power_kw,
                                              current_a,
                                              current_b,
                                              current_c)
                    VALUES
                 """

    def __init__(self, client):
        self.client = client

    def insert(self, command: Telemetry5sCommand) -> None:
        self.client.insert(
            "telemetry_5s",
            [[
                command.ts,
                command.machine_id,
                command.cycle_id,

                command.injection_pressure,
                command.hydraulic_pressure,
                command.injection_flow_pct,
                command.screw_rpm,
                command.screw_position_mm,
                command.back_pressure_bar,
                command.cavity_pressure_bar,
                command.cushion,

                command.mold_position_mm,
                command.clamp_force_kn,
                command.toggle_position_mm,

                command.nozzle_position_mm,
                command.ejector_position_mm,
                command.ejector_pressure,
                command.ejector_velocity_pct,

                command.oil_temperature_c,
                command.pump_temperature_c,
                command.hopper_temperature_c,
                command.cpu_temperature_c,

                command.zone1_temperature_c,
                command.zone2_temperature_c,
                command.zone3_temperature_c,
                command.zone4_temperature_c,
                command.zone5_temperature_c,
                command.zone6_temperature_c,

                command.power_kw,
                command.current_a,
                command.current_b,
                command.current_c,
            ]],
            column_names=[
                "ts",
                "machine_id",
                "cycle_id",

                "injection_pressure",
                "hydraulic_pressure",
                "injection_flow_pct",
                "screw_rpm",
                "screw_position_mm",
                "back_pressure_bar",
                "cavity_pressure_bar",
                "cushion",

                "mold_position_mm",
                "clamp_force_kn",
                "toggle_position_mm",

                "nozzle_position_mm",
                "ejector_position_mm",
                "ejector_pressure",
                "ejector_velocity_pct",

                "oil_temperature_c",
                "pump_temperature_c",
                "hopper_temperature_c",
                "cpu_temperature_c",

                "zone1_temperature_c",
                "zone2_temperature_c",
                "zone3_temperature_c",
                "zone4_temperature_c",
                "zone5_temperature_c",
                "zone6_temperature_c",

                "power_kw",
                "current_a",
                "current_b",
                "current_c",
            ],
        )

    def insert_batch(        self,        commands: list[Telemetry5sCommand]) -> None:
        if not commands:
            return

        rows = [
            [
                c.ts,
                c.machine_id,
                c.cycle_id,

                c.injection_pressure,
                c.hydraulic_pressure,
                c.injection_flow_pct,
                c.screw_rpm,
                c.screw_position_mm,
                c.back_pressure_bar,
                c.cavity_pressure_bar,
                c.cushion,

                c.mold_position_mm,
                c.clamp_force_kn,
                c.toggle_position_mm,

                c.nozzle_position_mm,
                c.ejector_position_mm,
                c.ejector_pressure,
                c.ejector_velocity_pct,

                c.oil_temperature_c,
                c.pump_temperature_c,
                c.hopper_temperature_c,
                c.cpu_temperature_c,

                c.zone1_temperature_c,
                c.zone2_temperature_c,
                c.zone3_temperature_c,
                c.zone4_temperature_c,
                c.zone5_temperature_c,
                c.zone6_temperature_c,

                c.power_kw,
                c.current_a,
                c.current_b,
                c.current_c,
            ]
            for c in commands
        ]

        self.client.insert(
            "telemetry_5s",
            rows,
            column_names=[
                "ts",
                "machine_id",
                "cycle_id",

                "injection_pressure",
                "hydraulic_pressure",
                "injection_flow_pct",
                "screw_rpm",
                "screw_position_mm",
                "back_pressure_bar",
                "cavity_pressure_bar",
                "cushion",

                "mold_position_mm",
                "clamp_force_kn",
                "toggle_position_mm",

                "nozzle_position_mm",
                "ejector_position_mm",
                "ejector_pressure",
                "ejector_velocity_pct",

                "oil_temperature_c",
                "pump_temperature_c",
                "hopper_temperature_c",
                "cpu_temperature_c",

                "zone1_temperature_c",
                "zone2_temperature_c",
                "zone3_temperature_c",
                "zone4_temperature_c",
                "zone5_temperature_c",
                "zone6_temperature_c",

                "power_kw",
                "current_a",
                "current_b",
                "current_c",
            ],
        )