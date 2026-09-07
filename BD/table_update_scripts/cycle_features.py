from dataclasses import dataclass
from typing import Optional
from datetime import datetime


@dataclass(slots=True)
class CycleFeaturesCommand:
    machine_id: int
    cycle_id: int
    started_at: datetime

    avg_injection_pressure: float
    max_injection_pressure: float

    avg_hydraulic_pressure: float
    max_hydraulic_pressure: float

    avg_screw_rpm: float
    max_screw_rpm: float

    avg_power_kw: float
    energy_kwh: float


class CycleFeaturesRepository:
    COLUMNS = [
        "machine_id",
        "cycle_id",
        "started_at",

        "avg_injection_pressure",
        "max_injection_pressure",

        "avg_hydraulic_pressure",
        "max_hydraulic_pressure",

        "avg_screw_rpm",
        "max_screw_rpm",

        "avg_power_kw",
        "energy_kwh",
    ]

    def __init__(self, client):
        self.client = client

    def insert(self, c: CycleFeaturesCommand) -> None:
        self.client.insert(
            "cycle_features",
            [[
                c.machine_id,
                c.cycle_id,
                c.started_at,

                c.avg_injection_pressure,
                c.max_injection_pressure,

                c.avg_hydraulic_pressure,
                c.max_hydraulic_pressure,

                c.avg_screw_rpm,
                c.max_screw_rpm,

                c.avg_power_kw,
                c.energy_kwh,
            ]],
            column_names=self.COLUMNS,
        )
