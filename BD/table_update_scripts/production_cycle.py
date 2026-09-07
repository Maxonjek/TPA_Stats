
from dataclasses import dataclass
from typing import Optional
from datetime import datetime

@dataclass(slots=True)
class ProductionCycleCommand:
    machine_id: int
    cycle_id: int

    started_at: datetime
    finished_at: datetime

    cycle_time_ms: int
    production_counter: int

    good_parts: int
    reject_parts: int

    cycle_quality: int
    rejects_in_series: int

    calculated_cycle_time_ms: Optional[int] = None
    machine_cycle_time_ms: Optional[int] = None

    cooling_time_ms: Optional[int] = None
    injection_time_ms: Optional[int] = None
    holding_time_ms: Optional[int] = None
    plasticizing_time_ms: Optional[int] = None
    decompression_time_ms: Optional[int] = None

    mold_open_time_ms: Optional[int] = None
    mold_close_time_ms: Optional[int] = None

    ejector_forward_time_ms: Optional[int] = None
    ejector_backward_time_ms: Optional[int] = None

    mold_id: Optional[str] = None
    article_id: Optional[str] = None
    material: Optional[str] = None
    production_dataset: Optional[str] = None

    production_order_id: Optional[int] = None


class ProductionCycleRepository:

    COLUMNS = [
        "machine_id",
        "cycle_id",
        "started_at",
        "finished_at",

        "cycle_time_ms",
        "calculated_cycle_time_ms",
        "machine_cycle_time_ms",

        "production_counter",
        "good_parts",
        "reject_parts",

        "cycle_quality",
        "rejects_in_series",

        "cooling_time_ms",
        "injection_time_ms",
        "holding_time_ms",
        "plasticizing_time_ms",
        "decompression_time_ms",

        "mold_open_time_ms",
        "mold_close_time_ms",

        "ejector_forward_time_ms",
        "ejector_backward_time_ms",

        "mold_id",
        "article_id",
        "material",
        "production_dataset",

        "production_order_id",
    ]

    def __init__(self, client):
        self.client = client

    def insert(self, c: ProductionCycleCommand) -> None:
        self.insert_batch([c])

    def insert_batch(    self,    commands: list[ProductionCycleCommand]) -> None:

        if not commands:
            return

        rows = [
            [
                c.machine_id,
                c.cycle_id,
                c.started_at,
                c.finished_at,

                c.cycle_time_ms,
                c.calculated_cycle_time_ms,
                c.machine_cycle_time_ms,

                c.production_counter,
                c.good_parts,
                c.reject_parts,

                c.cycle_quality,
                c.rejects_in_series,

                c.cooling_time_ms,
                c.injection_time_ms,
                c.holding_time_ms,
                c.plasticizing_time_ms,
                c.decompression_time_ms,

                c.mold_open_time_ms,
                c.mold_close_time_ms,

                c.ejector_forward_time_ms,
                c.ejector_backward_time_ms,

                c.mold_id,
                c.article_id,
                c.material,
                c.production_dataset,

                c.production_order_id,
            ]
            for c in commands
        ]

        self.client.insert(
            "production_cycles",
            rows,
            column_names=self.COLUMNS,
        )