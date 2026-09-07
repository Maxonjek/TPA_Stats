
from dataclasses import dataclass
from typing import Optional
from datetime import datetime


@dataclass(slots=True)
class MachineStateIntervalCommand:
    machine_id: int

    state: str

    started_at: datetime
    finished_at: datetime

    duration_ms: int

    reason_group: Optional[int] = None
    reason_code: Optional[int] = None

class MachineStateRepository:

    COLUMNS = [
        "machine_id",
        "state",
        "started_at",
        "finished_at",
        "duration_ms",
        "reason_group",
        "reason_code",
    ]

    def __init__(self, client):
        self.client = client

    def insert(self, c: MachineStateIntervalCommand) -> None:
        self.client.insert(
            "machine_state_intervals",
            [[
                c.machine_id,
                c.state,
                c.started_at,
                c.finished_at,
                c.duration_ms,
                c.reason_group,
                c.reason_code,
            ]],
            column_names=self.COLUMNS,
        )