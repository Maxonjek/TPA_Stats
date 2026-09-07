from dataclasses import dataclass
from typing import Optional
from datetime import datetime


@dataclass(slots=True)
class MachineAlarmCommand:
    machine_id: int

    alarm_code: str
    source: str

    started_at: datetime
    finished_at: Optional[datetime] = None

    duration_ms: Optional[int] = None

    severity: str = "UNKNOWN"


class MachineAlarmRepository:
    COLUMNS = [
        "machine_id",
        "alarm_code",
        "source",
        "started_at",
        "finished_at",
        "duration_ms",
        "severity",
    ]

    def __init__(self, client):
        self.client = client

    def insert(self, c: MachineAlarmCommand) -> None:
        self.client.insert(
            "machine_alarms",
            [[
                c.machine_id,
                c.alarm_code,
                c.source,
                c.started_at,
                c.finished_at,
                c.duration_ms,
                c.severity,
            ]],
            column_names=self.COLUMNS,
        )
