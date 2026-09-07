from dataclasses import dataclass
from typing import Optional
from datetime import datetime
import mysql.connector
from mysql.connector import MySQLConnection


@dataclass(slots=True)
class MachineSignalMappingCommand:
    """
    Привязка канонического сигнала к конкретной машине.

    signal_id:
        Канонический сигнал из telemetry_signals.

    source_path:
        Фактический путь сигнала на конкретной KEBA.
    """

    machine_id: int
    signal_id: int

    source_path: str

    enabled: bool = True

    transform_code: Optional[str] = None

class MachineSignalMappingRepository:
    def __init__(self, connection: MySQLConnection):
        self.connection = connection

    def upsert(    self,    command: MachineSignalMappingCommand,) -> None:

        sql = """
            INSERT INTO machine_signal_mapping (
                machine_id,
                signal_id,
                source_path,
                enabled,
                transform_code
            )
            VALUES (
                %(machine_id)s,
                %(signal_id)s,
                %(source_path)s,
                %(enabled)s,
                %(transform_code)s
            )
            ON DUPLICATE KEY UPDATE
                source_path = VALUES(source_path),
                enabled = VALUES(enabled),
                transform_code = VALUES(transform_code)
        """

        params = {
            "machine_id": command.machine_id,
            "signal_id": command.signal_id,
            "source_path": command.source_path,
            "enabled": command.enabled,
            "transform_code": command.transform_code,
        }

        cursor = self.connection.cursor()

        try:
            cursor.execute(sql, params)
            self.connection.commit()
        except Exception:
            self.connection.rollback()
            raise
        finally:
            cursor.close()

    def upsert_many(        self,        commands: list[MachineSignalMappingCommand],) -> None:

        if not commands:
            return

        sql = """
              INSERT INTO machine_signal_mapping (machine_id, \
                                                  signal_id, \
                                                  source_path, \
                                                  enabled, \
                                                  transform_code)
              VALUES (%(machine_id)s, \
                      %(signal_id)s, \
                      %(source_path)s, \
                      %(enabled)s, \
                      %(transform_code)s)
              ON DUPLICATE KEY UPDATE
                  source_path = \
              VALUES (source_path), enabled = \
              VALUES (enabled),
                   transform_code = \
              VALUES (transform_code) \
              """

        rows = [
            {
                "machine_id": c.machine_id,
                "signal_id": c.signal_id,
                "source_path": c.source_path,
                "enabled": c.enabled,
                "transform_code": c.transform_code,
            }
            for c in commands
        ]

        cursor = self.connection.cursor()

        try:
            cursor.executemany(sql, rows)
            self.connection.commit()
        except Exception:
            self.connection.rollback()
            raise
        finally:
            cursor.close()