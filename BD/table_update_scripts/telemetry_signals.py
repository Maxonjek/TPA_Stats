from dataclasses import dataclass
from typing import Optional
from datetime import datetime
import mysql.connector
from mysql.connector import MySQLConnection


@dataclass(slots=True)
class TelemetrySignalCommand:
    """
    Каноническое описание сигнала.

    Важно:
    keba_path здесь является дефолтным/эталонным путём.
    Реальный путь конкретной машины хранится в
    machine_signal_mapping.source_path.
    """

    id: int
    code: str
    keba_path: str
    name: str

    data_type: str

    unit: Optional[str]

    category: str
    collection_class: str

    sample_interval_ms: Optional[int] = None
    history_policy: Optional[str] = None

    enabled: bool = True


class TelemetrySignalRepository:
    def __init__(self, connection: MySQLConnection):
        self.connection = connection

    def upsert(self, command: TelemetrySignalCommand) -> None:
        sql = """
            INSERT INTO telemetry_signals (
                id,
                code,
                keba_path,
                name,
                data_type,
                unit,
                category,
                collection_class,
                sample_interval_ms,
                history_policy,
                enabled
            )
            VALUES (
                %(id)s,
                %(code)s,
                %(keba_path)s,
                %(name)s,
                %(data_type)s,
                %(unit)s,
                %(category)s,
                %(collection_class)s,
                %(sample_interval_ms)s,
                %(history_policy)s,
                %(enabled)s
            )
            ON DUPLICATE KEY UPDATE
                code = VALUES(code),
                keba_path = VALUES(keba_path),
                name = VALUES(name),
                data_type = VALUES(data_type),
                unit = VALUES(unit),
                category = VALUES(category),
                collection_class = VALUES(collection_class),
                sample_interval_ms = VALUES(sample_interval_ms),
                history_policy = VALUES(history_policy),
                enabled = VALUES(enabled)
        """

        params = {
            "id": command.id,
            "code": command.code,
            "keba_path": command.keba_path,
            "name": command.name,
            "data_type": command.data_type,
            "unit": command.unit,
            "category": command.category,
            "collection_class": command.collection_class,
            "sample_interval_ms": command.sample_interval_ms,
            "history_policy": command.history_policy,
            "enabled": command.enabled,
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

    def upsert_many(        self,        commands: list[TelemetrySignalCommand],) -> None:

        if not commands:
            return

        sql = """
            INSERT INTO telemetry_signals (
                id,
                code,
                keba_path,
                name,
                data_type,
                unit,
                category,
                collection_class,
                sample_interval_ms,
                history_policy,
                enabled
            )
            VALUES (
                %(id)s,
                %(code)s,
                %(keba_path)s,
                %(name)s,
                %(data_type)s,
                %(unit)s,
                %(category)s,
                %(collection_class)s,
                %(sample_interval_ms)s,
                %(history_policy)s,
                %(enabled)s
            )
            ON DUPLICATE KEY UPDATE
                code = VALUES(code),
                keba_path = VALUES(keba_path),
                name = VALUES(name),
                data_type = VALUES(data_type),
                unit = VALUES(unit),
                category = VALUES(category),
                collection_class = VALUES(collection_class),
                sample_interval_ms = VALUES(sample_interval_ms),
                history_policy = VALUES(history_policy),
                enabled = VALUES(enabled)
        """

        rows = [
            {
                "id": c.id,
                "code": c.code,
                "keba_path": c.keba_path,
                "name": c.name,
                "data_type": c.data_type,
                "unit": c.unit,
                "category": c.category,
                "collection_class": c.collection_class,
                "sample_interval_ms": c.sample_interval_ms,
                "history_policy": c.history_policy,
                "enabled": c.enabled,
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