from dataclasses import dataclass
from typing import Optional
from datetime import datetime

import mysql.connector
from mysql.connector import MySQLConnection

@dataclass(slots=True)
class MachineUpsertCommand:
    id: int
    machine_code: str
    name: str
    machine_type_id: int

    serial_number: Optional[str] = None

    manufacturer: Optional[str] = None
    model: Optional[str] = None

    location_id: Optional[int] = None

    timezone: str = "UTC"

    commissioned_at: Optional[datetime] = None
    decommissioned_at: Optional[datetime] = None

    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

class MachineRepository:
    def __init__(self, connection: MySQLConnection):
        self.connection = connection

    def upsert(self, command: MachineUpsertCommand) -> None:
        """
        Создаёт машину или обновляет существующую.

        Конфликт определяется:
        - PRIMARY KEY(id)
        - UNIQUE(machine_code)
        """

        now = datetime.utcnow()

        created_at = command.created_at or now
        updated_at = command.updated_at or now

        sql = """
            INSERT INTO machines (
                id,
                machine_code,
                name,
                machine_type_id,
                serial_number,
                manufacturer,
                model,
                location_id,
                timezone,
                commissioned_at,
                decommissioned_at,
                created_at,
                updated_at
            )
            VALUES (
                %(id)s,
                %(machine_code)s,
                %(name)s,
                %(machine_type_id)s,
                %(serial_number)s,
                %(manufacturer)s,
                %(model)s,
                %(location_id)s,
                %(timezone)s,
                %(commissioned_at)s,
                %(decommissioned_at)s,
                %(created_at)s,
                %(updated_at)s
            )
            ON DUPLICATE KEY UPDATE
                machine_code = VALUES(machine_code),
                name = VALUES(name),
                machine_type_id = VALUES(machine_type_id),
                serial_number = VALUES(serial_number),
                manufacturer = VALUES(manufacturer),
                model = VALUES(model),
                location_id = VALUES(location_id),
                timezone = VALUES(timezone),
                commissioned_at = VALUES(commissioned_at),
                decommissioned_at = VALUES(decommissioned_at),
                updated_at = VALUES(updated_at)
        """

        params = {
            "id": command.id,
            "machine_code": command.machine_code,
            "name": command.name,
            "machine_type_id": command.machine_type_id,
            "serial_number": command.serial_number,
            "manufacturer": command.manufacturer,
            "model": command.model,
            "location_id": command.location_id,
            "timezone": command.timezone,
            "commissioned_at": command.commissioned_at,
            "decommissioned_at": command.decommissioned_at,
            "created_at": created_at,
            "updated_at": updated_at,
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