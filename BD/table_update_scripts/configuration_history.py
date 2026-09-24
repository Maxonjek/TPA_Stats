from dataclasses import dataclass
from datetime import datetime
from typing import Optional


@dataclass(slots=True)
class MachineParameterHistoryCommand:
    machine_id: int
    parameter_code: str
    valid_from: datetime
    source: str
    value_string: Optional[str] = None
    value_numeric: Optional[float] = None
    value_bool: Optional[bool] = None
    valid_to: Optional[datetime] = None


class MachineParameterRepository:
    def __init__(self, connection):
        self.connection = connection

    def insert(self, c: MachineParameterHistoryCommand) -> int:
        cursor = self.connection.cursor()
        try:
            cursor.execute(
                """
                INSERT INTO machine_parameter_history (
                    machine_id, parameter_code,
                    value_string, value_numeric, value_bool,
                    valid_from, valid_to, source
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    c.machine_id, c.parameter_code,
                    c.value_string, c.value_numeric, c.value_bool,
                    c.valid_from, c.valid_to, c.source,
                ),
            )
            self.connection.commit()
            return cursor.lastrowid
        except Exception:
            self.connection.rollback()
            raise
        finally:
            cursor.close()

    def set_value_if_changed(self, c: MachineParameterHistoryCommand) -> bool:
        """Create a new history version only when the effective value changed."""
        cursor = self.connection.cursor(dictionary=True)
        try:
            # FOR UPDATE serializes concurrent writers for the current version.
            cursor.execute(
                """
                SELECT id, value_string, value_numeric, value_bool
                FROM machine_parameter_history
                WHERE machine_id = %s
                  AND parameter_code = %s
                  AND valid_to IS NULL
                ORDER BY valid_from DESC, id DESC
                LIMIT 1
                FOR UPDATE
                """,
                (c.machine_id, c.parameter_code),
            )
            current = cursor.fetchone()

            if current is not None and self._same_value(current, c):
                self.connection.commit()
                return False

            if current is not None:
                cursor.execute(
                    """
                    UPDATE machine_parameter_history
                    SET valid_to = %s
                    WHERE id = %s
                    """,
                    (c.valid_from, current["id"]),
                )

            cursor.execute(
                """
                INSERT INTO machine_parameter_history (
                    machine_id, parameter_code,
                    value_string, value_numeric, value_bool,
                    valid_from, valid_to, source
                ) VALUES (%s, %s, %s, %s, %s, %s, NULL, %s)
                """,
                (
                    c.machine_id,
                    c.parameter_code,
                    c.value_string,
                    c.value_numeric,
                    c.value_bool,
                    c.valid_from,
                    c.source,
                ),
            )
            self.connection.commit()
            return True
        except Exception:
            self.connection.rollback()
            raise
        finally:
            cursor.close()

    def set_value(self, c: MachineParameterHistoryCommand) -> None:
        self.set_value_if_changed(c)

    @staticmethod
    def _same_value(current: dict, c: MachineParameterHistoryCommand) -> bool:
        if current["value_string"] != c.value_string:
            return False

        current_bool = current["value_bool"]
        if current_bool is not None:
            current_bool = bool(current_bool)
        if current_bool != c.value_bool:
            return False

        current_numeric = current["value_numeric"]
        if current_numeric is None or c.value_numeric is None:
            return current_numeric is None and c.value_numeric is None

        return float(current_numeric) == float(c.value_numeric)
