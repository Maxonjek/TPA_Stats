from dataclasses import dataclass
from typing import Optional
from datetime import datetime


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

    def insert(
            self,
            c: MachineParameterHistoryCommand,
    ) -> int:
        cursor = self.connection.cursor()

        sql = """
              INSERT INTO machine_parameter_history (machine_id,
                                                     parameter_code,
                                                     value_string,
                                                     value_numeric,
                                                     value_bool,
                                                     valid_from,
                                                     valid_to,
                                                     source)
              VALUES (%s, %s,
                      %s, %s, %s,
                      %s, %s,
                      %s) \
              """

        cursor.execute(
            sql,
            (
                c.machine_id,
                c.parameter_code,

                c.value_string,
                c.value_numeric,
                c.value_bool,

                c.valid_from,
                c.valid_to,

                c.source,
            ),
        )

        self.connection.commit()

        return cursor.lastrowid

    def set_value(
            self,
            c: MachineParameterHistoryCommand,
    ) -> None:

        cursor = self.connection.cursor()

        try:
            cursor.execute(
                """
                UPDATE machine_parameter_history
                SET valid_to = %s
                WHERE machine_id = %s
                  AND parameter_code = %s
                  AND valid_to IS NULL
                """,
                (
                    c.valid_from,
                    c.machine_id,
                    c.parameter_code,
                ),
            )

            cursor.execute(
                """
                INSERT INTO machine_parameter_history (machine_id,
                                                       parameter_code,
                                                       value_string,
                                                       value_numeric,
                                                       value_bool,
                                                       valid_from,
                                                       valid_to,
                                                       source)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    c.machine_id,
                    c.parameter_code,
                    c.value_string,
                    c.value_numeric,
                    c.value_bool,
                    c.valid_from,
                    c.valid_to,
                    c.source,
                ),
            )

            self.connection.commit()

        except Exception:
            self.connection.rollback()
            raise
