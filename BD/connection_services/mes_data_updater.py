from __future__ import annotations

import copy
import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Mapping, Optional

from BD.table_update_scripts.Telemetry_5s import Telemetry5sCommand, TelemetryRepository
from BD.table_update_scripts.production_cycle import ProductionCycleCommand, ProductionCycleRepository
from BD.table_update_scripts.machine_state import MachineStateIntervalCommand, MachineStateRepository
from BD.table_update_scripts.configuration_history import (
    MachineParameterHistoryCommand,
    MachineParameterRepository,
)


@dataclass(slots=True)
class _MachineRuntime:
    data: dict[str, Any] = field(default_factory=dict)
    last_timestamp: Optional[datetime] = None
    last_telemetry_at: Optional[datetime] = None

    shot_counter: Optional[int] = None
    production_count: Optional[int] = None
    good_parts: Optional[int] = None
    reject_parts: Optional[int] = None
    rejects_in_series: int = 0
    last_cycle_finished_at: Optional[datetime] = None

    state_code: Optional[int] = None
    state_started_at: Optional[datetime] = None
    state_reason_group: Optional[int] = None
    state_reason_code: Optional[int] = None


class MesDataUpdater:
    """Translate KEBA polling snapshots into persistence commands.

    One instance must live for the lifetime of the collector process so it can
    keep per-machine transition state between polls.
    """

    CONFIG_FIELDS = {
        "mold_data": "mold_data",
        "unit_type": "unit_type",
        "production_target": "production_target",
        "max_cycle_time_s": "max_cycle_time_s",
    }

    def __init__(
        self,
        machine_ids: Mapping[str, int],
        telemetry_repository: TelemetryRepository,
        cycle_repository: ProductionCycleRepository,
        state_repository: MachineStateRepository,
        parameter_repository: MachineParameterRepository,
        telemetry_interval_s: float = 5.0,
        logger: Optional[logging.Logger] = None,
    ) -> None:
        if telemetry_interval_s <= 0:
            raise ValueError("telemetry_interval_s must be > 0")

        self._machine_ids = dict(machine_ids)
        self._telemetry = telemetry_repository
        self._cycles = cycle_repository
        self._states = state_repository
        self._parameters = parameter_repository
        self._telemetry_interval = timedelta(seconds=telemetry_interval_s)
        self._logger = logger or logging.getLogger(__name__)
        self._runtime: dict[str, _MachineRuntime] = {}

    def update(self, snapshot: dict[str, Any]) -> None:
        """Persist one partial KEBA polling response.

        The response may contain only fast, thermal or job fields. Runtime data
        is merged first, then only entities whose source group is fresh are
        evaluated.
        """
        machine_code = str(snapshot.get("machine_id", ""))
        if not machine_code:
            raise ValueError("snapshot.machine_id is required")

        machine_id = self._machine_ids.get(machine_code)
        if machine_id is None:
            raise KeyError(f"No database machine_id configured for {machine_code!r}")

        ts = self._parse_timestamp(snapshot.get("timestamp"))
        runtime = self._runtime.setdefault(machine_code, _MachineRuntime())
        runtime.last_timestamp = ts

        if not snapshot.get("connected", False):
            # Do not overwrite last known process values with an empty failed poll.
            return

        fresh_data = snapshot.get("data") or {}
        groups_read = set(snapshot.get("groups_read") or [])

        self._merge_data(runtime.data, fresh_data)

        if "fast" in groups_read:
            self._update_cycle(machine_id, machine_code, ts, fresh_data, runtime)
            self._update_state(machine_id, ts, fresh_data, runtime)

        if "thermal" in groups_read or "job" in groups_read or "fast" in groups_read:
            self._update_configuration(machine_id, ts, fresh_data)

        self._maybe_write_telemetry(machine_id, ts, runtime)

    @staticmethod
    def _parse_timestamp(value: Any) -> datetime:
        if isinstance(value, datetime):
            dt = value
        elif isinstance(value, str) and value:
            dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
        else:
            dt = datetime.now(timezone.utc)

        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)

    @classmethod
    def _merge_data(cls, target: dict[str, Any], incoming: dict[str, Any]) -> None:
        for key, value in incoming.items():
            if isinstance(value, dict) and isinstance(target.get(key), dict):
                cls._merge_data(target[key], value)
            else:
                target[key] = copy.deepcopy(value)

    @staticmethod
    def _as_int(value: Any) -> Optional[int]:
        if value is None or isinstance(value, bool):
            return None
        try:
            return int(value)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _as_float(value: Any) -> Optional[float]:
        if value is None or isinstance(value, bool):
            return None
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    def _maybe_write_telemetry(
        self,
        machine_id: int,
        ts: datetime,
        runtime: _MachineRuntime,
    ) -> None:
        if (
            runtime.last_telemetry_at is not None
            and ts - runtime.last_telemetry_at < self._telemetry_interval
        ):
            return

        d = runtime.data
        heating = d.get("heating") if isinstance(d.get("heating"), dict) else {}

        command = Telemetry5sCommand(
            ts=ts,
            machine_id=machine_id,
            cycle_id=self._as_int(d.get("shot_counter")),
            screw_position_mm=self._as_float(d.get("screw_position_raw")),
            cushion=self._as_float(d.get("cushion_raw")),
            mold_position_mm=self._as_float(d.get("mold_position_mm")),
            clamp_force_kn=self._as_float(d.get("clamp_force_kn")),
            nozzle_position_mm=self._as_float(d.get("nozzle_position_mm")),
            ejector_position_mm=self._as_float(d.get("ejector_position_mm")),
            oil_temperature_c=self._as_float(d.get("oil_temp_c")),
            pump_temperature_c=self._as_float(d.get("motor_temp_c")),
            hopper_temperature_c=self._as_float(heating.get("hopper_temp_c")),
            zone1_temperature_c=self._as_float(heating.get("zone_1_actual_c")),
            zone2_temperature_c=self._as_float(heating.get("zone_2_actual_c")),
            zone3_temperature_c=self._as_float(heating.get("zone_3_actual_c")),
            zone4_temperature_c=self._as_float(heating.get("zone_4_actual_c")),
            zone5_temperature_c=self._as_float(heating.get("zone_5_actual_c")),
            zone6_temperature_c=self._as_float(heating.get("zone_6_actual_c")),
        )

        self._telemetry.insert(command)
        runtime.last_telemetry_at = ts

    def _update_cycle(
        self,
        machine_id: int,
        machine_code: str,
        ts: datetime,
        fresh: dict[str, Any],
        runtime: _MachineRuntime,
    ) -> None:
        current_shot = self._as_int(fresh.get("shot_counter"))
        if current_shot is None:
            return

        current_prod = self._as_int(fresh.get("production_count"))
        current_good = self._as_int(fresh.get("good_parts"))
        current_reject = self._as_int(fresh.get("reject_parts"))

        previous_shot = runtime.shot_counter

        if previous_shot is None:
            runtime.shot_counter = current_shot
            runtime.production_count = current_prod
            runtime.good_parts = current_good
            runtime.reject_parts = current_reject
            runtime.last_cycle_finished_at = ts
            return

        shot_delta = current_shot - previous_shot

        if shot_delta == 0:
            return

        if shot_delta < 0:
            self._logger.warning(
                "%s shot counter reset: %s -> %s",
                machine_code,
                previous_shot,
                current_shot,
            )
            runtime.shot_counter = current_shot
            runtime.production_count = current_prod
            runtime.good_parts = current_good
            runtime.reject_parts = current_reject
            runtime.rejects_in_series = 0
            runtime.last_cycle_finished_at = ts
            return

        if shot_delta > 1:
            self._logger.error(
                "%s missed %s cycle transitions (%s -> %s); not fabricating cycle rows",
                machine_code,
                shot_delta - 1,
                previous_shot,
                current_shot,
            )
            runtime.shot_counter = current_shot
            runtime.production_count = current_prod
            runtime.good_parts = current_good
            runtime.reject_parts = current_reject
            runtime.last_cycle_finished_at = ts
            return

        good_delta = self._counter_delta(runtime.good_parts, current_good)
        reject_delta = self._counter_delta(runtime.reject_parts, current_reject)

        if reject_delta > 0:
            runtime.rejects_in_series += reject_delta
        else:
            runtime.rejects_in_series = 0

        last_cycle_s = self._as_float(fresh.get("last_cycle_time_s"))
        machine_cycle_s = self._as_float(fresh.get("cycle_time_s"))

        measured_ms: Optional[int] = None
        if runtime.last_cycle_finished_at is not None:
            measured_ms = max(0, int((ts - runtime.last_cycle_finished_at).total_seconds() * 1000))

        machine_cycle_ms = self._seconds_to_ms(last_cycle_s or machine_cycle_s)
        cycle_time_ms = machine_cycle_ms or measured_ms or 0
        started_at = ts - timedelta(milliseconds=cycle_time_ms)

        d = runtime.data
        command = ProductionCycleCommand(
            machine_id=machine_id,
            cycle_id=current_shot,
            started_at=started_at,
            finished_at=ts,
            cycle_time_ms=cycle_time_ms,
            calculated_cycle_time_ms=measured_ms,
            machine_cycle_time_ms=machine_cycle_ms,
            production_counter=current_prod or 0,
            good_parts=good_delta,
            reject_parts=reject_delta,
            cycle_quality=self._as_int(fresh.get("cycle_quality")) or 0,
            rejects_in_series=runtime.rejects_in_series,
            mold_id=self._string_or_none(d.get("job_mould_id") or d.get("mold_data")),
            article_id=self._string_or_none(d.get("article_name")),
            production_dataset=self._string_or_none(d.get("production_dataset")),
            production_order_id=self._as_int(d.get("job_id")),
        )
        self._cycles.insert(command)

        runtime.shot_counter = current_shot
        runtime.production_count = current_prod
        runtime.good_parts = current_good
        runtime.reject_parts = current_reject
        runtime.last_cycle_finished_at = ts

    def _update_state(
        self,
        machine_id: int,
        ts: datetime,
        fresh: dict[str, Any],
        runtime: _MachineRuntime,
    ) -> None:
        current_state = self._as_int(fresh.get("machine_state_code"))
        if current_state is None:
            return

        reason_group = self._as_int(fresh.get("standstill_reason_group"))
        reason_code = self._as_int(fresh.get("standstill_reason_no"))

        if runtime.state_code is None:
            runtime.state_code = current_state
            runtime.state_started_at = ts
            runtime.state_reason_group = reason_group
            runtime.state_reason_code = reason_code
            return

        if (
            current_state == runtime.state_code
            and reason_group == runtime.state_reason_group
            and reason_code == runtime.state_reason_code
        ):
            return

        started_at = runtime.state_started_at or ts
        duration_ms = max(0, int((ts - started_at).total_seconds() * 1000))

        self._states.insert(
            MachineStateIntervalCommand(
                machine_id=machine_id,
                state=f"STATE_{runtime.state_code}",
                started_at=started_at,
                finished_at=ts,
                duration_ms=duration_ms,
                reason_group=runtime.state_reason_group,
                reason_code=runtime.state_reason_code,
            )
        )

        runtime.state_code = current_state
        runtime.state_started_at = ts
        runtime.state_reason_group = reason_group
        runtime.state_reason_code = reason_code

    def _update_configuration(
        self,
        machine_id: int,
        ts: datetime,
        fresh: dict[str, Any],
    ) -> None:
        values: dict[str, Any] = {}

        for source_name, parameter_code in self.CONFIG_FIELDS.items():
            if source_name in fresh:
                values[parameter_code] = fresh[source_name]

        heating = fresh.get("heating")
        if isinstance(heating, dict):
            for zone in range(1, 7):
                key = f"zone_{zone}_set_c"
                if key in heating:
                    values[f"heating.zone_{zone}_set_c"] = heating[key]

        for parameter_code, value in values.items():
            command = self._parameter_command(
                machine_id=machine_id,
                parameter_code=parameter_code,
                value=value,
                valid_from=ts,
            )

            # New repository implementation below returns False when unchanged.
            method = getattr(self._parameters, "set_value_if_changed", None)
            if callable(method):
                method(command)
            else:
                # Compatibility with the current repository. Prefer replacing it
                # with configuration_history_fixed.py to avoid duplicate history.
                self._parameters.set_value(command)

    @staticmethod
    def _parameter_command(
        machine_id: int,
        parameter_code: str,
        value: Any,
        valid_from: datetime,
    ) -> MachineParameterHistoryCommand:
        kwargs: dict[str, Any] = {
            "machine_id": machine_id,
            "parameter_code": parameter_code,
            "valid_from": valid_from,
            "source": "KEBA",
        }

        if isinstance(value, bool):
            kwargs["value_bool"] = value
        elif isinstance(value, (int, float)) and not isinstance(value, bool):
            kwargs["value_numeric"] = float(value)
        elif value is not None:
            kwargs["value_string"] = str(value)

        return MachineParameterHistoryCommand(**kwargs)

    @staticmethod
    def _counter_delta(previous: Optional[int], current: Optional[int]) -> int:
        if previous is None or current is None or current < previous:
            return 0
        return current - previous

    @staticmethod
    def _seconds_to_ms(value: Optional[float]) -> Optional[int]:
        if value is None or value < 0:
            return None
        return int(round(value * 1000.0))

    @staticmethod
    def _string_or_none(value: Any) -> Optional[str]:
        if value is None:
            return None
        text = str(value).strip()
        return text or None
