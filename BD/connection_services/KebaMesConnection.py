from __future__ import annotations

import copy
import logging
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Iterable, Optional

from BD.connection_services.ConnectionCall import ConnectionRequest, ConnectionResponse
from BD.connection_services.keba_rpc_read import (
    NODE_TYPE_NAMES,
    OncRpcTcpClient,
    VARSERVER_PROGRAM,
    VARSERVER_VERSION,
    determine_port,
    read_resolved,
    resolve_variables,
    result_text,
    value_to_jsonable,
)


@dataclass(frozen=True, slots=True)
class PollGroup:
    name: str
    interval: float
    variables: tuple[str, ...]


FAST_VARIABLES = (
    "system.sv_bAutoCycleRunning",
    "system.sv_iShotCounterAct",
    "system.sv_iShotCounterRetain",
    "system.sv_iProdCounterAct",
    "system.sv_iProdCounterSet",
    "system.sv_iCavities",
    "system.sv_dCycleTime",
    "system.sv_dLastCycleTime",
    "system.sv_dMaxCycleTime",
    "system.sv_Production.iGoodPartsCounter",
    "system.sv_Production.iRejectCounter",
    "system.sv_Production.CycleQuality",
    "OperationMode1.sv_iProdCounterRemaining",
    "OperationMode1.sv_rYield",
    "OperationMode1.sv_iPendingAlarms",
    "OperationMode1.sv_iPendingMessages",
    "OperationMode1.sv_iAlarmLevel",
    "OperationMode1.sv_dNoMovementTimeAct",
    "Mold1.sv_rMoldPosition",
    "Mold1.sv_rClampForceActKN",
    "Mold1.sv_bMoldClosed",
    "Mold1.sv_bMoldOpen",
    "Injection1.sv_rScrewPosition",
    "Injection1.sv_rCushion",
    "Injection1.sv_rCutOffPosition",
    "Nozzle1.sv_rNozzlePosition",
    "Ejector1.sv_rEjectorPositionRel",
    "OilMaintenance1.ti_OilTemp",
    "Pump1.sv_rMotorTemp",
    "Pump1.sv_rCanRpm",
    "TIG.sv_TIG.machineStatus.iState",
    "TIG.sv_TIG.machineStatus.iStandstillReasonGrp",
    "TIG.sv_TIG.machineStatus.iStandstillReasonNo",
)


THERMAL_VARIABLES = (
    "HeatingNozzle1.ti_InTemp1",
    "HeatingNozzle1.ti_InTemp2",
    "HeatingNozzle1.ti_InTemp3",
    "HeatingNozzle1.ti_InTemp4",
    "HeatingNozzle1.ti_InTemp5",
    "HeatingNozzle1.ti_InTemp6",
    "HeatingNozzle1.ti_HopperTemp",
    "HeatingNozzle1.sv_ZoneRetain1.rSetValVis",
    "HeatingNozzle1.sv_ZoneRetain2.rSetValVis",
    "HeatingNozzle1.sv_ZoneRetain3.rSetValVis",
    "HeatingNozzle1.sv_ZoneRetain4.rSetValVis",
    "HeatingNozzle1.sv_ZoneRetain5.rSetValVis",
    "HeatingNozzle1.sv_ZoneRetain6.rSetValVis",
)


JOB_VARIABLES = (
    "system.sv_sMoldData",
    "system.sv_sUnitType",
    "OperationMode1.sv_rProdTimeAct",
    "OperationMode1.sv_rProdTimeRemaining",
    "OperationMode1.sv_rProdTimeTotal",
    "TIG.sv_TIG.job.jobData.iJobID",
    "TIG.sv_TIG.job.jobData.iJobStatus",
    "TIG.sv_TIG.job.jobData.sJobName",
    "TIG.sv_TIG.job.jobData.sJobCustomer",
    "TIG.sv_TIG.job.jobData.sMouldId",
    "TIG.sv_TIG.job.jobData.sProductionDatasetName",
    "TIG.sv_TIG.job.jobData.yJobArticle.[1].sArticleName",
)


DEFAULT_GROUPS = (
    PollGroup("fast", 0.5, FAST_VARIABLES),
    PollGroup("thermal", 5.0, THERMAL_VARIABLES),
    PollGroup("job", 10.0, JOB_VARIABLES),
)


MES_FIELD_MAP = {
    "auto_cycle": "system.sv_bAutoCycleRunning",
    "shot_counter": "system.sv_iShotCounterAct",
    "total_shot_counter": "system.sv_iShotCounterRetain",
    "production_count": "system.sv_iProdCounterAct",
    "production_target": "system.sv_iProdCounterSet",
    "production_remaining": "OperationMode1.sv_iProdCounterRemaining",
    "cavities": "system.sv_iCavities",
    "cycle_time_s": "system.sv_dCycleTime",
    "last_cycle_time_s": "system.sv_dLastCycleTime",
    "max_cycle_time_s": "system.sv_dMaxCycleTime",
    "good_parts": "system.sv_Production.iGoodPartsCounter",
    "reject_parts": "system.sv_Production.iRejectCounter",
    "cycle_quality": "system.sv_Production.CycleQuality",
    "yield_percent": "OperationMode1.sv_rYield",
    "pending_alarms": "OperationMode1.sv_iPendingAlarms",
    "pending_messages": "OperationMode1.sv_iPendingMessages",
    "alarm_level": "OperationMode1.sv_iAlarmLevel",
    "no_movement_time_s": "OperationMode1.sv_dNoMovementTimeAct",
    "mold_position_mm": "Mold1.sv_rMoldPosition",
    "clamp_force_kn": "Mold1.sv_rClampForceActKN",
    "mold_closed": "Mold1.sv_bMoldClosed",
    "mold_open": "Mold1.sv_bMoldOpen",
    "screw_position_raw": "Injection1.sv_rScrewPosition",
    "cushion_raw": "Injection1.sv_rCushion",
    "cutoff_position_raw": "Injection1.sv_rCutOffPosition",
    "nozzle_position_mm": "Nozzle1.sv_rNozzlePosition",
    "ejector_position_mm": "Ejector1.sv_rEjectorPositionRel",
    "oil_temp_c": "OilMaintenance1.ti_OilTemp",
    "motor_temp_c": "Pump1.sv_rMotorTemp",
    "motor_rpm": "Pump1.sv_rCanRpm",
    "machine_state_code": "TIG.sv_TIG.machineStatus.iState",
    "standstill_reason_group": "TIG.sv_TIG.machineStatus.iStandstillReasonGrp",
    "standstill_reason_no": "TIG.sv_TIG.machineStatus.iStandstillReasonNo",
    "mold_data": "system.sv_sMoldData",
    "unit_type": "system.sv_sUnitType",
    "production_time_h": "OperationMode1.sv_rProdTimeAct",
    "production_time_remaining_h": "OperationMode1.sv_rProdTimeRemaining",
    "production_time_total_h": "OperationMode1.sv_rProdTimeTotal",
    "job_id": "TIG.sv_TIG.job.jobData.iJobID",
    "job_status": "TIG.sv_TIG.job.jobData.iJobStatus",
    "job_name": "TIG.sv_TIG.job.jobData.sJobName",
    "job_customer": "TIG.sv_TIG.job.jobData.sJobCustomer",
    "job_mould_id": "TIG.sv_TIG.job.jobData.sMouldId",
    "production_dataset": "TIG.sv_TIG.job.jobData.sProductionDatasetName",
    "article_name": "TIG.sv_TIG.job.jobData.yJobArticle.[1].sArticleName",
}


class KebaConnectionRequest(ConnectionRequest):
    def __init__(
        self,
        machine_id: str,
        groups: Iterable[PollGroup] = DEFAULT_GROUPS,
        resolve_retry_interval: float = 10.0,
    ):
        if resolve_retry_interval <= 0:
            raise ValueError("resolve_retry_interval must be > 0")

        self.machine_id = machine_id
        self.groups = tuple(groups)
        self.resolve_retry_interval = resolve_retry_interval

        self._host: Optional[str] = None
        self._port: Optional[int] = None
        self._timeout: Optional[float] = None
        self._client: Optional[OncRpcTcpClient] = None
        self._resolved_by_name: dict[str, Any] = {}
        self._resolve_errors: dict[str, str] = {}
        self._last_group_read: dict[str, float] = {}
        self._last_resolve_retry: Optional[float] = None

    def close(self) -> None:
        if self._client is not None:
            self._client.close()

        self._client = None
        self._resolved_by_name.clear()
        self._resolve_errors.clear()
        self._last_group_read.clear()
        self._last_resolve_retry = None

    def _all_variables(self) -> list[str]:
        return list(
            dict.fromkeys(
                variable
                for group in self.groups
                for variable in group.variables
            )
        )

    def _connect(self, host: str, port: int, timeout: float) -> None:
        explicit_port = port if port > 0 else None
        resolved_port = determine_port(host, explicit_port, timeout)

        self._host = host
        self._port = resolved_port
        self._timeout = timeout

        self._client = OncRpcTcpClient(
            host,
            resolved_port,
            VARSERVER_PROGRAM,
            VARSERVER_VERSION,
            timeout,
        )
        self._client.connect()

        resolved = resolve_variables(
            self._client,
            self._all_variables(),
            raw_names=False,
        )

        self._resolved_by_name = {
            item.requested_name: item
            for item in resolved
            if item.result == 0 and item.node_id is not None
        }

        self._resolve_errors = {
            item.requested_name: result_text(item.result)
            for item in resolved
            if item.result != 0
        }

        # Start the retry timer from the initial resolve. Failed paths will be
        # retried periodically while the existing RPC connection stays alive.
        self._last_resolve_retry = time.monotonic()

    def _retry_failed_resolves(self, now_mono: float) -> None:
        if self._client is None or not self._resolve_errors:
            return

        if (
            self._last_resolve_retry is not None
            and now_mono - self._last_resolve_retry < self.resolve_retry_interval
        ):
            return

        self._last_resolve_retry = now_mono
        failed_names = list(self._resolve_errors)

        try:
            resolved = resolve_variables(
                self._client,
                failed_names,
                raw_names=False,
            )
        except Exception:
            # A retry failure must not tear down an otherwise working RPC
            # connection. Keep the previous errors and try again later.
            logging.getLogger(__name__).debug(
                "KEBA re-resolve failed for %s (%d variables)",
                self.machine_id,
                len(failed_names),
                exc_info=True,
            )
            return

        recovered: list[str] = []

        for item in resolved:
            name = item.requested_name

            if item.result == 0 and item.node_id is not None:
                self._resolved_by_name[name] = item
                self._resolve_errors.pop(name, None)
                recovered.append(name)
            else:
                self._resolve_errors[name] = result_text(item.result)

        if recovered:
            logging.getLogger(__name__).info(
                "KEBA %s: re-resolved %d variable(s): %s",
                self.machine_id,
                len(recovered),
                ", ".join(recovered),
            )

    def _ensure_connection(self, host: str, port: int, timeout: float) -> None:
        expected_port = port if port > 0 else None

        needs_connect = self._client is None or self._host != host

        if expected_port is not None and self._port != expected_port:
            needs_connect = True

        if self._timeout != timeout:
            needs_connect = True

        if needs_connect:
            self.close()
            self._connect(host, port, timeout)

    @staticmethod
    def _display_value(node_type: int, raw_value: Any) -> tuple[Any, Optional[str]]:
        if node_type == 55 and isinstance(raw_value, (int, float)):
            return float(raw_value) / 1_000_000.0, "s"

        return raw_value, None

    def _read_due_groups(self, now_mono: float) -> tuple[list[str], list[str]]:
        due_groups: list[str] = []
        variables: list[str] = []

        for group in self.groups:
            last_read = self._last_group_read.get(group.name)

            if last_read is None or now_mono - last_read >= group.interval:
                due_groups.append(group.name)
                variables.extend(group.variables)

        return due_groups, list(dict.fromkeys(variables))

    def __call__(self, host: str, port: int, timeout: float) -> dict[str, Any]:
        timestamp = datetime.now(timezone.utc).isoformat()
        now_mono = time.monotonic()

        try:
            self._ensure_connection(host, port, timeout)

            if self._client is None:
                raise RuntimeError("KEBA RPC client is not connected")

            self._retry_failed_resolves(now_mono)

            groups_read, variables_to_read = self._read_due_groups(now_mono)

            resolved = [
                self._resolved_by_name[name]
                for name in variables_to_read
                if name in self._resolved_by_name
            ]

            values = read_resolved(self._client, resolved)

            variable_data: dict[str, dict[str, Any]] = {}
            read_errors: dict[str, str] = {}

            for item in values:
                raw_value = value_to_jsonable(item.value)
                display_value, unit = self._display_value(
                    item.node_type,
                    raw_value,
                )

                variable_data[item.requested_name] = {
                    "value": display_value,
                    "raw_value": raw_value,
                    "unit": unit,
                    "type": NODE_TYPE_NAMES.get(
                        item.node_type,
                        str(item.node_type),
                    ),
                    "quality": item.quality,
                    "result": item.result,
                    "time_read": {
                        "sec": item.time_read[0],
                        "usec": item.time_read[1],
                    },
                    "time_changed": {
                        "sec": item.time_changed[0],
                        "usec": item.time_changed[1],
                    },
                }

                if item.result != 0:
                    read_errors[item.requested_name] = result_text(item.result)

            for group_name in groups_read:
                self._last_group_read[group_name] = now_mono

            mes_data: dict[str, Any] = {}

            for field_name, variable_name in MES_FIELD_MAP.items():
                item = variable_data.get(variable_name)

                if item is not None and item["result"] == 0:
                    mes_data[field_name] = item["value"]

            thermal = {}
            for zone in range(1, 7):
                actual_name = f"HeatingNozzle1.ti_InTemp{zone}"
                set_name = f"HeatingNozzle1.sv_ZoneRetain{zone}.rSetValVis"

                if actual_name in variable_data:
                    thermal[f"zone_{zone}_actual_c"] = variable_data[actual_name]["value"]

                if set_name in variable_data:
                    thermal[f"zone_{zone}_set_c"] = variable_data[set_name]["value"]

            hopper = variable_data.get("HeatingNozzle1.ti_HopperTemp")
            if hopper is not None:
                thermal["hopper_temp_c"] = hopper["value"]

            if thermal:
                mes_data["heating"] = thermal

            errors = dict(self._resolve_errors)
            errors.update(read_errors)

            return {
                "machine_id": self.machine_id,
                "host": host,
                "timestamp": timestamp,
                "connected": True,
                "rpc_port": self._port,
                "groups_read": groups_read,
                "data": mes_data,
                "variables": variable_data,
                "errors": errors,
            }

        except Exception as exc:
            logging.getLogger(__name__).debug(
                "KEBA request failed for %s",
                self.machine_id,
                exc_info=True,
            )

            self.close()

            return {
                "machine_id": self.machine_id,
                "host": host,
                "timestamp": timestamp,
                "connected": False,
                "rpc_port": None,
                "groups_read": [],
                "data": {},
                "variables": {},
                "errors": {
                    "connection": f"{exc.__class__.__name__}: {exc}",
                },
            }


class LatestMesBuffer:
    def __init__(self):
        self._lock = threading.RLock()
        self._machines: dict[str, dict[str, Any]] = {}

    def put(self, snapshot: dict[str, Any]) -> None:
        machine_id = snapshot["machine_id"]

        with self._lock:
            current = self._machines.setdefault(
                machine_id,
                {
                    "machine_id": machine_id,
                    "host": snapshot.get("host"),
                    "connected": False,
                    "timestamp": None,
                    "rpc_port": None,
                    "data": {},
                    "variables": {},
                    "errors": {},
                },
            )

            current["host"] = snapshot.get("host")
            current["connected"] = snapshot.get("connected", False)
            current["timestamp"] = snapshot.get("timestamp")
            current["rpc_port"] = snapshot.get("rpc_port")

            if snapshot.get("connected"):
                current["data"].update(snapshot.get("data", {}))
                current["variables"].update(snapshot.get("variables", {}))
                current["errors"] = snapshot.get("errors", {})
            else:
                current["errors"] = snapshot.get("errors", {})

    def get(self, machine_id: str) -> Optional[dict[str, Any]]:
        with self._lock:
            value = self._machines.get(machine_id)
            return copy.deepcopy(value) if value is not None else None

    def get_all(self) -> dict[str, dict[str, Any]]:
        with self._lock:
            return copy.deepcopy(self._machines)


class MesConnectionResponse(ConnectionResponse):
    def __init__(self, buffer: LatestMesBuffer,updater=None):
        self._buffer = buffer
        self._updater = updater

    def __call__(self, response: dict[str, Any]) -> None:
        if self._updater is not None:
            self._updater.update(response)
        self._buffer.put(response)
