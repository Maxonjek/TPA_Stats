#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
keba_rpc_read.py

Pure-Python, read-only client for the KEBA CP056/G-C VariableServer
used by the KePlast / KeView system found on the target controller.

Default target:
    192.168.100.100

Protocol details recovered from the controller's view2.jar:
    ONC/Sun RPC program : 550418950
    RPC version         : 1
    BrowsePaths2NodeIds : procedure 4230
    ReadValues          : procedure 4240
    Value attribute     : 11

The script uses ONLY:
    - rpcbind GETPORT (program 100000 / v2 / proc 3)
    - BrowsePaths2NodeIds (4230)
    - ReadValues (4240)

It contains no VariableServer write procedures.

Examples:
    python keba_rpc_read.py

    python keba_rpc_read.py system.sv_iShotCounterAct system.sv_dCycleTime

    python keba_rpc_read.py --watch 1

    python keba_rpc_read.py --host 192.168.100.100 --port 1022 --watch 1

Python:
    Standard library only; no pip packages are required.
"""

from __future__ import annotations

import argparse
import json
import random
import socket
import struct
import sys
import time
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Tuple


DEFAULT_HOST = "192.168.100.100"

# KEBA VariableServer
VARSERVER_PROGRAM = 550418950
VARSERVER_VERSION = 1
PROC_BROWSE_PATHS_TO_NODE_IDS = 4230
PROC_READ_VALUES = 4240
ATTR_VALUE = 11

# ONC RPC / rpcbind
RPCBIND_PORT = 111
PMAP_PROGRAM = 100000
PMAP_VERSION = 2
PMAPPROC_GETPORT = 3
IPPROTO_TCP = 6

# Known port on this controller from rpcinfo. Used only as a fallback.
KNOWN_VARSERVER_PORT = 1022

DEFAULT_VARIABLES = [
    "system.sv_bAutoCycleRunning",
    "system.sv_iShotCounterAct",
    "system.sv_iProdCounterAct",
    "system.sv_dCycleTime",
    "system.sv_dLastCycleTime",
    "system.sv_iCavities",
    "OperationMode1.sv_rProdTimeAct",
    "OperationMode1.sv_rYield",
    "system.sv_Production.iGoodPartsCounter",
    "system.sv_Production.iRejectCounter",
]

RESULT_NAMES = {
    -99: "NotYetImplemented",
    -2: "OutOfMemory",
    -1: "InternalError",
    0: "OK",
    1: "Failed",
    2: "PartsFailed",
    3: "NothingToDo",
    4: "InvalidContinuationPoint",
    5: "InvalidPath",
    6: "DamagedNodeId",
    7: "InvalidValue",
    8: "InvalidOption",
    20: "AlreadyExists",
    21: "NotExists",
    50: "NoReadRights",
    51: "NoWriteRights",
    52: "OutOfRange",
    53: "NotPlausible",
    60: "NotAvailable",
}

QUALITY_NAMES = {
    0: "Good",
    1: "Bad",
    2: "Uncertain",
}

NODE_TYPE_NAMES = {
    0: "NONE",
    1: "BOOL",
    2: "SINT8",
    3: "SINT16",
    4: "SINT32",
    5: "SINT64",
    6: "UINT8",
    7: "UINT16",
    8: "UINT32",
    9: "UINT64",
    10: "REAL",
    11: "LREAL",
    12: "BYTE",
    13: "WORD",
    14: "DWORD",
    15: "LWORD",
    16: "DATE",
    17: "TIME",
    18: "TOD",
    19: "DT",
    20: "ENUM",
    22: "STRING",
    23: "WSTRING",
    43: "SINT32RANGE",
    44: "LREALRANGE",
    45: "ENUM_SINT8",
    46: "ENUM_SINT16",
    47: "ENUM_SINT32",
    48: "ENUM_SINT64",
    49: "ENUM_UINT8",
    50: "ENUM_UINT16",
    51: "ENUM_UINT32",
    52: "ENUM_UINT64",
    54: "DATE64us",
    55: "TIME64us",
    56: "TOD64us",
    57: "DT64us",
    345: "CONST_SINT8",
    346: "CONST_SINT16",
    347: "CONST_SINT32",
    348: "CONST_SINT64",
    349: "CONST_UINT8",
    350: "CONST_UINT16",
    351: "CONST_UINT32",
    352: "CONST_UINT64",
    990: "DIRECTORY",
    999: "OPAQUE",
}


class RpcError(RuntimeError):
    pass


class XdrError(RuntimeError):
    pass


class XdrWriter:
    def __init__(self) -> None:
        self.data = bytearray()

    def i32(self, value: int) -> None:
        # Keep the exact lower 32 bits and encode as XDR int.
        self.data += struct.pack(">I", value & 0xFFFFFFFF)

    def u32(self, value: int) -> None:
        self.data += struct.pack(">I", value & 0xFFFFFFFF)

    def i64(self, value: int) -> None:
        self.data += struct.pack(">q", value)

    def u64(self, value: int) -> None:
        self.data += struct.pack(">Q", value & 0xFFFFFFFFFFFFFFFF)

    def f32(self, value: float) -> None:
        self.data += struct.pack(">f", value)

    def f64(self, value: float) -> None:
        self.data += struct.pack(">d", value)

    def opaque(self, value: bytes) -> None:
        self.u32(len(value))
        self.data += value
        self.data += b"\x00" * ((-len(value)) % 4)

    def string(self, value: Optional[str]) -> None:
        # KEBA's RPCOutputStream defaults to ISO-8859-1 when no override is set.
        raw = b"" if value is None else value.encode("latin-1")
        self.opaque(raw)

    def bytes(self) -> bytes:
        return bytes(self.data)


class XdrReader:
    def __init__(self, data: bytes) -> None:
        self.data = data
        self.pos = 0

    def _take(self, n: int) -> bytes:
        end = self.pos + n
        if end > len(self.data):
            raise XdrError(
                f"Unexpected end of XDR data at {self.pos}; "
                f"need {n} bytes, have {len(self.data) - self.pos}"
            )
        out = self.data[self.pos:end]
        self.pos = end
        return out

    def i32(self) -> int:
        return struct.unpack(">i", self._take(4))[0]

    def u32(self) -> int:
        return struct.unpack(">I", self._take(4))[0]

    def i64(self) -> int:
        return struct.unpack(">q", self._take(8))[0]

    def u64(self) -> int:
        return struct.unpack(">Q", self._take(8))[0]

    def f32(self) -> float:
        return struct.unpack(">f", self._take(4))[0]

    def f64(self) -> float:
        return struct.unpack(">d", self._take(8))[0]

    def boolean(self) -> bool:
        return self.i32() != 0

    def opaque(self) -> bytes:
        n = self.u32()
        raw = self._take(n)
        pad = (-n) % 4
        if pad:
            self._take(pad)
        return raw

    def string(self) -> str:
        return self.opaque().decode("latin-1", errors="replace")

    def remaining(self) -> int:
        return len(self.data) - self.pos


def recv_exact(sock: socket.socket, n: int) -> bytes:
    chunks = []
    left = n
    while left:
        part = sock.recv(left)
        if not part:
            raise RpcError("Connection closed by remote host")
        chunks.append(part)
        left -= len(part)
    return b"".join(chunks)


def recv_rpc_record(sock: socket.socket) -> bytes:
    """Receive one RFC 5531 record, including multi-fragment records."""
    chunks: List[bytes] = []
    while True:
        marker = struct.unpack(">I", recv_exact(sock, 4))[0]
        last = bool(marker & 0x80000000)
        length = marker & 0x7FFFFFFF
        if length > 64 * 1024 * 1024:
            raise RpcError(f"Unreasonable RPC fragment length: {length}")
        chunks.append(recv_exact(sock, length))
        if last:
            return b"".join(chunks)


class OncRpcTcpClient:
    def __init__(
        self,
        host: str,
        port: int,
        program: int,
        version: int,
        timeout: float = 3.0,
    ) -> None:
        self.host = host
        self.port = port
        self.program = program
        self.version = version
        self.timeout = timeout
        self.sock: Optional[socket.socket] = None
        self.xid = random.randint(1, 0x7FFFFFFF)

    def connect(self) -> None:
        if self.sock is not None:
            return
        self.sock = socket.create_connection(
            (self.host, self.port), timeout=self.timeout
        )
        self.sock.settimeout(self.timeout)

    def close(self) -> None:
        if self.sock is not None:
            try:
                self.sock.close()
            finally:
                self.sock = None

    def __enter__(self) -> "OncRpcTcpClient":
        self.connect()
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    def call(self, procedure: int, args: bytes = b"") -> bytes:
        self.connect()
        assert self.sock is not None

        self.xid = (self.xid + 1) & 0xFFFFFFFF
        xid = self.xid

        w = XdrWriter()
        w.u32(xid)
        w.u32(0)  # CALL
        w.u32(2)  # RPC version
        w.u32(self.program)
        w.u32(self.version)
        w.u32(procedure)

        # credential: AUTH_NONE, zero-length opaque body
        w.u32(0)
        w.u32(0)

        # verifier: AUTH_NONE, zero-length opaque body
        w.u32(0)
        w.u32(0)

        body = w.bytes() + args
        record = struct.pack(">I", 0x80000000 | len(body)) + body
        self.sock.sendall(record)

        response = recv_rpc_record(self.sock)
        r = XdrReader(response)

        rxid = r.u32()
        if rxid != xid:
            raise RpcError(f"RPC XID mismatch: sent {xid}, got {rxid}")

        msg_type = r.u32()
        if msg_type != 1:  # REPLY
            raise RpcError(f"Expected RPC REPLY, got message type {msg_type}")

        reply_stat = r.u32()

        if reply_stat == 0:  # MSG_ACCEPTED
            _verifier_flavor = r.u32()
            _verifier_body = r.opaque()
            accept_stat = r.u32()

            if accept_stat == 0:  # SUCCESS
                return response[r.pos:]
            if accept_stat == 1:
                raise RpcError("RPC program unavailable")
            if accept_stat == 2:
                low = r.u32()
                high = r.u32()
                raise RpcError(f"RPC program version mismatch ({low}..{high})")
            if accept_stat == 3:
                raise RpcError(f"RPC procedure {procedure} unavailable")
            if accept_stat == 4:
                raise RpcError("RPC server reports garbage arguments")
            if accept_stat == 5:
                raise RpcError("RPC server system error")
            raise RpcError(f"Unknown RPC accept status {accept_stat}")

        if reply_stat == 1:  # MSG_DENIED
            reject_stat = r.u32()
            if reject_stat == 0:
                low = r.u32()
                high = r.u32()
                raise RpcError(f"RPC protocol version mismatch ({low}..{high})")
            if reject_stat == 1:
                auth_stat = r.u32()
                raise RpcError(f"RPC authentication rejected (code {auth_stat})")
            raise RpcError(f"Unknown RPC reject status {reject_stat}")

        raise RpcError(f"Unknown RPC reply status {reply_stat}")


def rpcbind_getport(
    host: str,
    program: int,
    version: int,
    timeout: float,
) -> int:
    """Ask classic portmapper/rpcbind for a TCP service port."""
    w = XdrWriter()
    w.u32(program)
    w.u32(version)
    w.u32(IPPROTO_TCP)
    w.u32(0)

    with OncRpcTcpClient(
        host, RPCBIND_PORT, PMAP_PROGRAM, PMAP_VERSION, timeout
    ) as client:
        payload = client.call(PMAPPROC_GETPORT, w.bytes())

    r = XdrReader(payload)
    port = r.u32()
    return port


@dataclass
class NodeId:
    checksum: int
    node_owner: int
    node_type: int
    handle_data: List[int]


@dataclass
class ResolvedVariable:
    requested_name: str
    rpc_path: str
    result: int
    node_id: Optional[NodeId]
    browse_name: str = ""
    type_name: str = ""
    type_size: int = 0
    is_leaf: bool = False
    diagnostic_info: int = 0


@dataclass
class VariableValue:
    requested_name: str
    rpc_path: str
    result: int
    quality: int
    node_type: int
    value: Any
    time_read: Tuple[int, int]
    time_changed: Tuple[int, int]
    diagnostic_info: int


def pack_node_id(w: XdrWriter, node: NodeId) -> None:
    w.i32(node.checksum)
    w.i32(node.node_owner)
    w.i32(node.node_type)
    w.u32(len(node.handle_data))
    for x in node.handle_data:
        w.i32(x)


def unpack_node_id(r: XdrReader) -> NodeId:
    checksum = r.i32()
    node_owner = r.i32()
    node_type = r.i32()
    count = r.u32()
    if count > 128:
        raise XdrError(f"Invalid KEBA NodeId handle count: {count}")
    handles = [r.i32() for _ in range(count)]
    return NodeId(checksum, node_owner, node_type, handles)


def normalize_var_name(name: str) -> str:
    """
    Match SdrVarSvrNetworkClient.modifyVarName() from view2.jar.

    APPL.* and SYS.* are already absolute.
    IEC.* / IEC:.* lose their IEC prefix and become APPL.*.
    Other HMI names such as system.* or OperationMode1.* become APPL.*.
    """
    if name.startswith("APPL.") or name.startswith("SYS."):
        return name

    out = name
    if out.startswith("IEC.") or out.startswith("IEC:."):
        dot = out.find(".")
        if dot >= 0:
            out = out[dot + 1 :]

    return "APPL." + out


def _pack_browse_request(paths: Iterable[str]) -> bytes:
    paths = list(paths)
    w = XdrWriter()
    w.i32(0)  # sessionId
    w.u32(len(paths))

    for path in paths:
        # empty/default startingNode
        w.i32(0)  # checksum
        w.i32(0)  # nodeOwner
        w.i32(0)  # nodeType
        w.u32(0)  # handleData_count

        w.string(path)
        w.string(None)  # options == null -> empty XDR string

    return w.bytes()


def _parse_browse_response(
    data: bytes,
    requested_names: List[str],
    paths: List[str],
) -> Tuple[int, List[ResolvedVariable]]:
    r = XdrReader(data)
    master_result = r.i32()
    count = r.u32()

    if count > 10000:
        raise XdrError(f"Invalid browse result count: {count}")

    out: List[ResolvedVariable] = []
    for i in range(count):
        result = r.i32()
        node_id = unpack_node_id(r)
        browse_name = r.string()
        type_name = r.string()
        type_size = r.i32()
        is_leaf = r.boolean()
        diagnostic_info = r.i32()

        requested = requested_names[i] if i < len(requested_names) else f"#{i}"
        path = paths[i] if i < len(paths) else ""

        out.append(
            ResolvedVariable(
                requested_name=requested,
                rpc_path=path,
                result=result,
                node_id=node_id if result == 0 else None,
                browse_name=browse_name,
                type_name=type_name,
                type_size=type_size,
                is_leaf=is_leaf,
                diagnostic_info=diagnostic_info,
            )
        )

    if r.remaining() != 0:
        raise XdrError(f"Browse reply has {r.remaining()} trailing XDR bytes")

    return master_result, out


def resolve_variables(
    client: OncRpcTcpClient,
    names: List[str],
    raw_names: bool = False,
) -> List[ResolvedVariable]:
    paths = names[:] if raw_names else [normalize_var_name(x) for x in names]
    payload = client.call(
        PROC_BROWSE_PATHS_TO_NODE_IDS,
        _pack_browse_request(paths),
    )
    _master, results = _parse_browse_response(payload, names, paths)

    # Some firmware/application combinations may also accept the original path.
    # If the normalized APPL.* path failed, retry only those failures as raw names.
    if not raw_names:
        failed_indices = [
            i
            for i, item in enumerate(results)
            if item.result != 0 and paths[i] != names[i]
        ]
        if failed_indices:
            retry_names = [names[i] for i in failed_indices]
            retry_payload = client.call(
                PROC_BROWSE_PATHS_TO_NODE_IDS,
                _pack_browse_request(retry_names),
            )
            _m2, retry_results = _parse_browse_response(
                retry_payload, retry_names, retry_names
            )
            for idx, replacement in zip(failed_indices, retry_results):
                if replacement.result == 0:
                    results[idx] = replacement

    return results


def _pack_read_request(nodes: List[NodeId]) -> bytes:
    w = XdrWriter()
    w.i32(0)  # sessionId
    w.u32(len(nodes))

    for node in nodes:
        pack_node_id(w, node)
        w.i32(ATTR_VALUE)  # cSDRVarSvr_Attr_Value == 11
        w.i32(0)           # flags1
        w.i32(0)           # flags2

    w.i32(0)  # maxAgeOfValue
    return w.bytes()


def _read_timestamp(r: XdrReader) -> Tuple[int, int]:
    return r.i32(), r.i32()


def _read_data_value(r: XdrReader) -> Tuple[int, Any]:
    node_type = r.i32()

    if node_type == 0:  # NONE/Internal
        value = {"reserved1": r.i32(), "reserved2": r.i32()}

    elif node_type == 1:  # BOOL
        value = r.boolean()

    elif node_type in (2, 3, 4):  # signed integer <= 32 bit
        value = r.i32()

    elif node_type == 5:  # SINT64
        value = r.i64()

    elif node_type == 6:  # UINT8
        value = r.i32() & 0xFF

    elif node_type == 7:  # UINT16
        value = r.i32() & 0xFFFF

    elif node_type == 8:  # UINT32
        value = r.u32()

    elif node_type == 9:  # UINT64
        value = r.u64()

    elif node_type == 10:  # REAL
        value = r.f32()

    elif node_type == 11:  # LREAL
        value = r.f64()

    elif node_type == 12:  # BYTE
        value = r.i32() & 0xFF

    elif node_type == 13:  # WORD
        value = r.i32() & 0xFFFF

    elif node_type == 14:  # DWORD
        value = r.u32()

    elif node_type == 15:  # LWORD
        value = r.u64()

    elif node_type in (16, 17, 18, 19):  # DATE/TIME/TOD/DT
        sec, usec = _read_timestamp(r)
        if node_type == 17:
            # This mirrors KEBA's SdrIecConvertion for TIME.
            value = sec * 1000 + usec // 1000
        else:
            value = {"sec": sec, "usec": usec}

    elif node_type in (54, 55, 56, 57):  # *64us
        value = r.i64()

    elif node_type == 20:  # ENUM
        value = r.i32()

    elif node_type == 22:  # STRING
        value = r.string()

    elif node_type == 23:  # WSTRING
        count = r.u32()
        chars = [r.i32() for _ in range(count)]
        value = "".join(chr(x & 0xFFFF) for x in chars if x != 0)

    elif node_type == 43:  # SINT32RANGE
        value = {"min": r.i32(), "max": r.i32()}

    elif node_type == 44:  # LREALRANGE
        value = {
            "min": r.f64(),
            "max": r.f64(),
            "min_bound_type": r.i32(),
            "max_bound_type": r.i32(),
        }

    elif node_type in (45, 46, 47, 345, 346, 347):
        value = r.i32()

    elif node_type in (48, 348):
        value = r.i64()

    elif node_type == 49:
        value = r.i32() & 0xFF

    elif node_type == 50:
        value = r.i32() & 0xFFFF

    elif node_type in (51, 349, 350, 351):
        # CONST_UINT8/16 are still encoded as an XDR int; preserve useful range.
        raw = r.u32()
        if node_type == 349:
            value = raw & 0xFF
        elif node_type == 350:
            value = raw & 0xFFFF
        else:
            value = raw

    elif node_type in (52, 352):
        value = r.u64()

    elif node_type == 990:  # DIRECTORY/Internal
        value = {"reserved1": r.i32(), "reserved2": r.i32()}

    elif node_type == 999:  # OPAQUE
        value = r.opaque()

    else:
        # In SDRVarSvr_DataValue.read() these unsupported/structural node types
        # have no union payload, so there is nothing else to consume.
        value = None

    return node_type, value


def _parse_read_response(
    data: bytes,
    resolved: List[ResolvedVariable],
) -> Tuple[int, List[VariableValue]]:
    r = XdrReader(data)
    master_result = r.i32()
    count = r.u32()

    if count > 10000:
        raise XdrError(f"Invalid read result count: {count}")

    out: List[VariableValue] = []
    for i in range(count):
        result = r.i32()
        quality = r.i32()
        time_read = _read_timestamp(r)
        time_changed = _read_timestamp(r)
        node_type, value = _read_data_value(r)
        diagnostic_info = r.i32()

        source = resolved[i] if i < len(resolved) else None
        out.append(
            VariableValue(
                requested_name=source.requested_name if source else f"#{i}",
                rpc_path=source.rpc_path if source else "",
                result=result,
                quality=quality,
                node_type=node_type,
                value=value,
                time_read=time_read,
                time_changed=time_changed,
                diagnostic_info=diagnostic_info,
            )
        )

    if r.remaining() != 0:
        raise XdrError(f"Read reply has {r.remaining()} trailing XDR bytes")

    return master_result, out


def read_resolved(
    client: OncRpcTcpClient,
    resolved: List[ResolvedVariable],
) -> List[VariableValue]:
    valid = [x for x in resolved if x.result == 0 and x.node_id is not None]
    if not valid:
        return []

    nodes = [x.node_id for x in valid if x.node_id is not None]
    payload = client.call(PROC_READ_VALUES, _pack_read_request(nodes))
    _master, values = _parse_read_response(payload, valid)
    return values


def cycle_seconds_hint(name: str, value: Any) -> Optional[float]:
    """
    KeView's cycle-time model divides its cycle-time values by 1,000,000
    before displaying seconds. Keep the raw value and show this only as a hint.
    """
    cycle_names = {
        "system.sv_dCycleTime",
        "system.sv_dLastCycleTime",
        "system.sv_dMaxCycleTime",
    }
    short_name = name[5:] if name.startswith("APPL.") else name
    if short_name in cycle_names and isinstance(value, (int, float)):
        return float(value) / 1_000_000.0
    return None


def result_text(code: int) -> str:
    return RESULT_NAMES.get(code, f"Result({code})")


def value_to_jsonable(value: Any) -> Any:
    if isinstance(value, bytes):
        return value.hex()
    return value


def print_resolution_errors(resolved: List[ResolvedVariable]) -> None:
    for item in resolved:
        if item.result != 0:
            print(
                f"{item.requested_name}: resolve ERROR "
                f"{result_text(item.result)} path={item.rpc_path}",
                file=sys.stderr,
            )


def print_values(values: List[VariableValue]) -> None:
    for item in values:
        type_name = NODE_TYPE_NAMES.get(item.node_type, str(item.node_type))
        if item.result != 0:
            print(
                f"{item.requested_name}\tERROR={result_text(item.result)}"
                f"\tquality={QUALITY_NAMES.get(item.quality, item.quality)}"
            )
            continue

        line = (
            f"{item.requested_name}"
            f"\ttype={type_name}"
            f"\tquality={QUALITY_NAMES.get(item.quality, item.quality)}"
            f"\tvalue={item.value}"
        )

        seconds = cycle_seconds_hint(item.requested_name, item.value)
        if seconds is not None:
            line += f"\tcycle_seconds≈{seconds:.6f}"

        print(line)


def values_as_json(values: List[VariableValue]) -> str:
    obj: Dict[str, Any] = {}
    for item in values:
        entry: Dict[str, Any] = {
            "rpc_path": item.rpc_path,
            "result": item.result,
            "result_text": result_text(item.result),
            "quality": item.quality,
            "quality_text": QUALITY_NAMES.get(item.quality, str(item.quality)),
            "node_type": item.node_type,
            "type": NODE_TYPE_NAMES.get(item.node_type, str(item.node_type)),
            "value": value_to_jsonable(item.value),
            "time_read": {
                "sec": item.time_read[0],
                "usec": item.time_read[1],
            },
            "time_changed": {
                "sec": item.time_changed[0],
                "usec": item.time_changed[1],
            },
            "diagnostic_info": item.diagnostic_info,
        }
        seconds = cycle_seconds_hint(item.requested_name, item.value)
        if seconds is not None:
            entry["cycle_seconds_hint"] = seconds

        obj[item.requested_name] = entry

    return json.dumps(obj, ensure_ascii=False, indent=2)


def determine_port(
    host: str,
    explicit_port: Optional[int],
    timeout: float,
) -> int:
    if explicit_port is not None:
        return explicit_port

    try:
        port = rpcbind_getport(
            host,
            VARSERVER_PROGRAM,
            VARSERVER_VERSION,
            timeout,
        )
        if port:
            print(
                f"rpcbind: KEBA VariableServer "
                f"{VARSERVER_PROGRAM}/v{VARSERVER_VERSION} -> tcp/{port}",
                file=sys.stderr,
            )
            return port
        print(
            "rpcbind returned port 0; falling back to known tcp/1022",
            file=sys.stderr,
        )
    except Exception as exc:
        print(
            f"rpcbind lookup failed ({exc}); falling back to known tcp/1022",
            file=sys.stderr,
        )

    return KNOWN_VARSERVER_PORT


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Read KEBA CP056 VariableServer values over ONC RPC "
            "(pure Python, read-only)."
        )
    )
    parser.add_argument(
        "variables",
        nargs="*",
        help="Variable names. If omitted, MES-related defaults are read.",
    )
    parser.add_argument(
        "--host",
        default=DEFAULT_HOST,
        help=f"KEBA IP address (default: {DEFAULT_HOST})",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=None,
        help="VariableServer TCP port. Default: discover via rpcbind; fallback 1022.",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=3.0,
        help="Socket timeout in seconds (default: 3)",
    )
    parser.add_argument(
        "--watch",
        type=float,
        default=0.0,
        metavar="SECONDS",
        help="Repeat reads every N seconds. 0 = read once.",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Print values as JSON.",
    )
    parser.add_argument(
        "--raw-names",
        action="store_true",
        help="Do not apply KEBA HMI APPL.* variable-name normalization.",
    )
    args = parser.parse_args()

    variables = args.variables or DEFAULT_VARIABLES

    if args.watch < 0:
        parser.error("--watch must be >= 0")
    if args.timeout <= 0:
        parser.error("--timeout must be > 0")

    port = determine_port(args.host, args.port, args.timeout)

    try:
        with OncRpcTcpClient(
            args.host,
            port,
            VARSERVER_PROGRAM,
            VARSERVER_VERSION,
            args.timeout,
        ) as client:
            resolved = resolve_variables(
                client,
                variables,
                raw_names=args.raw_names,
            )

            print_resolution_errors(resolved)

            good = [x for x in resolved if x.result == 0]
            if not good:
                print(
                    "No requested variables could be resolved.",
                    file=sys.stderr,
                )
                return 2

            # Resolve once; in watch mode only ReadValues is repeated.
            while True:
                values = read_resolved(client, resolved)

                if args.json:
                    print(values_as_json(values))
                else:
                    if args.watch:
                        print(time.strftime("[%Y-%m-%d %H:%M:%S]"))
                    print_values(values)

                if not args.watch:
                    break

                sys.stdout.flush()
                time.sleep(args.watch)

    except KeyboardInterrupt:
        return 130
    except (OSError, RpcError, XdrError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
