from __future__ import annotations

import json
import random
import socket
import struct
from dataclasses import dataclass
from typing import Any, Optional

DEFAULT_HOST = "192.168.100.100"

VARSERVER_PROGRAM = 550418950
VARSERVER_VERSION = 1
PROC_BROWSE_PATHS_TO_NODE_IDS = 4230
PROC_READ_VALUES = 4240
ATTR_VALUE = 11

RPCBIND_PORT = 111
PMAP_PROGRAM = 100000
PMAP_VERSION = 2
PMAPPROC_GETPORT = 3
IPPROTO_TCP = 6

DEFAULT_VARIABLES = [
    "system.sv_bAutoCycleRunning",
    "system.sv_iShotCounterAct",
    "system.sv_iProdCounterAct",
    "system.sv_dCycleTime",
    "system.sv_dLastCycleTime",
    "OperationMode1.sv_iPendingAlarms",
    "OperationMode1.sv_iPendingMessages",
    "system.sv_Production.iGoodPartsCounter",
    "system.sv_Production.iRejectCounter",
    "OilMaintenance1.ti_OilTemp",
]

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
    54: "DATE64us",
    55: "TIME64us",
    56: "TOD64us",
    57: "DT64us",
    990: "DIRECTORY",
    999: "OPAQUE",
}

RESULT_NAMES = {
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

class RpcError(RuntimeError):
    pass

class XdrError(RuntimeError):
    pass

class XdrWriter:
    def __init__(self):
        self.buf = bytearray()

    def u32(self, value: int):
        self.buf += struct.pack(">I", value & 0xFFFFFFFF)

    def i32(self, value: int):
        self.buf += struct.pack(">I", value & 0xFFFFFFFF)

    def opaque(self, value: bytes):
        self.u32(len(value))
        self.buf += value
        self.buf += b"\x00" * ((-len(value)) % 4)

    def string(self, value: Optional[str]):
        raw = b"" if value is None else value.encode("latin-1")
        self.opaque(raw)

    def bytes(self) -> bytes:
        return bytes(self.buf)

class XdrReader:
    def __init__(self, data: bytes):
        self.data = data
        self.pos = 0

    def take(self, n: int) -> bytes:
        end = self.pos + n
        if end > len(self.data):
            raise XdrError("Unexpected end of XDR packet")
        out = self.data[self.pos:end]
        self.pos = end
        return out

    def u32(self) -> int:
        return struct.unpack(">I", self.take(4))[0]

    def i32(self) -> int:
        return struct.unpack(">i", self.take(4))[0]

    def i64(self) -> int:
        return struct.unpack(">q", self.take(8))[0]

    def u64(self) -> int:
        return struct.unpack(">Q", self.take(8))[0]

    def f32(self) -> float:
        return struct.unpack(">f", self.take(4))[0]

    def f64(self) -> float:
        return struct.unpack(">d", self.take(8))[0]

    def boolean(self) -> bool:
        return self.i32() != 0

    def opaque(self) -> bytes:
        length = self.u32()
        data = self.take(length)
        pad = (-length) % 4
        if pad:
            self.take(pad)
        return data

    def string(self) -> str:
        return self.opaque().decode("latin-1", errors="replace")

def recv_exact(sock: socket.socket, n: int) -> bytes:
    out = bytearray()
    while len(out) < n:
        part = sock.recv(n - len(out))
        if not part:
            raise RpcError("Remote host closed connection")
        out += part
    return bytes(out)

def recv_rpc_record(sock: socket.socket) -> bytes:
    
    chunks = []
    while True:
        marker = struct.unpack(">I", recv_exact(sock, 4))[0]
        last_fragment = bool(marker & 0x80000000)
        length = marker & 0x7FFFFFFF
        chunks.append(recv_exact(sock, length))
        if last_fragment:
            return b"".join(chunks)

class OncRpcTcpClient:
    def __init__(
        self,
        host: str,
        port: int,
        program: int,
        version: int,
        timeout: float = 3.0,
    ):
        self.host = host
        self.port = port
        self.program = program
        self.version = version
        self.timeout = timeout
        self.sock: Optional[socket.socket] = None
        self.xid = random.randint(1, 0x7FFFFFFF)

    def __enter__(self):
        self.sock = socket.create_connection(
            (self.host, self.port),
            timeout=self.timeout,
        )
        self.sock.settimeout(self.timeout)
        return self

    def __exit__(self, exc_type, exc, tb):
        if self.sock:
            self.sock.close()

    def call(self, procedure: int, args: bytes = b"") -> bytes:
        if self.sock is None:
            raise RpcError("RPC socket is not connected")

        self.xid = (self.xid + 1) & 0xFFFFFFFF
        xid = self.xid

        w = XdrWriter()
        w.u32(xid)
        w.u32(0)
        w.u32(2)
        w.u32(self.program)
        w.u32(self.version)
        w.u32(procedure)

        w.u32(0)
        w.u32(0)

        w.u32(0)
        w.u32(0)

        body = w.bytes() + args
        self.sock.sendall(
            struct.pack(">I", 0x80000000 | len(body)) + body
        )

        reply = recv_rpc_record(self.sock)
        r = XdrReader(reply)

        if r.u32() != xid:
            raise RpcError("RPC XID mismatch")
        if r.u32() != 1:
            raise RpcError("Unexpected RPC message type")

        reply_stat = r.u32()
        if reply_stat != 0:
            raise RpcError(f"RPC message denied, status={reply_stat}")

        _verifier_flavor = r.u32()
        _verifier_body = r.opaque()

        accept_stat = r.u32()
        if accept_stat != 0:
            raise RpcError(
                f"RPC call failed: proc={procedure}, accept_stat={accept_stat}"
            )

        return reply[r.pos:]

def rpcbind_getport(host: str, timeout: float) -> int:
    
    w = XdrWriter()
    w.u32(VARSERVER_PROGRAM)
    w.u32(VARSERVER_VERSION)
    w.u32(IPPROTO_TCP)
    w.u32(0)

    with OncRpcTcpClient(
        host,
        RPCBIND_PORT,
        PMAP_PROGRAM,
        PMAP_VERSION,
        timeout,
    ) as rpc:
        reply = rpc.call(PMAPPROC_GETPORT, w.bytes())

    port = XdrReader(reply).u32()
    if not port:
        raise RpcError(
            f"rpcbind does not expose KEBA program "
            f"{VARSERVER_PROGRAM}/v{VARSERVER_VERSION}"
        )
    return port

@dataclass
class NodeId:
    checksum: int
    owner: int
    node_type: int
    handles: list[int]

def pack_node_id(w: XdrWriter, node: NodeId):
    w.i32(node.checksum)
    w.i32(node.owner)
    w.i32(node.node_type)
    w.u32(len(node.handles))
    for item in node.handles:
        w.i32(item)

def unpack_node_id(r: XdrReader) -> NodeId:
    checksum = r.i32()
    owner = r.i32()
    node_type = r.i32()
    count = r.u32()
    if count > 128:
        raise XdrError(f"Invalid NodeId handle count: {count}")
    return NodeId(
        checksum=checksum,
        owner=owner,
        node_type=node_type,
        handles=[r.i32() for _ in range(count)],
    )

def normalize_path(name: str) -> str:
    if name.startswith(("APPL.", "SYS.")):
        return name
    if name.startswith("IEC."):
        name = name[4:]
    return "APPL." + name

def resolve_paths(
    rpc: OncRpcTcpClient,
    variables: list[str],
) -> list[tuple[str, str, int, Optional[NodeId]]]:
    
    paths = [normalize_path(v) for v in variables]

    w = XdrWriter()
    w.i32(0)
    w.u32(len(paths))

    for path in paths:
        w.i32(0)
        w.i32(0)
        w.i32(0)
        w.u32(0)

        w.string(path)
        w.string(None)

    reply = rpc.call(PROC_BROWSE_PATHS_TO_NODE_IDS, w.bytes())
    r = XdrReader(reply)

    master_result = r.i32()
    if master_result not in (0, 2):
        raise RpcError(
            f"BrowsePaths2NodeIds master result={master_result}"
        )

    count = r.u32()
    result = []

    for i in range(count):
        item_result = r.i32()
        node = unpack_node_id(r)

        _browse_name = r.string()
        _type_name = r.string()
        _type_size = r.i32()
        _is_leaf = r.boolean()
        _diagnostic = r.i32()

        original = variables[i]
        path = paths[i]

        result.append(
            (
                original,
                path,
                item_result,
                node if item_result == 0 else None,
            )
        )

    return result

def read_value_payload(r: XdrReader) -> tuple[int, Any]:
    node_type = r.i32()

    if node_type == 0:
        value = {"reserved1": r.i32(), "reserved2": r.i32()}
    elif node_type == 1:
        value = r.boolean()
    elif node_type in (2, 3, 4):
        value = r.i32()
    elif node_type == 5:
        value = r.i64()
    elif node_type == 6:
        value = r.i32() & 0xFF
    elif node_type == 7:
        value = r.i32() & 0xFFFF
    elif node_type == 8:
        value = r.u32()
    elif node_type == 9:
        value = r.u64()
    elif node_type == 10:
        value = r.f32()
    elif node_type == 11:
        value = r.f64()
    elif node_type == 12:
        value = r.i32() & 0xFF
    elif node_type == 13:
        value = r.i32() & 0xFFFF
    elif node_type == 14:
        value = r.u32()
    elif node_type == 15:
        value = r.u64()
    elif node_type in (16, 18, 19):
        value = {"sec": r.i32(), "usec": r.i32()}
    elif node_type == 17:
        sec = r.i32()
        usec = r.i32()
        value = sec * 1000 + usec // 1000
    elif node_type == 20:
        value = r.i32()
    elif node_type == 22:
        value = r.string()
    elif node_type == 23:
        count = r.u32()
        chars = [r.i32() for _ in range(count)]
        value = "".join(chr(c & 0xFFFF) for c in chars if c)
    elif node_type in (54, 55, 56, 57):
        value = r.i64()
    elif node_type == 999:
        value = r.opaque().hex()
    else:
        value = None

    return node_type, value

def read_values(
    rpc: OncRpcTcpClient,
    resolved: list[tuple[str, str, int, Optional[NodeId]]],
) -> list[dict[str, Any]]:
    
    valid = [item for item in resolved if item[2] == 0 and item[3] is not None]

    if not valid:
        return []

    w = XdrWriter()
    w.i32(0)
    w.u32(len(valid))

    for _name, _path, _result, node in valid:
        assert node is not None
        pack_node_id(w, node)
        w.i32(ATTR_VALUE)
        w.i32(0)
        w.i32(0)

    w.i32(0)

    reply = rpc.call(PROC_READ_VALUES, w.bytes())
    r = XdrReader(reply)

    master_result = r.i32()
    if master_result not in (0, 2):
        raise RpcError(f"ReadValues master result={master_result}")

    count = r.u32()
    out = []

    for i in range(count):
        result = r.i32()
        quality = r.i32()

        time_read = {
            "sec": r.i32(),
            "usec": r.i32(),
        }
        time_changed = {
            "sec": r.i32(),
            "usec": r.i32(),
        }

        node_type, value = read_value_payload(r)
        diagnostic = r.i32()

        name, path, _resolve_result, _node = valid[i]

        item = {
            "name": name,
            "path": path,
            "result": result,
            "result_text": RESULT_NAMES.get(result, f"Result({result})"),
            "quality": quality,
            "type": NODE_TYPE_NAMES.get(node_type, str(node_type)),
            "raw_value": value,
            "time_read": time_read,
            "time_changed": time_changed,
            "diagnostic": diagnostic,
        }

        if node_type == 55 and isinstance(value, (int, float)):
            item["seconds"] = value / 1_000_000.0

        out.append(item)

    return out

def main() -> None:
    host = DEFAULT_HOST
    timeout = 3.0
    variables = DEFAULT_VARIABLES

    port = rpcbind_getport(host, timeout)

    with OncRpcTcpClient(
        host,
        port,
        VARSERVER_PROGRAM,
        VARSERVER_VERSION,
        timeout,
    ) as rpc:
        resolved = resolve_paths(rpc, variables)

        errors = []
        for name, path, result, _node in resolved:
            if result != 0:
                errors.append({
                    "name": name,
                    "path": path,
                    "result": result,
                    "result_text": RESULT_NAMES.get(
                        result,
                        f"Result({result})",
                    ),
                })

        values = read_values(rpc, resolved)

    response = {
        "host": host,
        "variable_server": {
            "program": VARSERVER_PROGRAM,
            "version": VARSERVER_VERSION,
            "tcp_port": port,
        },
        "read_only": True,
        "values": values,
        "resolve_errors": errors,
    }

    print(json.dumps(response, ensure_ascii=False, indent=2))

if __name__ == "__main__":
    main()
