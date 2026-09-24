from __future__ import annotations

import asyncio
import contextlib
import logging
import multiprocessing as mp
import os
from contextlib import asynccontextmanager
from typing import Optional

import clickhouse_connect
import mysql.connector
from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect

from BD.connection_services.ConnectionCall import BaseConnection, ConnectionConfig
from BD.connection_services.KebaMesConnection import (
    KebaConnectionRequest,
    LatestMesBuffer,
    MesConnectionResponse,
)
from BD.connection_services.mes_data_updater import MesDataUpdater
from BD.table_update_scripts.Telemetry_5s import TelemetryRepository
from BD.table_update_scripts.configuration_history import MachineParameterRepository
from BD.table_update_scripts.machine_state import MachineStateRepository
from BD.table_update_scripts.production_cycle import ProductionCycleRepository


logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger(__name__)


# DB id, machine code, KEBA host.
# machine_id is numeric because MySQL/ClickHouse schemas use BIGINT/UInt64.
MACHINES = (
    (1, "TPA-01", "keba-TPA-01"),
    (2, "TPA-02", "keba-TPA-02"),
    (3, "TPA-03", "keba-TPA-03"),
    (4, "TPA-04", "keba-TPA-04"),
)

MACHINE_IDS = {
    machine_code: machine_id
    for machine_id, machine_code, _host in MACHINES
}


buffer = LatestMesBuffer()
connections: list[BaseConnection] = []

# Kept as globals only so lifespan can close them deterministically.
mysql_connection: Optional[object] = None
clickhouse_client: Optional[object] = None




def create_mysql_connection():
    return mysql.connector.connect(
        host=os.getenv("MYSQL_HOST", "mysql"),
        port=int(os.getenv("MYSQL_PORT", "3306")),
        database=os.getenv("MYSQL_DATABASE", "mes"),
        user=os.getenv("MYSQL_USER", "mes"),
        password=os.getenv("MYSQL_PASSWORD", ""),
        autocommit=False,
    )


def create_clickhouse_client():
    return clickhouse_connect.get_client(
        host=os.getenv("CLICKHOUSE_HOST", "clickhouse"),
        port=int(os.getenv("CLICKHOUSE_HTTP_PORT", "8123")),
        database=os.getenv("CLICKHOUSE_DATABASE", "mes"),
        username=os.getenv("CLICKHOUSE_USER", "default"),
        password=os.getenv("CLICKHOUSE_PASSWORD", ""),
    )
def build_updater(mysql_conn, ch_client) -> MesDataUpdater:
    return MesDataUpdater(
        machine_ids=MACHINE_IDS,
        telemetry_repository=TelemetryRepository(ch_client),
        cycle_repository=ProductionCycleRepository(ch_client),
        state_repository=MachineStateRepository(ch_client),
        parameter_repository=MachineParameterRepository(mysql_conn),
        telemetry_interval_s=5.0,
        logger=logger,
    )


def build_connections(updater: MesDataUpdater) -> list[BaseConnection]:
    result: list[BaseConnection] = []

    for _db_id, machine_code, host in MACHINES:
        response_handler = MesConnectionResponse(
            buffer=buffer,
            updater=updater,
        )

        result.append(
            BaseConnection(
                logger=logger,
                config=ConnectionConfig(
                    connection_name=f"keba-{machine_code}",
                    host=host,
                    port=0,
                    call_timeout=3.0,
                    call_rate=2.0,       # Base poll loop: 2 calls/sec.
                    queue_size=256,      # Do not lose cycle transitions.
                ),
                connection_request=KebaConnectionRequest(machine_code),
                on_connection_response=response_handler,
            )
        )

    return result


async def pump_connection_queues() -> None:
    """Drain KEBA process queues in the FastAPI parent process.

    Repository writes happen from MesConnectionResponse -> MesDataUpdater here,
    never from KEBA child processes.
    """
    while True:
        processed = 0

        for connection in connections:
            processed += connection.update(max_items=100)

        # Short sleep keeps latency low without busy-spinning the event loop.
        await asyncio.sleep(0.01 if processed else 0.05)


@asynccontextmanager
async def lifespan(app: FastAPI):
    global mysql_connection, clickhouse_client, connections

    logger.info("Starting MES service")

    try:
        mysql_connection = create_mysql_connection()
        logger.info("MySQL connected")

        clickhouse_client = create_clickhouse_client()
        # Fail startup immediately if ClickHouse is reachable but misconfigured.
        clickhouse_client.command("SELECT 1")
        logger.info("ClickHouse connected")

        updater = build_updater(mysql_connection, clickhouse_client)
        connections = build_connections(updater)

        for connection in connections:
            connection.start()

        pump_task = asyncio.create_task(
            pump_connection_queues(),
            name="keba-queue-pump",
        )

        try:
            yield
        finally:
            pump_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await pump_task

    finally:
        for connection in connections:
            try:
                connection.stop()
            except Exception:
                logger.exception("Failed to stop KEBA connection")

        connections.clear()

        if clickhouse_client is not None:
            try:
                clickhouse_client.close()
            except Exception:
                logger.exception("Failed to close ClickHouse client")
            finally:
                clickhouse_client = None

        if mysql_connection is not None:
            try:
                mysql_connection.close()
            except Exception:
                logger.exception("Failed to close MySQL connection")
            finally:
                mysql_connection = None

        logger.info("MES service stopped")


app = FastAPI(lifespan=lifespan)


@app.get("/")
def start_up():
    return {"status": "ok"}


@app.get("/ping")
def test_main():
    return {"pong": True}


@app.get("/api/machines")
def get_machines():
    return buffer.get_all()


@app.get("/api/machines/{machine_id}")
def get_machine(machine_id: str):
    data = buffer.get(machine_id)

    if data is None:
        raise HTTPException(status_code=404, detail="Machine not found")

    return data


@app.websocket("/ws/machines")
async def machines_websocket(websocket: WebSocket):
    await websocket.accept()

    try:
        while True:
            await websocket.send_json(buffer.get_all())
            await asyncio.sleep(0.5)
    except WebSocketDisconnect:
        pass


if __name__ == "__main__":
    mp.freeze_support()

    import uvicorn

    uvicorn.run(
        "main:app",
        host=os.getenv("API_HOST", "0.0.0.0"),
        port=int(os.getenv("API_PORT", "8000")),
        reload=False,
    )
