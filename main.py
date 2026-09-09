from __future__ import annotations

import asyncio
import contextlib
import multiprocessing as mp
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect

from BD.connection_services.ConnectionCall import BaseConnection, ConnectionConfig
from BD.connection_services.KebaMesConnection import LatestMesBuffer, MesConnectionResponse, KebaConnectionRequest

MACHINES = (
    ("TPA-01", "192.168.100.100"),
)

buffer = LatestMesBuffer()
connections: list[BaseConnection] = []

for machine_id, host in MACHINES:
    connections.append(
        BaseConnection(
            config=ConnectionConfig(
                connection_name=f"keba-{machine_id}",
                host=host,
                port=0,
                call_timeout=3.0,
                call_rate=2.0,
                queue_size=1),
            connection_request=KebaConnectionRequest(machine_id),
            on_connection_response=MesConnectionResponse(buffer),
        )
    )


async def pump_connection_queues() -> None:
    while True:
        for connection in connections:
            connection.update()

        await asyncio.sleep(0.05)


@asynccontextmanager
async def lifespan(app: FastAPI):
    for connection in connections:
        connection.start()

    pump_task = asyncio.create_task(pump_connection_queues())

    try:
        yield
    finally:
        pump_task.cancel()

        with contextlib.suppress(asyncio.CancelledError):
            await pump_task

        for connection in connections:
            connection.stop()


app = FastAPI(lifespan=lifespan)

@app.get('/')
def start_up():
    return {'status': 'ok'}
@app.get("/ping")
def test_main():
    return {"pong"}

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
        host="0.0.0.0",
        port=8000,
        reload=False,
    )
