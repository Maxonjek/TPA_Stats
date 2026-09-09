KEBA -> MES -> FastAPI example

Files:
- ConnectionCall.py
- KebaMesConnection.py
- keba_rpc_read.py
- fastapi_example.py

ConnectionCall.py:
- fixes the original _put_response signature problem
- worker process no longer serializes the FastAPI-side callback/buffer
- this is important for Windows multiprocessing
- queue_size=1 acts as a latest-value transport buffer
- call_rate=2 means the request loop runs at 2 Hz

KebaMesConnection.py:
- KebaConnectionRequest implements ConnectionRequest
- keeps one RPC TCP connection open in the worker
- resolves KEBA NodeIds once after connection
- uses rpcbind when ConnectionConfig.port=0
- only uses read-only VariableServer calls from keba_rpc_read.py
- reads fast variables every 0.5 s
- reads temperatures every 5 s
- reads job/mold data every 10 s
- MesConnectionResponse writes snapshots into LatestMesBuffer

FastAPI:
- FastAPI runs in the parent process
- pump_connection_queues() transfers the latest worker response into LatestMesBuffer
- GET /api/machines
- GET /api/machines/TPA-01
- WS  /ws/machines

Install:
    pip install fastapi uvicorn

Run:
    python fastapi_example.py

For four machines edit MACHINES:
    ("TPA-01", "IP-1"),
    ("TPA-02", "IP-2"),
    ("TPA-03", "IP-3"),
    ("TPA-04", "IP-4"),

Use port=0 for automatic KEBA VariableServer discovery via rpcbind TCP/111.

The buffer is intentionally in RAM. It is for current dashboard state.
Historical data, cycles, alarms and OEE should be written separately to PostgreSQL.
