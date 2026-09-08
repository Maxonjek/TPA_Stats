from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
import multiprocessing as mp
from typing import Optional, Any


@dataclass(slots=True)
class ConnectionConfig:
    connection_name: str
    host: str
    port: int
    call_timeout: float
    call_rate: float
    queue_size: int = 2


class ConnectionRequest:
    def __call__(self, *params) -> Any:
        pass


class ConnectionResponse:
    def __call__(self, *params) -> Any:
        pass


class BaseConnection(ABC):
    def __init__(self, config: ConnectionConfig,
                 connection_request: ConnectionRequest,
                 on_connection_response: ConnectionResponse,
                 logger: Optional[logging.Logger] = None):
        self._config = config
        self._process: Optional[mp.Process] = None
        self._stop_event = mp.Event()
        self._queue: mp.Queue = mp.Queue()
        self._connection_request = connection_request
        self._on_connection_response = on_connection_response

        self._logger = logger or logging.getLogger(__name__)

    @property
    def is_alife(self):
        return True if self._process is not None and self._process.is_alive() and not self._stop_event.is_set() else False

    def start(self):
        if self.is_alife:
            self._logger.warning(f"Connection {self._config.connection_name} already started.")
            return
        self._logger.debug(f"Starting connection \'{self._config.connection_name}\'...")
        self._stop_event.clear()
        self._process = mp.Process(target=self._run,
                                   name=self._config.connection_name,
                                   daemon=True)
        self._process.start()
        self._logger.info(f"Connection {self._config.connection_name} started at {self._process.pid}")

    def stop(self):
        if not self.is_alife:
            self._logger.warning(f"Connection {self._config.connection_name} already stopped.")
            return
        self._logger.debug(f"Stopping connection \'{self._config.connection_name}\'...")
        self._stop_event.set()
        self._process.join()
        if self._process.is_alive():
            self._logger.warning(f"Connection {self._config.connection_name} didn't stop gracefully. Terminating...")
            self._process.terminate()
            self._process.join()
        self._logger.info(f"Connection {self._config.connection_name} stopped.")

    def _run(self):
        process_logger = logging.getLogger(f"{self._process.name}:{self._process.pid}")
        process_logger.debug(f"Running connection \'{self._config.connection_name}\'...")
        while not self._stop_event.is_set():
            pass

