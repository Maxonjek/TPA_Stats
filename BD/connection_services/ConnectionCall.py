from __future__ import annotations

import logging
import queue
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass
import multiprocessing as mp
from typing import Optional, Any


@dataclass(slots=True, frozen=True)
class ConnectionConfig:
    connection_name: str
    host: str
    port: int
    call_timeout: float
    call_rate: float
    queue_size: int = 1

    def __post_init__(self):
        if self.call_rate <= 0:
            raise ValueError("call_rate must be > 0")
        if self.queue_size <= 0:
            raise ValueError("queue_size must be > 0")


class ConnectionRequest(ABC):
    @abstractmethod
    def __call__(self, *params) -> Any:
        raise NotImplementedError(f"{self.__class__.__name__} must be implemented.")


class ConnectionResponse(ABC):
    @abstractmethod
    def __call__(self, *params) -> Any:
        raise NotImplementedError(f"{self.__class__.__name__} must be implemented.")


class BaseConnection:
    def __init__(self, config: ConnectionConfig,
                 connection_request: ConnectionRequest,
                 on_connection_response: ConnectionResponse,
                 logger: Optional[logging.Logger] = None):
        self._config = config
        self._process: Optional[mp.Process] = None
        self._stop_event = mp.Event()
        self._queue: mp.Queue = mp.Queue(maxsize=config.queue_size)
        self._connection_request = connection_request
        self._on_connection_response = on_connection_response

        self._logger = logger or logging.getLogger(__name__)

    @property
    def is_alive(self):
        return True if self._process is not None and self._process.is_alive() and not self._stop_event.is_set() else False

    def start(self):
        if self.is_alive:
            self._logger.warning(f"Connection {self._config.connection_name} already started.")
            return
        self._logger.debug(f"Starting connection \'{self._config.connection_name}\'...")
        self._stop_event.clear()
        self._process = mp.Process(target=self._run,
                                   args=(self._config,
                                         self._connection_request,
                                         self._stop_event,
                                         self._queue),
                                   name=self._config.connection_name,
                                   daemon=True)
        self._process.start()
        self._logger.info(f"Connection {self._config.connection_name} started at {self._process.pid}")

    def stop(self):
        if not self.is_alive:
            self._logger.warning(f"Connection {self._config.connection_name} already stopped.")
            return
        self._logger.debug(f"Stopping connection \'{self._config.connection_name}\'...")
        self._stop_event.set()
        self._process.join(self._config.call_timeout + 1.)
        if self._process.is_alive():
            self._logger.warning(f"Connection {self._config.connection_name} didn't stop gracefully. Terminating...")
            self._process.terminate()
            self._process.join(self._config.call_timeout + 1)
        self._process = None
        self._logger.info(f"Connection {self._config.connection_name} stopped.")

    @staticmethod
    def _run(config: ConnectionConfig,
             connection_request: ConnectionRequest,
             stop_event: mp.Event,
             response_queue: mp.Queue):
        process_logger = logging.getLogger(f"{__name__}.{config.connection_name}")
        process_logger.debug(f"Running connection \'{config.connection_name}\'...")
        interval = 1.0 / config.call_rate
        try:
            while not stop_event.is_set():
                started = time.monotonic()
                try:
                    process_logger.debug("Request: %r -> %s:%s",
                                         connection_request,
                                         config.host,
                                         config.port)
                    connection_response = connection_request(config.host,
                                                             config.port,
                                                             config.call_timeout)
                    BaseConnection._put_response(response_queue=response_queue,
                                                 response=connection_response,
                                                 logger=process_logger)
                    elapsed = time.monotonic() - started
                    stop_event.wait(max(0.0, interval - elapsed))
                except Exception:
                    process_logger.debug(f"Connection {config.connection_name} didn't respond")


        finally:
            process_logger.debug(f"Connection {config.connection_name} stopped.")

    def update(self):
        while True:
            try:
                response = self._queue.get_nowait()
            except queue.Empty:
                break
            if self._on_connection_response is not None:
                try:
                    self._on_connection_response(response)
                except Exception as exc:
                    self._logger.exception(f"Exception occurred in on_state_change callback: {exc}")

    @staticmethod
    def _put_response(response_queue: mp.Queue, response, logger):
        try:
            response_queue.put_nowait(response)
            return
        except queue.Full:
            pass
        try:
            response_queue.get_nowait()
        except queue.Empty:
            pass
        try:
            response_queue.put_nowait(response)
        except queue.Full:
            logger.warning("Unable to enqueue response")

    def __enter__(self) -> 'BaseConnection':
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.stop()
