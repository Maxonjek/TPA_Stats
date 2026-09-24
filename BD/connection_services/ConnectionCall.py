from __future__ import annotations

import logging
import multiprocessing as mp
import queue
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Optional


@dataclass(slots=True, frozen=True)
class ConnectionConfig:
    connection_name: str
    host: str
    port: int
    call_timeout: float
    call_rate: float
    queue_size: int = 256

    def __post_init__(self) -> None:
        if self.call_rate <= 0:
            raise ValueError("call_rate must be > 0")
        if self.call_timeout <= 0:
            raise ValueError("call_timeout must be > 0")
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
    def __init__(
        self,
        config: ConnectionConfig,
        connection_request: ConnectionRequest,
        on_connection_response: Optional[ConnectionResponse],
        logger: Optional[logging.Logger] = None,
    ) -> None:
        self._config = config
        self._process: Optional[mp.Process] = None
        self._stop_event = mp.Event()
        self._queue: mp.Queue = mp.Queue(maxsize=config.queue_size)
        self._connection_request = connection_request
        self._on_connection_response = on_connection_response
        self._logger = logger or logging.getLogger(__name__)

    @property
    def is_alive(self) -> bool:
        return (
            self._process is not None
            and self._process.is_alive()
            and not self._stop_event.is_set()
        )

    def start(self) -> None:
        if self.is_alive:
            self._logger.warning(
                "Connection %s already started.", self._config.connection_name
            )
            return

        self._stop_event.clear()
        self._process = mp.Process(
            target=self._run,
            args=(
                self._config,
                self._connection_request,
                self._stop_event,
                self._queue,
            ),
            name=self._config.connection_name,
            daemon=True,
        )
        self._process.start()
        self._logger.info(
            "Connection %s started at pid=%s",
            self._config.connection_name,
            self._process.pid,
        )

    def stop(self) -> None:
        process = self._process
        if process is None:
            return

        self._stop_event.set()
        process.join(self._config.call_timeout + 1.0)

        if process.is_alive():
            self._logger.warning(
                "Connection %s didn't stop gracefully; terminating",
                self._config.connection_name,
            )
            process.terminate()
            process.join(self._config.call_timeout + 1.0)

        self._process = None
        self._logger.info("Connection %s stopped.", self._config.connection_name)

    @staticmethod
    def _run(
        config: ConnectionConfig,
        connection_request: ConnectionRequest,
        stop_event: mp.Event,
        response_queue: mp.Queue,
    ) -> None:
        logger = logging.getLogger(f"{__name__}.{config.connection_name}")
        interval = 1.0 / config.call_rate

        try:
            while not stop_event.is_set():
                started = time.monotonic()

                try:
                    response = connection_request(
                        config.host,
                        config.port,
                        config.call_timeout,
                    )
                    BaseConnection._put_response(
                        response_queue=response_queue,
                        response=response,
                        logger=logger,
                    )
                except Exception:
                    logger.exception("Connection request failed")

                # Always throttle, including failures.
                elapsed = time.monotonic() - started
                stop_event.wait(max(0.0, interval - elapsed))
        finally:
            close = getattr(connection_request, "close", None)
            if callable(close):
                try:
                    close()
                except Exception:
                    logger.exception("Error while closing connection request")

    def update(self, max_items: Optional[int] = None) -> int:
        """Drain queued snapshots in the parent process.

        Returns the number of processed responses.  `max_items` can be used
        to cap work per application loop iteration.
        """
        processed = 0

        while max_items is None or processed < max_items:
            try:
                response = self._queue.get_nowait()
            except queue.Empty:
                break

            if self._on_connection_response is not None:
                try:
                    self._on_connection_response(response)
                except Exception:
                    self._logger.exception(
                        "Exception in response handler for %s",
                        self._config.connection_name,
                    )

            processed += 1

        return processed

    @staticmethod
    def _put_response(response_queue: mp.Queue, response: Any, logger: logging.Logger) -> None:
        """Enqueue without silently overwriting an older telemetry snapshot.

        A bounded queue protects memory. If the consumer is too slow, we log
        and drop the *new* snapshot instead of mutating queue history; this
        makes overload visible and avoids hiding intermediate transitions.
        """
        try:
            response_queue.put(response, timeout=0.05)
        except queue.Full:
            logger.error("Response queue is full; dropping newest snapshot")

    def __enter__(self) -> "BaseConnection":
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.stop()
