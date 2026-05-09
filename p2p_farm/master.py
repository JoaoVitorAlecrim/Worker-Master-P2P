from __future__ import annotations

import queue
import socket
import threading
from dataclasses import dataclass, field
from typing import Any
from queue import Queue
from uuid import uuid4

from p2p_farm.session import JsonSession


@dataclass(slots=True)
class MasterFarm:
    master_id: str
    address: tuple[str, int]
    saturation_threshold: int
    release_threshold: int
    tasks: Queue[dict[str, object]] = field(default_factory=Queue)
    _server_socket: socket.socket | None = field(default=None, init=False, repr=False)
    _server_thread: threading.Thread | None = field(default=None, init=False, repr=False)
    _stop_event: threading.Event = field(default_factory=threading.Event, init=False, repr=False)
    _state_lock: threading.Lock = field(default_factory=threading.Lock, init=False, repr=False)
    _listen_address: tuple[str, int] | None = field(default=None, init=False, repr=False)

    def enqueue_task(self, task: dict[str, object]) -> None:
        self.tasks.put(task)

    def is_saturated(self) -> bool:
        return self.tasks.qsize() > self.saturation_threshold

    @property
    def listen_address(self) -> tuple[str, int]:
        if self._listen_address is not None:
            return self._listen_address
        return self.address

    def start(self) -> None:
        if self._server_socket is not None:
            return

        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server_socket.bind(self.address)
        server_socket.listen()
        server_socket.settimeout(0.5)

        self._server_socket = server_socket
        self._listen_address = server_socket.getsockname()
        self._stop_event.clear()
        self._server_thread = threading.Thread(target=self._accept_loop, daemon=True)
        self._server_thread.start()

    def stop(self) -> None:
        self._stop_event.set()
        if self._server_socket is not None:
            try:
                self._server_socket.close()
            finally:
                self._server_socket = None
        if self._server_thread is not None:
            self._server_thread.join(timeout=1)
            self._server_thread = None

    def _accept_loop(self) -> None:
        assert self._server_socket is not None
        while not self._stop_event.is_set():
            try:
                connection, _ = self._server_socket.accept()
            except TimeoutError:
                continue
            except OSError:
                return

            thread = threading.Thread(target=self._handle_connection, args=(connection,), daemon=True)
            thread.start()

    def _handle_connection(self, connection: socket.socket) -> None:
        session = JsonSession(connection)
        try:
            while not self._stop_event.is_set():
                try:
                    message = session.receive(timeout=1.0)
                except queue.Empty:
                    continue
                except (ConnectionError, OSError):
                    return

                response = self._handle_message(message)
                if response is not None:
                    session.send(response)
        finally:
            session.close()

    def _handle_message(self, message: dict[str, Any]) -> dict[str, Any] | None:
        if message.get("TASK") == "HEARTBEAT":
            return {
                "SERVER_UUID": message.get("SERVER_UUID", self.master_id),
                "TASK": "HEARTBEAT",
                "RESPONSE": "ALIVE",
            }

        if message.get("WORKER") == "ALIVE":
            with self._state_lock:
                if not self.tasks.empty():
                    task = self.tasks.get()
                    return dict(task)
            return {"TASK": "NO_TASK"}

        if message.get("STATUS") in {"OK", "NOK"} and message.get("TASK"):
            return {"STATUS": "ACK", "WORKER_UUID": message.get("WORKER_UUID")}

        if message.get("type") == "request_help":
            return self._handle_request_help(message)

        if message.get("type") == "register_temporary_worker":
            return {"type": "response_accepted", "request_id": message.get("request_id"), "payload": {}}

        if message.get("type") == "notify_worker_returned":
            return {"type": "response_accepted", "request_id": message.get("request_id"), "payload": {}}

        return None

    def _handle_request_help(self, message: dict[str, Any]) -> dict[str, Any]:
        request_id = message.get("request_id")
        payload = message.get("payload", {})
        if self.is_saturated():
            return {
                "type": "response_rejected",
                "request_id": request_id,
                "payload": {"reason": "high_load"},
            }

        workers_needed = int(payload.get("workers_needed", 0))
        return {
            "type": "response_accepted",
            "request_id": request_id,
            "payload": {"workers_offered": workers_needed, "worker_details": []},
        }

    def request_help(
        self,
        neighbor_address: tuple[str, int],
        *,
        current_load: int,
        capacity: int,
        workers_needed: int,
    ) -> dict[str, Any]:
        connection = socket.create_connection(neighbor_address, timeout=5.0)
        session = JsonSession(connection)
        try:
            message = build_request_help_message(
                master_id=self.master_id,
                current_load=current_load,
                capacity=capacity,
                workers_needed=workers_needed,
            )
            return session.request(message)
        finally:
            session.close()


def build_request_help_message(*, master_id: str, current_load: int, capacity: int, workers_needed: int) -> dict[str, object]:
    return {
        "type": "request_help",
        "request_id": str(uuid4()),
        "payload": {
            "master_id": master_id,
            "current_load": current_load,
            "capacity": capacity,
            "workers_needed": workers_needed,
        },
    }


def build_response_rejected_message(*, request_id: str, reason: str) -> dict[str, object]:
    return {
        "type": "response_rejected",
        "request_id": request_id,
        "payload": {"reason": reason},
    }


def build_response_accepted_message(*, request_id: str, workers_offered: int, worker_details: list[dict[str, Any]]) -> dict[str, object]:
    return {
        "type": "response_accepted",
        "request_id": request_id,
        "payload": {"workers_offered": workers_offered, "worker_details": worker_details},
    }