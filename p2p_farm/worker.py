from __future__ import annotations

from dataclasses import dataclass
import socket
from uuid import uuid4

from p2p_farm.session import JsonSession


@dataclass(slots=True)
class WorkerClient:
    worker_id: str
    server_address: tuple[str, int]
    server_uuid: str
    _session: JsonSession | None = None
    home_master_address: tuple[str, int] | None = None

    def __post_init__(self) -> None:
        if self.home_master_address is None:
            self.home_master_address = self.server_address

    def heartbeat_message(self) -> dict[str, str]:
        return {"SERVER_UUID": self.server_uuid, "TASK": "HEARTBEAT"}

    def connect(self) -> None:
        if self._session is not None:
            return
        connection = socket.create_connection(self.server_address, timeout=5.0)
        self._session = JsonSession(connection)

    def close(self) -> None:
        if self._session is None:
            return
        self._session.close()
        self._session = None

    def heartbeat_once(self) -> dict[str, object]:
        session = self._require_session()
        return session.request(self.heartbeat_message())

    def request_task_once(self) -> dict[str, object]:
        session = self._require_session()
        message = {"WORKER": "ALIVE", "WORKER_UUID": self.worker_id}
        if self.server_uuid:
            message["SERVER_UUID"] = self.server_uuid
        return session.request(message)

    def submit_task_result(self, task: dict[str, object], *, status: str) -> dict[str, object]:
        session = self._require_session()
        message = {"STATUS": status, "TASK": task["TASK"], "WORKER_UUID": self.worker_id}
        return session.request(message)

    def register_temporary_worker(self, original_master_address: tuple[str, int] | None = None) -> dict[str, object]:
        session = self._require_session()
        payload_address = original_master_address or self.home_master_address or self.server_address
        message = {
            "type": "register_temporary_worker",
            "request_id": str(uuid4()),
            "payload": {
                "worker_id": self.worker_id,
                "original_master_address": self._format_address(payload_address),
            },
        }
        return session.request(message)

    def notify_worker_returned(self, original_master_address: tuple[str, int] | None = None) -> dict[str, object]:
        session = self._require_session()
        payload_address = original_master_address or self.home_master_address or self.server_address
        message = {
            "type": "notify_worker_returned",
            "request_id": str(uuid4()),
            "payload": {"worker_id": self.worker_id, "original_master_address": self._format_address(payload_address)},
        }
        return session.request(message)

    def handle_control_message(self, message: dict[str, object]) -> dict[str, object] | None:
        message_type = message.get("type")
        payload = message.get("payload", {})

        if message_type == "command_redirect":
            new_master_address = self._parse_address(str(payload["new_master_address"]))
            self.close()
            self.server_address = new_master_address
            self.connect()
            return self.register_temporary_worker(original_master_address=self.home_master_address)

        if message_type == "command_release":
            original_master_address = self._parse_address(str(payload["original_master_address"]))
            response = self.notify_worker_returned(original_master_address=original_master_address)
            self.close()
            self.server_address = original_master_address
            self.connect()
            return response

        return None

    def _require_session(self) -> JsonSession:
        if self._session is None:
            raise RuntimeError("worker is not connected")
        return self._session

    def _format_address(self, address: tuple[str, int]) -> str:
        return f"{address[0]}:{address[1]}"

    def _parse_address(self, value: str) -> tuple[str, int]:
        host, port_text = value.rsplit(":", 1)
        return host, int(port_text)