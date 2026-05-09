from __future__ import annotations

import socket
import threading
from typing import Any

from p2p_farm.protocol import decode_message, encode_message


class JsonLineSocket:
    def __init__(self, sock: socket.socket) -> None:
        self._socket = sock
        self._socket.settimeout(5.0)
        self._reader = sock.makefile("r", encoding="utf-8", newline="\n")
        self._writer = sock.makefile("w", encoding="utf-8", newline="\n")
        self._send_lock = threading.Lock()

    def send(self, message: dict[str, Any]) -> None:
        raw = encode_message(message)
        with self._send_lock:
            self._writer.write(raw)
            self._writer.flush()

    def receive(self) -> dict[str, Any]:
        raw = self._reader.readline()
        if raw == "":
            raise ConnectionError("socket closed")
        return decode_message(raw)

    def close(self) -> None:
        self._reader.close()
        self._writer.close()
        self._socket.close()