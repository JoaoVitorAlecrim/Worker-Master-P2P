from __future__ import annotations

import queue
import threading
from dataclasses import dataclass, field
from typing import Any

from p2p_farm.transport import JsonLineSocket


@dataclass(slots=True)
class JsonSession:
    sock: JsonLineSocket
    inbox: queue.Queue[dict[str, Any]] = field(default_factory=queue.Queue)
    _request_lock: threading.Lock = field(default_factory=threading.Lock, init=False, repr=False)
    _closed: threading.Event = field(default_factory=threading.Event, init=False, repr=False)
    _reader_thread: threading.Thread = field(init=False, repr=False)

    def __init__(self, sock) -> None:
        object.__setattr__(self, "sock", JsonLineSocket(sock))
        object.__setattr__(self, "inbox", queue.Queue())
        object.__setattr__(self, "_request_lock", threading.Lock())
        object.__setattr__(self, "_closed", threading.Event())
        reader_thread = threading.Thread(target=self._reader_loop, daemon=True)
        object.__setattr__(self, "_reader_thread", reader_thread)
        reader_thread.start()

    def _reader_loop(self) -> None:
        while not self._closed.is_set():
            try:
                message = self.sock.receive()
            except (ConnectionError, TimeoutError, OSError):
                self._closed.set()
                return
            self.inbox.put(message)

    def send(self, message: dict[str, Any]) -> None:
        self.sock.send(message)

    def receive(self, timeout: float = 5.0) -> dict[str, Any]:
        return self.inbox.get(timeout=timeout)

    def request(self, message: dict[str, Any], timeout: float = 5.0) -> dict[str, Any]:
        with self._request_lock:
            self.send(message)
            return self.receive(timeout=timeout)

    def close(self) -> None:
        self._closed.set()
        self.sock.close()