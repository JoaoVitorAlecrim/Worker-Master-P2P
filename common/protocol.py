import json
import socket
from typing import Dict, Any, Optional


def send_json(sock: socket.socket, data: Dict[str, Any]) -> None:
    message = json.dumps(data) + "\n"
    sock.sendall(message.encode("utf-8"))


def recv_json_line(sock_file) -> Optional[Dict[str, Any]]:
    line = sock_file.readline()

    if not line:
        return None

    try:
        return json.loads(line.strip())
    except json.JSONDecodeError:
        return {"ERROR": "JSON_INVALIDO"}