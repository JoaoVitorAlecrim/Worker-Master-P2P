import json
import socket
from typing import Any, Dict, Optional
import uuid


def build_master_envelope(mtype: str, payload: Dict[str, Any], request_id: Optional[str] = None) -> Dict[str, Any]:
    """Constroi o envelope padrão entre masters: {"type","request_id","payload"}.

    Mantém compatibilidade com mensagens legadas que usam a chave `MASTER`.
    """
    return {
        "type": mtype.lower(),
        "request_id": request_id or str(uuid.uuid4()),
        "payload": payload
    }


def parse_master_envelope(data: Dict[str, Any]) -> Dict[str, Any]:
    """Normaliza uma mensagem de master para o formato {type, request_id, payload}.

    Se a mensagem já estiver no novo formato, retorna-a praticamente inalterada.
    Se for a versão legada (chave `MASTER`), encapsula o payload para compatibilidade.
    """
    if not isinstance(data, dict):
        return {"type": "unknown", "request_id": None, "payload": data}

    if "type" in data and "payload" in data:
        return {"type": str(data.get("type")).lower(), "request_id": data.get("request_id"), "payload": data.get("payload")}

    # Compatibilidade com chave legada `MASTER`
    if "MASTER" in data:
        m = str(data.get("MASTER"))
        rid = data.get("REQUEST_ID") or data.get("REQUEST_ID")
        # Use o corpo inteiro como payload para que handlers ainda possam ler
        return {"type": m.lower(), "request_id": rid, "payload": data}

    return {"type": "unknown", "request_id": None, "payload": data}


def send_json(sock: socket.socket, data: Dict[str, Any]) -> None:
    message = json.dumps(data) + "\n"
    sock.sendall(message.encode("utf-8"))


def recv_json_line(sock_file) -> Optional[Dict[str, Any]]:
    line = sock_file.readline()

    if not line:
        return None

    line = line.strip()

    if not line:
        return None

    try:
        return json.loads(line)
    except json.JSONDecodeError:
        return {"TASK": "ERROR", "MESSAGE": "JSON_INVALIDO"}