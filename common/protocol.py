import json
import socket
from typing import Any, Dict, Optional
import uuid


def build_master_envelope(mtype: str, payload: Dict[str, Any], request_id: Optional[str] = None) -> Dict[str, Any]:
    """Constroi o envelope padrão entre masters: {"type","request_id","payload"}.

    Mantém compatibilidade com mensagens legadas que usam a chave `MASTER`.
    """
    return {"type": mtype.lower(), "request_id": request_id or str(uuid.uuid4()), "payload": payload}


def parse_master_envelope(data: Dict[str, Any]) -> Dict[str, Any]:
    """Normaliza uma mensagem de master para o formato {type, request_id, payload}.

    Se a mensagem já estiver no novo formato, retorna-a praticamente inalterada.
    Se for a versão legada (chave `MASTER`), encapsula o payload para compatibilidade.
    """
    if not isinstance(data, dict):
        return {"type": "unknown", "request_id": None, "payload": data}

    if "type" in data and "payload" in data:
        return {
            "type": str(data.get("type")).lower(),
            "request_id": data.get("request_id"),
            "payload": data.get("payload"),
        }

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


def build_master_envelope_spec(mtype: str, payload: Dict[str, Any], request_id: Optional[str] = None) -> Dict[str, Any]:
    """Builds a master↔master envelope using the PDF-style spec keys.

    Spec format (exact keys):
    {
        "MASTER": "REQUEST_HELP",   # uppercase type name
        "REQUEST_ID": "...",
        "PAYLOAD": { ... }
    }

    This function does not mutate `payload` and keeps payload keys as-is.
    """
    return {"MASTER": str(mtype).upper(), "REQUEST_ID": request_id or str(uuid.uuid4()), "PAYLOAD": payload}


def parse_master_envelope_spec(data: Dict[str, Any]) -> Dict[str, Any]:
    """Parses/validates a master↔master envelope in the PDF spec format.

    Returns normalized dict: {"type": <lowercase>, "request_id": <id>, "payload": <dict>}.
    If required fields are missing, returns an error dict with keys
    {"error": "MISSING_FIELDS", "missing": [<fields>] }.

    Unknown extra top-level fields are ignored.
    """
    if not isinstance(data, dict):
        return {"error": "INVALID_FORMAT", "message": "envelope must be an object"}

    missing = []
    if "MASTER" not in data:
        missing.append("MASTER")
    if "REQUEST_ID" not in data:
        missing.append("REQUEST_ID")
    if "PAYLOAD" not in data:
        missing.append("PAYLOAD")

    if missing:
        return {"error": "MISSING_FIELDS", "missing": missing}

    mtype = str(data.get("MASTER"))
    rid = data.get("REQUEST_ID")
    payload = data.get("PAYLOAD")

    return {"type": mtype.lower(), "request_id": rid, "payload": payload}
