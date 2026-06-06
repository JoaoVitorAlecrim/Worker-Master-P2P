import json
import socket
from typing import Any, Dict, Optional
import uuid


def get_ci_value(data: Dict[str, Any], key: str, default: Any = None) -> Any:
    """Return a dict value using case-insensitive key matching."""
    if not isinstance(data, dict):
        return default

    if key in data:
        return data.get(key, default)

    key_lower = str(key).lower()
    for existing_key, value in data.items():
        if isinstance(existing_key, str) and existing_key.lower() == key_lower:
            return value

    return default


def has_ci_key(data: Dict[str, Any], key: str) -> bool:
    """Return True when a dict contains a key using case-insensitive matching."""
    if not isinstance(data, dict):
        return False

    key_lower = str(key).lower()
    for existing_key in data.keys():
        if isinstance(existing_key, str) and existing_key.lower() == key_lower:
            return True

    return False


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
    """Builds a master↔master envelope using the PDF-style keys.

    The PDF examples use the lowercase envelope: {"type": ..., "request_id": ..., "payload": {...}}
    This function returns that exact shape and keeps payload keys as-is.
    """
    return {"type": str(mtype).lower(), "request_id": request_id or str(uuid.uuid4()), "payload": payload}


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
    if get_ci_value(data, "type") is None:
        missing.append("type")
    if get_ci_value(data, "request_id") is None:
        missing.append("request_id")
    if get_ci_value(data, "payload") is None:
        missing.append("payload")

    # Backwards compatibility: accept legacy or mixed-case keys in master envelopes.
    if missing and has_ci_key(data, "MASTER"):
        legacy_type = get_ci_value(data, "MASTER")
        legacy_request_id = get_ci_value(data, "REQUEST_ID")
        legacy_payload = get_ci_value(data, "PAYLOAD")

        return {
            "type": str(legacy_type).lower() if legacy_type is not None else None,
            "request_id": legacy_request_id,
            "payload": legacy_payload,
        }

    if missing:
        return {"error": "MISSING_FIELDS", "missing": missing}

    mtype = str(get_ci_value(data, "type"))
    rid = get_ci_value(data, "request_id")
    payload = get_ci_value(data, "payload")

    return {"type": mtype.lower(), "request_id": rid, "payload": payload}
