from __future__ import annotations

import json
from typing import Any


def encode_message(message: dict[str, Any]) -> str:
    return json.dumps(message, separators=(",", ":")) + "\n"


def decode_message(raw: str) -> dict[str, Any]:
    payload = json.loads(raw.strip())
    if not isinstance(payload, dict):
        raise ValueError("message must be a JSON object")
    return payload