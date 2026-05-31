from typing import List, Dict


def compute_winner(candidates: List[Dict]) -> Dict:
    """Compute the winner among candidates.

    Rule: highest FREE_DISK_BYTES wins. Tie-breaker: lexicographically smaller WORKER_UUID.
    Each candidate is a dict with keys: WORKER_UUID, FREE_DISK_BYTES, HOST, SERVER_UUID
    Returns the chosen candidate dict.
    """
    if not candidates:
        return {}

    # Normalize FREE_DISK_BYTES
    for c in candidates:
        try:
            c["FREE_DISK_BYTES"] = int(c.get("FREE_DISK_BYTES") or 0)
        except Exception:
            c["FREE_DISK_BYTES"] = 0

    sorted_candidates = sorted(candidates, key=lambda item: (-item["FREE_DISK_BYTES"], item["WORKER_UUID"]))
    return sorted_candidates[0]


def build_election_message_spec(mtype: str, payload: Dict, request_id: str | None = None) -> Dict:
    """Build an election UDP message using spec keys.

    Spec uses uppercase top-level keys: `ELECTION`, `REQUEST_ID`, `PAYLOAD`.
    `mtype` should be one of: 'START', 'VOTE', 'RESULT'.
    """
    import uuid

    return {"ELECTION": str(mtype).upper(), "REQUEST_ID": request_id or str(uuid.uuid4()), "PAYLOAD": payload}


def parse_election_message_spec(data: Dict) -> Dict:
    """Parse a spec-format election message.

    Returns normalized dict: {"type": <lower>, "request_id": <id>, "payload": <dict>}.
    If missing required fields, returns {"error": "MISSING_FIELDS", "missing": [...] }.
    """
    if not isinstance(data, dict):
        return {"error": "INVALID_FORMAT", "message": "envelope must be an object"}

    missing = []
    if "ELECTION" not in data:
        missing.append("ELECTION")
    if "REQUEST_ID" not in data:
        missing.append("REQUEST_ID")
    if "PAYLOAD" not in data:
        missing.append("PAYLOAD")

    if missing:
        return {"error": "MISSING_FIELDS", "missing": missing}

    return {
        "type": str(data.get("ELECTION")).lower(),
        "request_id": data.get("REQUEST_ID"),
        "payload": data.get("PAYLOAD"),
    }
