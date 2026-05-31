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
            c['FREE_DISK_BYTES'] = int(c.get('FREE_DISK_BYTES') or 0)
        except Exception:
            c['FREE_DISK_BYTES'] = 0

    sorted_candidates = sorted(candidates, key=lambda item: (-item['FREE_DISK_BYTES'], item['WORKER_UUID']))
    return sorted_candidates[0]
