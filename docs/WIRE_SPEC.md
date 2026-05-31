Wire shapes implemented in this repository
=========================================

This document records the exact JSON wire shapes implemented by the codebase (as enforced by tests).

TCP: Worker <-> Master
----------------------

1) Worker presentation / heartbeat (worker -> master)

   - Allowed keys (exact):
     - "WORKER": "ALIVE"
     - "WORKER_UUID": <string>
     - "SERVER_UUID": <string>  # optional, original master
     - "FREE_DISK_BYTES": <int>  # optional

   - Forbidden keys: `AUTH_TOKEN` is not required and must not be used on the wire for ALIVE.

2) Master response when worker presents (master -> worker)

   - HEARTBEAT / ALIVE response (exact keys):
     - "SERVER_UUID": <string>
     - "TASK": "HEARTBEAT"
     - "RESPONSE": "ALIVE"

   - No `WORKERS` list or other extra fields are included in the heartbeat response.

3) Task delivery (master -> worker)

   - When a task is assigned the master sends exactly:
     - "TASK": "QUERY"
     - "USER": <string>  # opaque payload, may be JSON-serialized internals

   - The master does NOT include `TASK_ID`, `WORKERS`, or `RESULT` in the task payload.

4) No-task / redirect (master -> worker)

   - "TASK": "NO_TASK"
   - or redirect:
     - "TASK": "REDIRECT"
     - "TARGET_HOST": <string>
     - "TARGET_PORT": <int>
     - "TARGET_SERVER_UUID": <string>

5) Worker task report (worker -> master)

   - Success payload (exact keys):
     - "STATUS": "OK"
     - "WORKER_UUID": <string>

   - Failure payload (exact keys):
     - "STATUS": "NOK"
     - "WORKER_UUID": <string>
     - "ERROR": <string>  # optional

   - The worker MUST NOT send `TASK_ID`, `TASK`, `RESULT`, or `AUTH_TOKEN` in these reports.


Master <-> Master (TCP envelope)
--------------------------------

The repository prefers the PDF-style envelope and the code emits/parses messages using these exact keys:

Spec envelope (exact keys):

  {
    "type": "request_help",
    "request_id": "...",
    "payload": { ... }
  }

Parsing is strict: missing `type`, `request_id`, or `payload` will be reported as an error by the parser used by `master.py`.


UDP election messages
---------------------

Spec-format election messages use these exact top-level keys:

  {
    "ELECTION": "START" | "VOTE" | "RESULT",
    "REQUEST_ID": "...",
    "PAYLOAD": { ... }
  }

Payload shapes used by this implementation:

  - START: PAYLOAD contains key `CANDIDATE` with fields `{WORKER_UUID, HOST, FREE_DISK_BYTES, SERVER_UUID}`
  - VOTE:  PAYLOAD contains key `CANDIDATE` (same shape as above)
  - RESULT: PAYLOAD contains key `WINNER` with the winning candidate dict

Legacy `type`/`request_id`/`payload` messages are also accepted for compatibility, but new messages are emitted using the spec keys.


Notes
-----
- Tests in `tests/` enforce the shapes described above. If you need exact PDF text to differ from these shapes, update the spec and tests together.
