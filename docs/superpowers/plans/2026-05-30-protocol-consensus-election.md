# Protocol Compliance and Consensus Election Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make all network JSON strictly match the PDF protocol and add internal (same-farm) consensus election so only one worker becomes master.

**Architecture:** Keep internal task tracking but enforce PDF-compliant payloads at the network boundary. Implement master-to-master negotiation using the PDF envelope, and implement a UDP broadcast election channel for same-farm consensus.

**Tech Stack:** Python 3.13, sockets (TCP/UDP), threading, unittest.

---

### Task 1: Add protocol helpers and schema tests

**Files:**
- Modify: common/protocol.py
- Create: tests/test_protocol_schema.py

- [ ] **Step 1: Write the failing test**

```python
# tests/test_protocol_schema.py
import unittest
from common.protocol import (
    require_fields,
    build_worker_alive,
    build_task_query,
    build_no_task,
    build_status_ok,
    build_status_nok,
    build_ack,
    build_heartbeat_request,
    build_heartbeat_response,
    build_master_message,
)


class ProtocolSchemaTests(unittest.TestCase):
    def test_worker_alive_required(self):
        msg = build_worker_alive("W-123")
        self.assertIsNone(require_fields(msg, ["WORKER", "WORKER_UUID"]))
        self.assertEqual(msg["WORKER"], "ALIVE")
        self.assertNotIn("SERVER_UUID", msg)

    def test_worker_alive_with_server_uuid(self):
        msg = build_worker_alive("W-999", "Master-B")
        self.assertEqual(msg["SERVER_UUID"], "Master-B")

    def test_task_query(self):
        msg = build_task_query("Michel")
        self.assertEqual(msg, {"TASK": "QUERY", "USER": "Michel"})

    def test_no_task(self):
        self.assertEqual(build_no_task(), {"TASK": "NO_TASK"})

    def test_status_ok(self):
        msg = build_status_ok("W-123")
        self.assertEqual(msg["STATUS"], "OK")
        self.assertEqual(msg["TASK"], "QUERY")
        self.assertEqual(msg["WORKER_UUID"], "W-123")

    def test_status_nok(self):
        msg = build_status_nok("W-123")
        self.assertEqual(msg["STATUS"], "NOK")
        self.assertEqual(msg["TASK"], "QUERY")
        self.assertEqual(msg["WORKER_UUID"], "W-123")

    def test_ack(self):
        self.assertEqual(build_ack("W-123"), {"STATUS": "ACK", "WORKER_UUID": "W-123"})

    def test_heartbeat(self):
        req = build_heartbeat_request("Master_A")
        resp = build_heartbeat_response("Master_A")
        self.assertEqual(req["TASK"], "HEARTBEAT")
        self.assertEqual(resp["RESPONSE"], "ALIVE")

    def test_master_message_envelope(self):
        msg = build_master_message("request_help", "uuid", {"a": 1})
        self.assertEqual(msg["type"], "request_help")
        self.assertEqual(msg["request_id"], "uuid")
        self.assertEqual(msg["payload"], {"a": 1})


if __name__ == "__main__":
    unittest.main()
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python tests/test_protocol_schema.py`
Expected: FAIL with `ImportError` or `AttributeError` for missing helpers.

- [ ] **Step 3: Write minimal implementation**

```python
# common/protocol.py
from typing import Any, Dict, Optional, Iterable


def require_fields(data: Dict[str, Any], fields: Iterable[str]) -> Optional[str]:
    for field in fields:
        if field not in data or data[field] in (None, ""):
            return field
    return None


def build_worker_alive(worker_uuid: str, server_uuid: Optional[str] = None) -> Dict[str, Any]:
    msg = {"WORKER": "ALIVE", "WORKER_UUID": worker_uuid}
    if server_uuid:
        msg["SERVER_UUID"] = server_uuid
    return msg


def build_task_query(user: str) -> Dict[str, Any]:
    return {"TASK": "QUERY", "USER": user}


def build_no_task() -> Dict[str, Any]:
    return {"TASK": "NO_TASK"}


def build_status_ok(worker_uuid: str) -> Dict[str, Any]:
    return {"STATUS": "OK", "TASK": "QUERY", "WORKER_UUID": worker_uuid}


def build_status_nok(worker_uuid: str) -> Dict[str, Any]:
    return {"STATUS": "NOK", "TASK": "QUERY", "WORKER_UUID": worker_uuid}


def build_ack(worker_uuid: str) -> Dict[str, Any]:
    return {"STATUS": "ACK", "WORKER_UUID": worker_uuid}


def build_heartbeat_request(server_uuid: str) -> Dict[str, Any]:
    return {"SERVER_UUID": server_uuid, "TASK": "HEARTBEAT"}


def build_heartbeat_response(server_uuid: str) -> Dict[str, Any]:
    return {"SERVER_UUID": server_uuid, "TASK": "HEARTBEAT", "RESPONSE": "ALIVE"}


def build_master_message(msg_type: str, request_id: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    return {"type": msg_type, "request_id": request_id, "payload": payload}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `python tests/test_protocol_schema.py`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add common/protocol.py tests/test_protocol_schema.py
git commit -m "test: add protocol helpers and schema tests"
```

---

### Task 2: Convert task model to USER-only payload

**Files:**
- Modify: common/models.py
- Modify: common/task_manager.py
- Modify: common/tasks.py
- Create: tests/test_task_manager.py

- [ ] **Step 1: Write the failing test**

```python
# tests/test_task_manager.py
import unittest
from common.task_manager import TaskManager


class TaskManagerTests(unittest.TestCase):
    def test_assign_and_complete_clears_worker_task(self):
        tm = TaskManager()
        task = tm.create_task("User_1")
        tm.register_worker("Worker_1", "Master_A")

        self.assertTrue(tm.assign_task(task.task_id, "Worker_1"))
        self.assertEqual(tm.worker_tasks["Worker_1"], task.task_id)

        self.assertTrue(tm.complete_task(task.task_id, "OK"))
        self.assertIsNone(tm.worker_tasks["Worker_1"])
        worker = tm.get_worker("Worker_1")
        self.assertIsNone(worker.current_task_id)


if __name__ == "__main__":
    unittest.main()
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python tests/test_task_manager.py`
Expected: FAIL due to missing `create_task` signature or `current_task_id` behavior.

- [ ] **Step 3: Write minimal implementation**

```python
# common/models.py (Task changes)
@dataclass
class Task:
    task_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    user: str = ""
    status: TaskStatus = TaskStatus.PENDING
    assigned_worker: Optional[str] = None
    assigned_timestamp: Optional[float] = None
    start_time: Optional[float] = None
    end_time: Optional[float] = None
    result: Optional[Any] = None
    error_message: Optional[str] = None
    created_at: float = field(default_factory=time.time)
    updated_at: float = field(default_factory=time.time)
    retries: int = 0
    max_retries: int = 3
```

```python
# common/models.py (Worker additions)
@dataclass
class Worker:
    worker_uuid: str
    server_uuid: str
    host: Optional[str] = None
    client_port: Optional[int] = None
    free_disk_bytes: Optional[int] = None
    status: WorkerStatus = WorkerStatus.OFFLINE
    last_heartbeat: float = field(default_factory=time.time)
    connection_failures: int = 0
    current_task_id: Optional[str] = None
    completed_tasks: int = 0
    failed_tasks: int = 0
    original_master_address: Optional[str] = None
    is_temporary: bool = False
    created_at: float = field(default_factory=time.time)
    updated_at: float = field(default_factory=time.time)
```

```python
# common/task_manager.py (create_task and assign/complete)

def create_task(self, user: str) -> Task:
    with self.lock:
        task = Task(user=user)
        self.tasks[task.task_id] = task
        self.pending_queue.put(task.task_id)
        self._log_event(task.task_id, "created", details={"user": user})
        if self.persistence_server_uuid:
            self.save_state(self.persistence_server_uuid)
        return task


def assign_task(self, task_id: str, worker_uuid: str) -> bool:
    with self.lock:
        task = self.tasks.get(task_id)
        if not task or task.status not in (TaskStatus.PENDING, TaskStatus.REASSIGNED):
            return False
        task.mark_in_progress(worker_uuid)
        self.worker_tasks[worker_uuid] = task_id
        worker = self.workers.get(worker_uuid)
        if worker:
            worker.current_task_id = task_id
        self._log_event(task_id, "assigned", worker_uuid=worker_uuid, details={"worker": worker_uuid})
        if self.persistence_server_uuid:
            self.save_state(self.persistence_server_uuid)
        return True


def complete_task(self, task_id: str, result: Any) -> bool:
    with self.lock:
        task = self.tasks.get(task_id)
        if not task:
            return False
        task.mark_completed(result)
        if task.assigned_worker:
            self.worker_tasks[task.assigned_worker] = None
            worker = self.workers.get(task.assigned_worker)
            if worker:
                worker.completed_tasks += 1
                worker.current_task_id = None
        self._log_event(task_id, "completed", worker_uuid=task.assigned_worker, details={"result": str(result)[:100]})
        if self.persistence_server_uuid:
            self.save_state(self.persistence_server_uuid)
        return True
```

```python
# common/tasks.py (simulate work)
import time
from typing import Any, Dict


def execute_task(task: Dict[str, Any]) -> Any:
    time.sleep(1)
    return "OK"
```

Also update `load_state` / `load_state_dict` to map legacy `operation` to `user` if present.

- [ ] **Step 4: Run test to verify it passes**

Run: `python tests/test_task_manager.py`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add common/models.py common/task_manager.py common/tasks.py tests/test_task_manager.py
git commit -m "refactor: align task model with PDF user payload"
```

---

### Task 3: Update Worker protocol for Sprint 01/02 and redirect commands

**Files:**
- Modify: worker.py

- [ ] **Step 1: Write failing test**

Update the integration test later (Task 7). For now, create a unit test that verifies parsing of command_redirect and command_release.

```python
# tests/test_worker_redirect_parse.py
import unittest
from worker import WorkerClient


class WorkerRedirectTests(unittest.TestCase):
    def test_parse_command_redirect(self):
        worker = WorkerClient(worker_uuid="W-1", server_uuid="Master_A")
        msg = {
            "type": "command_redirect",
            "request_id": "uuid",
            "payload": {"new_master_address": "127.0.0.1:5100"}
        }
        redirect = worker._parse_redirect(msg)
        self.assertEqual(redirect["HOST"], "127.0.0.1")
        self.assertEqual(redirect["PORT"], 5100)

    def test_parse_command_release(self):
        worker = WorkerClient(worker_uuid="W-1", server_uuid="Master_A")
        msg = {
            "type": "command_release",
            "request_id": "uuid",
            "payload": {"original_master_address": "127.0.0.1:5000"}
        }
        redirect = worker._parse_release(msg)
        self.assertEqual(redirect["HOST"], "127.0.0.1")
        self.assertEqual(redirect["PORT"], 5000)


if __name__ == "__main__":
    unittest.main()
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python tests/test_worker_redirect_parse.py`
Expected: FAIL due to missing helpers.

- [ ] **Step 3: Implement worker protocol changes**

```python
# worker.py (helpers)
from common.protocol import (
    build_worker_alive,
    build_task_query,
    build_status_ok,
    build_status_nok,
)


def _parse_address(address: str) -> tuple[str, int]:
    host, port_str = address.split(":")
    return host, int(port_str)
```

```python
# worker.py (new helpers)

def _parse_redirect(self, msg: dict) -> dict:
    payload = msg.get("payload") or {}
    host, port = _parse_address(payload.get("new_master_address", "127.0.0.1:5000"))
    return {"HOST": host, "PORT": port}


def _parse_release(self, msg: dict) -> dict:
    payload = msg.get("payload") or {}
    host, port = _parse_address(payload.get("original_master_address", "127.0.0.1:5000"))
    return {"HOST": host, "PORT": port}
```

```python
# worker.py (send_alive/request_task)
message = build_worker_alive(self.worker_uuid, self.server_uuid if self.is_temporary else None)
send_json(sock, message)
```

```python
# worker.py (execute_and_report)
message = build_status_ok(self.worker_uuid)
send_json(sock, message)
```

```python
# worker.py (handle responses)
if response.get("type") == "command_redirect":
    redirect = self._parse_redirect(response)
    return {"REDIRECT": redirect}
if response.get("type") == "command_release":
    redirect = self._parse_release(response)
    return {"REDIRECT": redirect}
```

```python
# worker.py (register temporary worker after redirect)
register = {
    "type": "register_temporary_worker",
    "request_id": str(uuid.uuid4()),
    "payload": {
        "worker_id": self.worker_uuid,
        "original_master_address": f"{self.original_master_host}:{self.original_master_port}",
    },
}
send_json(sock, register)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `python tests/test_worker_redirect_parse.py`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add worker.py tests/test_worker_redirect_parse.py
git commit -m "feat: align worker payloads to PDF"
```

---

### Task 4: Update Master protocol for Sprint 01/02

**Files:**
- Modify: master.py

- [ ] **Step 1: Write failing test**

Add a unit test to validate status handling without TASK_ID:

```python
# tests/test_master_status.py
import unittest
from master import MasterServer


class MasterStatusTests(unittest.TestCase):
    def test_status_without_task_id(self):
        master = MasterServer("Master_A")
        master.task_manager.create_task("User_1")
        master.task_manager.register_worker("Worker_1", "Master_A")
        task_id = master.task_manager.get_pending_task()
        master.task_manager.assign_task(task_id, "Worker_1")

        data = {"STATUS": "OK", "TASK": "QUERY", "WORKER_UUID": "Worker_1"}
        resp = master.handle_task_result(data, ("127.0.0.1", 1234))
        self.assertEqual(resp["STATUS"], "ACK")
        self.assertEqual(resp["WORKER_UUID"], "Worker_1")


if __name__ == "__main__":
    unittest.main()
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python tests/test_master_status.py`
Expected: FAIL because handler still expects TASK_ID.

- [ ] **Step 3: Implement master changes**

```python
# master.py (heartbeat)
if data.get("TASK") == "HEARTBEAT":
    return {"SERVER_UUID": self.server_uuid, "TASK": "HEARTBEAT", "RESPONSE": "ALIVE"}
```

```python
# master.py (handle_task_result)
worker_uuid = data.get("WORKER_UUID")
status = data.get("STATUS")
missing = require_fields(data, ["STATUS", "TASK", "WORKER_UUID"])
if missing:
    logger.warning(f"STATUS missing field: {missing}")
    return None

if data.get("TASK") != "QUERY":
    logger.warning("STATUS message without TASK=QUERY")
    return None

task_id = self.task_manager.worker_tasks.get(worker_uuid)
if not task_id:
    logger.warning(f"No task assigned to {worker_uuid}")
    return None

if status == "OK":
    self.task_manager.complete_task(task_id, "OK")
else:
    self.task_manager.fail_task(task_id, "NOK")

return {"STATUS": "ACK", "WORKER_UUID": worker_uuid}
```

Also remove `WORKERS` from all responses and remove `TASK_ID/OPERATION/VALUES` fields in task delivery.

- [ ] **Step 4: Run test to verify it passes**

Run: `python tests/test_master_status.py`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add master.py tests/test_master_status.py
git commit -m "refactor: align master sprint 01/02 protocol"
```

---

### Task 5: Implement master-to-master negotiation and borrowed worker lifecycle

**Files:**
- Modify: master.py
- Modify: common/models.py (borrowed fields already added in Task 2)
- Modify: tests/run_redirect_integration.py

- [ ] **Step 1: Write the failing test**

Update redirect integration to follow the PDF flow: Master A is saturated, Master B redirects its worker to Master A.

```python
# tests/run_redirect_integration.py (key changes)
# Start Master B with one worker
# Start Master A with SATURATION_THRESHOLD low so it requests help
# Expect worker to log command_redirect and then connect to Master A
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python tests/run_redirect_integration.py`
Expected: FAIL because master-to-master protocol is not PDF-compliant and no command_redirect.

- [ ] **Step 3: Implement master-to-master flow**

```python
# master.py (request_help sender)
request = build_master_message(
    "request_help",
    str(uuid.uuid4()),
    {
        "master_id": self.server_uuid,
        "current_load": current_load,
        "capacity": CAPACITY,
        "workers_needed": workers_needed,
    },
)
```

```python
# master.py (handle request_help)
if msg_type == "request_help":
    missing = require_fields(data, ["type", "request_id", "payload"])
    if missing:
        logger.warning("request_help missing fields")
        return None
    payload = data.get("payload") or {}
    workers_needed = int(payload.get("workers_needed", 0))
    idle_workers = self.task_manager.get_idle_workers()
    offered = idle_workers[:workers_needed]

    response_type = "response_accepted" if offered else "response_rejected"
    response_payload = (
        {
            "workers_offered": len(offered),
            "worker_details": [
                {"id": w.worker_uuid, "address": f"{w.host}:{w.client_port}"}
                for w in offered
            ],
        }
        if offered
        else {"reason": "no_workers_available"}
    )

    response = build_master_message(response_type, data["request_id"], response_payload)
    if offered:
        self._send_command_redirect(offered, from_master_id=payload.get("master_id"))
    return response
```

```python
# master.py (command_redirect to local workers)

def _send_command_redirect(self, workers, from_master_id: str) -> None:
    target = self.peer_directory.get(from_master_id)
    if not target:
        return
    target_host, target_port = target
    for worker in workers:
        conn = self.worker_connections.get(worker.worker_uuid)
        if not conn:
            continue
        msg = {
            "type": "command_redirect",
            "request_id": str(uuid.uuid4()),
            "payload": {"new_master_address": f"{target_host}:{target_port}"},
        }
        send_json(conn, msg)
```

```python
# master.py (register temporary worker)
if data.get("type") == "register_temporary_worker":
    payload = data.get("payload") or {}
    worker_id = payload.get("worker_id")
    original_master_address = payload.get("original_master_address")
    self.task_manager.register_temporary_worker(worker_id, original_master_address, addr)
    return None
```

```python
# master.py (release borrowed workers when load normalizes)
if current_load < RELEASE_THRESHOLD and self.borrowed_workers:
    for worker_id, origin_addr in list(self.borrowed_workers.items()):
        self._send_command_release(worker_id, origin_addr)
        self._notify_worker_returned(origin_addr, worker_id)
        del self.borrowed_workers[worker_id]
```

- [ ] **Step 4: Run test to verify it passes**

Run: `python tests/run_redirect_integration.py`
Expected: PASS with log showing command_redirect and worker reconnect to Master A.

- [ ] **Step 5: Commit**

```bash
git add master.py tests/run_redirect_integration.py
git commit -m "feat: implement master-to-master negotiation flow"
```

---

### Task 6: Add UDP consensus election for workers

**Files:**
- Modify: worker.py
- Modify: tests/run_election_test.py
- Create: tests/test_election_logic.py

- [ ] **Step 1: Write failing test**

```python
# tests/test_election_logic.py
import unittest
from worker import WorkerClient


class ElectionLogicTests(unittest.TestCase):
    def test_winner_selection(self):
        worker = WorkerClient(worker_uuid="W-1", server_uuid="Master_A")
        votes = [
            {"WORKER_UUID": "Worker_A", "FREE_DISK_BYTES": 100, "HOST": "127.0.0.1", "PORT": 5000},
            {"WORKER_UUID": "Worker_B", "FREE_DISK_BYTES": 250, "HOST": "127.0.0.1", "PORT": 5000},
        ]
        winner = worker._choose_winner_from_votes(votes)
        self.assertEqual(winner["WORKER_UUID"], "Worker_B")


if __name__ == "__main__":
    unittest.main()
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python tests/test_election_logic.py`
Expected: FAIL due to missing helper.

- [ ] **Step 3: Implement election listener and helpers**

```python
# worker.py (init)
self.election_port = int(os.getenv("ELECTION_PORT", "5200"))
self.election_term = 0
self.election_votes = {}
self.election_in_progress = False
self.election_lock = threading.Lock()
```

```python
# worker.py (winner selection)

def _choose_winner_from_votes(self, votes: list[dict]) -> dict:
    votes_sorted = sorted(
        votes,
        key=lambda v: (-int(v.get("FREE_DISK_BYTES", 0)), v.get("WORKER_UUID", "")),
    )
    return votes_sorted[0] if votes_sorted else {}
```

```python
# worker.py (broadcast and listener)

def _broadcast_election(self, payload: dict) -> None:
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
    sock.sendto((json.dumps(payload) + "\n").encode("utf-8"), ("<broadcast>", self.election_port))
    sock.close()


def _listen_election(self) -> None:
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind(("", self.election_port))
    while self.running:
        data, addr = sock.recvfrom(65535)
        msg = json.loads(data.decode("utf-8").strip())
        self._handle_election_message(msg, addr)
```

```python
# worker.py (trigger election)

def _trigger_election(self) -> None:
    term = int(time.time() * 1000)
    with self.election_lock:
        self.election_term = term
        self.election_in_progress = True
        self.election_votes = {}
        self.election_votes[self.worker_uuid] = {
            "WORKER_UUID": self.worker_uuid,
            "FREE_DISK_BYTES": self.get_free_disk_bytes(),
            "HOST": self.master_host,
            "PORT": self.master_port,
        }
    self._broadcast_election({"election": "start", "term": term, "from": self.worker_uuid})
    threading.Thread(target=self._finalize_election, args=(term,), daemon=True).start()
```

```python
# worker.py (finalize election)

def _finalize_election(self, term: int) -> None:
    time.sleep(1.5)
    with self.election_lock:
        if term != self.election_term:
            return
        votes = list(self.election_votes.values())
    winner = self._choose_winner_from_votes(votes)
    if winner:
        self._broadcast_election({
            "election": "result",
            "term": term,
            "winner": winner["WORKER_UUID"],
            "host": winner["HOST"],
            "port": winner["PORT"],
            "free_disk": winner["FREE_DISK_BYTES"],
        })
```

- [ ] **Step 4: Run test to verify it passes**

Run: `python tests/test_election_logic.py`
Expected: PASS

- [ ] **Step 5: Update run_election_test.py**

Keep the existing test but call `_choose_winner_from_votes` to validate the same rule. Ensure it prints the same success line.

- [ ] **Step 6: Commit**

```bash
git add worker.py tests/test_election_logic.py tests/run_election_test.py
git commit -m "feat: add udp consensus election"
```

---

### Task 7: Update integration tests for new protocol

**Files:**
- Modify: tests/run_promotion_test.py
- Modify: tests/run_redirect_integration.py

- [ ] **Step 1: Update promotion test**

Ensure the test looks for an election result log and only one promotion. For example, accept logs containing "election result" or "winner" in addition to existing "Promoted".

- [ ] **Step 2: Update redirect integration**

Adjust the script to follow the PDF flow: Master A is saturated (CAPACITY low, tasks high), Master B redirects its worker to Master A. Confirm the worker logs the redirect and reconnects to Master A.

- [ ] **Step 3: Run integration tests**

Run:
- `python tests/run_promotion_test.py`
- `python tests/run_redirect_integration.py`
Expected: PASS

- [ ] **Step 4: Commit**

```bash
git add tests/run_promotion_test.py tests/run_redirect_integration.py
git commit -m "test: update integration tests for PDF protocol"
```

---

## Plan checklist
- Protocol payloads exactly match PDF
- No extra fields on the wire
- Election consensus uses UDP broadcast and yields a single winner
- Borrowed worker lifecycle matches request_help -> command_redirect -> register_temporary_worker -> command_release -> notify_worker_returned
