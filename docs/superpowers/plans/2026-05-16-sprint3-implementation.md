# Sprint 3 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add farm-to-farm negotiation, temporary worker borrowing, and automatic worker-to-master election so one farm can borrow workers from another and survive a master failure.

**Architecture:** Keep the current Master-Worker protocol intact and layer Sprint 3 on top of it. Shared farm policy code will live in small helper functions so the routing, load calculation, and election winner selection can be tested without sockets. `master.py` will own saturation detection, help requests, worker redirection, and borrowed-worker release; `worker.py` will own redirect handling and master promotion; `common/models.py` and `common/task_manager.py` will carry the extra farm and worker state.

**Tech Stack:** Python 3 standard library only, `socket`, `threading`, `queue`, `dataclasses`, `enum`, `unittest`.

---

### Task 1: Add farm state and shared cluster policies

**Files:**
- Modify: `common/models.py`
- Modify: `common/task_manager.py`
- Create: `common/cluster_policies.py`
- Create: `tests/test_cluster_policies.py`

- [ ] **Step 1: Write the failing test**

```python
import unittest

from common.cluster_policies import calculate_workers_needed, choose_election_winner, should_request_help
from common.models import Worker, WorkerStatus
from common.task_manager import TaskManager


class TestClusterPolicies(unittest.TestCase):
    def test_help_request_threshold_and_proportional_workers(self):
        self.assertFalse(should_request_help(current_load=99, capacity=100))
        self.assertTrue(should_request_help(current_load=100, capacity=100))
        self.assertEqual(calculate_workers_needed(current_load=125, capacity=100), 1)
        self.assertEqual(calculate_workers_needed(current_load=151, capacity=100), 3)

    def test_election_winner_is_deterministic(self):
        self.assertEqual(
            choose_election_winner(["Worker_3", "Worker_1", "Worker_2"]),
            "Worker_1"
        )

    def test_available_workers_use_current_task_state(self):
        manager = TaskManager()
        manager.register_worker("Worker_1", "Farm_A")
        manager.register_worker("Worker_2", "Farm_A")
        task = manager.create_task("soma", [1, 2])
        manager.assign_task(task.task_id, "Worker_1")

        available = [worker.worker_uuid for worker in manager.get_available_workers()]
        self.assertEqual(available, ["Worker_2"])

        worker = manager.get_worker("Worker_1")
        self.assertIsNotNone(worker)
        self.assertEqual(worker.status, WorkerStatus.ONLINE)
        self.assertEqual(worker.current_task_id, task.task_id)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m unittest tests.test_cluster_policies -v`
Expected: fail because `common.cluster_policies` does not exist yet and `Worker.can_accept_task()` / `get_available_workers()` still need farm-aware state.

- [ ] **Step 3: Write minimal implementation**

Implement these exact behaviors:
- `common/models.py`
  - add `FarmPeer` with `farm_uuid`, `host`, `port`, `priority`, `status`, `borrowed_workers`
  - add `home_farm_uuid`, `current_farm_uuid`, `is_borrowed` to `Worker`
  - add `Worker.can_accept_task()` returning `True` only when the worker is online and `current_task_id is None`
- `common/task_manager.py`
  - fix `get_available_workers()` to use `worker.can_accept_task()` instead of `worker.worker_tasks`
  - add `get_system_load()` returning `pending + in_progress`
  - add `mark_worker_borrowed(worker_uuid, target_farm_uuid)` and `mark_worker_released(worker_uuid)`
- `common/cluster_policies.py`
  - add `should_request_help(current_load, capacity)`
  - add `calculate_workers_needed(current_load, capacity, worker_share_unit=25)` using `math.ceil((current_load - capacity) / worker_share_unit)`
  - add `choose_election_winner(worker_uuids)` returning the deterministic winner by sorted worker UUID

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m unittest tests.test_cluster_policies -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add common/models.py common/task_manager.py common/cluster_policies.py tests/test_cluster_policies.py
git commit -m "feat: add shared farm policies"
```

---

### Task 2: Implement farm-to-farm negotiation in the master

**Files:**
- Modify: `master.py`
- Create: `tests/test_master_negotiation.py`

- [ ] **Step 1: Write the failing test**

```python
import unittest
from unittest.mock import patch

from master import MasterServer


class TestMasterNegotiation(unittest.TestCase):
    @patch("master.send_json")
    @patch("master.recv_json_line")
    def test_request_help_accepts_worker_count(self, recv_json_line, send_json):
        master = MasterServer(server_uuid="Farm_A")
        peer = {"farm_uuid": "Farm_B", "host": "127.0.0.1", "port": 5001, "priority": 2, "status": "online"}

        recv_json_line.side_effect = [
            {"MASTER": "RESPONSE_ACCEPTED", "WORKERS_AVAILABLE": 2, "WORKERS": ["Worker_7", "Worker_8"]}
        ]

        response = master.request_help_from_peer(peer, required_workers=2, current_load=130)
        self.assertEqual(response["status"], "accepted")
        self.assertEqual(response["workers"], ["Worker_7", "Worker_8"])
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m unittest tests.test_master_negotiation -v`
Expected: fail because `MasterServer.request_help_from_peer()` does not exist yet.

- [ ] **Step 3: Write minimal implementation**

Implement these exact behaviors in `master.py`:
- add a hardcoded peer farm registry at the top of the file
- add `request_help_from_peer(peer_farm, required_workers, current_load)`
- add `handle_peer_message(data, conn)` for incoming `MASTER` messages
- add `borrow_workers_from_peer(peer_farm, workers)` and `release_borrowed_workers()`
- add `maybe_request_help()` in the monitor loop using:

```python
current_load = self.task_manager.get_system_load()
if current_load >= CAPACITY:
    required_workers = calculate_workers_needed(current_load=current_load, capacity=CAPACITY)
    peer = self.select_next_peer_farm()
    self.request_help_from_peer(peer, required_workers, current_load)
```

The negotiation payloads must be explicit and stable:

```python
{"MASTER": "REQUEST_HELP", "FROM_FARM": "Farm_A", "CURRENT_LOAD": 130, "WORKERS_NEEDED": 2}
{"MASTER": "RESPONSE_ACCEPTED", "FROM_FARM": "Farm_B", "WORKERS_AVAILABLE": 2, "WORKERS": ["Worker_7", "Worker_8"]}
{"MASTER": "RESPONSE_REJECTED", "FROM_FARM": "Farm_B", "REASON": "busy"}
```

Borrowed workers must be tracked in the task manager so release can reverse the assignment cleanly.

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m unittest tests.test_master_negotiation -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add master.py tests/test_master_negotiation.py
git commit -m "feat: add farm-to-farm negotiation"
```

---

### Task 3: Implement worker redirection, release, and master promotion

**Files:**
- Modify: `worker.py`
- Create: `tests/test_worker_failover.py`

- [ ] **Step 1: Write the failing test**

```python
import unittest

from common.cluster_policies import choose_election_winner
from worker import WorkerClient


class TestWorkerFailover(unittest.TestCase):
    def test_redirect_and_release_flags(self):
        worker = WorkerClient(worker_uuid="Worker_1", server_uuid="Farm_A")
        worker.handle_redirect({"TASK": "REDIRECT", "TARGET_FARM": "Farm_B", "TARGET_HOST": "127.0.0.1", "TARGET_PORT": 5001})
        self.assertEqual(worker.current_master_uuid, "Farm_B")
        self.assertTrue(worker.is_borrowed)

        worker.handle_release({"TASK": "RELEASE", "RETURN_FARM": "Farm_A"})
        self.assertEqual(worker.current_master_uuid, "Farm_A")
        self.assertFalse(worker.is_borrowed)

    def test_election_winner_is_the_lowest_worker_uuid(self):
        self.assertEqual(choose_election_winner(["Worker_3", "Worker_2", "Worker_1"]), "Worker_1")
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m unittest tests.test_worker_failover -v`
Expected: fail because `handle_redirect()`, `handle_release()`, and the promotion path do not exist yet.

- [ ] **Step 3: Write minimal implementation**

Implement these exact behaviors in `worker.py`:
- add `current_master_uuid`, `current_master_host`, `current_master_port`, and `is_borrowed` to `WorkerClient`
- add `handle_redirect(message)` to switch the worker to the target farm
- add `handle_release(message)` to return the worker to its original farm
- add `should_promote(candidates)` and `promote_to_master()`
- when the master disappears, use the deterministic election winner from `choose_election_winner()` and start a `MasterServer` in the same process
- keep the existing ALIVE / HEARTBEAT / QUERY / ACK flow unchanged for normal operation

Use these message shapes:

```python
{"TASK": "REDIRECT", "TARGET_FARM": "Farm_B", "TARGET_HOST": "10.0.0.22", "TARGET_PORT": 5000}
{"TASK": "RELEASE", "RETURN_FARM": "Farm_A"}
{"MASTER": "BECOME_MASTER", "WINNER": "Worker_1"}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m unittest tests.test_worker_failover -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add worker.py tests/test_worker_failover.py
git commit -m "feat: add worker redirect and promotion"
```

---

### Task 4: Lab smoke test and documentation updates

**Files:**
- Modify: `docs/TESTE_GUIDE.md`
- Modify: `docs/VERIFICACAO_FINAL.md`
- Create: `tests/sprint3_lab_smoke.py`

- [ ] **Step 1: Write the smoke test script**

```python
"""
Smoke test for Sprint 3.
Run manually in the lab after starting the farms.
"""

import time


def main() -> None:
    print("Start Farm A master")
    print("Start Farm B master")
    print("Start workers in both farms")
    print("Wait for saturation and confirm a REQUEST_HELP event")
    print("Disconnect Farm A master from the network")
    print("Confirm a worker promotion event and continued task processing")
    time.sleep(1)


if __name__ == "__main__":
    main()
```

- [ ] **Step 2: Run test to verify it exists and is executable**

Run: `python tests/sprint3_lab_smoke.py`
Expected: prints the lab checklist without errors.

- [ ] **Step 3: Write documentation updates**

Add these sections:
- in `docs/TESTE_GUIDE.md`, add a Sprint 3 scenario describing two farms, a help request, worker borrowing, and master failover
- in `docs/VERIFICACAO_FINAL.md`, add a Sprint 3 verification summary with the exact log events observed in the lab

Use these exact example commands in the guide:

```bash
# Farm A
python master.py
python worker.py Worker_1 Farm_A
python worker.py Worker_2 Farm_A
python worker.py Worker_3 Farm_A

# Farm B
python master.py
python worker.py Worker_4 Farm_B
python worker.py Worker_5 Farm_B
```

- [ ] **Step 4: Run the manual lab validation**

Run:
- start Farm A and Farm B on different machines or ports
- saturate Farm A until a `REQUEST_HELP` message appears
- verify one or more workers are redirected from Farm B to Farm A
- disconnect Farm A master from the network
- verify one worker becomes the new master and the farm keeps processing tasks

Expected log evidence:
- `MASTER: REQUEST_HELP`
- `MASTER: RESPONSE_ACCEPTED`
- `TASK: REDIRECT`
- `TASK: RELEASE`
- `MASTER: BECOME_MASTER`

- [ ] **Step 5: Commit**

```bash
git add docs/TESTE_GUIDE.md docs/VERIFICACAO_FINAL.md tests/sprint3_lab_smoke.py
git commit -m "docs: add sprint 3 lab validation"
```

---

## Plan Coverage Check

- Farm-to-farm negotiation: Task 2
- Borrowed worker redirection and release: Task 3
- Automatic master election after failure: Task 3
- Lab validation across multiple farms: Task 4
- Shared farm metadata and policies: Task 1
- Regression safety for existing ALIVE / HEARTBEAT / QUERY / ACK flow: Tasks 1 through 3

## Notes for Execution

- Keep the existing protocol unchanged for the current master-worker flow.
- Keep peer farms hardcoded per farm implementation, matching the lab setup.
- Prefer small helper functions in `common/cluster_policies.py` so the negotiation and election rules stay testable.
- Do not widen scope into full consensus; the lab only needs deterministic election and explicit worker borrowing.
