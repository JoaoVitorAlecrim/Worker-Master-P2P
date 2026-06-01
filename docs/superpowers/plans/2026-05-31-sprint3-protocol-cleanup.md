# Sprint 3 Protocol Cleanup Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the remaining master-to-master legacy messages with the PDF Sprint 3 envelope and make worker redirection/release follow the documented `command_redirect` and `command_release` flow.

**Architecture:** Keep Sprint 2 worker/task behavior intact. Move only the Sprint 3 negotiation path to the PDF envelope: `type`, `request_id`, and `payload`. `master.py` will parse and emit `request_help`, `response_accepted`, `response_rejected`, and `notify_worker_returned`. `worker.py` will accept `command_redirect` and `command_release`, switch masters cleanly, and preserve the current task loop for normal work.

**Tech Stack:** Python 3.13, sockets, threading, unittest, JSON over TCP.

---

### Task 1: Add strict Sprint 3 protocol tests

**Files:**
- Create: `tests/test_sprint3_protocol.py`

- [ ] **Step 1: Write the failing test**

```python
import unittest

from master import MasterServer
from worker import WorkerClient


class TestSprint3Protocol(unittest.TestCase):
    def test_master_request_help_uses_pdf_envelope(self):
        master = MasterServer(server_uuid="Master_A")
        response = master.handle_master_request(
            {
                "type": "request_help",
                "request_id": "RID-1",
                "payload": {
                    "master_id": "A",
                    "current_load": 150,
                    "capacity": 100,
                    "workers_needed": 2,
                },
            },
            ("127.0.0.1", 5001),
        )

        self.assertEqual(response["type"], "response_accepted")
        self.assertEqual(response["request_id"], "RID-1")
        self.assertIn("payload", response)

    def test_worker_handles_command_redirect(self):
        worker = WorkerClient(worker_uuid="Worker_1", server_uuid="Master_A")
        worker.handle_redirect(
            {
                "type": "command_redirect",
                "request_id": "RID-2",
                "payload": {"new_master_address": "127.0.0.1:5100"},
            }
        )

        self.assertEqual(worker.master_host, "127.0.0.1")
        self.assertEqual(worker.master_port, 5100)
``` 

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m unittest tests.test_sprint3_protocol -v`
Expected: FAIL because the master still emits legacy `MASTER` messages and the worker lacks `handle_redirect()`.

- [ ] **Step 3: Write minimal implementation**

Implement the master request/response path with `build_master_envelope_spec()` / `parse_master_envelope_spec()` and add worker `handle_redirect()` plus `command_release` handling.

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m unittest tests.test_sprint3_protocol -v`
Expected: PASS.

---

### Task 2: Update the live socket path

**Files:**
- Modify: `master.py`
- Modify: `worker.py`

- [ ] **Step 1: Implement the new envelope on the live master-to-master path**
- [ ] **Step 2: Update the worker to reconnect and register on `command_redirect`**
- [ ] **Step 3: Preserve Sprint 2 `ALIVE`/`QUERY`/`STATUS`/`ACK` behavior**
- [ ] **Step 4: Run the full suite**

Run: `python -m unittest discover -v tests`
Expected: all tests pass.