# Sprint 3 Protocol Implementation Guide

## Overview

Sprint 3 implements **Farm-to-Farm Negotiation**, **Dynamic Worker Redirection**, and **Automatic Worker Promotion**.

## Messages

### Master-to-Master: REQUEST_HELP

Sent when a master is saturated and needs help from a peer farm.

```json
{
  "type": "request_help",
  "request_id": "uuid-xxx",
  "payload": {
    "master_id": "Master_A",
    "workers_needed": 1,
    "current_load": { "tasks": {...}, "workers": {...} }
  }
}
```

**Response:** `response_accepted` or `response_rejected`

```json
{
  "type": "response_accepted",
  "request_id": "uuid-xxx",
  "payload": {
    "workers_offered": 2,
    "worker_details": [ { "id": "B1", "address": "ip:port" } ]
  }
}
```

**Usage:** When a master has no pending tasks, queries peer masters and sends REDIRECT to worker if a peer accepts help.

---

### Master-to-Worker: REDIRECT

Instructs a worker to reconnect to a different master (load balancing / help scenario).

```json
{
  "TASK": "REDIRECT",
  "TARGET_HOST": "127.0.0.1",
  "TARGET_PORT": 5101,
  "TARGET_SERVER_UUID": "Master_B"
}
```

**Worker Behavior:**
- Updates `self.master_host`, `self.master_port`, `self.server_uuid`
- Breaks current connection and reconnects to new target
- Resumes task solicitation loop with new master

---

### Master-to-Master: REQUEST_STATE

Requests persisted state of a specific server_uuid (used during worker promotion).

```json
{
  "type": "request_state",
  "request_id": "uuid-yyy",
  "payload": { "target_server": "Master_A", "from_worker": "TestWorker_1" }
}
```

**Response:** `response_state`

```json
{
  "type": "response_state",
  "request_id": "uuid-yyy",
  "payload": {
    "found": true,
    "target_server": "Master_A",
    "state": {
      "tasks": { "task-id": {...}, ... },
      "workers": { "worker-uuid": {...}, ... },
      "logs": [...]
    }
  }
}
```

If not found:
```json
{
  "MASTER": "RESPONSE_STATE",
  "FOUND": false
}
```

**Usage:** When a worker promotes itself to master, it queries peer masters for saved state of its original `server_uuid` and loads it into the new master.

---

## Environment Variables

### Master Configuration

```bash
MASTER_HOST="0.0.0.0"        # Binding address
MASTER_PORT="5000"            # Listening port
SERVER_UUID="Master_A"        # Unique ID
INITIAL_TASKS="60"            # Tasks to load on startup (if no saved state)
MASTER_PEERS="127.0.0.1:5101:Master_B,127.0.0.1:5102:Master_C"  # Comma-separated peers
MASTER_AUTH_TOKEN="secret123" # Optional auth token for master-to-master
```

### Worker Configuration

```bash
MASTER_HOST="127.0.0.1"       # Initial master host
MASTER_PORT="5000"             # Initial master port
AUTH_TOKEN="secret123"         # Optional auth token (matches MASTER_AUTH_TOKEN)
PROMOTE_THRESHOLD="5"          # Failed attempts before promoting to master
RECONNECT_DELAY="3"            # Seconds between reconnection attempts
HEARTBEAT_INTERVAL="5"         # Seconds between heartbeat checks
SOCKET_TIMEOUT="15"            # Socket timeout
```

---

## State Persistence

All masters automatically persist state to `data/tasks_{SERVER_UUID}.json`:

```
data/
  tasks_Master_A.json      # Tasks, workers, logs for Master_A
  tasks_Master_B.json      # Tasks, workers, logs for Master_B
  tasks_Promoted_Worker_1.json  # State of promoted worker-turned-master
```

**Save triggers:**
- Task created / assigned / completed / failed / reassigned
- Worker registered / heartbeat updated

**Load triggers:**
- On master startup (if file exists and no tasks are in memory)
- On worker promotion (queries peers via REQUEST_STATE, then loads state_dict)

---

## Promotion Flow

When a worker fails to connect `PROMOTE_THRESHOLD` times:

1. Worker starts `MasterServer` using its original `server_uuid` (e.g., "Master_A")
2. Master sets persistence and attempts to load saved state from disk
3. If no disk state, queries `MASTER_PEERS` for `REQUEST_STATE` of original `server_uuid`
4. If peer responds with state, loads it via `load_state_dict()`
5. Master now owns the task queue and can accept new workers

**Example:** Worker_1 (from Master_A) promotes itself after 5 failed reconnects:
- Starts as Master_A (same UUID)
- Loads any saved state from `data/tasks_Master_A.json` or from peers
- Continues task distribution from where original master left off

---

## Redirection Flow

When a master has no local tasks and a peer accepts help:

1. Master receives `WORKER: ALIVE` request with no pending tasks
2. Queries `MASTER_PEERS` via `REQUEST_HELP`
3. Peer responds with `ACCEPT: true`
4. Master sends `REDIRECT` to worker pointing to peer master
5. Worker breaks connection and reconnects to peer
6. Peer master now handles the worker's tasks

**Example:** Master_A saturated → sends REDIRECT to Worker_1 → Worker_1 reconnects to Master_B (5101) → Master_B assigns tasks.

---

## Testing Scenarios

### Scenario 1: Promotion on Master Failure

```bash
# Terminal 1: Start Master_A
MASTER_PORT=5000 SERVER_UUID=Master_A python master.py

# Terminal 2: Start Worker_1 pointing to Master_A
MASTER_PORT=5000 python worker.py Worker_1

# [Worker executes tasks from Master_A]

# Kill Master_A (Ctrl+C)
# [Worker fails to reconnect 5 times, then promotes to Master_A]
# [Worker now listens on default port and accepts connections]
```

### Scenario 2: Redirection on Saturation

```bash
# Terminal 1: Master_B with tasks (helper farm)
MASTER_PORT=5101 SERVER_UUID=Master_B python master.py

# Terminal 2: Master_A with no tasks (saturated - 0 initial tasks)
MASTER_PORT=5100 SERVER_UUID=Master_A INITIAL_TASKS=0 MASTER_PEERS=127.0.0.1:5101:Master_B python master.py

# Terminal 3: Worker_1 pointing to Master_A (saturated)
MASTER_PORT=5100 python worker.py Worker_1

# [Worker requests task from Master_A]
# [Master_A has no tasks, queries Master_B]
# [Master_B accepts, sends back ACCEPT]
# [Master_A sends REDIRECT to Worker_1]
# [Worker_1 reconnects to Master_B and gets tasks]
```

### Scenario 3: State Handoff During Promotion

```bash
# Terminal 1: Master_A with tasks (will fail)
MASTER_PORT=5000 SERVER_UUID=Master_A python master.py

# Terminal 2: Master_B (peer farm, helper)
MASTER_PORT=5101 SERVER_UUID=Master_B MASTER_PEERS=127.0.0.1:5000:Master_A python master.py

# Terminal 3: Worker_1 from Master_A
MASTER_PORT=5000 MASTER_PEERS=127.0.0.1:5101:Master_B python worker.py Worker_1

# [Worker executes several tasks on Master_A]
# [Master_A saves state to data/tasks_Master_A.json]

# Kill Master_A and Worker's master connection
# [Worker fails to reconnect, promotes to Master_A]
# [Promoted worker queries Master_B via REQUEST_STATE for Master_A state]
# [Master_B responds with saved state from data/tasks_Master_A.json]
# [Promoted Master_A loads state and continues task distribution]
```

---

## Architecture Diagram

```
Farm A (Master_A)          Farm B (Master_B)
┌─────────────────┐       ┌─────────────────┐
│   Master_A      │       │   Master_B      │
│  Port: 5000     │       │  Port: 5101     │
│  Tasks: 60      │──────>│  Tasks: 60      │
└────────┬────────┘       └─────────┬───────┘
         │ REQUEST_HELP             │
         │ (no tasks)               │
         │<──────────────────────────┤
         │ RESPONSE_HELP (ACCEPT)   │
         │
    ┌────┴──────────┐
    │  Worker_1     │
    │  (Master_A)   │
    │  Receives     │
    │  REDIRECT to  │
    │  Master_B:    │
    │  5101         │
    └────────┬──────┘
             │
             │ Reconnects
             ▼
         Master_B ────> Tasks continue
```

---

## Logs to Watch For

### Successful Redirection

```
[MASTER] INFO: Nenhuma tarefa para Worker_1
[MASTER] INFO: Master request from Master_B: requested=1, load=0, accept=True
[MASTER] INFO: Peer Master_B aceita redirecionamento (available=10)
[WORKER] INFO: ↪ Redirecionamento recebido: 127.0.0.1:5101 (server=Master_B)
[WORKER] INFO: ↪ Atualizando master alvo para 127.0.0.1:5101 and reconnecting
```

### Successful Promotion

```
[WORKER] WARNING: Master indisponível: [WinError 10061] ...
[WORKER] WARNING: Limite de tentativas excedido (5). Promovendo Worker_1 a Master...
[MASTER] INFO: 🚀 Master Server iniciado em 0.0.0.0:5000
[MASTER] INFO: UUID: Master_A
[MASTER] INFO: Estado recebido de peer 127.0.0.1:5101, carregando...
```

---

## Security Notes

- Optional `MASTER_AUTH_TOKEN` / `AUTH_TOKEN` for message authentication
- Auth checks on `WORKER: ALIVE`, `MASTER: REQUEST_HELP`, `MASTER: REQUEST_STATE`
- No encryption by default (lab environment; can add TLS if needed)

---

## Known Limitations

- Neighbor farms are hardcoded (no dynamic discovery)
- Saturation threshold and timeouts are constants
- No conflict resolution if original master returns after promotion
- State transfer is unencrypted (lab assumption)
- No distributed consensus (simple timeout-based failover)
