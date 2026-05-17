# Sprint 3 Testing Guide

## Overview

This guide covers the current P2P extensions on top of the original Master-Worker system:
- Worker auto-promotion after repeated master failures
- Master election among workers connected to the same master
- Failback to the original master when it returns
- State handoff / persistence loading during promotion
- Peer-master redirection for load sharing

## Quick Start Tests

### Test 1: Promotion (Worker Auto-Promotes to Master)

**Goal:** Verify that a worker promotes itself to master after failing to connect N times.

**Setup:**
```bash
# Terminal 1: Start worker pointing to non-existent master (port 5100)
cd Worker-Master-P2P
MASTER_PORT=5100 PROMOTE_THRESHOLD=2 RECONNECT_DELAY=1 python worker.py TestWorker_Promote
```

**Expected Output:**
```
[WORKER] INFO: Tentando conectar ao Master (127.0.0.1:5100)...
[WORKER] WARNING: Master indisponível: [WinError 10061] ... Reconectando em 1s...
[WORKER] INFO: Tentando conectar ao Master (127.0.0.1:5100)...
[WORKER] WARNING: Master indisponível: [WinError 10061] ... Reconectando em 1s...
[WORKER] WARNING: Limite de tentativas excedido (2). Promovendo TestWorker_Promote a Master...
[MASTER] INFO: 🚀 Master Server iniciado em 0.0.0.0:5100
[MASTER] INFO: UUID: Master_A (or worker's server_uuid)
[MASTER] INFO: Carregando 60 tarefas iniciais...
[MASTER] INFO: ✓ 60 tarefas carregadas
[MASTER] INFO: Worker monitor iniciado
```

**Verdict:** ✓ PASS if you see "Master Server iniciado" and task loading logs.

> Note: the current implementation keeps the promoted master alive in-process. The worker does not terminate immediately after promotion; instead it enters master mode and continues serving connections.

---

### Test 2: Redirection (Worker Redirected to Peer Master)

**Goal:** Verify that a master with no local tasks redirects a worker to a peer master.

**Setup:**
```bash
# Terminal 1: Start Master_B (helper farm with tasks)
cd Worker-Master-P2P
MASTER_PORT=5101 SERVER_UUID=Master_B python master.py

# Wait 1-2 seconds for Master_B to start

# Terminal 2: Start Master_A (saturated - 0 tasks, peer list configured)
MASTER_PORT=5100 SERVER_UUID=Master_A INITIAL_TASKS=0 MASTER_PEERS=127.0.0.1:5101:Master_B python master.py

# Wait 1-2 seconds for Master_A to start

# Terminal 3: Start Worker_1 pointing to Master_A (saturated)
MASTER_PORT=5100 python worker.py TestWorker_Redirect
```

**Expected Output:**

**Terminal 1 (Master_B):**
```
[MASTER] INFO: 🚀 Master Server iniciado em 0.0.0.0:5101
[MASTER] INFO: UUID: Master_B
[MASTER] INFO: ✓ 60 tarefas carregadas
[MASTER] INFO: Worker monitor iniciado
[MASTER] INFO: Conexão recebida de ('127.0.0.1', ...)
[MASTER] INFO: ✓ Worker TestWorker_Redirect apresentado (origin: Master_B)
[MASTER] INFO: → Tarefa ... atribuída a TestWorker_Redirect (soma)
[MASTER] INFO: ✓ Tarefa ... completada por TestWorker_Redirect: 3
```

**Terminal 2 (Master_A):**
```
[MASTER] INFO: 🚀 Master Server iniciado em 0.0.0.0:5100
[MASTER] INFO: UUID: Master_A
[MASTER] INFO: Worker monitor iniciado (no tasks loaded - INITIAL_TASKS=0)
[MASTER] INFO: Conexão recebida de ('127.0.0.1', ...)
[MASTER] INFO: ✓ Worker TestWorker_Redirect apresentado (origin: Master_A)
[MASTER] INFO: Nenhuma tarefa para TestWorker_Redirect
[MASTER] INFO: Master request from Master_B: requested=1, load=0, accept=True
[MASTER] INFO: Peer Master_B aceita redirecionamento (available=10)
[MASTER] WARNING: ⚠ Worker TestWorker_Redirect desconectado!
```

**Terminal 3 (Worker):**
```
[WORKER] INFO: Tentando conectar ao Master (127.0.0.1:5100)...
[WORKER] INFO: ✓ Conectado ao Master
[WORKER] INFO: ✓ Apresentação enviada (ALIVE)
[WORKER] INFO: ↪ Redirecionamento recebido: 127.0.0.1:5101 (server=Master_B)
[WORKER] INFO: ↪ Atualizando master alvo para 127.0.0.1:5101 and reconnecting
[WORKER] INFO: Tentando conectar ao Master (127.0.0.1:5101)...
[WORKER] INFO: ✓ Conectado ao Master
[WORKER] INFO: ✓ Apresentação enviada (ALIVE)
[WORKER] INFO: → Tarefa recebida: ... (soma)
[WORKER] INFO: Executando tarefa ...
[WORKER] INFO: ✓ Resultado enviado: 3
```

**Verdict:** ✓ PASS if worker gets REDIRECT, reconnects to Master_B (5101), and executes tasks there.

> Note: redirection is triggered when the active master has no local tasks and a peer master accepts the load.

---

### Test 3: State Handoff During Promotion

**Goal:** Verify that when a worker promotes itself, it can load persisted state from peers.

**Setup:**
```bash
# Terminal 1: Start Master_A (original master, will fail)
cd Worker-Master-P2P
MASTER_PORT=5000 SERVER_UUID=Master_A python master.py

# Wait for tasks to load

# Terminal 2: Start Master_B (peer farm, can provide state)
MASTER_PORT=5101 SERVER_UUID=Master_B MASTER_PEERS=127.0.0.1:5000:Master_A python master.py

# Terminal 3: Start Worker_1 from Master_A
MASTER_PORT=5000 MASTER_PEERS=127.0.0.1:5101:Master_B PROMOTE_THRESHOLD=2 RECONNECT_DELAY=1 python worker.py Worker_1

# [Worker will execute a few tasks and Master_A will save state]
# Wait 5-10 seconds for state to persist

# Terminal 1: Kill Master_A (Ctrl+C)
# [Master_A shuts down]

# [Worker tries to reconnect, fails PROMOTE_THRESHOLD times, then promotes itself]
```

**Expected Output:**

**Terminal 3 (Worker) - Initial Phase:**
```
[WORKER] INFO: Tentando conectar ao Master (127.0.0.1:5000)...
[WORKER] INFO: ✓ Conectado ao Master
[WORKER] INFO: ✓ Apresentação enviada (ALIVE)
[WORKER] INFO: → Tarefa recebida: ... (soma)
[WORKER] INFO: Executando tarefa ...
[WORKER] INFO: ✓ Resultado enviado: 3
```

**Terminal 3 (Worker) - After Master_A Dies:**
```
[WORKER] WARNING: Master indisponível: ... Reconectando em 1s...
[WORKER] INFO: Tentando conectar ao Master (127.0.0.1:5000)...
[WORKER] WARNING: Master indisponível: ... Reconectando em 1s...
[WORKER] WARNING: Limite de tentativas excedido (2). Promovendo Worker_1 a Master...
[MASTER] INFO: 🚀 Master Server iniciado em 0.0.0.0:5000
[MASTER] INFO: UUID: Master_A
[MASTER] INFO: Estado recebido de peer 127.0.0.1:5101, carregando...
[MASTER] INFO: ✓ 60 tarefas carregadas (from persisted state)
[MASTER] INFO: Worker monitor iniciado
```

**Verdict:** ✓ PASS if "Estado recebido de peer" is logged and master continues task distribution with restored state.

> Note: the promoted worker first starts the local master, then attempts to hydrate it from peers using `REQUEST_STATE`.

---

### Test 4: Authentication (Token Validation)

**Goal:** Verify that masters reject workers/requests without valid auth token.

**Setup:**
```bash
# Terminal 1: Start Master with auth enabled
MASTER_PORT=5000 SERVER_UUID=Master_A MASTER_AUTH_TOKEN=secret123 python master.py

# Terminal 2: Try to connect worker WITHOUT token (should fail gracefully)
python worker.py TestWorker_NoToken

# Terminal 3: Connect worker WITH token
AUTH_TOKEN=secret123 python worker.py TestWorker_WithToken
```

**Expected Output:**

**Terminal 1 (Master):**
```
[MASTER] WARNING: Auth failed for worker at ('127.0.0.1', ...)
```
(Worker without token gets error message and should disconnect/retry)

**Terminal 3 (Worker with token):**
```
[WORKER] INFO: ✓ Conectado ao Master
[WORKER] INFO: ✓ Apresentação enviada (ALIVE)
[WORKER] INFO: → Tarefa recebida: ...
```

**Verdict:** ✓ PASS if worker with wrong/missing token is rejected; worker with correct token succeeds.

> Note: this is optional and only applies when `MASTER_AUTH_TOKEN` / `AUTH_TOKEN` are configured.

---

### Test 5: State Persistence

**Goal:** Verify that master state is saved and loaded correctly.

**Setup:**
```bash
# Terminal 1: Start Master_A with initial tasks
MASTER_PORT=5000 SERVER_UUID=Master_A INITIAL_TASKS=10 python master.py

# Wait a few seconds, then kill it (Ctrl+C)

# Check that state file was created
ls -la data/tasks_Master_A.json

# Terminal 2: Restart Master_A
MASTER_PORT=5000 SERVER_UUID=Master_A INITIAL_TASKS=0 python master.py
# (Note: INITIAL_TASKS=0, but state should be loaded from disk)
```

**Expected Output:**

**After Restart:**
```
[MASTER] INFO: 🚀 Master Server iniciado em 0.0.0.0:5000
[MASTER] INFO: UUID: Master_A
[MASTER] INFO: Stats - Pending: X, In Progress: 0, Completed: Y, Workers: 0/0
# (X + Y should match previous run; tasks were not reinitialized)
```

**Verdict:** ✓ PASS if state from previous run was restored (task counts match).

> Tip: set `LOAD_STATE=0` to force a clean start and verify that fresh tasks are loaded instead of persisted state.

### Test 6: Master Election by Disk Space

**Goal:** Verify that workers connected to the same master elect the worker with the most free disk when the master becomes unavailable.

**Setup:**
```bash
# Start a master and at least 2 workers connected to it.
# Force the master down.
# Wait for 4 consecutive connection failures.
```

**Expected Output:**
```text
[WORKER] WARNING: Falha de conexão ao master (4/4)
[WORKER] WARNING: Eleição disparada após 4 falhas. Vencedor: Worker_X (... bytes livres)
[WORKER] INFO: ↪ Reapontando conexão para o novo master Worker_X em 127.0.0.1:5000
```

**Verdict:** ✓ PASS if the worker with the highest `FREE_DISK_BYTES` becomes the new master and the others reconnect to it.

### Test 7: Failback to Original Master

**Goal:** Verify that the promoted master returns to worker mode when the original master comes back online.

**Setup:**
```bash
# 1. Force promotion by killing the original master.
# 2. Wait for a worker to become master.
# 3. Bring the original master back.
```

**Expected Output:**
```text
[WORKER] WARNING: Master original voltou a responder em 127.0.0.1:5000. Iniciando failback...
[WORKER] INFO: ↩ Failback concluído; retornando ao papel de worker
```

**Verdict:** ✓ PASS if the promoted node stops serving as master, redirects workers back, and resumes as a worker.

---

## Integration Test (Automated)

Run the provided automated test scripts:

```bash
# Test 1: Promotion
python tests/run_promotion_test.py
# Expected: prints "PROMOTED: True"

# Test 2: Redirection
python tests/run_redirect_integration.py
# Expected: prints "Redirected: True" and task assigned to Master_B

# Test 3: Election by disk space
python tests/run_election_test.py
# Expected: prints "Election rule verified: highest disk wins, tie breaks by worker UUID."
```

---

## Manual Lab Scenario (4 machines)

**Setup:**
- Machine 1: Farm A Master (Master_A, 5000)
- Machine 2: Farm A Worker (Worker_1, connects to Master_A)
- Machine 3: Farm B Master (Master_B, 5101)
- Machine 4: Farm B Worker (Worker_2, connects to Master_B)

**Test Flow:**
1. Start both masters (A and B)
2. Start both workers
3. Observe tasks being distributed
4. Kill Master_A network (disconnect or kill process)
5. Observe Worker_1 fail to reconnect, then promote itself to Master_A
6. Observe tasks continue on the promoted Master_A
7. Restart Master_A (original) and observe failback/redirection to the original master

---

## Troubleshooting

| Issue | Cause | Solution |
|-------|-------|----------|
| Worker stuck in reconnect loop | Master not running | Start master on correct port |
| Redirection not working | `MASTER_PEERS` not set | Set env var: `MASTER_PEERS=host:port:uuid` |
| Auth error "AUTH_FAILED" | Token mismatch | Ensure `MASTER_AUTH_TOKEN` matches `AUTH_TOKEN` |
| State not persisting | Permission denied on `data/` | Check write permissions in repo root |
| Promotion never triggers | `PROMOTE_THRESHOLD` too high | Lower threshold: `PROMOTE_THRESHOLD=2` or `PROMOTE_THRESHOLD=1` for testing |
| Election does not pick the expected worker | Missing `FREE_DISK_BYTES` in ALIVE / task responses | Ensure workers are sending disk info and that all workers are connected to the same master |
| Failback never triggers | Original master still not reachable from promoted worker | Confirm the original master is actually back online on the same host/port |
| Port already in use | Another process on same port | Kill old process or use different port: `MASTER_PORT=5200` |

---

## Performance Notes

- Redirection happens in ~100-500ms (network latency dependent)
- Promotion takes ~1-5 seconds (state transfer via network, disk I/O)
- Failback detection is poll-based and typically takes a few seconds after the original master is reachable again
- State file size: ~10-100 KB for 60 tasks (JSON format)
- No significant CPU overhead from persistence (async saves)

---

## Cleanup

After testing, remove persisted state files if you want a clean restart:

```bash
rm -rf data/tasks_*.json
```

Or on Windows:
```powershell
Remove-Item -Path "data/tasks_*.json" -Force
```
