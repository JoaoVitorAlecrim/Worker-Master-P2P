# Sprint 4 - Performance Monitor Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implementar envio periódico de métricas de desempenho ao supervisor do professor (`nuted-ia.dev:443`) via TLS/TCP a cada 10 segundos, alimentando o dashboard do cluster na apresentação final.

**Architecture:** Novo módulo `common/monitor.py` coleta métricas via `psutil` e envia o payload `performance_report` como TLS/TCP fire-and-forget. `master.py` recebe mudanças mínimas: novo dict `lent_workers` para rastrear workers emprestados para fora, dict `_peer_last_seen` para status de vizinhos via ping M2M periódico, e início das threads daemon de monitoramento no `start()`.

**Tech Stack:** Python 3.13, psutil, ssl (stdlib), socket (stdlib), threading (stdlib), unittest.

---

### Task 1: Instalar psutil e criar `common/monitor.py` com testes

**Files:**
- Create: `requirements.txt`
- Create: `common/monitor.py`
- Create: `tests/test_sprint4_monitor.py`

- [ ] **Step 1: Criar `requirements.txt`**

```
psutil>=5.9.0
```

Instalar: `pip install psutil`

- [ ] **Step 2: Escrever os testes que devem falhar**

Criar `tests/test_sprint4_monitor.py`:

```python
import unittest
import time
from unittest.mock import MagicMock, patch
from common.monitor import build_performance_report, collect_system_metrics


def _make_mock_master(
    server_uuid="Test_A",
    lent_workers=None,
    peer_last_seen=None,
    peer_masters=None,
    capacity=100,
    is_temporary_workers=None,
):
    """Cria um MasterServer mock com os atributos necessários para o monitor."""
    master = MagicMock()
    master.server_uuid = server_uuid
    master._capacity = capacity
    master.lent_workers = lent_workers or {}
    master._peer_last_seen = peer_last_seen or {}
    master.peer_masters = peer_masters or []

    # Worker mock helper
    def make_worker(uuid, temporary=False, server_uuid_w="Test_A", task_id=None, offline=False):
        w = MagicMock()
        w.worker_uuid = uuid
        w.server_uuid = server_uuid_w
        w.is_temporary = temporary
        w.current_task_id = task_id
        from common.models import WorkerStatus
        w.status = WorkerStatus.OFFLINE if offline else WorkerStatus.IDLE
        return w

    workers = []
    for spec in (is_temporary_workers or []):
        workers.append(make_worker(**spec))
    if not workers:
        workers = [make_worker("W1"), make_worker("W2")]

    master.task_manager.get_all_workers.return_value = workers
    master.task_manager.get_statistics.return_value = {
        "tasks": {"pending": 5, "in_progress": 2, "completed": 10, "failed": 1},
        "workers": {"online": len(workers), "offline": 0, "total": len(workers)},
    }

    from common.models import TaskStatus
    mock_task = MagicMock()
    mock_task.created_at = time.time() - 30
    master.task_manager.get_tasks_by_status.return_value = [mock_task]

    return master


class TestCollectSystemMetrics(unittest.TestCase):
    def test_returns_required_keys(self):
        metrics = collect_system_metrics()
        self.assertIn("uptime_seconds", metrics)
        self.assertIn("load_average_1m", metrics)
        self.assertIn("load_average_5m", metrics)
        self.assertIn("cpu", metrics)
        self.assertIn("memory", metrics)
        self.assertIn("disk", metrics)

    def test_cpu_keys(self):
        metrics = collect_system_metrics()
        self.assertIn("usage_percent", metrics["cpu"])
        self.assertIn("count_logical", metrics["cpu"])
        self.assertIn("count_physical", metrics["cpu"])

    def test_memory_keys(self):
        metrics = collect_system_metrics()
        self.assertIn("total_mb", metrics["memory"])
        self.assertIn("available_mb", metrics["memory"])
        self.assertIn("percent_used", metrics["memory"])
        self.assertIn("memory_used", metrics["memory"])

    def test_disk_keys(self):
        metrics = collect_system_metrics()
        self.assertIn("total_gb", metrics["disk"])
        self.assertIn("free_gb", metrics["disk"])
        self.assertIn("percent_used", metrics["disk"])

    def test_uptime_is_positive_int(self):
        metrics = collect_system_metrics()
        self.assertIsInstance(metrics["uptime_seconds"], int)
        self.assertGreater(metrics["uptime_seconds"], 0)


class TestBuildPerformanceReport(unittest.TestCase):
    def test_top_level_fields_present(self):
        master = _make_mock_master()
        report = build_performance_report(master)
        for field in ["server_uuid", "hostname", "role", "task", "timestamp",
                      "message_id", "payload_version", "performance"]:
            self.assertIn(field, report, f"Campo ausente: {field}")

    def test_role_and_task_values(self):
        master = _make_mock_master()
        report = build_performance_report(master)
        self.assertEqual(report["role"], "master")
        self.assertEqual(report["task"], "performance_report")
        self.assertEqual(report["payload_version"], "sprint4-monitor")

    def test_server_uuid_matches_master(self):
        master = _make_mock_master(server_uuid="My_Master")
        report = build_performance_report(master)
        self.assertEqual(report["server_uuid"], "My_Master")

    def test_performance_sections_present(self):
        master = _make_mock_master()
        perf = build_performance_report(master)["performance"]
        for section in ["system", "farm_state", "config_thresholds", "neighbors"]:
            self.assertIn(section, perf, f"Seção ausente: {section}")

    def test_workers_borrowed_out(self):
        master = _make_mock_master(lent_workers={"W5": "Master_B"})
        report = build_performance_report(master)
        borrowed = report["performance"]["farm_state"]["workers"]["borrowed_workers"]
        out_entries = [e for e in borrowed if e["direction"] == "out"]
        self.assertEqual(len(out_entries), 1)
        self.assertEqual(out_entries[0]["peer_uuid"], "Master_B")

    def test_workers_received_in(self):
        workers_spec = [
            {"uuid": "W_ext", "temporary": True, "server_uuid_w": "Master_B"},
            {"uuid": "W_local"},
        ]
        master = _make_mock_master(is_temporary_workers=workers_spec)
        report = build_performance_report(master)
        borrowed = report["performance"]["farm_state"]["workers"]["borrowed_workers"]
        in_entries = [e for e in borrowed if e["direction"] == "in"]
        self.assertEqual(len(in_entries), 1)
        self.assertEqual(in_entries[0]["peer_uuid"], "Master_B")

    def test_neighbor_available_when_recently_seen(self):
        peer_masters = [("127.0.0.1", 5001, "Master_B")]
        peer_last_seen = {"Master_B": time.time() - 10}
        master = _make_mock_master(peer_masters=peer_masters, peer_last_seen=peer_last_seen)
        report = build_performance_report(master)
        neighbors = report["performance"]["neighbors"]
        self.assertEqual(len(neighbors), 1)
        self.assertEqual(neighbors[0]["server_uuid"], "Master_B")
        self.assertEqual(neighbors[0]["status"], "available")

    def test_neighbor_unavailable_when_not_seen(self):
        peer_masters = [("127.0.0.1", 5001, "Master_B")]
        master = _make_mock_master(peer_masters=peer_masters, peer_last_seen={})
        report = build_performance_report(master)
        neighbors = report["performance"]["neighbors"]
        self.assertEqual(neighbors[0]["status"], "unavailable")

    def test_config_thresholds(self):
        master = _make_mock_master(capacity=100)
        report = build_performance_report(master)
        thresholds = report["performance"]["config_thresholds"]
        self.assertEqual(thresholds["max_task"], 100)
        self.assertEqual(thresholds["release_task"], 60)
        self.assertEqual(thresholds["warn_cpu_percent"], 85)
        self.assertEqual(thresholds["warn_memory_percent"], 85)

    def test_oldest_task_age_s_is_non_negative(self):
        master = _make_mock_master()
        report = build_performance_report(master)
        age = report["performance"]["farm_state"]["tasks"]["oldest_task_age_s"]
        self.assertGreaterEqual(age, 0)


if __name__ == "__main__":
    unittest.main()
```

- [ ] **Step 3: Rodar os testes para confirmar que falham**

```
python -m unittest tests.test_sprint4_monitor -v
```

Esperado: `ImportError: cannot import name 'build_performance_report' from 'common.monitor'` (ou ModuleNotFoundError se o arquivo não existir ainda).

- [ ] **Step 4: Criar `common/monitor.py`**

```python
"""Sprint 4: envio periódico de métricas ao supervisor do professor."""
import ssl
import socket
import json
import uuid
import time
import datetime
import threading
import logging

try:
    import psutil
    _HAS_PSUTIL = True
except ImportError:
    _HAS_PSUTIL = False

logger = logging.getLogger(__name__)

SUPERVISOR_HOST = "nuted-ia.dev"
SUPERVISOR_PORT = 443
NEIGHBOR_STALE_SECONDS = 60


def collect_system_metrics() -> dict:
    """Retorna métricas reais do sistema via psutil."""
    if not _HAS_PSUTIL:
        return {
            "uptime_seconds": 0,
            "load_average_1m": 0.0,
            "load_average_5m": 0.0,
            "cpu": {"usage_percent": 0.0, "count_logical": 1, "count_physical": 1},
            "memory": {"total_mb": 0, "available_mb": 0, "percent_used": 0.0, "memory_used": 0},
            "disk": {"total_gb": 0.0, "free_gb": 0.0, "percent_used": 0.0},
        }

    cpu_pct = psutil.cpu_percent(interval=0.1)
    cpu_logical = psutil.cpu_count(logical=True) or 1
    cpu_physical = psutil.cpu_count(logical=False) or 1
    mem = psutil.virtual_memory()
    disk = psutil.disk_usage("/")
    uptime = int(time.time() - psutil.boot_time())

    try:
        load_1m, load_5m, _ = psutil.getloadavg()
    except (AttributeError, OSError):
        load_1m, load_5m = 0.0, 0.0

    return {
        "uptime_seconds": uptime,
        "load_average_1m": round(load_1m, 2),
        "load_average_5m": round(load_5m, 2),
        "cpu": {
            "usage_percent": round(cpu_pct, 2),
            "count_logical": cpu_logical,
            "count_physical": cpu_physical,
        },
        "memory": {
            "total_mb": mem.total // (1024 * 1024),
            "available_mb": mem.available // (1024 * 1024),
            "percent_used": round(mem.percent, 2),
            "memory_used": mem.used // (1024 * 1024),
        },
        "disk": {
            "total_gb": round(disk.total / (1024 ** 3), 1),
            "free_gb": round(disk.free / (1024 ** 3), 1),
            "percent_used": round(disk.percent, 1),
        },
    }


def _build_farm_state(master) -> dict:
    from common.models import WorkerStatus, TaskStatus

    all_workers = master.task_manager.get_all_workers()
    received = [w for w in all_workers if w.is_temporary]
    alive = [w for w in all_workers if w.status != WorkerStatus.OFFLINE]
    busy = [w for w in all_workers if w.current_task_id is not None]
    failed = [w for w in all_workers if w.status == WorkerStatus.OFFLINE]
    idle = [w for w in alive if w.current_task_id is None]
    home = [w for w in all_workers if not w.is_temporary]
    lent = getattr(master, "lent_workers", {})

    borrowed_workers = []
    for w in received:
        borrowed_workers.append({"direction": "in", "peer_uuid": w.server_uuid})
    for peer_uuid in lent.values():
        borrowed_workers.append({"direction": "out", "peer_uuid": peer_uuid})

    stats = master.task_manager.get_statistics()
    tasks_pending = stats["tasks"]["pending"]
    tasks_running = stats["tasks"]["in_progress"]
    tasks_completed = stats["tasks"]["completed"]
    tasks_failed = stats["tasks"]["failed"]

    oldest_age = 0
    try:
        pending_tasks = master.task_manager.get_tasks_by_status(TaskStatus.PENDING)
        if pending_tasks:
            oldest_age = int(time.time() - min(t.created_at for t in pending_tasks))
    except Exception:
        pass

    return {
        "workers": {
            "total_registered": len(all_workers),
            "workers_utilization": len(busy),
            "workers_alive": len(alive),
            "workers_idle": len(idle),
            "workers_borrowed": len(lent),
            "workers_received": len(received),
            "workers_failed": len(failed),
            "workers_home": len(home),
            "workers_available_capacity": len(idle),
            "borrowed_workers": borrowed_workers,
        },
        "tasks": {
            "tasks_pending": tasks_pending,
            "tasks_running": tasks_running,
            "tasks_completed": tasks_completed,
            "tasks_failed": tasks_failed,
            "oldest_task_age_s": oldest_age,
        },
    }


def _build_neighbors(master) -> list:
    now = time.time()
    last_seen = getattr(master, "_peer_last_seen", {})
    neighbors = []
    for _host, _port, peer_uuid in getattr(master, "peer_masters", []):
        seen_at = last_seen.get(peer_uuid)
        if seen_at and (now - seen_at) < NEIGHBOR_STALE_SECONDS:
            status = "available"
            last_hb = datetime.datetime.utcfromtimestamp(seen_at).strftime("%Y-%m-%dT%H:%M:%SZ")
        else:
            status = "unavailable"
            last_hb = ""
        neighbors.append({"server_uuid": peer_uuid, "status": status, "last_heartbeat": last_hb})
    return neighbors


def build_performance_report(master) -> dict:
    """Monta o payload completo conforme spec do professor."""
    hostname = socket.gethostname()
    capacity = getattr(master, "_capacity", 100)

    try:
        system = collect_system_metrics()
    except Exception as exc:
        logger.warning(f"[monitor] falha ao coletar métricas de sistema: {exc}")
        system = {
            "uptime_seconds": 0, "load_average_1m": 0.0, "load_average_5m": 0.0,
            "cpu": {"usage_percent": 0.0, "count_logical": 1, "count_physical": 1},
            "memory": {"total_mb": 0, "available_mb": 0, "percent_used": 0.0, "memory_used": 0},
            "disk": {"total_gb": 0.0, "free_gb": 0.0, "percent_used": 0.0},
        }

    return {
        "server_uuid": master.server_uuid,
        "hostname": hostname,
        "role": "master",
        "task": "performance_report",
        "timestamp": datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
        "message_id": str(uuid.uuid4()),
        "payload_version": "sprint4-monitor",
        "performance": {
            "system": system,
            "farm_state": _build_farm_state(master),
            "config_thresholds": {
                "max_task": capacity,
                "warn_cpu_percent": 85,
                "warn_memory_percent": 85,
                "release_task": int(capacity * 0.6),
            },
            "neighbors": _build_neighbors(master),
        },
    }


def send_to_supervisor(payload: dict) -> None:
    """Envia o payload ao supervisor via TLS/TCP (fire-and-forget)."""
    ctx = ssl.create_default_context()
    raw = socket.create_connection((SUPERVISOR_HOST, SUPERVISOR_PORT), timeout=5.0)
    with ctx.wrap_socket(raw, server_hostname=SUPERVISOR_HOST) as tls:
        tls.sendall((json.dumps(payload) + "\n").encode("utf-8"))


def _monitor_loop(master, interval: int) -> None:
    while True:
        try:
            payload = build_performance_report(master)
            send_to_supervisor(payload)
            logger.info(f"[monitor] métricas enviadas (message_id={payload['message_id']})")
        except Exception as exc:
            logger.warning(f"[monitor] falha ao enviar métricas: {exc}")
        time.sleep(interval)


def _peer_ping_loop(master, interval: int) -> None:
    from common.protocol import send_json, recv_json_line, build_master_envelope_spec
    while True:
        time.sleep(interval)
        for peer_host, peer_port, peer_uuid in getattr(master, "peer_masters", []):
            try:
                with socket.create_connection((peer_host, peer_port), timeout=3.0) as conn:
                    send_json(conn, build_master_envelope_spec(
                        "ping", {}, request_id=str(uuid.uuid4())
                    ))
                    # Não aguarda resposta — só confirma que a porta está aberta
                with master.lock if hasattr(master, "lock") else __import__("contextlib").nullcontext():
                    master._peer_last_seen[peer_uuid] = time.time()
            except Exception:
                pass


def start_monitor_thread(master, interval: int = 10) -> None:
    """Inicia thread daemon de envio de métricas ao supervisor."""
    t = threading.Thread(target=_monitor_loop, args=(master, interval), daemon=True, name="monitor-metrics")
    t.start()
    logger.info(f"[monitor] thread de métricas iniciada (intervalo={interval}s)")


def start_peer_ping_thread(master, interval: int = 30) -> None:
    """Inicia thread daemon de ping M2M para rastrear status dos vizinhos."""
    if not getattr(master, "peer_masters", []):
        return
    t = threading.Thread(target=_peer_ping_loop, args=(master, interval), daemon=True, name="monitor-peer-ping")
    t.start()
    logger.info(f"[monitor] thread de ping M2M iniciada (intervalo={interval}s)")
```

- [ ] **Step 5: Rodar os testes para confirmar que passam**

```
python -m unittest tests.test_sprint4_monitor -v
```

Esperado: todos os testes PASS.

- [ ] **Step 6: Commit**

```bash
git add requirements.txt common/monitor.py tests/test_sprint4_monitor.py
git commit -m "feat(sprint4): add performance monitor module with psutil metrics and supervisor TLS sender"
```

---

### Task 2: Adicionar rastreamento de workers emprestados em `master.py`

**Files:**
- Modify: `master.py`

- [ ] **Step 1: Adicionar atributos ao `__init__` do `MasterServer`**

Em `master.py`, dentro de `MasterServer.__init__` (após `self._worker_poll_cooldown = WORKER_POLL_COOLDOWN`), adicionar:

```python
# Sprint 4: rastreamento para o monitor de métricas
self.lent_workers: Dict[str, str] = {}   # worker_uuid -> peer_uuid (direction "out")
self._peer_last_seen: Dict[str, float] = {}  # peer_uuid -> timestamp último ping/contato
self.peer_masters = PEER_MASTERS          # acessível pelo common/monitor.py
self._capacity = CAPACITY                 # acessível pelo common/monitor.py
```

- [ ] **Step 2: Popular `lent_workers` ao enviar `command_redirect`**

No handler `handle_master_request`, dentro do bloco `if mtype == "request_help"`, após o `send_json(conn, envelope)` do `command_redirect` (em torno da linha 452), adicionar:

```python
with self.lock:
    self.lent_workers[worker.worker_uuid] = requester_master_id
```

O bloco completo do loop de escolha de workers fica assim (substituir o trecho existente):

```python
for worker in chosen:
    worker_details.append({"id": worker.worker_uuid, "address": worker.host or self.server_uuid})

    if requester_host and requester_port:
        conn = None
        with self.lock:
            conn = self.worker_connections.get(worker.worker_uuid)
        if conn:
            envelope = build_master_envelope_spec(
                "command_redirect",
                {"new_master_address": f"{requester_host}:{requester_port}"},
                request_id=str(uuid.uuid4()),
            )
            try:
                send_json(conn, envelope)
                logger.info(f"↪ command_redirect enviado a {worker.worker_uuid} -> {requester_host}:{requester_port}")
                with self.lock:
                    self.lent_workers[worker.worker_uuid] = requester_master_id
            except Exception:
                logger.warning(f"Falha ao enviar command_redirect para worker {worker.worker_uuid}")
```

- [ ] **Step 3: Limpar `lent_workers` ao receber `notify_worker_returned`**

No handler `if mtype == "notify_worker_returned"` (em torno da linha 468), após `worker.mark_online()`, adicionar:

```python
with self.lock:
    self.lent_workers.pop(worker_id, None)
```

O bloco completo fica:

```python
if mtype == "notify_worker_returned":
    worker_id = _ci(payload, "worker_id")
    if worker_id:
        worker = self.task_manager.get_worker(worker_id)
        if worker:
            worker.server_uuid = self.server_uuid
            worker.mark_online()
    with self.lock:
        self.lent_workers.pop(worker_id, None)
    return build_master_envelope_spec("response_accepted", {"worker_id": worker_id}, request_id=request_id)
```

- [ ] **Step 4: Adicionar handler para mensagem `ping` no bloco M2M**

No bloco `handle_master_request`, antes do `return` final de tipo desconhecido, adicionar:

```python
if mtype == "ping":
    return build_master_envelope_spec("pong", {}, request_id=request_id)
```

- [ ] **Step 5: Rodar a suite completa para confirmar que não há regressão**

```
python -m unittest discover -v tests
```

Esperado: todos os testes PASS (incluindo os testes de sprint 3 e wire shapes).

- [ ] **Step 6: Commit**

```bash
git add master.py
git commit -m "feat(sprint4): track lent_workers and peer_last_seen in MasterServer for monitor"
```

---

### Task 3: Iniciar threads de monitoramento no `start()` do master

**Files:**
- Modify: `master.py`

- [ ] **Step 1: Importar as funções do monitor no topo de `master.py`**

No bloco de imports do `master.py`, adicionar:

```python
from common.monitor import start_monitor_thread, start_peer_ping_thread
```

- [ ] **Step 2: Iniciar as threads daemon no `start()`**

No método `MasterServer.start()`, após a linha que inicia `monitor_thread` (o `monitor_thread.start()` existente que chama `worker_monitor_thread`), adicionar:

```python
# Sprint 4: thread de envio de métricas ao supervisor
start_monitor_thread(self)
# Sprint 4: thread de ping M2M para rastrear status dos vizinhos
start_peer_ping_thread(self)
```

- [ ] **Step 3: Verificar que o master inicia sem erros e envia métricas**

```bash
python master.py
```

Verificar no log as linhas:
```
[monitor] thread de métricas iniciada (intervalo=10s)
[monitor] thread de ping M2M iniciada (intervalo=30s)
[monitor] métricas enviadas (message_id=...)
```

A terceira linha deve aparecer ~10 segundos após o start.

- [ ] **Step 4: Verificar o dashboard do professor**

Acessar `https://nuted-ia.dev/supervisor/dashboard/` e confirmar que o farm aparece na topologia com as métricas da máquina.

- [ ] **Step 5: Rodar a suite completa**

```
python -m unittest discover -v tests
```

Esperado: todos os testes PASS.

- [ ] **Step 6: Commit**

```bash
git add master.py
git commit -m "feat(sprint4): start monitor and peer-ping threads in MasterServer.start()"
```

---

### Task 4: Verificação final e testes de integração

**Files:**
- Verify: `master.py`, `common/monitor.py`, `tests/test_sprint4_monitor.py`

- [ ] **Step 1: Teste de empréstimo de worker com dois Masters locais**

Terminal 1 — Master A (sem peers):
```powershell
$env:SERVER_UUID="Master_A"; $env:MASTER_PORT="5000"; python master.py
```

Terminal 2 — Master B (peer de A):
```powershell
$env:SERVER_UUID="Master_B"; $env:MASTER_PORT="5001"; $env:MASTER_PEERS="127.0.0.1:5000:Master_A"; python master.py
```

Terminal 3 — Worker:
```powershell
$env:MASTER_PORT="5000"; python worker.py Worker_1 Master_A
```

Verificar no dashboard que ambos os Masters aparecem e que `workers_borrowed`/`workers_received` refletem o empréstimo quando Master_A satura.

- [ ] **Step 2: Verificar campos obrigatórios no payload real com tcpdump / log**

Adicionar temporariamente ao `_monitor_loop` um `logger.debug(json.dumps(payload, indent=2))` e rodar com `PYTHONPATH=. python -c "import logging; logging.basicConfig(level=logging.DEBUG); ..."` para inspecionar o payload completo.

- [ ] **Step 3: Rodar suite completa uma última vez**

```
python -m unittest discover -v tests
```

Esperado: todos os testes PASS.

- [ ] **Step 4: Commit final**

```bash
git add -A
git commit -m "feat(sprint4): complete performance monitor — ready for final presentation"
```
