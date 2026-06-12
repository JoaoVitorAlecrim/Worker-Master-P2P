# Sprint 4 Design - Performance Monitor e Relatório de Métricas

## Context

O projeto já possui um sistema distribuído completo com:
- Protocolo Worker↔Master (ALIVE, HEARTBEAT, QUERY, STATUS, ACK)
- Rastreamento de tarefas (pending, in_progress, completed, failed)
- Detecção de falha e reassignment automático
- Negociação Farm-to-Farm (request_help, response_accepted/rejected)
- Redirecionamento dinâmico de workers (command_redirect, command_release)
- Eleição automática de master (UDP broadcast, critério: espaço livre em disco)

Sprint 4 é a **apresentação final**. O único requisito novo é que cada Master deve reportar
métricas de desempenho ao supervisor do professor a cada 10 segundos via TLS/TCP,
alimentando um dashboard web que monitora o cluster em tempo real.

## User Decisions

- `psutil` para coleta de métricas reais de sistema (CPU, RAM, disco, uptime, load average)
- Ping periódico M2M a cada 30s para determinar o status de vizinhos (`available`/`unavailable`)
- Workers emprestados para fora (`lent_workers`) rastreados em novo dict no `MasterServer`
- Workers recebidos (`is_temporary=True`) já existem no `TaskManager`
- Conexão TLS/TCP fire-and-forget: connect → send JSON+`\n` → close (sem recv)
- `payload_version`: `"sprint4-monitor"`

## Goals

1. Coletar métricas reais do sistema operacional via `psutil`
2. Montar o payload `performance_report` com todos os campos do spec do professor
3. Enviar ao supervisor (`nuted-ia.dev:443`) via TLS/TCP a cada 10 segundos
4. Rastrear workers emprestados para fora (`lent_workers`) para o campo `workers_borrowed`
5. Reportar status real dos vizinhos via ping M2M periódico

## Non-Goals

- Mudança no protocolo Worker↔Master (Sprints 1-2)
- Mudança no protocolo de negociação Farm-to-Farm (Sprint 3)
- Mudança no mecanismo de eleição
- Persistência das métricas localmente
- Dashboard próprio

## Proposed Architecture

### Módulo `common/monitor.py` (novo)

Responsabilidade única: coletar métricas e enviar ao supervisor. Sem dependência de
lógica de negócio do master — só usa a instância como fonte de dados.

Funções principais:
- `collect_system_metrics()` → dict com CPU, RAM, disco, uptime, load_average via psutil
- `build_performance_report(master)` → monta o payload completo do spec
- `send_to_supervisor(payload)` → abre TLS/TCP, envia JSON+`\n`, fecha
- `start_monitor_thread(master, interval=10)` → daemon thread de envio
- `start_peer_ping_thread(master, interval=30)` → daemon thread de ping M2M

### Mudanças em `master.py`

Adicionar ao `MasterServer.__init__`:
```python
self.lent_workers: Dict[str, str] = {}   # worker_uuid -> peer_uuid (direction "out")
self._peer_last_seen: Dict[str, float] = {}  # peer_uuid -> timestamp
self.peer_masters = PEER_MASTERS          # acessível pelo monitor
self._capacity = CAPACITY                 # acessível pelo monitor
```

Atualizar handler `request_help`:
- Após `send_json(conn, command_redirect)`: `self.lent_workers[worker.worker_uuid] = requester_master_id`

Atualizar handler `notify_worker_returned`:
- Após processar: `self.lent_workers.pop(worker_id, None)`

Adicionar handler `ping` no handler M2M:
- Responde com `{"type": "pong", "request_id": request_id, "payload": {}}`

Iniciar threads no `start()`:
```python
from common.monitor import start_monitor_thread, start_peer_ping_thread
start_monitor_thread(self)
start_peer_ping_thread(self)
```

### Payload do Supervisor (spec completo)

```json
{
  "server_uuid": "Master_A",
  "hostname": "hostname-da-maquina",
  "role": "master",
  "task": "performance_report",
  "timestamp": "2026-06-11T12:34:56Z",
  "message_id": "uuid-v4",
  "payload_version": "sprint4-monitor",
  "performance": {
    "system": {
      "uptime_seconds": 12345,
      "load_average_1m": 3.20,
      "load_average_5m": 2.50,
      "cpu": {"usage_percent": 85.42, "count_logical": 8, "count_physical": 4},
      "memory": {"total_mb": 16384, "available_mb": 8192, "percent_used": 62.18, "memory_used": 8000},
      "disk": {"total_gb": 512.0, "free_gb": 250.0, "percent_used": 45.0}
    },
    "farm_state": {
      "workers": {
        "total_registered": 6,
        "workers_utilization": 4,
        "workers_alive": 6,
        "workers_idle": 2,
        "workers_borrowed": 1,
        "workers_received": 1,
        "workers_failed": 0,
        "workers_home": 5,
        "workers_available_capacity": 2,
        "borrowed_workers": [
          {"direction": "out", "peer_uuid": "Master_B"},
          {"direction": "in",  "peer_uuid": "Master_B"}
        ]
      },
      "tasks": {
        "tasks_pending": 42,
        "tasks_running": 4,
        "tasks_completed": 150,
        "tasks_failed": 3,
        "oldest_task_age_s": 312
      }
    },
    "config_thresholds": {
      "max_task": 100,
      "warn_cpu_percent": 85,
      "warn_memory_percent": 85,
      "release_task": 60
    },
    "neighbors": [
      {
        "server_uuid": "Master_B",
        "status": "available",
        "last_heartbeat": "2026-06-11T12:34:56Z"
      }
    ]
  }
}
```

### Mapeamento de campos → fonte de dados

| Campo do payload | Fonte |
|---|---|
| `server_uuid` | `master.server_uuid` |
| `hostname` | `socket.gethostname()` |
| `uptime_seconds` | `int(time.time() - psutil.boot_time())` |
| `load_average_1m/5m` | `psutil.getloadavg()[0:2]` |
| `cpu.usage_percent` | `psutil.cpu_percent(interval=0.1)` |
| `cpu.count_logical/physical` | `psutil.cpu_count(logical=True/False)` |
| `memory.*` | `psutil.virtual_memory()` |
| `disk.*` | `psutil.disk_usage('/')` |
| `total_registered` | `len(task_manager.get_all_workers())` |
| `workers_utilization` | workers com `current_task_id is not None` |
| `workers_alive` | workers com `status != OFFLINE` |
| `workers_idle` | workers alive sem current_task_id |
| `workers_borrowed` | `len(master.lent_workers)` |
| `workers_received` | workers com `is_temporary=True` |
| `workers_failed` | workers com `status == OFFLINE` |
| `workers_home` | workers com `is_temporary=False` |
| `workers_available_capacity` | = workers_idle |
| `borrowed_workers` (in) | `[{"direction":"in","peer_uuid":w.server_uuid} for w in received]` |
| `borrowed_workers` (out) | `[{"direction":"out","peer_uuid":v} for v in lent_workers.values()]` |
| `tasks_pending/running/completed/failed` | `task_manager.get_statistics()` |
| `oldest_task_age_s` | `min(t.created_at for t in pending_tasks)` |
| `config_thresholds.max_task` | `master._capacity` (= CAPACITY = 100) |
| `config_thresholds.release_task` | `int(master._capacity * 0.6)` (= 60) |
| `neighbors[].status` | `"available"` se `_peer_last_seen[uuid] > now - 60s`, else `"unavailable"` |
| `neighbors[].last_heartbeat` | `_peer_last_seen[uuid]` formatado em ISO-8601 |

## Conexão com o Supervisor

```
Host:     nuted-ia.dev
Porta:    443
Protocolo: TLS sobre TCP (sem HTTP)
SNI:      nuted-ia.dev
Comportamento: connect → sendall(JSON + "\n") → close (sem recv)
```

## Testing Strategy

### Testes unitários (sem rede)
- `test_sprint4_monitor.py`: testa `build_performance_report()` com um master mock
  - Verifica campos obrigatórios presentes
  - Verifica tipos corretos (int, float, string)
  - Verifica `borrowed_workers` com workers lent e received
  - Verifica `neighbors` com `_peer_last_seen` populado e vazio

### Teste de integração (com rede)
- Executar `python master.py` com `SERVER_UUID=<nome>`
- Verificar log: `[monitor] métricas enviadas ao supervisor`
- Acessar `https://nuted-ia.dev/supervisor/dashboard/` e confirmar farm aparece

### Regressão
- Suite completa: `python -m unittest discover -v tests` — zero falhas

## Success Criteria

Sprint 4 está completa quando:
- Master envia payload `performance_report` a cada 10s ao supervisor sem erros
- Dashboard mostra o farm na topologia com métricas reais (CPU, RAM, disco)
- Campos `workers_borrowed`/`workers_received` refletem estado real do empréstimo
- Status dos vizinhos (`available`/`unavailable`) muda corretamente via ping M2M
- Suite de testes existente passa sem regressões
