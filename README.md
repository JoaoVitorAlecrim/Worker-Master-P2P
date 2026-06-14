# Worker-Master-P2P

Projeto de Faculdade da Disciplina de Arquitetura de Sistemas Distribuídos — Sprints 1 a 4.

---

## Status

| Sprint | Funcionalidade | Status |
|--------|---------------|--------|
| 1 | Protocolo ALIVE / HEARTBEAT (Worker ↔ Master) | ✅ |
| 2 | Ciclo de tarefas: QUERY → STATUS OK/NOK → ACK | ✅ |
| 3 | Negociação M2M: request_help, command_redirect, register_temporary_worker, command_release, notify_worker_returned | ✅ |
| 3 | Eleição de master entre workers (UDP broadcast, critério: maior espaço livre em disco) | ✅ |
| 3 | Failback do master original | ✅ |
| 4 | Monitor de métricas enviando JSON a cada 10s ao supervisor via TLS/TCP (porta 443) | ✅ |
| 4 | Interoperabilidade com outros grupos (master_address, register sem resposta, fallback SSL) | ✅ |

**Testes:** 43/43 passando

---

## Quick Start

```bash
# Terminal 1 — Master
python master.py

# Terminal 2 — Worker 1
python worker.py

# Terminal 3 — Worker 2 (opcional)
WORKER_UUID=Worker_2 python worker.py
```

### Configuração para a apresentação (múltiplos masters em rede)

```powershell
# Variáveis de ambiente relevantes
$env:SERVER_UUID   = "MASTER_2"            # UUID do seu grupo (já é o default)
$env:MASTER_PORT   = "5000"                # porta em que este master escuta
$env:MASTER_PEERS  = "ip_grupo1:5000:MASTER_1,ip_grupo3:5000:MASTER_3"
python master.py
```

Formato de `MASTER_PEERS`: `host:porta:uuid` separados por vírgula.

### Start limpo (sem estado salvo)

```powershell
$env:LOAD_STATE = "0"
python master.py
```

---

## Estrutura

```
Worker-Master-P2P/
├── common/
│   ├── election.py        — eleição UDP entre workers
│   ├── models.py          — modelos de dados (Worker, Task, TaskStatus…)
│   ├── monitor.py         — Sprint 4: envio de métricas ao supervisor
│   ├── protocol.py        — send_json, recv_json_line, envelopes M2M
│   ├── task_manager.py    — gerenciamento de tarefas e workers
│   └── tasks.py           — execução de tarefas (soma, multiplicação, sleep)
├── tests/
│   ├── test_election_compute_winner.py
│   ├── test_election_spec_shape.py
│   ├── test_integration_election_udp.py
│   ├── test_master_envelope.py
│   ├── test_protocol_normalization.py
│   ├── test_sprint3_protocol.py
│   ├── test_sprint4_monitor.py
│   ├── test_strict_wire_shapes_tcp.py
│   ├── test_task_manager_lifecycle_user.py
│   ├── test_task_manager_user_payload.py
│   ├── test_wire_shapes.py
│   ├── run_election_test.py
│   ├── run_promotion_test.py
│   ├── run_redirect_integration.py
│   └── run_two_worker_promotion.py
├── master.py
├── worker.py
└── README.md
```

---

## Rodar os testes

```bash
python -m pytest tests/ --ignore=tests/run_promotion_test.py -v
```

> `run_promotion_test.py` usa `sys.exit()` no nível do módulo e deve ser executado diretamente, não via pytest.

---

## Variáveis de ambiente

| Variável | Default | Descrição |
|----------|---------|-----------|
| `SERVER_UUID` | `MASTER_2` | UUID deste master (aparece no dashboard e nas mensagens M2M) |
| `MASTER_HOST` | `0.0.0.0` | Interface de escuta do master |
| `MASTER_PORT` | `5000` | Porta TCP do master |
| `MASTER_PEERS` | _(vazio)_ | Masters vizinhos: `host:porta:uuid,...` |
| `MASTER_AUTH_TOKEN` | _(vazio)_ | Token opcional de autenticação |
| `LOAD_STATE` | `1` | `0` para ignorar estado salvo em disco |
| `WORKER_UUID` | `Worker_1` | UUID do worker |
| `HEARTBEAT_INTERVAL` | `10` | Segundos entre heartbeats do worker |
| `HELP_REQUEST_COOLDOWN` | `5` | Segundos mínimos entre pedidos de ajuda a peers |
