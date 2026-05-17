# 🚀 PLANO DE IMPLEMENTAÇÃO - Correções e Melhorias

## Roadmap Executivo

```
┌─────────────────────────────────────────────────────────┐
│ FASE 1: Corrigir Protocolos (Sprints 1-2)              │
│ ├─ Trocar HEARTBEAT → ALIVE                            │
│ ├─ Corrigir campos (WORKER_UUID, SERVER_UUID)          │
│ ├─ Separar apresentação de distribuição                │
│ └─ Implementar ACK                                     │
├─────────────────────────────────────────────────────────┤
│ FASE 2: Rastreamento de Tarefas                        │
│ ├─ Criar TaskManager com persistência                  │
│ ├─ Adicionar ID único para tarefas                     │
│ ├─ Rastrear status e worker atribuído                  │
│ └─ Manter histórico                                    │
├─────────────────────────────────────────────────────────┤
│ FASE 3: Detecção e Remande de Tarefas                 │
│ ├─ Detectar desconexão de worker                       │
│ ├─ Identificar tarefas pendentes do worker             │
│ ├─ Remande automático para outro worker                │
│ └─ Log de falhas                                       │
├─────────────────────────────────────────────────────────┤
│ FASE 4: Sprint 3 (Opcional - Futuro)                   │
│ └─ P2P Master-to-Master (8-10h)                        │
└─────────────────────────────────────────────────────────┘
```

---

## 📌 Estado Atual da Implementação

### Já implementado
- Protocolo ALIVE, QUERY, NO_TASK, STATUS/ACK
- TaskManager com persistência, histórico e remande básico
- Detecção de worker offline por monitor do master
- Redirecionamento entre farms via REQUEST_HELP / REDIRECT
- Eleição de master entre workers conectados ao mesmo master
- Failback do master original com redirecionamento de volta

### Parcial / diferente do plano original
- A estratégia de remande não segue a prioridade descrita no plano original
- O plano fala em 3 timeouts consecutivos; a implementação usa timeout de heartbeat + monitor periódico
- O helper `get_available_workers()` ainda merece revisão porque a lógica de disponibilidade não está alinhada ao campo `current_task_id`

### Ainda não coberto exatamente como estava descrito no plano
- Contador explícito de timeouts consecutivos por worker
- Política formal de remande por prioridade (worker que completou tarefa / menor carga / fila)
- Log específico com a razão de remande seguindo o formato planejado

### Extensões extras entregues depois do plano original
- Eleição por maior espaço livre em disco
- Failback do master original
- Persistência com carga opcional via `LOAD_STATE`

---

## 📐 FASE 1 - Corrigir Protocolos (2-3 horas)

### 1.1 Novo Protocolo de Apresentação (ALIVE)

**Worker envia:**
```json
{
  "WORKER": "ALIVE",
  "WORKER_UUID": "Worker_1",
  "SERVER_UUID": "Master_A"
}
```

**Master responde:**
```json
{
  "SERVER_UUID": "Master_A",
  "TASK": "HEARTBEAT",
  "RESPONSE": "ALIVE"
}
```

### 1.2 Protocolo de Distribuição de Tarefas (Separado)

**Master envia:**
- Com tarefa:
```json
{
  "TASK": "QUERY",
  "USER": "system",
  "TASK_ID": "uuid-1234",
  "OPERATION": "soma",
  "VALUES": [1, 2]
}
```

- Sem tarefa:
```json
{
  "TASK": "NO_TASK"
}
```

### 1.3 Protocolo de Reporte de Status

**Worker envia:**
```json
{
  "STATUS": "OK",
  "TASK_ID": "uuid-1234",
  "WORKER_UUID": "Worker_1",
  "RESULT": 3
}
```

**Master responde:**
```json
{
  "STATUS": "ACK",
  "TASK_ID": "uuid-1234",
  "WORKER_UUID": "Worker_1"
}
```

---

## 📊 FASE 2 - Rastreamento de Tarefas (1-2 horas)

### 2.1 Estrutura de Dados

```python
class Task:
    id: str  # UUID
    status: str  # pending, in_progress, completed, failed
    operation: str  # soma, multiplicacao, sleep
    values: list
    assigned_worker: Optional[str]  # WORKER_UUID
    start_time: Optional[float]
    end_time: Optional[float]
    result: Optional[Any]
    retries: int
    created_at: float
```

### 2.2 TaskManager

Responsabilidades:
- Criar tarefa com ID único
- Atualizar status
- Rastrear qual worker está fazendo qual tarefa
- Histórico de tentativas
- Reassign automático em caso de falha
- Consultar tarefas por status/worker

---

## 🔍 FASE 3 - Detecção e Remande (1-2 horas)

### 3.1 Detecção de Queda

```python
# Quando worker desconecta:
1. Identificar todas as tarefas com:
   - status = "in_progress"
   - assigned_worker = "Worker_1"

2. Mudar para status = "pending"

3. Remande para próximo worker disponível

4. Log: "Worker_1 caiu. Remandada tarefa uuid-1234 para Worker_2"
```

### 3.2 Heartbeat/ALIVE com Timeout

```python
# Master monitora cada worker:
- Espera por ALIVE a cada 5-10 segundos
- Se timeout → worker offline
- Se 3 timeouts consecutivos → worker morto
- Busca tarefas "em execução" desse worker
- Remande para outro
```

**Observação da implementação atual:** o master executa monitor periódico com `HEARTBEAT_TIMEOUT` e `WORKER_CHECK_INTERVAL`, mas não mantém um contador de 3 timeouts consecutivos.

### 3.3 Reassign Strategy

```python
# Ordem de prioridade:
1. Remande para worker que completou uma tarefa
2. Remande para worker com menos tarefas
3. Fila de espera se nenhum worker disponível
4. Log: task_id, old_worker, new_worker, reason
```

**Observação da implementação atual:** o remande básico acontece quando um worker cai ou quando uma tarefa expira, mas sem a prioridade formal acima.

---

## 📁 Estrutura de Arquivos (Novo)

```
Worker-Master-P2P/
├── master.py (REFATORADO)
├── worker.py (REFATORADO)
├── common/
│   ├── protocol.py (SEM MUDANÇA)
│   ├── tasks.py (SEM MUDANÇA)
│   ├── task_manager.py (NOVO) ⭐
│   └── models.py (NOVO) ⭐
├── ANALISE_COMPLIANCE.md (CRIADO)
└── PLANO_IMPLEMENTACAO.md (ESTE ARQUIVO)
```

---

## 🔨 Arquivos a Modificar

### 1️⃣ `common/models.py` (NOVO)
- Classe `Task`
- Enums de status
- UUID generation

### 2️⃣ `common/task_manager.py` (NOVO)
- Classe `TaskManager`
- CRUD de tarefas
- Rastreamento worker-tarefa
- Lógica de reassign

### 3️⃣ `master.py` (REFATORADO)
- Novo protocolo ALIVE
- Integração com TaskManager
- Detecção de queda de worker
- Remande automático
- ACK de confirmação

### 4️⃣ `worker.py` (REFATORADO)
- Novo protocolo ALIVE
- Enviar WORKER_UUID, SERVER_UUID
- Novo ciclo: apresentação → aguardar tarefa → executar → reportar
- Reportar com TASK_ID e resultado

---

## ✅ Checklist de Testes

### Fase 1 Testes:
- [ ] Worker envia ALIVE correto
- [ ] Master responde ALIVE correto
- [ ] Master distribui tarefa com TASK_ID
- [ ] Worker executa e reporta STATUS correto
- [ ] Master envia ACK

### Fase 2 Testes:
- [ ] TaskManager cria tarefa com UUID
- [ ] Status atualiza: pending → in_progress → completed
- [ ] Histórico de tarefas mantido
- [ ] Consulta por status funciona

### Fase 3 Testes:
- [ ] Worker cai / conexão interrompida
- [ ] Master detecta queda
- [ ] Master remanda tarefa
- [ ] Novo worker executa e reporta
- [ ] Log de remande gerado

### Testes adicionais já cobertos na implementação atual:
- [x] Eleição de master por disco livre entre workers conectados ao mesmo master
- [x] Failback do master original
- [x] Redirecionamento de workers durante eleição/failback

---

## 📋 Estimativas de Tempo

| Tarefa | Tempo | Prioridade |
|--------|-------|-----------|
| Criar models.py | 30 min | 🔴 Critical |
| Criar task_manager.py | 45 min | 🔴 Critical |
| Refatorar master.py | 1h | 🔴 Critical |
| Refatorar worker.py | 45 min | 🔴 Critical |
| Testes Phase 1 | 30 min | 🔴 Critical |
| Testes Phase 2 | 30 min | 🟠 High |
| Testes Phase 3 | 1h | 🟠 High |
| **TOTAL** | **~5h** | |

**Nota:** essas estimativas descrevem o esforço original do plano. A implementação atual já extrapolou o escopo com eleição e failback P2P.

---

## 🚀 Próximo Passo

**Estado da implementação:** o núcleo do plano está implementado. O que permanece em aberto é apenas o refinamento das regras de remande e timeout para ficar exatamente igual ao texto original.

**Pendências se quisermos fechar 100% com o plano original:**

1. Implementar contador explícito de 3 timeouts consecutivos por worker.
2. Ajustar `get_available_workers()` e o critério de remande para usar `current_task_id`/carga real.
3. Formalizar o log de remande com o motivo exato da reatribuição.
