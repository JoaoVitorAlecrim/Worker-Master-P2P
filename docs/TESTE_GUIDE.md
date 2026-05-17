# 🚀 Guia de Teste - P2P Worker-Master System (Refatorado)

## 📋 Mudanças Implementadas

✅ **Sprint 1-2 Refatorado (Protocolos Corretos)**
- ✓ Protocolo ALIVE (apresentação de worker)
- ✓ Separação: Apresentação → Distribuição → Execução → Reporte
- ✓ ACK de confirmação
- ✓ WORKER_UUID em vez de WORKER_ID
- ✓ SERVER_UUID para workers emprestados
- ✓ TASK_ID para rastreamento

✅ **Rastreamento de Tarefas (Feedback Professor)**
- ✓ Cada tarefa tem ID único (UUID)
- ✓ Estados: pending → in_progress → completed/failed
- ✓ Histórico completo de eventos
- ✓ Saber qual worker faz qual tarefa

✅ **Detecção e Remande (Feedback Professor)**
- ✓ Detecta queda de worker (timeout ou desconexão)
- ✓ Identifica tarefas em execução do worker
- ✓ Remande automático para outro worker
- ✓ Log completo de falhas

✅ **Sprint 3 Expandido (P2P entre Masters)**
- ✓ Eleição de master entre workers conectados ao mesmo master
- ✓ Critério de desempate por maior espaço livre em disco
- ✓ Failback do master original com redirecionamento dos workers

---

## 🧪 Como Testar

### Pré-requisitos
```bash
cd c:\Users\morai\Documents\estudos-programação\p2p\Worker-Master-P2P
# Nenhuma dependência externa além da stdlib Python
```

### Terminal 1: Iniciar Master
```bash
python master.py
```

**Saída esperada:**
```
[...] [MASTER] INFO: 🚀 Master Server iniciado em 0.0.0.0:5000
[...] [MASTER] INFO:    UUID: Master_A
[...] [MASTER] INFO: Carregando 60 tarefas iniciais...
[...] [MASTER] INFO: ✓ 60 tarefas carregadas
[...] [MASTER] INFO: Worker monitor iniciado
```

### Terminal 2: Iniciar Worker 1
```bash
python worker.py Worker_1 Master_A
```

**Saída esperada:**
```
[...] [WORKER] INFO: Tentando conectar ao Master (127.0.0.1:5000)...
[...] [WORKER] INFO: ✓ Conectado ao Master
[...] [WORKER] INFO: ✓ Apresentação enviada (ALIVE)
[...] [WORKER] INFO: → Tarefa <task_id> (soma)
[...] [WORKER] INFO: ✓ Tarefa <task_id> completada (resultado: 3)
```

### Terminal 3: Iniciar Worker 2 (Opcional)
```bash
python worker.py Worker_2 Master_A
```

---

## 🧬 Cenários de Teste

### Teste 1: Distribuição Normal
**O que testar:** Múltiplos workers executando tarefas concorrentemente

```bash
# Terminal 1: Master
python master.py

# Terminal 2: Worker 1
python worker.py Worker_1 Master_A

# Terminal 3: Worker 2
python worker.py Worker_2 Master_A

# Esperado: Ambos workers recebem tarefas e as executam
# Log: "Stats - Pending: X, In Progress: Y, Completed: Z"
```

### Teste 2: Detecção de Queda de Worker (⭐ NOVO)
**O que testar:** Worker cai, tarefas são remandadas

```bash
# Terminal 1: Master
python master.py

# Terminal 2: Worker 1
python worker.py Worker_1 Master_A
# [Deixar executar algumas tarefas...]
# [Depois: Ctrl+C para matar worker]

# Esperado:
# - Master detecta: "⚠ Worker Worker_1 desconectado!"
# - Master remanda: "⚠ Remandadas X tarefas de Worker_1"
# - Tarefas reaparecem na fila

# Terminal 3: Worker 2
python worker.py Worker_2 Master_A
# [Agora o Worker 2 vai executar as tarefas remandadas]
```

### Teste 3: Timeout de Tarefa (⭐ NOVO)
**O que testar:** Tarefa que demora muito tempo

```bash
# No master.py, verificar logs a cada 5 segundos:
# - Tarefas "sleep" que expiram voltam à fila
# - Automaticamente remandadas para outro worker

# Log esperado:
# "[MASTER] ⚠ X tarefas expiradas detectadas e remandadas"
```

### Teste 4: Worker Emprestado (Para Sprint 3)
**O que testar:** Worker de outro Master

```bash
# Terminal 1: Master A
python master.py

# Terminal 2: Worker do Master B (emprestado)
python worker.py Worker_B Master_B
# Nota: Mesmo conectando a Master A, envia SERVER_UUID=Master_B

# Log esperado:
# "[MASTER] ✓ Worker Worker_B apresentado (origin: Master_B)"
```

### Teste 5: Eleição de Master (⭐ NOVO)
**O que testar:** Master cai, workers elegem o nó com mais disco livre

```bash
# Terminal 1: Master
python master.py

# Terminal 2: Worker 1
python worker.py Worker_1 Master_A

# Terminal 3: Worker 2
python worker.py Worker_2 Master_A

# Forçar queda do master original

# Esperado:
# - Após 4 falhas consecutivas de conexão, um worker dispara eleição
# - O worker com maior FREE_DISK_BYTES assume como novo master
# - Os demais workers reapontam a conexão
```

### Teste 6: Failback do Master Original (⭐ NOVO)
**O que testar:** Master original volta e o cluster retorna para ele

```bash
# 1. Suba o master original e os workers
# 2. Force a queda do master original
# 3. Aguarde a eleição do novo master
# 4. Restaure o master original

# Esperado:
# - O master promovido detecta o retorno do original
# - Os workers recebem REDIRECT de volta
# - O nó promovido volta ao papel de worker
```

---

## 📊 O que Observar nos Logs

### Ciclo Normal (Por tarefa)
```
[...] [MASTER] INFO: → Tarefa atribuída a Worker_1 (soma)
[...] [WORKER] INFO: → Tarefa recebida: <id> (soma)
[...] [WORKER] INFO: Executando tarefa...
[...] [WORKER] INFO: ✓ Resultado enviado: 3
[...] [MASTER] INFO: ✓ Tarefa <id> completada por Worker_1: 3
```

### Eleição de Master (Novo)
```
[... ] [WORKER] WARNING: Falha de conexão ao master (4/4)
[... ] [WORKER] WARNING: Eleição disparada após 4 falhas. Vencedor: Worker_X (.... bytes livres)
[... ] [WORKER] INFO: ↪ Reapontando conexão para o novo master Worker_X em 127.0.0.1:5000
```

### Failback do Master Original (Novo)
```
[... ] [WORKER] WARNING: Master original voltou a responder em 127.0.0.1:5000. Iniciando failback...
[... ] [MASTER] INFO: Conexão recebida de ('127.0.0.1', ....)
[... ] [MASTER] INFO: Redirecionando workers de volta ao master original
```

### Queda de Worker (⭐ NOVO)
```
[...] [MASTER] WARNING: ⚠ Worker Worker_1 desconectado!
[...] [MASTER] WARNING: ⚠ Remandadas 5 tarefas de Worker_1:
[...] [MASTER] WARNING:   - <id1>
[...] [MASTER] WARNING:   - <id2>
...
```

### Estatísticas (A cada 5 segundos)
```
[...] [MASTER] INFO: Stats - Pending: 45, In Progress: 3, Completed: 12, Workers: 2/2
```

---

## 🔍 Arquivos Importantes

- **common/models.py** - Estruturas de dados (Task, Worker, TaskLog)
- **common/task_manager.py** - Gerenciador de tarefas e workers
- **master.py** - Servidor Master com detecção de falhas
- **worker.py** - Cliente Worker com novo protocolo

---

## ⚙️ Configuração

Editar `master.py` ou `worker.py` para ajustar:

```python
# Master
PORT = 5000
HEARTBEAT_TIMEOUT = 15  # Segundos até worker offline
TASK_TIMEOUT = 30  # Segundos até tarefa expirada
WORKER_CHECK_INTERVAL = 5  # Frequência de checks

# Worker
MASTER_HOST = "127.0.0.1"
MASTER_PORT = 5000
HEARTBEAT_INTERVAL = 10  # Frequência de ALIVE
PROMOTE_THRESHOLD = 4    # Falhas antes da eleição
FAILBACK_GRACE_SECONDS = 5  # Janela antes de encerrar o master promovido
SOCKET_TIMEOUT = 15  # Timeout de operações
```

---

## 📝 Protocolo Implementado

### Apresentação (ALIVE)
```json
// Worker → Master
{
  "WORKER": "ALIVE",
  "WORKER_UUID": "Worker_1",
  "SERVER_UUID": "Master_A"
}

// Master → Worker (HEARTBEAT)
{
  "SERVER_UUID": "Master_A",
  "TASK": "HEARTBEAT",
  "RESPONSE": "ALIVE"
}
```

### Distribuição
```json
// Master → Worker (Com tarefa)
{
  "TASK": "QUERY",
  "TASK_ID": "uuid-123",
  "OPERATION": "soma",
  "VALUES": [1, 2]
}

// Master → Worker (Sem tarefa)
{
  "TASK": "NO_TASK"
}
```

### Eleição / Failback
```json
// Master → Worker (redirecionamento para o novo master ou retorno ao original)
{
  "TASK": "REDIRECT",
  "TARGET_HOST": "127.0.0.1",
  "TARGET_PORT": 5000,
  "TARGET_SERVER_UUID": "Master_A"
}
```

### Reporte
```json
// Worker → Master (Sucesso)
{
  "STATUS": "OK",
  "TASK_ID": "uuid-123",
  "WORKER_UUID": "Worker_1",
  "RESULT": 3
}

// Master → Worker (ACK)
{
  "STATUS": "ACK",
  "TASK_ID": "uuid-123",
  "WORKER_UUID": "Worker_1"
}
```

---

## ✅ Checklist de Funcionalidades

- [x] Protocolo ALIVE correto
- [x] WORKER_UUID em lugar de WORKER_ID
- [x] SERVER_UUID para workers emprestados
- [x] TASK_ID único para cada tarefa
- [x] Separação: apresentação vs distribuição vs execução
- [x] ACK de confirmação
- [x] Rastreamento completo de tarefas
- [x] Histórico de eventos (logs)
- [x] Detecção automática de worker offline
- [x] Remande automático de tarefas
- [x] Monitor de saúde (thread separada)
- [x] Logging estruturado
- [x] Eleição de master por disco livre
- [x] Failback do master original

---

## 🐛 Troubleshooting

**Erro: "Conexão recusada"**
- Verificar se Master está rodando
- Verificar se porta 5000 está livre

**Worker não recebe tarefas**
- Verificar logs do Master para "Nenhuma tarefa"
- Verificar se fila foi carregada com load_initial_tasks()

**Tarefas não são remandadas**
- Verificar HEARTBEAT_TIMEOUT (padrão 15s)
- Verificar TASK_TIMEOUT (padrão 30s)
- Verificar se worker monitor thread está rodando

---

## 🎯 Próximos Passos (Sprint 3)

Para evoluir o comportamento atual:
1. Formalizar contador de 3 timeouts consecutivos por worker
2. Refinar a estratégia de remande por prioridade/carga
3. Ajustar o log de remande para registrar old_worker, new_worker e reason
