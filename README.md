# Worker-Master-P2P

Projeto de Faculdade da Disciplina de Arquitetura de Sistemas Distribuídos

---

## 📚 Documentação

Toda a análise, planejamento e resultados de testes estão organizados na pasta **`docs/`**:

### 🎯 Comece por aqui:
- **[📖 INDEX.md](docs/INDEX.md)** - Guia de navegação da documentação

### 📋 Documentos principais (Sprints 1-2):
1. [ANALISE_COMPLIANCE.md](docs/ANALISE_COMPLIANCE.md) - Análise da implementação original
2. [RESUMO_PROBLEMAS.md](docs/RESUMO_PROBLEMAS.md) - Comparativa visual do que foi corrigido
3. [PLANO_IMPLEMENTACAO.md](docs/PLANO_IMPLEMENTACAO.md) - Roadmap das correções
4. [TESTE_GUIDE.md](docs/TESTE_GUIDE.md) - Como testar o sistema
5. [VERIFICACAO_FINAL.md](docs/VERIFICACAO_FINAL.md) - Resultados finais dos testes

### 📋 Sprint 3 (Farm-to-Farm Extension):
6. [SPRINT3_PROTOCOL.md](docs/SPRINT3_PROTOCOL.md) - Protocolo de negociação entre farms
7. [SPRINT3_TESTING.md](docs/SPRINT3_TESTING.md) - Testes de redirecionamento e promoção

---

## ✅ Status

### Sprints 1-2 (Requisitos do Professor)
- **Protocolo ALIVE:** ✅ Implementado e testado
- **Rastreamento de Tarefas:** ✅ Implementado e testado  
- **Detecção de Falha & Reassignment:** ✅ Implementado e testado
- **Compliance:** 15% → **100%**

### Sprint 3 (Farm-to-Farm Extension)
- **Farm Negotiation (REQUEST_HELP):** ✅ Implementado
- **Worker Redirection (REDIRECT):** ✅ Implementado
- **Auto-Promotion:** ✅ Implementado
- **Eleição de Master:** ✅ 4 falhas consecutivas de heartbeat disparam eleição entre os workers conectados; vence quem tiver mais espaço livre em disco
- **Failback do Master Original:** ✅ O worker promovido monitora o retorno do master original, redireciona os workers e volta ao papel de worker
- **State Handoff & Persistence:** ✅ Implementado
- **Auth Token (optional):** ✅ Implementado
- **Status:** Pronto para Testes ✅

---

## 🚀 Quick Start

```bash
# Terminal 1: Master
python master.py

# Terminal 2: Worker 1
python worker.py Worker_1 Master_A

# Terminal 3: Worker 2 (opcional)
python worker.py Worker_2 Master_A
```

### Iniciar sem carregar estado salvo

Se houver um arquivo de estado (`data/tasks_{SERVER_UUID}.json`) e você quiser forçar um start limpo (recarregar as tasks iniciais), defina a variável de ambiente `LOAD_STATE=0` antes de iniciar o `master`.

PowerShell:

```powershell
$env:LOAD_STATE=0
python master.py
```

Linux/macOS:

```bash
LOAD_STATE=0 python master.py
```

### Teste de eleição

Para validar a regra de eleição do novo master entre workers conectados ao mesmo master:

```powershell
.\.venv\Scripts\python.exe tests\run_election_test.py
```

Ver [docs/TESTE_GUIDE.md](docs/TESTE_GUIDE.md) para mais detalhes.

---

## 🔌 Uso da mesma porta (`MASTER_PORT`)

Existem várias formas seguras de operar dois ou mais `Master` usando o mesmo número de porta. Resumo rápido:

- **Máquinas diferentes (hosts distintos):** cada host tem sua própria pilha TCP/IP — portanto, Masters em hosts diferentes podem usar a mesma `MASTER_PORT` sem conflito.
- **Mesmo host, IPs diferentes:** se a máquina tem múltiplos endereços IP locais, cada processo pode dar `bind` em um IP distinto com a mesma porta.
- **Mesmo host, portas distintas:** a solução mais simples para testes locais é usar portas diferentes (ex.: `5100` e `5101`).
- **Containers / Namespaces de rede:** executar cada Master em um container (Docker) ou namespace separa as pilhas de rede e permite reutilizar a mesma porta por container.
- **Proxy / Multiplexer:** colocar um proxy na porta única e rotear para Masters distintos por hostname/URI — útil para produção.
- **SO_REUSEPORT / balanceamento:** existe `SO_REUSEPORT` em Linux para distribuição de conexões entre processos, mas não é uma solução portátil (não funciona igual no Windows) e não é recomendada aqui.

Exemplos práticos (copiáveis):

1) Dois Masters em hosts distintos (mesma porta `5100`):

```bash
# No Host A
MASTER_PORT=5100 SERVER_UUID=Master_A python master.py

# No Host B (outro host - mesma porta ok)
MASTER_PORT=5100 SERVER_UUID=Master_B python master.py
```

2) Dois Masters na mesma máquina (usar portas diferentes):

```bash
# Terminal 1
MASTER_PORT=5100 SERVER_UUID=Master_A python master.py

# Terminal 2
MASTER_PORT=5101 SERVER_UUID=Master_B python master.py
```

3) Rodando Masters em containers (mesma porta por container):

```bash
# build e rodar container para Master A
docker build -t master-a .
docker run -e MASTER_PORT=5100 -e SERVER_UUID=Master_A master-a

# container para Master B
docker build -t master-b .
docker run -e MASTER_PORT=5100 -e SERVER_UUID=Master_B master-b
```

Observações rápidas:
- Workers fazem conexões de saída para `master_host:master_port` — muitos workers podem conectar ao mesmo `MASTER_PORT` sem problema.
- No Windows não conte com `SO_REUSEPORT` para compartilhar escuta de TCP entre processos; prefira portas distintas ou containers.


## 📁 Estrutura

```
Worker-Master-P2P/
├── docs/                   ← 📚 DOCUMENTAÇÃO (comece aqui)
│   ├── INDEX.md           (navegação)
│   ├── ANALISE_COMPLIANCE.md (Sprints 1-2)
│   ├── RESUMO_PROBLEMAS.md
│   ├── PLANO_IMPLEMENTACAO.md
│   ├── TESTE_GUIDE.md
│   ├── VERIFICACAO_FINAL.md
│   ├── SPRINT3_PROTOCOL.md (Sprint 3)
│   └── SPRINT3_TESTING.md
├── tests/
│   ├── run_promotion_test.py
│   └── run_redirect_integration.py
├── common/
│   ├── models.py          (NOVO)
│   ├── task_manager.py    (NOVO)
│   ├── protocol.py
│   └── tasks.py
├── master.py              (refatorado)
├── worker.py              (refatorado)
└── README.md              (este arquivo)
```
