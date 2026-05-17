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
