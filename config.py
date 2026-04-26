# IP inicial do Master original
INITIAL_MASTER_HOST = "192.168.0.10"
MASTER_PORT = 5000

# Porta usada pelos Workers para eleição
ELECTION_PORT = 6000

# Configuração deste nó
# Em cada Worker, mude o NODE_ID e NODE_HOST
NODE_ID = "Worker_1"
NODE_HOST = "192.168.0.11"

# Lista de Workers candidatos à eleição
# Coloque aqui os IPs reais dos Workers
WORKER_NODES = [
    {
        "id": "Worker_1",
        "host": "192.168.0.11",
        "election_port": 6000
    },
    {
        "id": "Worker_2",
        "host": "192.168.0.12",
        "election_port": 6000
    }
]

HEARTBEAT_INTERVAL = 5
RECONNECT_DELAY = 3
MAX_HEARTBEAT_FAILURES = 4
SOCKET_TIMEOUT = 10