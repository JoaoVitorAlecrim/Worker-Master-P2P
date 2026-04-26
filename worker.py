import shutil
import socket
import threading
import time
from typing import Dict, Any, List

from common.protocol import send_json, recv_json_line
from common.tasks import execute_task
from master import MasterServer

from config import (
    INITIAL_MASTER_HOST,
    MASTER_PORT,
    ELECTION_PORT,
    NODE_ID,
    NODE_HOST,
    WORKER_NODES,
    HEARTBEAT_INTERVAL,
    RECONNECT_DELAY,
    MAX_HEARTBEAT_FAILURES,
    SOCKET_TIMEOUT
)


class WorkerNode:
    def __init__(self) -> None:
        self.node_id = NODE_ID
        self.node_host = NODE_HOST

        self.current_master_host = INITIAL_MASTER_HOST
        self.current_master_port = MASTER_PORT

        self.heartbeat_failures = 0
        self.election_in_progress = False
        self.is_master = False
        self.local_master_started = False

        self.lock = threading.Lock()

    def get_free_disk_space(self) -> int:
        usage = shutil.disk_usage("/")
        return usage.free

    def start_election_listener(self) -> None:
        thread = threading.Thread(
            target=self.election_listener,
            daemon=True
        )
        thread.start()

    def election_listener(self) -> None:
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.bind(("0.0.0.0", ELECTION_PORT))
        server.listen()

        print(f"[{self.node_id}] Listener de eleição rodando em {self.node_host}:{ELECTION_PORT}")

        while True:
            try:
                conn, addr = server.accept()

                thread = threading.Thread(
                    target=self.handle_election_message,
                    args=(conn, addr),
                    daemon=True
                )
                thread.start()

            except Exception as exc:
                print(f"[{self.node_id}] Erro no listener de eleição: {exc}")

    def handle_election_message(self, conn: socket.socket, addr) -> None:
        try:
            conn.settimeout(SOCKET_TIMEOUT)

            with conn:
                sock_file = conn.makefile("r", encoding="utf-8")
                data = recv_json_line(sock_file)

                if data is None:
                    return

                task = data.get("TASK")

                if task == "ELECTION_REQUEST":
                    response = {
                        "TASK": "ELECTION_RESPONSE",
                        "NODE_ID": self.node_id,
                        "HOST": self.node_host,
                        "MASTER_PORT": MASTER_PORT,
                        "FREE_DISK": self.get_free_disk_space()
                    }

                    send_json(conn, response)
                    print(f"[{self.node_id}] Respondeu eleição para {addr}: {response}")

                elif task == "NEW_MASTER":
                    new_master_id = data.get("MASTER_ID")
                    new_master_host = data.get("MASTER_HOST")
                    new_master_port = data.get("MASTER_PORT", MASTER_PORT)

                    print(f"[{self.node_id}] Novo Master recebido: {new_master_id} em {new_master_host}:{new_master_port}")

                    with self.lock:
                        self.current_master_host = new_master_host
                        self.current_master_port = new_master_port
                        self.heartbeat_failures = 0
                        self.election_in_progress = False

                        if new_master_id == self.node_id:
                            self.become_master()
                        else:
                            self.is_master = False

                    send_json(conn, {
                        "TASK": "NEW_MASTER_ACK",
                        "NODE_ID": self.node_id,
                        "RESPONSE": "OK"
                    })

        except Exception as exc:
            print(f"[{self.node_id}] Erro tratando mensagem de eleição: {exc}")

    def send_election_request(self, node: Dict[str, Any]) -> Dict[str, Any] | None:
        try:
            with socket.create_connection(
                (node["host"], node["election_port"]),
                timeout=SOCKET_TIMEOUT
            ) as sock:
                sock.settimeout(SOCKET_TIMEOUT)
                sock_file = sock.makefile("r", encoding="utf-8")

                send_json(sock, {
                    "TASK": "ELECTION_REQUEST",
                    "FROM": self.node_id
                })

                response = recv_json_line(sock_file)
                return response

        except Exception as exc:
            print(f"[{self.node_id}] Não conseguiu consultar {node['id']}: {exc}")
            return None

    def broadcast_new_master(self, winner: Dict[str, Any]) -> None:
        for node in WORKER_NODES:
            try:
                with socket.create_connection(
                    (node["host"], node["election_port"]),
                    timeout=SOCKET_TIMEOUT
                ) as sock:
                    send_json(sock, {
                        "TASK": "NEW_MASTER",
                        "MASTER_ID": winner["NODE_ID"],
                        "MASTER_HOST": winner["HOST"],
                        "MASTER_PORT": MASTER_PORT
                    })

                    print(f"[{self.node_id}] Avisou {node['id']} sobre novo Master: {winner['NODE_ID']}")

            except Exception as exc:
                print(f"[{self.node_id}] Falha ao avisar {node['id']}: {exc}")

    def choose_winner(self, candidates: List[Dict[str, Any]]) -> Dict[str, Any]:
        candidates.sort(
            key=lambda item: (item["FREE_DISK"], item["NODE_ID"]),
            reverse=True
        )
        return candidates[0]

    def start_election(self) -> None:
        with self.lock:
            if self.election_in_progress:
                return

            self.election_in_progress = True

        print(f"[{self.node_id}] Iniciando eleição de novo Master...")

        candidates = []

        self_candidate = {
            "TASK": "ELECTION_RESPONSE",
            "NODE_ID": self.node_id,
            "HOST": self.node_host,
            "MASTER_PORT": MASTER_PORT,
            "FREE_DISK": self.get_free_disk_space()
        }

        candidates.append(self_candidate)

        for node in WORKER_NODES:
            if node["id"] == self.node_id:
                continue

            response = self.send_election_request(node)

            if response and response.get("TASK") == "ELECTION_RESPONSE":
                candidates.append(response)

        if not candidates:
            print(f"[{self.node_id}] Nenhum candidato encontrado.")
            with self.lock:
                self.election_in_progress = False
            return

        winner = self.choose_winner(candidates)

        print(f"[{self.node_id}] Candidatos:")
        for candidate in candidates:
            free_gb = candidate["FREE_DISK"] / (1024 ** 3)
            print(f" - {candidate['NODE_ID']} | Livre: {free_gb:.2f} GB")

        print(f"[{self.node_id}] Novo Master eleito: {winner['NODE_ID']}")

        self.broadcast_new_master(winner)

        with self.lock:
            self.current_master_host = winner["HOST"]
            self.current_master_port = MASTER_PORT
            self.heartbeat_failures = 0
            self.election_in_progress = False

            if winner["NODE_ID"] == self.node_id:
                self.become_master()
            else:
                self.is_master = False

    def become_master(self) -> None:
        if self.local_master_started:
            return

        self.is_master = True
        self.local_master_started = True

        print(f"[{self.node_id}] Assumindo papel de novo Master...")

        master = MasterServer(
            host="0.0.0.0",
            port=MASTER_PORT,
            server_id=self.node_id
        )

        thread = threading.Thread(
            target=master.start,
            daemon=True
        )
        thread.start()

        time.sleep(1)

    def heartbeat_loop(self) -> None:
        while True:
            try:
                with self.lock:
                    master_host = self.current_master_host
                    master_port = self.current_master_port

                print(f"[{self.node_id}] Tentando conectar ao Master {master_host}:{master_port}...")

                with socket.create_connection(
                    (master_host, master_port),
                    timeout=SOCKET_TIMEOUT
                ) as sock:
                    sock.settimeout(SOCKET_TIMEOUT)
                    sock_file = sock.makefile("r", encoding="utf-8")

                    print(f"[{self.node_id}] Conectado ao Master.")

                    with self.lock:
                        self.heartbeat_failures = 0

                    while True:
                        heartbeat = {
                            "WORKER_ID": self.node_id,
                            "TASK": "HEARTBEAT"
                        }

                        send_json(sock, heartbeat)
                        print(f"[{self.node_id}] Heartbeat enviado.")

                        response = recv_json_line(sock_file)

                        if response is None:
                            raise ConnectionError("Master encerrou a conexão.")

                        print(f"[{self.node_id}] Resposta recebida: {response}")

                        if response.get("TASK") == "HEARTBEAT_ACK":
                            with self.lock:
                                self.heartbeat_failures = 0

                            if response.get("HAS_TASK"):
                                task = response.get("DATA")
                                print(f"[{self.node_id}] Executando tarefa: {task}")

                                result = execute_task(task)

                                send_json(sock, {
                                    "WORKER_ID": self.node_id,
                                    "TASK": "RESULT",
                                    "RESULT": result
                                })

                                print(f"[{self.node_id}] Resultado enviado: {result}")

                        time.sleep(HEARTBEAT_INTERVAL)

            except Exception as exc:
                with self.lock:
                    self.heartbeat_failures += 1
                    failures = self.heartbeat_failures

                print(f"[{self.node_id}] Erro no heartbeat ({failures}/{MAX_HEARTBEAT_FAILURES}): {exc}")

                if failures >= MAX_HEARTBEAT_FAILURES:
                    self.start_election()

                time.sleep(RECONNECT_DELAY)

    def start(self) -> None:
        self.start_election_listener()
        self.heartbeat_loop()


if __name__ == "__main__":
    node = WorkerNode()
    node.start()