import socket
import threading
from queue import Queue, Empty

from common.protocol import send_json, recv_json_line
from config import MASTER_PORT, SOCKET_TIMEOUT


class MasterServer:
    def __init__(self, host: str = "0.0.0.0", port: int = MASTER_PORT, server_id: str = "Master_A") -> None:
        self.host = host
        self.port = port
        self.server_id = server_id
        self.running = False
        self.task_queue = Queue()
        self.load_tasks()

    def load_tasks(self) -> None:
        tasks = [
            {"operation": "soma", "values": [2, 3]},
            {"operation": "multiplicacao", "values": [4, 5]},
            {"operation": "sleep", "values": [2]},
            {"operation": "soma", "values": [10, 20]},
            {"operation": "multiplicacao", "values": [3, 7]},
        ]

        for task in tasks:
            self.task_queue.put(task)

    def get_next_task(self):
        try:
            return self.task_queue.get_nowait()
        except Empty:
            return None

    def handle_client(self, conn: socket.socket, addr) -> None:
        print(f"[{self.server_id}] Worker conectado: {addr}")

        try:
            conn.settimeout(SOCKET_TIMEOUT)

            with conn:
                sock_file = conn.makefile("r", encoding="utf-8")

                while self.running:
                    data = recv_json_line(sock_file)

                    if data is None:
                        print(f"[{self.server_id}] Worker desconectado: {addr}")
                        break

                    print(f"[{self.server_id}] Recebido de {addr}: {data}")

                    task_type = data.get("TASK")

                    if task_type == "HEARTBEAT":
                        next_task = self.get_next_task()

                        response = {
                            "SERVER_ID": self.server_id,
                            "TASK": "HEARTBEAT_ACK",
                            "RESPONSE": "ALIVE",
                            "HAS_TASK": next_task is not None,
                            "DATA": next_task
                        }

                        send_json(conn, response)
                        print(f"[{self.server_id}] Enviado para {addr}: {response}")

                    elif task_type == "RESULT":
                        print(f"[{self.server_id}] Resultado recebido de {data.get('WORKER_ID')}: {data.get('RESULT')}")

                    else:
                        send_json(conn, {
                            "SERVER_ID": self.server_id,
                            "TASK": "ERROR",
                            "MESSAGE": "TASK_NAO_SUPORTADA"
                        })

        except socket.timeout:
            print(f"[{self.server_id}] Timeout com {addr}")
        except Exception as exc:
            print(f"[{self.server_id}] Erro com {addr}: {exc}")

    def start(self) -> None:
        self.running = True

        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.bind((self.host, self.port))
        server.listen()

        print(f"[{self.server_id}] Rodando em {self.host}:{self.port}")

        try:
            while self.running:
                conn, addr = server.accept()

                thread = threading.Thread(
                    target=self.handle_client,
                    args=(conn, addr),
                    daemon=True
                )
                thread.start()

        except KeyboardInterrupt:
            print(f"\n[{self.server_id}] Encerrado pelo usuário.")
        except Exception as exc:
            print(f"[{self.server_id}] Erro no servidor: {exc}")
        finally:
            self.running = False
            server.close()


if __name__ == "__main__":
    master = MasterServer(
        host="0.0.0.0",
        port=MASTER_PORT,
        server_id="Master_A"
    )
    master.start()