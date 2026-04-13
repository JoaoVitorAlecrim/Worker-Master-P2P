import socket
import threading
from queue import Queue, Empty
from common.protocol import send_json, recv_json_line

HOST = "0.0.0.0"
PORT = 5000
SERVER_UUID = "Master_A"
SOCKET_TIMEOUT = 15


class MasterServer:
    def __init__(self) -> None:
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
        print(f"[MASTER] Worker conectado: {addr}")

        try:
            conn.settimeout(SOCKET_TIMEOUT)

            with conn:
                sock_file = conn.makefile("r", encoding="utf-8")

                while True:
                    data = recv_json_line(sock_file)

                    if data is None:
                        print(f"[MASTER] Worker desconectado: {addr}")
                        break

                    task_type = data.get("TASK")
                    print(f"[MASTER] Recebido de {addr}: {data}")

                    if task_type == "HEARTBEAT":
                        next_task = self.get_next_task()

                        response = {
                            "SERVER_UUID": SERVER_UUID,
                            "TASK": "HEARTBEAT_ACK",
                            "RESPONSE": "ALIVE",
                            "HAS_TASK": next_task is not None,
                            "DATA": next_task
                        }

                        send_json(conn, response)
                        print(f"[MASTER] Enviado para {addr}: {response}")

                    elif task_type == "RESULT":
                        print(f"[MASTER] Resultado de {addr}: {data.get('RESULT')}")

                    else:
                        response = {
                            "SERVER_UUID": SERVER_UUID,
                            "TASK": "ERROR",
                            "MESSAGE": "TASK_NAO_SUPORTADA"
                        }
                        send_json(conn, response)

        except socket.timeout:
            print(f"[MASTER] Timeout com {addr}")
        except Exception as exc:
            print(f"[MASTER] Erro com {addr}: {exc}")

    def start(self) -> None:
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.bind((HOST, PORT))
        server.listen()

        print(f"[MASTER] Rodando em {HOST}:{PORT}")

        try:
            while True:
                conn, addr = server.accept()

                worker_thread = threading.Thread(
                    target=self.handle_client,
                    args=(conn, addr),
                    daemon=True
                )
                worker_thread.start()

        except KeyboardInterrupt:
            print("\n[MASTER] Encerrado pelo usuário.")
        finally:
            server.close()


if __name__ == "__main__":
    MasterServer().start()