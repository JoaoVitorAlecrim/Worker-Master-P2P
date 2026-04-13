import socket
import threading
from queue import Queue
from common.protocol import send_json, recv_json_line

# Pra executar é só abrir um terminal e digitar python worker.py. E depois abrir outro terminal e digitar python master.py. 


HOST = "0.0.0.0"
PORT = 5000
SERVER_UUID = "Master_A"


class MasterServer:
    def __init__(self):
        self.task_queue = Queue()

        # tarefas iniciais
        self.load_tasks()

    def load_tasks(self):
        tasks = [
            {"operation": "soma", "values": [2, 3]},
            {"operation": "multiplicacao", "values": [4, 5]},
            {"operation": "sleep", "values": [2]},
            {"operation": "soma", "values": [10, 20]}
        ]

        for task in tasks:
            self.task_queue.put(task)

    def handle_client(self, conn, addr):
        print(f"[MASTER] Worker conectado: {addr}")

        sock_file = conn.makefile("r")

        try:
            while True:
                data = recv_json_line(sock_file)

                if data is None:
                    print(f"[MASTER] Worker desconectado: {addr}")
                    break

                task_type = data.get("TASK")

                if task_type == "HEARTBEAT":
                    send_json(conn, {
                        "SERVER_UUID": SERVER_UUID,
                        "TASK": "HEARTBEAT",
                        "RESPONSE": "ALIVE"
                    })

                    # envia tarefa se houver
                    if not self.task_queue.empty():
                        task = self.task_queue.get()

                        send_json(conn, {
                            "TASK": "EXECUTE",
                            "DATA": task
                        })

                        print(f"[MASTER] Enviou tarefa para {addr}: {task}")

                elif task_type == "RESULT":
                    print(f"[MASTER] Resultado recebido de {addr}: {data.get('RESULT')}")

        except Exception as e:
            print(f"[MASTER] Erro: {e}")

        finally:
            conn.close()

    def start(self):
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.bind((HOST, PORT))
        server.listen()

        print(f"[MASTER] Rodando em {HOST}:{PORT}")

        while True:
            conn, addr = server.accept()

            thread = threading.Thread(
                target=self.handle_client,
                args=(conn, addr),
                daemon=True
            )
            thread.start()


if __name__ == "__main__":
    MasterServer().start()