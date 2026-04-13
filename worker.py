import socket
import time
from common.protocol import send_json, recv_json_line
from common.tasks import execute_task

# Pra executar é só abrir um terminal e digitar python worker.py. E depois abrir outro terminal e digitar python master.py. 

MASTER_HOST = "127.0.0.1"
MASTER_PORT = 5000
WORKER_ID = "Worker_1"

INTERVAL = 5
RECONNECT_DELAY = 3


class WorkerClient:
    def run(self):
        while True:
            try:
                print(f"[{WORKER_ID}] Conectando ao Master...")

                with socket.create_connection((MASTER_HOST, MASTER_PORT)) as sock:
                    sock_file = sock.makefile("r")

                    print(f"[{WORKER_ID}] Conectado!")

                    while True:
                        # heartbeat
                        send_json(sock, {
                            "WORKER_ID": WORKER_ID,
                            "TASK": "HEARTBEAT"
                        })

                        response = recv_json_line(sock_file)

                        if response is None:
                            break

                        if response.get("TASK") == "HEARTBEAT":
                            print(f"[{WORKER_ID}] ALIVE")

                        # verifica se chegou tarefa
                        next_msg = recv_json_line(sock_file)

                        if next_msg and next_msg.get("TASK") == "EXECUTE":
                            task = next_msg.get("DATA")
                            print(f"[{WORKER_ID}] Executando: {task}")

                            result = execute_task(task)

                            send_json(sock, {
                                "TASK": "RESULT",
                                "RESULT": result
                            })

                            print(f"[{WORKER_ID}] Resultado enviado: {result}")

                        time.sleep(INTERVAL)

            except Exception as e:
                print(f"[{WORKER_ID}] OFFLINE: {e}")
                time.sleep(RECONNECT_DELAY)


if __name__ == "__main__":
    WorkerClient().run()