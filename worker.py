import socket
import time
from common.protocol import send_json, recv_json_line
from common.tasks import execute_task

MASTER_HOST = "127.0.0.1"
MASTER_PORT = 5000
WORKER_ID = "Worker_1"

HEARTBEAT_INTERVAL = 5
RECONNECT_DELAY = 3
SOCKET_TIMEOUT = 15


class WorkerClient:
    def run(self) -> None:
        while True:
            try:
                print(f"[{WORKER_ID}] Tentando conectar ao Master...")

                with socket.create_connection((MASTER_HOST, MASTER_PORT), timeout=10) as sock:
                    sock.settimeout(SOCKET_TIMEOUT)
                    sock_file = sock.makefile("r", encoding="utf-8")

                    print(f"[{WORKER_ID}] Conectado ao Master.")

                    while True:
                        heartbeat = {
                            "WORKER_ID": WORKER_ID,
                            "TASK": "HEARTBEAT"
                        }

                        send_json(sock, heartbeat)
                        print(f"[{WORKER_ID}] Heartbeat enviado.")

                        response = recv_json_line(sock_file)

                        if response is None:
                            print(f"[{WORKER_ID}] Conexão encerrada pelo Master.")
                            break

                        print(f"[{WORKER_ID}] Resposta recebida: {response}")

                        if response.get("TASK") == "HEARTBEAT_ACK":
                            if response.get("RESPONSE") == "ALIVE":
                                print(f"[{WORKER_ID}] Master está ALIVE.")

                            if response.get("HAS_TASK"):
                                task = response.get("DATA")
                                print(f"[{WORKER_ID}] Executando tarefa: {task}")

                                result = execute_task(task)

                                result_message = {
                                    "WORKER_ID": WORKER_ID,
                                    "TASK": "RESULT",
                                    "RESULT": result
                                }

                                send_json(sock, result_message)
                                print(f"[{WORKER_ID}] Resultado enviado: {result}")

                        elif response.get("TASK") == "ERROR":
                            print(f"[{WORKER_ID}] Erro recebido: {response.get('MESSAGE')}")

                        time.sleep(HEARTBEAT_INTERVAL)

            except socket.timeout:
                print(f"[{WORKER_ID}] Timeout de conexão/comunicação. Reconectando...")
                time.sleep(RECONNECT_DELAY)
            except (ConnectionRefusedError, OSError) as exc:
                print(f"[{WORKER_ID}] Master indisponível: {exc}")
                time.sleep(RECONNECT_DELAY)
            except KeyboardInterrupt:
                print(f"\n[{WORKER_ID}] Encerrado pelo usuário.")
                break


if __name__ == "__main__":
    WorkerClient().run()