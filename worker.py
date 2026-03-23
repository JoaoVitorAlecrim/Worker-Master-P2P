import json
import socket
import time
from typing import Dict, Any

#Grupo João Vitor de Morais, Bryan Barros e Pedro Vinicius
#para rodar o código basta abrir dois terminais, no primeiro digitar: python master.py
#no segundo terminal digitar python worker.py

MASTER_HOST = "127.0.0.1"
MASTER_PORT = 5000
SERVER_UUID = "Master_A"
INTERVALO = 10


def send_json(sock: socket.socket, data: Dict[str, Any]) -> None:
    message = json.dumps(data) + "\n"
    sock.sendall(message.encode("utf-8"))


def recv_json_line(sock_file) -> Dict[str, Any] | None:
    line = sock_file.readline()
    if not line:
        return None

    line = line.strip()
    if not line:
        return None

    try:
        return json.loads(line)
    except json.JSONDecodeError:
        return {"ERROR": "JSON_INVALIDO"}


def heartbeat_loop() -> None:
    while True:
        try:
            print("[WORKER] Tentando conectar ao Master...")

            with socket.create_connection((MASTER_HOST, MASTER_PORT), timeout=5) as sock:
                sock.settimeout(None)
                sock_file = sock.makefile("r", encoding="utf-8")
                print("[WORKER] Conectado ao Master.")

                while True:
                    payload = {
                        "SERVER_UUID": SERVER_UUID,
                        "TASK": "HEARTBEAT"
                    }

                    send_json(sock, payload)
                    print(f"[WORKER] Enviado: {payload}")

                    response = recv_json_line(sock_file)

                    if response is None:
                        print("[WORKER] Conexão encerrada pelo Master.")
                        break

                    if response.get("RESPONSE") == "ALIVE":
                        print('[WORKER] Status: ALIVE')
                    else:
                        print(f"[WORKER] Resposta inesperada: {response}")

                    time.sleep(INTERVALO)

        except (ConnectionRefusedError, socket.timeout, OSError) as e:
            print(f"[WORKER] Status: OFFLINE - Tentando reconectar. Motivo: {e}")
            time.sleep(5)
        except KeyboardInterrupt:
            print("\n[WORKER] Encerrado.")
            break


if __name__ == "__main__":
    heartbeat_loop()