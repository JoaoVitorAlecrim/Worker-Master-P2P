import json
import socket
import threading
from typing import Dict, Any

#Grupo João Vitor de Morais, Bryan Barros e Pedro Vinicius
#para rodar o código basta abrir dois terminais, no primeiro digitar: python master.py
#no segundo terminal digitar python worker.py

HOST = "0.0.0.0"
PORT = 5001
SERVER_UUID = "Master_A"


def send_json(conn: socket.socket, data: Dict[str, Any]) -> None:
    message = json.dumps(data) + "\n"
    conn.sendall(message.encode("utf-8"))


def recv_json_line(conn_file) -> Dict[str, Any] | None:
    line = conn_file.readline()
    if not line:
        return None

    line = line.strip()
    if not line:
        return None

    try:
        return json.loads(line)
    except json.JSONDecodeError:
        return {"ERROR": "JSON_INVALIDO"}


def handle_client(conn: socket.socket, addr) -> None:
    print(f"[NOVA CONEXÃO] {addr}")

    try:
        with conn:
            conn_file = conn.makefile("r", encoding="utf-8")

            while True:
                data = recv_json_line(conn_file)

                if data is None:
                    print(f"[DESCONECTADO] {addr}")
                    break

                print(f"[RECEBIDO DE {addr}] {data}")

                if data.get("TASK") == "HEARTBEAT":
                    response = {
                        "SERVER_UUID": SERVER_UUID,
                        "TASK": "HEARTBEAT",
                        "RESPONSE": "ALIVE"
                    }
                    send_json(conn, response)
                    print(f"[ENVIADO PARA {addr}] {response}")
                else:
                    response = {
                        "SERVER_UUID": SERVER_UUID,
                        "TASK": data.get("TASK", "UNKNOWN"),
                        "RESPONSE": "UNSUPPORTED_TASK"
                    }
                    send_json(conn, response)
                    print(f"[ENVIADO PARA {addr}] {response}")

    except Exception as e:
        print(f"[ERRO {addr}] {e}")


def start_server() -> None:
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind((HOST, PORT))
    server.listen()

    print(f"[MASTER ATIVO] {SERVER_UUID} escutando em {HOST}:{PORT}")

    try:
        while True:
            conn, addr = server.accept()
            thread = threading.Thread(target=handle_client, args=(conn, addr), daemon=True)
            thread.start()
    except KeyboardInterrupt:
        print("\n[ENCERRANDO SERVIDOR]")
    finally:
        server.close()


if _name_ == "_main_":
    start_server()