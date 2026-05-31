"""
Worker Client - Executa tarefas do Master.
Implementa protocolo P2P de apresentação, distribuição e reporte.
"""

import socket
import time
import logging
import sys
import threading
import os
import shutil
import hashlib
from typing import Optional
from common.protocol import send_json, recv_json_line
from common.tasks import execute_task
from common.election import compute_winner, build_election_message_spec, parse_election_message_spec
import json

# Configuração
MASTER_HOST = os.getenv("MASTER_HOST", "127.0.0.1")
MASTER_PORT = int(os.getenv("MASTER_PORT", "5000"))
WORKER_UUID = "Worker_1"
SERVER_UUID = "Master_A"

HEARTBEAT_INTERVAL = int(os.getenv("HEARTBEAT_INTERVAL", "10"))
RECONNECT_DELAY = int(os.getenv("RECONNECT_DELAY", "3"))
SOCKET_TIMEOUT = int(os.getenv("SOCKET_TIMEOUT", "15"))
PROMOTE_THRESHOLD = int(os.getenv("PROMOTE_THRESHOLD", "4"))
ELECTION_PORT = int(os.getenv("ELECTION_PORT", "5200"))
ELECTION_BROADCAST_ADDR = os.getenv("ELECTION_BROADCAST_ADDR", "255.255.255.255")

# Logging
logging.basicConfig(level=logging.INFO, format="[%(asctime)s] [WORKER] %(levelname)s: %(message)s")
logger = logging.getLogger(__name__)


class WorkerClient:
    """Cliente Worker que conecta ao Master e executa tarefas."""

    def __init__(
        self,
        worker_uuid: str = WORKER_UUID,
        server_uuid: str = SERVER_UUID,
        master_host: str = MASTER_HOST,
        master_port: int = MASTER_PORT,
    ):
        self.worker_uuid = worker_uuid
        self.server_uuid = server_uuid
        self.master_host = master_host
        self.master_port = master_port
        self.original_master_host = master_host
        self.original_master_port = master_port
        self.auth_token = os.getenv("AUTH_TOKEN")
        self.running = True
        self._buffered_response = None
        self.peer_registry = {}
        self.master_failure_count = 0
        self.mode = "worker"
        self.promoted_server = None
        self.failback_requested = False
        self.failback_target = None
        self.failback_detected_at = None
        self.election_started_at = None
        self.election_pending_winner = None
        self.election_settle_seconds = float(os.getenv("ELECTION_SETTLE_SECONDS", "1.5"))
        self.failback_grace_seconds = int(os.getenv("FAILBACK_GRACE_SECONDS", "5"))
        # Election state
        self._election_lock = threading.RLock()
        self._election_votes = {}  # request_id -> list of candidate dicts
        self._last_announced_winner = None

        # UDP listener for internal election messages
        try:
            t = threading.Thread(target=self._udp_election_listener, daemon=True)
            t.start()
        except Exception:
            logger.warning("Falha ao iniciar listener UDP de eleição")

    def get_free_disk_bytes(self) -> int:
        try:
            return shutil.disk_usage(os.getcwd()).free
        except Exception:
            return 0

    def update_peer_registry(self, response: dict) -> None:
        workers = response.get("WORKERS")
        if not isinstance(workers, list):
            return

        for worker in workers:
            worker_uuid = worker.get("WORKER_UUID")
            if not worker_uuid:
                continue

            self.peer_registry[worker_uuid] = {
                "WORKER_UUID": worker_uuid,
                "HOST": worker.get("HOST"),
                "FREE_DISK_BYTES": worker.get("FREE_DISK_BYTES") or 0,
                "SERVER_UUID": worker.get("SERVER_UUID"),
                "STATUS": worker.get("STATUS"),
            }

        self.peer_registry[self.worker_uuid] = {
            "WORKER_UUID": self.worker_uuid,
            "HOST": self.peer_registry.get(self.worker_uuid, {}).get("HOST"),
            "FREE_DISK_BYTES": self.get_free_disk_bytes(),
            "SERVER_UUID": self.server_uuid,
            "STATUS": "online",
        }

    def choose_election_winner(self) -> dict:
        candidates = []

        for worker_uuid, info in self.peer_registry.items():
            host = info.get("HOST")
            if worker_uuid != self.worker_uuid and not host:
                continue

            free_disk = info.get("FREE_DISK_BYTES")
            if free_disk is None and worker_uuid == self.worker_uuid:
                free_disk = self.get_free_disk_bytes()

            candidates.append(
                {
                    "WORKER_UUID": worker_uuid,
                    "HOST": host,
                    "FREE_DISK_BYTES": int(free_disk or 0),
                    "SERVER_UUID": info.get("SERVER_UUID") or self.server_uuid,
                }
            )

        if not candidates:
            return {
                "WORKER_UUID": self.worker_uuid,
                "HOST": self.master_host,
                "FREE_DISK_BYTES": self.get_free_disk_bytes(),
                "SERVER_UUID": self.server_uuid,
            }

        candidates.sort(key=lambda item: (-item["FREE_DISK_BYTES"], item["WORKER_UUID"]))
        return candidates[0]

    def _udp_election_listener(self) -> None:
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            try:
                sock.bind(("", ELECTION_PORT))
            except Exception:
                pass

            while self.running:
                try:
                    data, addr = sock.recvfrom(65536)
                    if not data:
                        continue
                    try:
                        msg = json.loads(data.decode("utf-8"))
                    except Exception:
                        continue

                    if "ELECTION" in msg:
                        parsed = parse_election_message_spec(msg)
                        if parsed.get("error"):
                            continue
                        mtype = parsed.get("type")
                        reqid = parsed.get("request_id")
                        payload = parsed.get("payload") or {}
                    else:
                        mtype = msg.get("type")
                        reqid = msg.get("request_id")
                        payload = msg.get("payload") or {}

                    if mtype in ("election_start", "start"):
                        candidate = {
                            "WORKER_UUID": self.worker_uuid,
                            "HOST": self.peer_registry.get(self.worker_uuid, {}).get("HOST") or self.master_host,
                            "FREE_DISK_BYTES": self.get_free_disk_bytes(),
                            "SERVER_UUID": self.server_uuid,
                        }
                        reply = build_election_message_spec("vote", {"CANDIDATE": candidate})
                        try:
                            sock.sendto(json.dumps(reply).encode("utf-8"), (addr[0], ELECTION_PORT))
                        except Exception:
                            pass

                    elif mtype in ("election_vote", "vote"):
                        candidate = (
                            payload.get("CANDIDATE")
                            if isinstance(payload, dict) and payload.get("CANDIDATE")
                            else payload
                        )
                        with self._election_lock:
                            votes = self._election_votes.setdefault(reqid, [])
                            votes.append(candidate)

                    elif mtype in ("election_result", "result"):
                        winner = None
                        if isinstance(payload, dict):
                            winner = payload.get("WINNER") or payload.get("winner")
                        if winner:
                            self._last_announced_winner = winner
                            if winner.get("WORKER_UUID") != self.worker_uuid:
                                host = winner.get("HOST") or self.master_host
                                try:
                                    self.master_host = host
                                    self.master_port = int(os.getenv("MASTER_PORT", str(self.master_port)))
                                    logger.info(f"↪ Eleição: novo master anunciado {winner.get('WORKER_UUID')} em {host}")
                                except Exception:
                                    pass

                except Exception:
                    time.sleep(0.1)
        except Exception:
            logger.debug("UDP election listener terminated")

    def _send_udp_broadcast(self, message: dict) -> None:
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
            sock.sendto(json.dumps(message).encode("utf-8"), (ELECTION_BROADCAST_ADDR, ELECTION_PORT))
            try:
                sock.close()
            except Exception:
                pass
        except Exception:
            pass

    def _run_election(self, timeout: float = 1.0) -> dict:
        reqid = str(time.time()) + "-" + self.worker_uuid
        our_candidate = {
            "WORKER_UUID": self.worker_uuid,
            "HOST": self.peer_registry.get(self.worker_uuid, {}).get("HOST") or self.master_host,
            "FREE_DISK_BYTES": self.get_free_disk_bytes(),
            "SERVER_UUID": self.server_uuid,
        }
        start_msg = build_election_message_spec("start", {"CANDIDATE": our_candidate}, request_id=reqid)
        with self._election_lock:
            self._election_votes[reqid] = [our_candidate]

        self._send_udp_broadcast(start_msg)

        waited = 0.0
        interval = 0.05
        while waited < timeout:
            time.sleep(interval)
            waited += interval

        with self._election_lock:
            votes = list(self._election_votes.get(reqid, []))

        winner = compute_winner(votes)

        result_msg = build_election_message_spec("result", {"WINNER": winner}, request_id=reqid)
        self._send_udp_broadcast(result_msg)

        return winner

    def has_peer_candidates(self) -> bool:
        for worker_uuid, info in self.peer_registry.items():
            if worker_uuid == self.worker_uuid:
                continue
            if info.get("HOST"):
                return True
        return False

    def announce_election_leader(self, response: dict) -> None:
        election = response.get("ELECTION")
        if not isinstance(election, dict):
            return

        leader_uuid = election.get("LEADER_UUID")
        leader_host = election.get("LEADER_HOST")
        leader_port = election.get("LEADER_PORT")
        leader_server_uuid = election.get("LEADER_SERVER_UUID") or election.get("SOURCE_SERVER_UUID")

        if not leader_uuid or leader_uuid == self.worker_uuid:
            return

        if leader_host:
            self.master_host = leader_host

        if leader_port is not None:
            try:
                self.master_port = int(leader_port)
            except Exception:
                pass

        if leader_server_uuid:
            self.server_uuid = leader_server_uuid

        self.master_failure_count = 0
        self.election_started_at = None
        self.election_pending_winner = None

        logger.info(f"↪ Líder da eleição anunciado na rede: {leader_uuid} ({self.master_host}:{self.master_port})")

    def _election_backoff_delay(self) -> float:
        digest = hashlib.sha1(self.worker_uuid.encode("utf-8")).hexdigest()
        return (int(digest[:8], 16) % 1000) / 1000.0

    def start_promoted_master(self, leader_host: Optional[str] = None) -> None:
        from master import MasterServer

        try:
            base_port = int(os.getenv('MASTER_PORT', str(self.master_port)))
        except Exception:
            base_port = int(self.master_port)

        election_lock_port = base_port + 10000
        lock_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            lock_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 0)
            lock_sock.bind(('127.0.0.1', election_lock_port))
            lock_sock.listen(1)
        except Exception:
            try:
                lock_sock.close()
            except Exception:
                pass
            logger.info(f"Election lock unavailable on port {election_lock_port}; another node likely promoted. Adotando líder.")
            if self.election_pending_winner:
                w = self.election_pending_winner
                self.master_host = w.get('HOST') or self.master_host
                self.master_port = int(os.getenv('MASTER_PORT', str(self.master_port)))
            return

        promoted_server_uuid = self.server_uuid
        server = MasterServer(server_uuid=promoted_server_uuid)
        server.task_manager.set_persistence(promoted_server_uuid)
        server.failback_target = None
        leader_host = leader_host or self.peer_registry.get(self.worker_uuid, {}).get("HOST") or self.master_host
        server.election_leader_info = {
            "LEADER_UUID": self.worker_uuid,
            "LEADER_HOST": leader_host,
            "LEADER_PORT": self.master_port,
            "LEADER_SERVER_UUID": promoted_server_uuid,
        }
        server.election_lock_socket = lock_sock
        self.promoted_server = server

        t = threading.Thread(target=server.start, daemon=False)
        t.start()

        logger.info(f"Promoted Master iniciado com UUID {promoted_server_uuid} (escutando em 0.0.0.0:{self.master_port})")

        peers_env = os.getenv("MASTER_PEERS")
        if peers_env:
            for part in peers_env.split(","):
                try:
                    h, p, u = part.split(":")
                    p = int(p)
                except Exception:
                    continue

                try:
                    sock = socket.create_connection((h, p), timeout=5)
                    sock.settimeout(5)
                    sf = sock.makefile("r", encoding="utf-8")
                    from common.protocol import (
                        send_json,
                        recv_json_line,
                        build_master_envelope_spec,
                        parse_master_envelope_spec,
                    )

                    req_payload = {"target_server": promoted_server_uuid, "from_worker": self.worker_uuid}
                    if self.auth_token:
                        req_payload["auth_token"] = self.auth_token

                    envelope = build_master_envelope_spec("request_state", req_payload)
                    send_json(sock, envelope)
                    resp = recv_json_line(sf)
                    try:
                        sock.close()
                    except Exception:
                        pass

                    parsed = None
                    if resp and isinstance(resp, dict):
                        parsed = parse_master_envelope_spec(resp)

                    if parsed and parsed.get("type") == "response_state":
                        payload = parsed.get("payload") or {}
                        if payload.get("found") or payload.get("FOUND"):
                            state = payload.get("state") or payload.get("STATE")
                        if state:
                            logger.info(f"Estado recebido de peer {h}:{p}, carregando...")
                            server.task_manager.load_state_dict(state)
                            server.task_manager.save_state(promoted_server_uuid)
                            break
                except Exception:
                    continue

        monitor = threading.Thread(target=self.monitor_original_master_return, daemon=True)
        monitor.start()

    def monitor_original_master_return(self) -> None:
        while self.running and self.mode == "master" and self.promoted_server:
            try:
                with socket.create_connection((self.original_master_host, self.original_master_port), timeout=3):
                    self.failback_requested = True
                    self.failback_detected_at = time.time()
                    if self.promoted_server:
                        self.promoted_server.failback_target = {
                            "TARGET_HOST": self.original_master_host,
                            "TARGET_PORT": self.original_master_port,
                            "TARGET_SERVER_UUID": self.server_uuid,
                        }
                    logger.warning(f"Master original voltou a responder em {self.original_master_host}:{self.original_master_port}. Iniciando failback...")
                    return
            except Exception:
                time.sleep(3)

    def stop_promoted_master(self) -> None:
        if self.promoted_server and hasattr(self.promoted_server, "running"):
            self.promoted_server.running = False
        self.mode = "worker"
        self.failback_requested = False
        self.failback_detected_at = None
        self.master_host = self.original_master_host
        self.master_port = self.original_master_port
        logger.info("↩ Failback concluído; retornando ao papel de worker")

    def handle_master_failure(self) -> bool:
        self.master_failure_count += 1
        logger.warning(f"Falha de conexão ao master ({self.master_failure_count}/{PROMOTE_THRESHOLD})")

        if self.master_failure_count < PROMOTE_THRESHOLD:
            return False

        logger.warning(f"Eleição disparada após {self.master_failure_count} falhas. Iniciando protocolo de eleição UDP...")

        try:
            winner = self._run_election(timeout=1.0)
        except Exception as exc:
            logger.warning(f"Falha no protocolo de eleição: {exc}")
            winner = self.choose_election_winner()

        winner_uuid = winner.get("WORKER_UUID")
        winner_host = winner.get("HOST") or self.master_host
        self.election_pending_winner = winner
        settle_seconds = self.election_settle_seconds + self._election_backoff_delay()

        logger.warning(f"Eleição disparada após {self.master_failure_count} falhas. Vencedor: {winner_uuid} ({winner.get('FREE_DISK_BYTES', 0)} bytes livres)")

        if winner_uuid == self.worker_uuid:
            if self.election_started_at is None:
                self.election_started_at = time.time()
                logger.warning(f"Líder local detectado; aguardando {settle_seconds:.1f}s para consenso na rede antes de promover...")
                return False

            elapsed = time.time() - self.election_started_at
            if elapsed < settle_seconds:
                logger.info(f"Aguardando confirmação da eleição ({elapsed:.1f}/{settle_seconds:.1f}s)...")
                return False

            try:
                test_sock = socket.create_connection((winner_host, self.master_port), timeout=0.8)
                try:
                    test_sock.close()
                except Exception:
                    pass
                logger.info(f"Detecção: master já ativo em {winner_host}:{self.master_port}, adotando líder {winner_uuid}")
                self.master_host = winner_host
                self.master_port = int(os.getenv("MASTER_PORT", str(self.master_port)))
                self.master_failure_count = 0
                self.election_started_at = None
                self.election_pending_winner = None
                return False
            except Exception:
                self.start_promoted_master(leader_host=winner_host)
                self.mode = "master"
                return True

        self.master_host = winner_host
        self.master_port = int(os.getenv("MASTER_PORT", str(self.master_port)))
        self.master_failure_count = 0
        self.election_started_at = None
        logger.info(f"↪ Reapontando conexão para o novo master {winner_uuid} em {self.master_host}:{self.master_port}")
        return False

    def send_alive(self, sock: socket.socket) -> bool:
        try:
            message = {
                "WORKER": "ALIVE",
                "WORKER_UUID": self.worker_uuid,
                "SERVER_UUID": self.server_uuid,
                "FREE_DISK_BYTES": self.get_free_disk_bytes(),
            }
            if self.auth_token:
                message["AUTH_TOKEN"] = self.auth_token
            send_json(sock, message)
            logger.info("✓ Apresentação enviada (ALIVE)")
            return True
        except Exception as exc:
            logger.error(f"Erro ao enviar ALIVE: {exc}")
            return False

    def wait_heartbeat_response(self, sock_file) -> bool:
        try:
            response = recv_json_line(sock_file)
            if response is None:
                logger.debug("Conexão encerrada pelo Master")
                return False

            self.announce_election_leader(response)

            task_type = response.get("TASK")
            if task_type in ["HEARTBEAT", "QUERY", "NO_TASK", "REDIRECT"]:
                self.update_peer_registry(response)
                logger.info("✓ Conectado ao Master")
                return True

            logger.warning(f"Resposta inesperada: {response}")
            return False

        except Exception as exc:
            logger.debug(f"Erro ao receber resposta: {exc}")
            return False

    def request_task(self, sock: socket.socket) -> bool:
        try:
            message = {
                "WORKER": "ALIVE",
                "WORKER_UUID": self.worker_uuid,
                "SERVER_UUID": self.server_uuid,
                "FREE_DISK_BYTES": self.get_free_disk_bytes(),
            }
            if self.auth_token:
                message["AUTH_TOKEN"] = self.auth_token
            send_json(sock, message)
            return True
        except Exception as exc:
            logger.warning(f"Erro ao solicitar tarefa: {exc}")
            return False

    def wait_task_response(self, sock_file) -> dict:
        try:
            response = recv_json_line(sock_file)
            if response is None:
                logger.warning("Conexão encerrada pelo Master")
                return None

            self.announce_election_leader(response)

            task_type = response.get("TASK")
            if task_type == "NO_TASK":
                self.update_peer_registry(response)
                logger.debug("Nenhuma tarefa disponível")
                return None

            elif task_type == "QUERY":
                task_id = response.get("TASK_ID")
                if "USER" in response and isinstance(response.get("USER"), str):
                    user = response.get("USER")
                    self.update_peer_registry(response)
                    logger.info(f"→ Tarefa {task_id[:8] if task_id else 'unknown'} (user)")
                    return {"TASK_ID": task_id, "USER": user}

                operation = response.get("OPERATION")
                values = response.get("VALUES")
                if task_id and operation and values is not None:
                    self.update_peer_registry(response)
                    logger.info(f"→ Tarefa {task_id[:8]} ({operation})")
                    return {"TASK_ID": task_id, "OPERATION": operation, "VALUES": values}

                logger.error(f"Tarefa incompleta: {response}")
                return None

            elif response.get("TASK") == "ERROR":
                logger.error(f"Erro do Master: {response.get('MESSAGE')}")
                return None

            elif response.get("TASK") == "REDIRECT":
                self.update_peer_registry(response)
                target_host = response.get("TARGET_HOST")
                target_port = response.get("TARGET_PORT")
                target_server = response.get("TARGET_SERVER_UUID")
                logger.info(f"↪ Redirecionamento recebido: {target_host}:{target_port} (server={target_server})")
                return {"REDIRECT": {"HOST": target_host, "PORT": target_port, "SERVER_UUID": target_server}}

            else:
                logger.warning(f"Resposta desconhecida: {response}")
                return None

        except Exception as exc:
            logger.error(f"Erro ao receber tarefa: {exc}")
            return None

    def execute_and_report(self, sock: socket.socket, sock_file, task: dict) -> bool:
        task_id = task.get("TASK_ID")
        try:
            result = execute_task(task)
            message = {"STATUS": "OK", "WORKER_UUID": self.worker_uuid}
            send_json(sock, message)
            logger.info(f"✓ Tarefa {task_id[:8] if task_id else 'unknown'} completada (resultado: {result})")
            return self.wait_ack(sock_file)
        except Exception as exc:
            logger.warning(f"Erro ao executar tarefa {task_id[:8] if task_id else 'unknown'}: {exc}")
            message = {"STATUS": "NOK", "WORKER_UUID": self.worker_uuid, "ERROR": str(exc)}
            send_json(sock, message)
            logger.debug("Falha reportada ao master")
            return self.wait_ack(sock_file)

    def wait_ack(self, sock_file) -> bool:
        try:
            response = recv_json_line(sock_file)
            if response and response.get("STATUS") == "ACK":
                self.announce_election_leader(response)
                return True
            logger.debug(f"ACK não recebido: {response}")
            return False
        except Exception as exc:
            logger.debug(f"Erro ao receber ACK: {exc}")
            return False

    def run(self) -> None:
        while self.running:
            if self.mode == "master":
                if self.failback_requested and self.failback_detected_at is not None:
                    if time.time() - self.failback_detected_at >= self.failback_grace_seconds:
                        self.stop_promoted_master()
                        continue
                time.sleep(1)
                continue

            try:
                logger.info(f"Tentando conectar ao Master ({self.master_host}:{self.master_port})...")
                with socket.create_connection((self.master_host, self.master_port), timeout=10) as sock:
                    sock.settimeout(SOCKET_TIMEOUT)
                    sock_file = sock.makefile("r", encoding="utf-8")
                    logger.info("✓ Conectado ao Master")
                    self.master_failure_count = 0

                    if not self.send_alive(sock):
                        if self.handle_master_failure():
                            continue
                        continue

                    if not self.wait_heartbeat_response(sock_file):
                        if self.handle_master_failure():
                            continue
                        continue

                    while self.running:
                        if not self.request_task(sock):
                            break
                        task = self.wait_task_response(sock_file)
                        if isinstance(task, dict) and task.get("REDIRECT"):
                            redirect = task["REDIRECT"]
                            self.master_host = redirect.get("HOST") or self.master_host
                            self.master_port = redirect.get("PORT") or self.master_port
                            self.server_uuid = redirect.get("SERVER_UUID") or self.server_uuid
                            logger.info(f"↪ Atualizando master alvo para {self.master_host}:{self.master_port} (server={self.server_uuid}) and reconnecting")
                            break

                        if task is None:
                            time.sleep(HEARTBEAT_INTERVAL)
                            continue

                        self.execute_and_report(sock, sock_file, task)
                        time.sleep(0.5)

            except socket.timeout:
                logger.warning(f"Timeout de conexão/comunicação. Reconectando em {RECONNECT_DELAY}s...")
                if self.handle_master_failure():
                    continue
                time.sleep(RECONNECT_DELAY)

            except (ConnectionRefusedError, OSError) as exc:
                logger.warning(f"Master indisponível: {exc}. Reconectando em {RECONNECT_DELAY}s...")
                if self.handle_master_failure():
                    continue
                time.sleep(RECONNECT_DELAY)

            except KeyboardInterrupt:
                logger.info("\n🛑 Worker encerrado pelo usuário")
                self.running = False
                break

            except Exception as exc:
                logger.error(f"Erro inesperado: {exc}")
                if self.handle_master_failure():
                    continue
                time.sleep(RECONNECT_DELAY)


if __name__ == "__main__":
    worker_uuid = sys.argv[1] if len(sys.argv) > 1 else WORKER_UUID
    server_uuid = sys.argv[2] if len(sys.argv) > 2 else SERVER_UUID

    worker = WorkerClient(worker_uuid=worker_uuid, server_uuid=server_uuid)
    worker.run()
"""
Worker Client - Executa tarefas do Master.
Implementa protocolo P2P de apresentação, distribuição e reporte.
"""

import socket
import time
import logging
import sys
import threading
import os
import shutil
import hashlib
from typing import Optional
from common.protocol import send_json, recv_json_line
from common.tasks import execute_task
from common.election import compute_winner, build_election_message_spec, parse_election_message_spec
import json

# Configuração
MASTER_HOST = os.getenv("MASTER_HOST", "127.0.0.1")
MASTER_PORT = int(os.getenv("MASTER_PORT", "5000"))
WORKER_UUID = "Worker_1"
SERVER_UUID = "Master_A"

HEARTBEAT_INTERVAL = int(os.getenv("HEARTBEAT_INTERVAL", "10"))
RECONNECT_DELAY = int(os.getenv("RECONNECT_DELAY", "3"))
SOCKET_TIMEOUT = int(os.getenv("SOCKET_TIMEOUT", "15"))
PROMOTE_THRESHOLD = int(os.getenv("PROMOTE_THRESHOLD", "4"))
ELECTION_PORT = int(os.getenv("ELECTION_PORT", "5200"))
ELECTION_BROADCAST_ADDR = os.getenv("ELECTION_BROADCAST_ADDR", "255.255.255.255")

# Logging
logging.basicConfig(level=logging.INFO, format="[%(asctime)s] [WORKER] %(levelname)s: %(message)s")
logger = logging.getLogger(__name__)


class WorkerClient:
    """Cliente Worker que conecta ao Master e executa tarefas."""

    def __init__(
        self,
        worker_uuid: str = WORKER_UUID,
        server_uuid: str = SERVER_UUID,
        master_host: str = MASTER_HOST,
        master_port: int = MASTER_PORT,
    ):
        self.worker_uuid = worker_uuid
        self.server_uuid = server_uuid
        self.master_host = master_host
        self.master_port = master_port
        self.original_master_host = master_host
        self.original_master_port = master_port
        self.auth_token = os.getenv("AUTH_TOKEN")
        self.running = True
        self._buffered_response = None
        self.peer_registry = {}
        self.master_failure_count = 0
        self.mode = "worker"
        self.promoted_server = None
        self.failback_requested = False
        self.failback_target = None
        self.failback_detected_at = None
        self.election_started_at = None
        self.election_pending_winner = None
        self.election_settle_seconds = float(os.getenv("ELECTION_SETTLE_SECONDS", "1.5"))
        self.failback_grace_seconds = int(os.getenv("FAILBACK_GRACE_SECONDS", "5"))
        # Election state
        self._election_lock = threading.RLock()
        self._election_votes = {}  # request_id -> list of candidate dicts
        self._last_announced_winner = None

        # UDP listener for internal election messages
        try:
            t = threading.Thread(target=self._udp_election_listener, daemon=True)
            t.start()
        except Exception:
            logger.warning("Falha ao iniciar listener UDP de eleição")

    def get_free_disk_bytes(self) -> int:
        try:
            return shutil.disk_usage(os.getcwd()).free
        except Exception:
            return 0

    def update_peer_registry(self, response: dict) -> None:
        workers = response.get("WORKERS")
        if not isinstance(workers, list):
            return

        for worker in workers:
            worker_uuid = worker.get("WORKER_UUID")
            if not worker_uuid:
                continue

            self.peer_registry[worker_uuid] = {
                "WORKER_UUID": worker_uuid,
                "HOST": worker.get("HOST"),
                "FREE_DISK_BYTES": worker.get("FREE_DISK_BYTES") or 0,
                "SERVER_UUID": worker.get("SERVER_UUID"),
                "STATUS": worker.get("STATUS"),
            }

        self.peer_registry[self.worker_uuid] = {
            "WORKER_UUID": self.worker_uuid,
            "HOST": self.peer_registry.get(self.worker_uuid, {}).get("HOST"),
            "FREE_DISK_BYTES": self.get_free_disk_bytes(),
            "SERVER_UUID": self.server_uuid,
            "STATUS": "online",
        }

    def choose_election_winner(self) -> dict:
        candidates = []

        for worker_uuid, info in self.peer_registry.items():
            host = info.get("HOST")
            if worker_uuid != self.worker_uuid and not host:
                continue

            free_disk = info.get("FREE_DISK_BYTES")
            if free_disk is None and worker_uuid == self.worker_uuid:
                free_disk = self.get_free_disk_bytes()

            candidates.append(
                {
                    "WORKER_UUID": worker_uuid,
                    "HOST": host,
                    "FREE_DISK_BYTES": int(free_disk or 0),
                    "SERVER_UUID": info.get("SERVER_UUID") or self.server_uuid,
                }
            )

        if not candidates:
            return {
                "WORKER_UUID": self.worker_uuid,
                "HOST": self.master_host,
                "FREE_DISK_BYTES": self.get_free_disk_bytes(),
                "SERVER_UUID": self.server_uuid,
            }

        candidates.sort(key=lambda item: (-item["FREE_DISK_BYTES"], item["WORKER_UUID"]))
        return candidates[0]

    def _udp_election_listener(self) -> None:
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            try:
                sock.bind(("", ELECTION_PORT))
            except Exception:
                pass

            while self.running:
                try:
                    data, addr = sock.recvfrom(65536)
                    if not data:
                        continue
                    try:
                        msg = json.loads(data.decode("utf-8"))
                    except Exception:
                        continue

                    # Support spec-format (ELECTION/REQUEST_ID/PAYLOAD) and legacy
                    if "ELECTION" in msg:
                        parsed = parse_election_message_spec(msg)
                        if parsed.get("error"):
                            continue
                        mtype = parsed.get("type")
                        reqid = parsed.get("request_id")
                        payload = parsed.get("payload") or {}
                    else:
                        mtype = msg.get("type")
                        reqid = msg.get("request_id")
                        payload = msg.get("payload") or {}

                    if mtype in ("election_start", "start"):
                        candidate = {
                            "WORKER_UUID": self.worker_uuid,
                            "HOST": self.peer_registry.get(self.worker_uuid, {}).get("HOST") or self.master_host,
                            "FREE_DISK_BYTES": self.get_free_disk_bytes(),
                            "SERVER_UUID": self.server_uuid,
                        }
                        reply = build_election_message_spec("vote", {"CANDIDATE": candidate})
                        try:
                            sock.sendto(json.dumps(reply).encode("utf-8"), (addr[0], ELECTION_PORT))
                        except Exception:
                            pass

                    elif mtype in ("election_vote", "vote"):
                        candidate = (
                            payload.get("CANDIDATE")
                            if isinstance(payload, dict) and payload.get("CANDIDATE")
                            else payload
                        )
                        with self._election_lock:
                            votes = self._election_votes.setdefault(reqid, [])
                            votes.append(candidate)

                    elif mtype in ("election_result", "result"):
                        winner = None
                        if isinstance(payload, dict):
                            winner = payload.get("WINNER") or payload.get("winner")
                        if winner:
                            self._last_announced_winner = winner
                            if winner.get("WORKER_UUID") != self.worker_uuid:
                                host = winner.get("HOST") or self.master_host
                                try:
                                    self.master_host = host
                                    self.master_port = int(os.getenv("MASTER_PORT", str(self.master_port)))
                                    logger.info(
                                        f"↪ Eleição: novo master anunciado {winner.get('WORKER_UUID')} em {host}"
                                    )
                                except Exception:
                                    pass

                except Exception:
                    time.sleep(0.1)
        except Exception:
            logger.debug("UDP election listener terminated")

    def _send_udp_broadcast(self, message: dict) -> None:
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
            sock.sendto(json.dumps(message).encode("utf-8"), (ELECTION_BROADCAST_ADDR, ELECTION_PORT))
            try:
                sock.close()
            except Exception:
                pass
        except Exception:
            pass

    def _run_election(self, timeout: float = 1.0) -> dict:
        reqid = str(time.time()) + "-" + self.worker_uuid
        our_candidate = {
            "WORKER_UUID": self.worker_uuid,
            "HOST": self.peer_registry.get(self.worker_uuid, {}).get("HOST") or self.master_host,
            "FREE_DISK_BYTES": self.get_free_disk_bytes(),
            "SERVER_UUID": self.server_uuid,
        }
        start_msg = build_election_message_spec("start", {"CANDIDATE": our_candidate}, request_id=reqid)
        with self._election_lock:
            self._election_votes[reqid] = [our_candidate]

        self._send_udp_broadcast(start_msg)

        waited = 0.0
        interval = 0.05
        while waited < timeout:
            time.sleep(interval)
            waited += interval

        with self._election_lock:
            votes = list(self._election_votes.get(reqid, []))

        winner = compute_winner(votes)

        result_msg = build_election_message_spec("result", {"WINNER": winner}, request_id=reqid)
        self._send_udp_broadcast(result_msg)

        return winner

    def has_peer_candidates(self) -> bool:
        for worker_uuid, info in self.peer_registry.items():
            if worker_uuid == self.worker_uuid:
                continue
            if info.get("HOST"):
                return True
        return False

    def announce_election_leader(self, response: dict) -> None:
        election = response.get("ELECTION")
        if not isinstance(election, dict):
            return

        leader_uuid = election.get("LEADER_UUID")
        leader_host = election.get("LEADER_HOST")
        leader_port = election.get("LEADER_PORT")
        leader_server_uuid = election.get("LEADER_SERVER_UUID") or election.get("SOURCE_SERVER_UUID")

        if not leader_uuid or leader_uuid == self.worker_uuid:
            return

        if leader_host:
            self.master_host = leader_host

        if leader_port is not None:
            try:
                self.master_port = int(leader_port)
            except Exception:
                pass

        if leader_server_uuid:
            self.server_uuid = leader_server_uuid

        self.master_failure_count = 0
        self.election_started_at = None
        self.election_pending_winner = None

        logger.info(
            f"↪ Líder da eleição anunciado na rede: {leader_uuid} ({self.master_host}:{self.master_port})"
        )

    def _election_backoff_delay(self) -> float:
        digest = hashlib.sha1(self.worker_uuid.encode("utf-8")).hexdigest()
        return (int(digest[:8], 16) % 1000) / 1000.0

    def start_promoted_master(self, leader_host: Optional[str] = None) -> None:
        from master import MasterServer

        try:
            base_port = int(os.getenv('MASTER_PORT', str(self.master_port)))
        except Exception:
            base_port = int(self.master_port)

        election_lock_port = base_port + 10000
        lock_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            lock_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 0)
            lock_sock.bind(('127.0.0.1', election_lock_port))
            lock_sock.listen(1)
        except Exception:
            try:
                lock_sock.close()
            except Exception:
                pass
            logger.info(f"Election lock unavailable on port {election_lock_port}; another node likely promoted. Adotando líder.")
            if self.election_pending_winner:
                w = self.election_pending_winner
                self.master_host = w.get('HOST') or self.master_host
                self.master_port = int(os.getenv('MASTER_PORT', str(self.master_port)))
            return

        promoted_server_uuid = self.server_uuid
        server = MasterServer(server_uuid=promoted_server_uuid)
        server.task_manager.set_persistence(promoted_server_uuid)
        server.failback_target = None
        leader_host = leader_host or self.peer_registry.get(self.worker_uuid, {}).get("HOST") or self.master_host
        server.election_leader_info = {
            "LEADER_UUID": self.worker_uuid,
            "LEADER_HOST": leader_host,
            "LEADER_PORT": self.master_port,
            "LEADER_SERVER_UUID": promoted_server_uuid,
        }
        server.election_lock_socket = lock_sock
        self.promoted_server = server

        t = threading.Thread(target=server.start, daemon=False)
        t.start()

        logger.info(f"Promoted Master iniciado com UUID {promoted_server_uuid} (escutando em 0.0.0.0:{self.master_port})")

        peers_env = os.getenv("MASTER_PEERS")
        if peers_env:
            for part in peers_env.split(","):
                try:
                    h, p, u = part.split(":")
                    p = int(p)
                except Exception:
                    continue

                try:
                    sock = socket.create_connection((h, p), timeout=5)
                    sock.settimeout(5)
                    sf = sock.makefile("r", encoding="utf-8")
                    from common.protocol import (
                        send_json,
                        recv_json_line,
                        build_master_envelope_spec,
                        parse_master_envelope_spec,
                    )

                    req_payload = {"target_server": promoted_server_uuid, "from_worker": self.worker_uuid}
                    if self.auth_token:
                        req_payload["auth_token"] = self.auth_token

                    envelope = build_master_envelope_spec("request_state", req_payload)
                    send_json(sock, envelope)
                    resp = recv_json_line(sf)
                    try:
                        sock.close()
                    except Exception:
                        pass

                    parsed = None
                    if resp and isinstance(resp, dict):
                        parsed = parse_master_envelope_spec(resp)

                    if parsed and parsed.get("type") == "response_state":
                        payload = parsed.get("payload") or {}
                        if payload.get("found") or payload.get("FOUND"):
                            state = payload.get("state") or payload.get("STATE")
                        if state:
                            logger.info(f"Estado recebido de peer {h}:{p}, carregando...")
                            server.task_manager.load_state_dict(state)
                            server.task_manager.save_state(promoted_server_uuid)
                            break
                except Exception:
                    continue

        monitor = threading.Thread(target=self.monitor_original_master_return, daemon=True)
        monitor.start()

    def monitor_original_master_return(self) -> None:
        while self.running and self.mode == "master" and self.promoted_server:
            try:
                with socket.create_connection((self.original_master_host, self.original_master_port), timeout=3):
                    self.failback_requested = True
                    self.failback_detected_at = time.time()
                    if self.promoted_server:
                        self.promoted_server.failback_target = {
                            "TARGET_HOST": self.original_master_host,
                            "TARGET_PORT": self.original_master_port,
                            "TARGET_SERVER_UUID": self.server_uuid,
                        }
                    logger.warning(
                        f"Master original voltou a responder em {self.original_master_host}:{self.original_master_port}. Iniciando failback..."
                    )
                    return
            except Exception:
                time.sleep(3)

    def stop_promoted_master(self) -> None:
        if self.promoted_server and hasattr(self.promoted_server, "running"):
            self.promoted_server.running = False
        self.mode = "worker"
        self.failback_requested = False
        self.failback_detected_at = None
        self.master_host = self.original_master_host
        self.master_port = self.original_master_port
        logger.info("↩ Failback concluído; retornando ao papel de worker")

    def handle_master_failure(self) -> bool:
        self.master_failure_count += 1
        logger.warning(f"Falha de conexão ao master ({self.master_failure_count}/{PROMOTE_THRESHOLD})")

        if self.master_failure_count < PROMOTE_THRESHOLD:
            return False

        logger.warning(f"Eleição disparada após {self.master_failure_count} falhas. Iniciando protocolo de eleição UDP...")

        try:
            winner = self._run_election(timeout=1.0)
        except Exception as exc:
            logger.warning(f"Falha no protocolo de eleição: {exc}")
            winner = self.choose_election_winner()

        winner_uuid = winner.get("WORKER_UUID")
        winner_host = winner.get("HOST") or self.master_host
        self.election_pending_winner = winner
        settle_seconds = self.election_settle_seconds + self._election_backoff_delay()

        logger.warning(
            f"Eleição disparada após {self.master_failure_count} falhas. Vencedor: {winner_uuid} ({winner.get('FREE_DISK_BYTES', 0)} bytes livres)"
        )

        if winner_uuid == self.worker_uuid:
            if self.election_started_at is None:
                self.election_started_at = time.time()
                logger.warning(f"Líder local detectado; aguardando {settle_seconds:.1f}s para consenso na rede antes de promover...")
                return False

            elapsed = time.time() - self.election_started_at
            if elapsed < settle_seconds:
                logger.info(f"Aguardando confirmação da eleição ({elapsed:.1f}/{settle_seconds:.1f}s)...")
                return False

            try:
                test_sock = socket.create_connection((winner_host, self.master_port), timeout=0.8)
                try:
                    test_sock.close()
                except Exception:
                    pass
                logger.info(f"Detecção: master já ativo em {winner_host}:{self.master_port}, adotando líder {winner_uuid}")
                self.master_host = winner_host
                self.master_port = int(os.getenv("MASTER_PORT", str(self.master_port)))
                self.master_failure_count = 0
                self.election_started_at = None
                self.election_pending_winner = None
                return False
            except Exception:
                self.start_promoted_master(leader_host=winner_host)
                self.mode = "master"
                return True

        self.master_host = winner_host
        self.master_port = int(os.getenv("MASTER_PORT", str(self.master_port)))
        self.master_failure_count = 0
        self.election_started_at = None
        logger.info(f"↪ Reapontando conexão para o novo master {winner_uuid} em {self.master_host}:{self.master_port}")
        return False

    def send_alive(self, sock: socket.socket) -> bool:
        try:
            message = {
                "WORKER": "ALIVE",
                "WORKER_UUID": self.worker_uuid,
                "SERVER_UUID": self.server_uuid,
                "FREE_DISK_BYTES": self.get_free_disk_bytes(),
            }
            if self.auth_token:
                message["AUTH_TOKEN"] = self.auth_token
            send_json(sock, message)
            logger.info("✓ Apresentação enviada (ALIVE)")
            return True
        except Exception as exc:
            logger.error(f"Erro ao enviar ALIVE: {exc}")
            return False

    def wait_heartbeat_response(self, sock_file) -> bool:
        try:
            response = recv_json_line(sock_file)

            if response is None:
                logger.debug("Conexão encerrada pelo Master")
                return False

            self.announce_election_leader(response)

            task_type = response.get("TASK")
            if task_type in ["HEARTBEAT", "QUERY", "NO_TASK", "REDIRECT"]:
                self.update_peer_registry(response)
                logger.info("✓ Conectado ao Master")
                return True

            logger.warning(f"Resposta inesperada: {response}")
            return False

        except Exception as exc:
            logger.debug(f"Erro ao receber resposta: {exc}")
            return False

    def request_task(self, sock: socket.socket) -> bool:
        try:
            message = {
                "WORKER": "ALIVE",
                "WORKER_UUID": self.worker_uuid,
                "SERVER_UUID": self.server_uuid,
                "FREE_DISK_BYTES": self.get_free_disk_bytes(),
            }
            if self.auth_token:
                message["AUTH_TOKEN"] = self.auth_token
            send_json(sock, message)
            return True
        except Exception as exc:
            logger.warning(f"Erro ao solicitar tarefa: {exc}")
            return False

    def wait_task_response(self, sock_file) -> dict:
        try:
            response = recv_json_line(sock_file)

            if response is None:
                logger.warning("Conexão encerrada pelo Master")
                return None

            self.announce_election_leader(response)

            task_type = response.get("TASK")

            if task_type == "NO_TASK":
                self.update_peer_registry(response)
                logger.debug("Nenhuma tarefa disponível")
                return None

            elif task_type == "QUERY":
                task_id = response.get("TASK_ID")

                if "USER" in response and isinstance(response.get("USER"), str):
                    user = response.get("USER")
                    self.update_peer_registry(response)
                    logger.info(f"→ Tarefa {task_id[:8] if task_id else 'unknown'} (user)")
                    return {"TASK_ID": task_id, "USER": user}

                operation = response.get("OPERATION")
                values = response.get("VALUES")

                if task_id and operation and values is not None:
                    self.update_peer_registry(response)
                    logger.info(f"→ Tarefa {task_id[:8]} ({operation})")
                    return {"TASK_ID": task_id, "OPERATION": operation, "VALUES": values}

                logger.error(f"Tarefa incompleta: {response}")
                return None

            elif response.get("TASK") == "ERROR":
                logger.error(f"Erro do Master: {response.get('MESSAGE')}")
                return None

            elif response.get("TASK") == "REDIRECT":
                self.update_peer_registry(response)
                target_host = response.get("TARGET_HOST")
                target_port = response.get("TARGET_PORT")
                target_server = response.get("TARGET_SERVER_UUID")

                logger.info(f"↪ Redirecionamento recebido: {target_host}:{target_port} (server={target_server})")

                return {"REDIRECT": {"HOST": target_host, "PORT": target_port, "SERVER_UUID": target_server}}

            else:
                logger.warning(f"Resposta desconhecida: {response}")
                return None

        except Exception as exc:
            logger.error(f"Erro ao receber tarefa: {exc}")
            return None

    def execute_and_report(self, sock: socket.socket, sock_file, task: dict) -> bool:
        task_id = task.get("TASK_ID")

        try:
            result = execute_task(task)

            message = {
                "STATUS": "OK",
                "WORKER_UUID": self.worker_uuid,
            }

            send_json(sock, message)
            logger.info(f"✓ Tarefa {task_id[:8] if task_id else 'unknown'} completada (resultado: {result})")

            return self.wait_ack(sock_file)

        except Exception as exc:
            logger.warning(f"Erro ao executar tarefa {task_id[:8] if task_id else 'unknown'}: {exc}")

            message = {"STATUS": "NOK", "WORKER_UUID": self.worker_uuid, "ERROR": str(exc)}

            send_json(sock, message)
            logger.debug("Falha reportada ao master")

            return self.wait_ack(sock_file)

    def wait_ack(self, sock_file) -> bool:
        try:
            response = recv_json_line(sock_file)

            if response and response.get("STATUS") == "ACK":
                self.announce_election_leader(response)
                return True

            logger.debug(f"ACK não recebido: {response}")
            return False

        except Exception as exc:
            logger.debug(f"Erro ao receber ACK: {exc}")
            return False

    def run(self) -> None:
        while self.running:
            if self.mode == "master":
                if self.failback_requested and self.failback_detected_at is not None:
                    if time.time() - self.failback_detected_at >= self.failback_grace_seconds:
                        self.stop_promoted_master()
                        continue

                time.sleep(1)
                continue

            try:
                logger.info(f"Tentando conectar ao Master ({self.master_host}:{self.master_port})...")

                with socket.create_connection((self.master_host, self.master_port), timeout=10) as sock:
                    sock.settimeout(SOCKET_TIMEOUT)
                    sock_file = sock.makefile("r", encoding="utf-8")

                    logger.info("✓ Conectado ao Master")
                    self.master_failure_count = 0

                    if not self.send_alive(sock):
                        if self.handle_master_failure():
                            continue
                        continue

                    if not self.wait_heartbeat_response(sock_file):
                        if self.handle_master_failure():
                            continue
                        continue

                    while self.running:
                        if not self.request_task(sock):
                            break

                        task = self.wait_task_response(sock_file)

                        if isinstance(task, dict) and task.get("REDIRECT"):
                            redirect = task["REDIRECT"]
                            self.master_host = redirect.get("HOST") or self.master_host
                            self.master_port = redirect.get("PORT") or self.master_port
                            self.server_uuid = redirect.get("SERVER_UUID") or self.server_uuid
                            logger.info(
                                f"↪ Atualizando master alvo para {self.master_host}:{self.master_port} (server={self.server_uuid}) and reconnecting"
                            )
                            break

                        if task is None:
                            time.sleep(HEARTBEAT_INTERVAL)
                            continue

                        self.execute_and_report(sock, sock_file, task)

                        time.sleep(0.5)

            except socket.timeout:
                logger.warning(f"Timeout de conexão/comunicação. Reconectando em {RECONNECT_DELAY}s...")
                if self.handle_master_failure():
                    continue
                time.sleep(RECONNECT_DELAY)

            except (ConnectionRefusedError, OSError) as exc:
                logger.warning(f"Master indisponível: {exc}. Reconectando em {RECONNECT_DELAY}s...")
                if self.handle_master_failure():
                    continue
                time.sleep(RECONNECT_DELAY)

            except KeyboardInterrupt:
                logger.info("\n🛑 Worker encerrado pelo usuário")
                self.running = False
                break

            except Exception as exc:
                logger.error(f"Erro inesperado: {exc}")
                if self.handle_master_failure():
                    continue
                time.sleep(RECONNECT_DELAY)


if __name__ == "__main__":
    worker_uuid = sys.argv[1] if len(sys.argv) > 1 else WORKER_UUID
    server_uuid = sys.argv[2] if len(sys.argv) > 2 else SERVER_UUID

    worker = WorkerClient(worker_uuid=worker_uuid, server_uuid=server_uuid)
    worker.run()
"""
Worker Client - Executa tarefas do Master.
Implementa protocolo P2P de apresentação, distribuição e reporte.
"""

import socket
import time
import logging
import sys
import threading
import os
import shutil
import hashlib
from typing import Optional
from common.protocol import send_json, recv_json_line
from common.tasks import execute_task
from common.election import compute_winner, build_election_message_spec, parse_election_message_spec
import json

# Configuração
MASTER_HOST = os.getenv("MASTER_HOST", "127.0.0.1")
MASTER_PORT = int(os.getenv("MASTER_PORT", "5000"))
WORKER_UUID = "Worker_1"  # Alterado de WORKER_ID para WORKER_UUID
SERVER_UUID = "Master_A"  # Master ao qual pertence originalmente

HEARTBEAT_INTERVAL = int(os.getenv("HEARTBEAT_INTERVAL", "10"))
RECONNECT_DELAY = int(os.getenv("RECONNECT_DELAY", "3"))
SOCKET_TIMEOUT = int(os.getenv("SOCKET_TIMEOUT", "15"))
PROMOTE_THRESHOLD = int(os.getenv("PROMOTE_THRESHOLD", "4"))  # falhas consecutivas antes da eleição
ELECTION_PORT = int(os.getenv("ELECTION_PORT", "5200"))
ELECTION_BROADCAST_ADDR = os.getenv("ELECTION_BROADCAST_ADDR", "255.255.255.255")

# Logging
logging.basicConfig(level=logging.INFO, format="[%(asctime)s] [WORKER] %(levelname)s: %(message)s")
logger = logging.getLogger(__name__)


class WorkerClient:
    """Cliente Worker que conecta ao Master e executa tarefas."""

    def __init__(
        self,
        worker_uuid: str = WORKER_UUID,
        server_uuid: str = SERVER_UUID,
        master_host: str = MASTER_HOST,
        master_port: int = MASTER_PORT,
    ):
        self.worker_uuid = worker_uuid
        self.server_uuid = server_uuid
        self.master_host = master_host
        self.master_port = master_port
        self.original_master_host = master_host
        self.original_master_port = master_port
        self.auth_token = os.getenv("AUTH_TOKEN")
        self.running = True
        self._buffered_response = None
        self.peer_registry = {}
        self.master_failure_count = 0
        self.mode = "worker"
        self.promoted_server = None
        self.failback_requested = False
        self.failback_target = None
        self.failback_detected_at = None
        self.election_started_at = None
        self.election_pending_winner = None
        self.election_settle_seconds = float(os.getenv("ELECTION_SETTLE_SECONDS", "1.5"))
        self.failback_grace_seconds = int(os.getenv("FAILBACK_GRACE_SECONDS", "5"))
        # Election state
        self._election_lock = threading.RLock()
        self._election_votes = {}  # request_id -> list of candidate dicts
        self._last_announced_winner = None

        # UDP listener for internal election messages
        try:
            t = threading.Thread(target=self._udp_election_listener, daemon=True)
            t.start()
        except Exception:
            logger.warning("Falha ao iniciar listener UDP de eleição")

    def get_free_disk_bytes(self) -> int:
        """Retorna espaço livre em disco do diretório atual."""
        try:
            return shutil.disk_usage(os.getcwd()).free
        except Exception:
            return 0

    def update_peer_registry(self, response: dict) -> None:
        """Atualiza a visão local dos workers conectados ao master."""
        workers = response.get("WORKERS")
        if not isinstance(workers, list):
            return

        for worker in workers:
            worker_uuid = worker.get("WORKER_UUID")
            if not worker_uuid:
                continue

            self.peer_registry[worker_uuid] = {
                "WORKER_UUID": worker_uuid,
                "HOST": worker.get("HOST"),
                "FREE_DISK_BYTES": worker.get("FREE_DISK_BYTES") or 0,
                "SERVER_UUID": worker.get("SERVER_UUID"),
                "STATUS": worker.get("STATUS"),
            }

        self.peer_registry[self.worker_uuid] = {
            "WORKER_UUID": self.worker_uuid,
            "HOST": self.peer_registry.get(self.worker_uuid, {}).get("HOST"),
            "FREE_DISK_BYTES": self.get_free_disk_bytes(),
            "SERVER_UUID": self.server_uuid,
            "STATUS": "online",
        }

    def choose_election_winner(self) -> dict:
        """Escolhe o worker com maior espaço livre em disco entre os conhecidos."""
        candidates = []

        for worker_uuid, info in self.peer_registry.items():
            host = info.get("HOST")
            if worker_uuid != self.worker_uuid and not host:
                continue

            free_disk = info.get("FREE_DISK_BYTES")
            if free_disk is None and worker_uuid == self.worker_uuid:
                free_disk = self.get_free_disk_bytes()

            candidates.append(
                {
                    "WORKER_UUID": worker_uuid,
                    "HOST": host,
                    "FREE_DISK_BYTES": int(free_disk or 0),
                    "SERVER_UUID": info.get("SERVER_UUID") or self.server_uuid,
                }
            )

        if not candidates:
            return {
                "WORKER_UUID": self.worker_uuid,
                "HOST": self.master_host,
                "FREE_DISK_BYTES": self.get_free_disk_bytes(),
                "SERVER_UUID": self.server_uuid,
            }

        candidates.sort(key=lambda item: (-item["FREE_DISK_BYTES"], item["WORKER_UUID"]))
        return candidates[0]

<<<<<<< HEAD
    # ============ ELECTION (UDP BROADCAST) ============

    def _udp_election_listener(self) -> None:
        """Listener UDP que responde a pedidos de eleição e processa resultados anunciados."""
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            try:
                sock.bind(("", ELECTION_PORT))
            except Exception:
                # Binding might fail if another process bound it; still try to recv
                pass

            while self.running:
                try:
                    data, addr = sock.recvfrom(65536)
                    if not data:
                        continue
                    try:
                        msg = json.loads(data.decode("utf-8"))
                    except Exception:
                        continue

                    # Support spec-format (ELECTION/REQUEST_ID/PAYLOAD) and legacy (type/request_id/payload)
                    if "ELECTION" in msg:
                        parsed = parse_election_message_spec(msg)
                        if parsed.get("error"):
                            continue
                        mtype = parsed.get("type")
                        reqid = parsed.get("request_id")
                        payload = parsed.get("payload") or {}
                    else:
                        mtype = msg.get("type")
                        reqid = msg.get("request_id")
                        payload = msg.get("payload") or {}

                    if mtype in ("election_start", "start"):
                        # Reply with vote (our candidate info)
                        candidate = {
                            "WORKER_UUID": self.worker_uuid,
                            "HOST": self.peer_registry.get(self.worker_uuid, {}).get("HOST") or self.master_host,
                            "FREE_DISK_BYTES": self.get_free_disk_bytes(),
                            "SERVER_UUID": self.server_uuid,
                        }
                        reply = build_election_message_spec("vote", {"CANDIDATE": candidate})
                        try:
                            sock.sendto(json.dumps(reply).encode("utf-8"), (addr[0], ELECTION_PORT))
                        except Exception:
                            pass

                    elif mtype in ("election_vote", "vote"):
                        # payload may contain CANDIDATE key or be the candidate itself
                        candidate = (
                            payload.get("CANDIDATE")
                            if isinstance(payload, dict) and payload.get("CANDIDATE")
                            else payload
                        )
                        with self._election_lock:
                            votes = self._election_votes.setdefault(reqid, [])
                            votes.append(candidate)

                    elif mtype in ("election_result", "result"):
                        # payload may contain WINNER key
                        winner = None
                        if isinstance(payload, dict):
                            winner = payload.get("WINNER") or payload.get("winner")
                        if winner:
                            self._last_announced_winner = winner
                            # If winner is not self, update master pointer
                            if winner.get("WORKER_UUID") != self.worker_uuid:
                                host = winner.get("HOST") or self.master_host
                                try:
                                    self.master_host = host
                                    self.master_port = int(os.getenv("MASTER_PORT", str(self.master_port)))
                                    logger.info(
                                        f"↪ Eleição: novo master anunciado {winner.get('WORKER_UUID')} em {host}"
                                    )
                                except Exception:
                                    pass

                except Exception:
                    time.sleep(0.1)
        except Exception:
            logger.debug("UDP election listener terminated")

    def _send_udp_broadcast(self, message: dict) -> None:
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
            sock.sendto(json.dumps(message).encode("utf-8"), (ELECTION_BROADCAST_ADDR, ELECTION_PORT))
            try:
                sock.close()
            except Exception:
                pass
        except Exception:
            pass

    def _run_election(self, timeout: float = 1.0) -> dict:
        """Run a UDP broadcast election and return chosen winner dict."""
        reqid = str(time.time()) + "-" + self.worker_uuid
        # Broadcast election_start with our candidate info
        our_candidate = {
            "WORKER_UUID": self.worker_uuid,
            "HOST": self.peer_registry.get(self.worker_uuid, {}).get("HOST") or self.master_host,
            "FREE_DISK_BYTES": self.get_free_disk_bytes(),
            "SERVER_UUID": self.server_uuid,
        }
        start_msg = build_election_message_spec("start", {"CANDIDATE": our_candidate}, request_id=reqid)
        # Reset votes store for this reqid
        with self._election_lock:
            self._election_votes[reqid] = [our_candidate]

        self._send_udp_broadcast(start_msg)

        # Wait for votes
        waited = 0.0
        interval = 0.05
        while waited < timeout:
            time.sleep(interval)
            waited += interval

        # Collect votes
        with self._election_lock:
            votes = list(self._election_votes.get(reqid, []))

        winner = compute_winner(votes)

        # Announce winner
        result_msg = build_election_message_spec("result", {"WINNER": winner}, request_id=reqid)
        self._send_udp_broadcast(result_msg)

        # Apply winner locally
        if winner.get("WORKER_UUID") == self.worker_uuid:
            # Promote self
            return winner
        else:
            return winner

    def start_promoted_master(self) -> None:
=======
    def has_peer_candidates(self) -> bool:
        """Indica se há pelo menos um outro worker conhecido para a eleição."""
        for worker_uuid, info in self.peer_registry.items():
            if worker_uuid == self.worker_uuid:
                continue
            if info.get("HOST"):
                return True
        return False

    def announce_election_leader(self, response: dict) -> None:
        """Atualiza o master alvo quando um líder de eleição é anunciado na rede."""
        election = response.get("ELECTION")
        if not isinstance(election, dict):
            return

        leader_uuid = election.get("LEADER_UUID")
        leader_host = election.get("LEADER_HOST")
        leader_port = election.get("LEADER_PORT")
        leader_server_uuid = election.get("LEADER_SERVER_UUID") or election.get("SOURCE_SERVER_UUID")

        if not leader_uuid or leader_uuid == self.worker_uuid:
            return

        if leader_host:
            self.master_host = leader_host

        if leader_port is not None:
            try:
                self.master_port = int(leader_port)
            except Exception:
                pass

        if leader_server_uuid:
            self.server_uuid = leader_server_uuid

        self.master_failure_count = 0
        self.election_started_at = None
        self.election_pending_winner = None

        logger.info(
            f"↪ Líder da eleição anunciado na rede: {leader_uuid} "
            f"({self.master_host}:{self.master_port})"
        )

    def _election_backoff_delay(self) -> float:
        """Calcula um atraso curto e estável para evitar promoção simultânea."""
        digest = hashlib.sha1(self.worker_uuid.encode("utf-8")).hexdigest()
        # spread up to ~1s to reduce collision window
        return (int(digest[:8], 16) % 1000) / 1000.0

    def start_promoted_master(self, leader_host: Optional[str] = None) -> None:
>>>>>>> origin/main
        """Sobe um Master local usando o server_uuid original."""
        from master import MasterServer

        # Election lock: try to bind a deterministic port so only one process can promote
        try:
            base_port = int(os.getenv('MASTER_PORT', str(self.master_port)))
        except Exception:
            base_port = int(self.master_port)

        election_lock_port = base_port + 10000
        lock_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            lock_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 0)
            lock_sock.bind(('127.0.0.1', election_lock_port))
            # keep the socket open to hold the lock
            lock_sock.listen(1)
        except Exception as exc:
            try:
                lock_sock.close()
            except:
                pass
            logger.info(f"Election lock unavailable on port {election_lock_port}; another node likely promoted. Adotando líder.")
            # Adopt the pending winner if known
            if self.election_pending_winner:
                w = self.election_pending_winner
                self.master_host = w.get('HOST') or self.master_host
                self.master_port = int(os.getenv('MASTER_PORT', str(self.master_port)))
            return

        promoted_server_uuid = self.server_uuid
        server = MasterServer(server_uuid=promoted_server_uuid)
        server.task_manager.set_persistence(promoted_server_uuid)
        server.failback_target = None
        leader_host = leader_host or self.peer_registry.get(self.worker_uuid, {}).get("HOST") or self.master_host
        server.election_leader_info = {
            "LEADER_UUID": self.worker_uuid,
            "LEADER_HOST": leader_host,
            "LEADER_PORT": self.master_port,
            "LEADER_SERVER_UUID": promoted_server_uuid,
        }
        # attach lock socket so it remains open while master runs
        server.election_lock_socket = lock_sock
        self.promoted_server = server

        # Não pode ser daemon: o processo precisa continuar vivo como novo master.
        t = threading.Thread(target=server.start, daemon=False)
        t.start()

        logger.info(
            f"Promoted Master iniciado com UUID {promoted_server_uuid} (escutando em 0.0.0.0:{self.master_port})"
        )

        peers_env = os.getenv("MASTER_PEERS")
        if peers_env:
            for part in peers_env.split(","):
                try:
                    h, p, u = part.split(":")
                    p = int(p)
                except Exception:
                    continue

                try:
                    sock = socket.create_connection((h, p), timeout=5)
                    sock.settimeout(5)
                    sf = sock.makefile("r", encoding="utf-8")
                    from common.protocol import (
                        send_json,
                        recv_json_line,
                        build_master_envelope_spec,
                        parse_master_envelope_spec,
                    )

                    req_payload = {"target_server": promoted_server_uuid, "from_worker": self.worker_uuid}
                    if self.auth_token:
                        req_payload["auth_token"] = self.auth_token

                    envelope = build_master_envelope_spec("request_state", req_payload)
                    send_json(sock, envelope)
                    resp = recv_json_line(sf)
                    try:
                        sock.close()
                    except Exception:
                        pass

                    parsed = None
                    if resp and isinstance(resp, dict):
                        parsed = parse_master_envelope_spec(resp)

                    if parsed and parsed.get("type") == "response_state":
                        payload = parsed.get("payload") or {}
                        if payload.get("found") or payload.get("FOUND"):
                            state = payload.get("state") or payload.get("STATE")
                        if state:
                            logger.info(f"Estado recebido de peer {h}:{p}, carregando...")
                            server.task_manager.load_state_dict(state)
                            server.task_manager.save_state(promoted_server_uuid)
                            break
                except Exception:
                    continue

        monitor = threading.Thread(target=self.monitor_original_master_return, daemon=True)
        monitor.start()

    def monitor_original_master_return(self) -> None:
        """Monitora o retorno do master original para permitir failback."""
        while self.running and self.mode == "master" and self.promoted_server:
            try:
                with socket.create_connection((self.original_master_host, self.original_master_port), timeout=3):
                    self.failback_requested = True
                    self.failback_detected_at = time.time()
                    if self.promoted_server:
                        self.promoted_server.failback_target = {
                            "TARGET_HOST": self.original_master_host,
                            "TARGET_PORT": self.original_master_port,
                            "TARGET_SERVER_UUID": self.server_uuid,
                        }
                    logger.warning(
                        f"Master original voltou a responder em {self.original_master_host}:{self.original_master_port}. "
                        f"Iniciando failback..."
                    )
                    return
            except Exception:
                time.sleep(3)

    def stop_promoted_master(self) -> None:
        """Encerra o master promovido e devolve o worker ao modo cliente."""
        if self.promoted_server and hasattr(self.promoted_server, "running"):
            self.promoted_server.running = False
        self.mode = "worker"
        self.failback_requested = False
        self.failback_detected_at = None
        self.master_host = self.original_master_host
        self.master_port = self.original_master_port
        logger.info("↩ Failback concluído; retornando ao papel de worker")

    def handle_master_failure(self) -> bool:
        """Conta falhas e dispara eleição quando o master ficar indisponível."""
        self.master_failure_count += 1
        logger.warning(f"Falha de conexão ao master ({self.master_failure_count}/{PROMOTE_THRESHOLD})")

        if self.master_failure_count < PROMOTE_THRESHOLD:
            return False

        logger.warning(
            f"Eleição disparada após {self.master_failure_count} falhas. Iniciando protocolo de eleição UDP..."
        )

        # Run networked election
        try:
            winner = self._run_election(timeout=1.0)
        except Exception as exc:
            logger.warning(f"Falha no protocolo de eleição: {exc}")
            winner = self.choose_election_winner()

        winner_uuid = winner.get("WORKER_UUID")
        winner_host = winner.get("HOST") or self.master_host
<<<<<<< HEAD
        logger.warning(f"Eleição concluída. Vencedor: {winner_uuid} ({winner.get('FREE_DISK_BYTES', 0)} bytes livres)")
=======
        self.election_pending_winner = winner
        settle_seconds = self.election_settle_seconds + self._election_backoff_delay()

        logger.warning(
            f"Eleição disparada após {self.master_failure_count} falhas. "
            f"Vencedor: {winner_uuid} ({winner.get('FREE_DISK_BYTES', 0)} bytes livres)"
        )
>>>>>>> origin/main

        if winner_uuid == self.worker_uuid:
            # start/continue election settle window
            if self.election_started_at is None:
                self.election_started_at = time.time()
                logger.warning(
                    f"Líder local detectado; aguardando {settle_seconds:.1f}s "
                    "para consenso na rede antes de promover..."
                )
                return False

            elapsed = time.time() - self.election_started_at
            if elapsed < settle_seconds:
                logger.info(
                    f"Aguardando confirmação da eleição ({elapsed:.1f}/"
                    f"{settle_seconds:.1f}s)..."
                )
                return False

            # Antes de promover, checar se outro master já subiu no alvo
            try:
                test_sock = socket.create_connection((winner_host, self.master_port), timeout=0.8)
                try:
                    test_sock.close()
                except:
                    pass
                # Encontrou um master ativo — adota líder
                logger.info(f"Detecção: master já ativo em {winner_host}:{self.master_port}, adotando líder {winner_uuid}")
                self.master_host = winner_host
                self.master_port = int(os.getenv("MASTER_PORT", str(self.master_port)))
                self.master_failure_count = 0
                self.election_started_at = None
                self.election_pending_winner = None
                return False
            except Exception:
                # Nenhum master detectado — promover
                self.start_promoted_master(leader_host=winner_host)
                self.mode = "master"
                return True

        self.master_host = winner_host
        self.master_port = int(os.getenv("MASTER_PORT", str(self.master_port)))
        self.master_failure_count = 0
        self.election_started_at = None
        logger.info(f"↪ Reapontando conexão para o novo master {winner_uuid} em {self.master_host}:{self.master_port}")
        return False

    def send_alive(self, sock: socket.socket) -> bool:
        """
        Envia apresentação ALIVE ao Master.

        Payload:
        {
            "WORKER": "ALIVE",
            "WORKER_UUID": "Worker_1",
            "SERVER_UUID": "Master_A"
        }
        """
        try:
            message = {
                "WORKER": "ALIVE",
                "WORKER_UUID": self.worker_uuid,
                "SERVER_UUID": self.server_uuid,
                "FREE_DISK_BYTES": self.get_free_disk_bytes(),
            }
            if self.auth_token:
                message["AUTH_TOKEN"] = self.auth_token
            send_json(sock, message)
            logger.info("✓ Apresentação enviada (ALIVE)")
            return True
        except Exception as exc:
            logger.error(f"Erro ao enviar ALIVE: {exc}")
            return False

    def wait_heartbeat_response(self, sock_file) -> bool:
        """Aguarda resposta do Master (HEARTBEAT ou tarefa direto)."""
        try:
            response = recv_json_line(sock_file)

            if response is None:
                logger.debug("Conexão encerrada pelo Master")
                return False

<<<<<<< HEAD
=======
            self.announce_election_leader(response)
            
>>>>>>> origin/main
            # Aceita qualquer resposta TASK válida
            task_type = response.get("TASK")
            if task_type in ["HEARTBEAT", "QUERY", "NO_TASK", "REDIRECT"]:
                self.update_peer_registry(response)
                logger.info("✓ Conectado ao Master")
                return True

            logger.warning(f"Resposta inesperada: {response}")
            return False

        except Exception as exc:
            logger.debug(f"Erro ao receber resposta: {exc}")
            return False

    def request_task(self, sock: socket.socket) -> bool:
        """
        Solicita tarefa ao Master (mesmo payload da apresentação).
        """
        try:
            message = {
                "WORKER": "ALIVE",
                "WORKER_UUID": self.worker_uuid,
                "SERVER_UUID": self.server_uuid,
                "FREE_DISK_BYTES": self.get_free_disk_bytes(),
            }
            if self.auth_token:
                message["AUTH_TOKEN"] = self.auth_token
            send_json(sock, message)
            return True
        except Exception as exc:
            logger.warning(f"Erro ao solicitar tarefa: {exc}")
            return False

    def wait_task_response(self, sock_file) -> dict:
        """
        Aguarda resposta com tarefa ou NO_TASK.
        Retorna dicionário com tarefa ou None.
        """
        try:
            response = recv_json_line(sock_file)

            if response is None:
                logger.warning("Conexão encerrada pelo Master")
                return None

<<<<<<< HEAD
=======
            self.announce_election_leader(response)
            
>>>>>>> origin/main
            task_type = response.get("TASK")

            if task_type == "NO_TASK":
                self.update_peer_registry(response)
                logger.debug("Nenhuma tarefa disponível")
                return None

            elif task_type == "QUERY":
                task_id = response.get("TASK_ID")

                # Novo contrato: master envia `USER` apenas. Suportamos ambos
                # formatos para compatibilidade.
                if "USER" in response and isinstance(response.get("USER"), str):
                    user = response.get("USER")
                    self.update_peer_registry(response)
                    logger.info(f"→ Tarefa {task_id[:8] if task_id else 'unknown'} (user)")
                    return {"TASK_ID": task_id, "USER": user}

                # Formato legado com OPERATION/VALUES
                operation = response.get("OPERATION")
                values = response.get("VALUES")

                if task_id and operation and values is not None:
                    self.update_peer_registry(response)
                    logger.info(f"→ Tarefa {task_id[:8]} ({operation})")
                    return {"TASK_ID": task_id, "OPERATION": operation, "VALUES": values}

                logger.error(f"Tarefa incompleta: {response}")
                return None

            elif response.get("TASK") == "ERROR":
                logger.error(f"Erro do Master: {response.get('MESSAGE')}")
                return None

            elif response.get("TASK") == "REDIRECT":
                self.update_peer_registry(response)
                # Instrução para redirecionar worker para outro master
                target_host = response.get("TARGET_HOST")
                target_port = response.get("TARGET_PORT")
                target_server = response.get("TARGET_SERVER_UUID")

                logger.info(f"↪ Redirecionamento recebido: {target_host}:{target_port} (server={target_server})")

                return {"REDIRECT": {"HOST": target_host, "PORT": target_port, "SERVER_UUID": target_server}}

            else:
                logger.warning(f"Resposta desconhecida: {response}")
                return None

        except Exception as exc:
            logger.error(f"Erro ao receber tarefa: {exc}")
            return None

    def execute_and_report(self, sock: socket.socket, sock_file, task: dict) -> bool:
        """
        Executa tarefa e reporta resultado.

        Payload de sucesso:
        {
            "STATUS": "OK",
            "TASK_ID": "uuid-1234",
            "WORKER_UUID": "Worker_1",
            "RESULT": <resultado>
        }

        Payload de falha:
        {
            "STATUS": "NOK",
            "TASK_ID": "uuid-1234",
            "WORKER_UUID": "Worker_1",
            "ERROR": "mensagem de erro"
        }
        """
        task_id = task.get("TASK_ID")

        # Executa tarefa (aceita formatos internos e user)
        try:
            result = execute_task(task)

            # Reportar sucesso — conforme contrato de rede, enviar mensagem mínima
            message = {
                "STATUS": "OK",
                "WORKER_UUID": self.worker_uuid,
            }

            send_json(sock, message)
            logger.info(f"✓ Tarefa {task_id[:8] if task_id else 'unknown'} completada (resultado: {result})")

            # Aguardar ACK
            return self.wait_ack(sock_file)

        except Exception as exc:
            logger.warning(f"Erro ao executar tarefa {task_id[:8] if task_id else 'unknown'}: {exc}")

            # Reportar falha (mensagem mínima)
            message = {"STATUS": "NOK", "WORKER_UUID": self.worker_uuid, "ERROR": str(exc)}

            send_json(sock, message)
            logger.debug("Falha reportada ao master")

            # Aguardar ACK mesmo na falha
            return self.wait_ack(sock_file)

    def wait_ack(self, sock_file) -> bool:
        """Aguarda confirmação ACK do Master."""
        try:
            response = recv_json_line(sock_file)

            if response and response.get("STATUS") == "ACK":
                self.announce_election_leader(response)
                return True

            logger.debug(f"ACK não recebido: {response}")
            return False

        except Exception as exc:
            logger.debug(f"Erro ao receber ACK: {exc}")
            return False

    def run(self) -> None:
        """Loop principal do worker."""
        while self.running:
            if self.mode == "master":
                if self.failback_requested and self.failback_detected_at is not None:
                    if time.time() - self.failback_detected_at >= self.failback_grace_seconds:
                        self.stop_promoted_master()
                        continue

                time.sleep(1)
                continue

            try:
                logger.info(f"Tentando conectar ao Master ({self.master_host}:{self.master_port})...")

                with socket.create_connection((self.master_host, self.master_port), timeout=10) as sock:
                    sock.settimeout(SOCKET_TIMEOUT)
                    sock_file = sock.makefile("r", encoding="utf-8")

                    logger.info("✓ Conectado ao Master")
                    self.master_failure_count = 0

                    # Fase 1: Apresentação (ALIVE)
                    if not self.send_alive(sock):
                        if self.handle_master_failure():
                            continue
                        continue

                    if not self.wait_heartbeat_response(sock_file):
                        if self.handle_master_failure():
                            continue
                        continue

                    # Fase 2-4: Loop de trabalho
                    while self.running:
                        # Solicitar próxima tarefa
                        if not self.request_task(sock):
                            break

                        # Aguardar tarefa
                        task = self.wait_task_response(sock_file)

                        # Redirecionamento: atualizar target e reconectar
                        if isinstance(task, dict) and task.get("REDIRECT"):
                            redirect = task["REDIRECT"]
                            self.master_host = redirect.get("HOST") or self.master_host
                            self.master_port = redirect.get("PORT") or self.master_port
                            self.server_uuid = redirect.get("SERVER_UUID") or self.server_uuid
                            logger.info(
                                f"↪ Atualizando master alvo para {self.master_host}:{self.master_port} (server={self.server_uuid}) and reconnecting"
                            )
                            # Force reconnection by breaking inner loop
                            break

                        if task is None:
                            # Sem tarefa, aguardar e tentar novamente
                            time.sleep(HEARTBEAT_INTERVAL)
                            continue

                        # Executar tarefa e reportar
                        self.execute_and_report(sock, sock_file, task)

                        # Pequeno delay antes de solicitar próxima tarefa
                        time.sleep(0.5)

            except socket.timeout:
                logger.warning(f"Timeout de conexão/comunicação. Reconectando em {RECONNECT_DELAY}s...")
                if self.handle_master_failure():
                    continue
                time.sleep(RECONNECT_DELAY)

            except (ConnectionRefusedError, OSError) as exc:
                logger.warning(f"Master indisponível: {exc}. Reconectando em {RECONNECT_DELAY}s...")
                if self.handle_master_failure():
                    continue
                time.sleep(RECONNECT_DELAY)

            except KeyboardInterrupt:
                logger.info("\n🛑 Worker encerrado pelo usuário")
                self.running = False
                break

            except Exception as exc:
                logger.error(f"Erro inesperado: {exc}")
                if self.handle_master_failure():
                    continue
                time.sleep(RECONNECT_DELAY)


if __name__ == "__main__":
    # Permitir passar UUID via argumentos
    worker_uuid = sys.argv[1] if len(sys.argv) > 1 else WORKER_UUID
    server_uuid = sys.argv[2] if len(sys.argv) > 2 else SERVER_UUID

    worker = WorkerClient(worker_uuid=worker_uuid, server_uuid=server_uuid)
    worker.run()
