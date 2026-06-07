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
import json
from common.election import build_election_message_spec, parse_election_message_spec, compute_winner
from typing import Optional
import uuid
from common.protocol import send_json, recv_json_line, build_master_envelope_spec, parse_master_envelope_spec, get_ci_value
from common.tasks import execute_task

# Configuração
MASTER_HOST = os.getenv("MASTER_HOST", "127.0.0.1")
MASTER_PORT = int(os.getenv("MASTER_PORT", "5000"))
WORKER_UUID = "Worker_1"  # Alterado de WORKER_ID para WORKER_UUID
SERVER_UUID = "Master_A"  # Master ao qual pertence originalmente

HEARTBEAT_INTERVAL = int(os.getenv("HEARTBEAT_INTERVAL", "10"))
RECONNECT_DELAY = int(os.getenv("RECONNECT_DELAY", "3"))
SOCKET_TIMEOUT = int(os.getenv("SOCKET_TIMEOUT", "15"))
PROMOTE_THRESHOLD = int(os.getenv("PROMOTE_THRESHOLD", "4"))  # falhas consecutivas antes da eleição
TASK_REQUEST_INTERVAL = float(os.getenv("TASK_REQUEST_INTERVAL", "1"))

# Logging
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] [WORKER] %(levelname)s: %(message)s'
)
logger = logging.getLogger(__name__)


def _ci(data: dict, key: str, default=None):
    return get_ci_value(data, key, default)

# --- Eleição UDP (listener compartilhado) -------------------------------------------
_ELECTION_PORT = int(os.getenv("ELECTION_PORT", "54000"))
_ELECTION_BROADCAST_ADDR = os.getenv("ELECTION_BROADCAST_ADDR", "255.255.255.255")
_ELECTION_SOCKET = None
_ELECTION_THREAD = None
_ELECTION_CLIENTS = []  # list of WorkerClient instances
_ELECTION_RESPONSES = {}  # request_id -> [payloads]
_ELECTION_COND = threading.Condition()


def _start_election_listener():
    global _ELECTION_SOCKET, _ELECTION_THREAD
    if _ELECTION_SOCKET is not None:
        return

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    try:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
    except Exception:
        pass
    try:
        sock.bind(("", _ELECTION_PORT))
    except Exception:
        try:
            sock.bind(("0.0.0.0", _ELECTION_PORT))
        except Exception:
            # cannot bind; election disabled
            _ELECTION_SOCKET = None
            return

    _ELECTION_SOCKET = sock

    def _loop():
        while True:
            try:
                data, addr = sock.recvfrom(65536)
                try:
                    msg = json.loads(data.decode("utf-8"))
                except Exception:
                    continue

                parsed = parse_election_message_spec(msg)
                if isinstance(parsed, dict) and parsed.get("error"):
                    continue
                mtype = parsed.get("type")
                reqid = parsed.get("request_id")
                payload = parsed.get("payload") or {}

                if mtype == "start":
                    # Para cada cliente registrado, envia um VOTE com sua informação local
                    for c in list(_ELECTION_CLIENTS):
                        vote = {
                            "WORKER_UUID": c.worker_uuid,
                            "HOST": c.master_host,
                            "FREE_DISK_BYTES": c.get_free_disk_bytes(),
                            "SERVER_UUID": c.server_uuid,
                        }
                        vmsg = build_election_message_spec("VOTE", vote, request_id=reqid)
                        try:
                            sock.sendto(json.dumps(vmsg).encode("utf-8"), addr)
                        except Exception:
                            pass

                elif mtype == "vote":
                    with _ELECTION_COND:
                        _ELECTION_RESPONSES.setdefault(reqid, []).append(payload)
                        _ELECTION_COND.notify_all()

                elif mtype == "result":
                    # opcional: poderia atualizar o estado local
                    pass

            except Exception:
                # mantém o listener ativo
                time.sleep(0.1)

    t = threading.Thread(target=_loop, daemon=True)
    t.start()
    _ELECTION_THREAD = t


def _register_election_client(client):
    # garante que o socket foi iniciado e registra o cliente
    _ELECTION_CLIENTS.append(client)
    _start_election_listener()


def _prepare_election_request(request_id: str):
    with _ELECTION_COND:
        _ELECTION_RESPONSES[request_id] = []


def _wait_for_election_responses(request_id: str, timeout: float):
    end = time.time() + timeout
    with _ELECTION_COND:
        while time.time() < end:
            if _ELECTION_RESPONSES.get(request_id):
                return list(_ELECTION_RESPONSES.get(request_id))
            remaining = end - time.time()
            if remaining <= 0:
                break
            _ELECTION_COND.wait(remaining)
    return list(_ELECTION_RESPONSES.get(request_id) or [])

# ----------------------------------------------------------------------------------


class WorkerClient:
    """Cliente Worker que conecta ao Master e executa tarefas."""
    
    def __init__(self, worker_uuid: str = WORKER_UUID, server_uuid: str = SERVER_UUID, master_host: str = MASTER_HOST, master_port: int = MASTER_PORT):
        self.worker_uuid = worker_uuid
        self.server_uuid = server_uuid
        self.original_server_uuid = server_uuid
        self.master_host = master_host
        self.master_port = master_port
        self.original_master_host = master_host
        self.original_master_port = master_port
        self.auth_token = os.getenv('AUTH_TOKEN')
        self.running = True
        self._server_retry_after: Optional[float] = None
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

        # Register for UDP election listener (shared per-process)
        _register_election_client(self)

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

    def send_alive(self, sock: socket.socket) -> bool:
        """Envia a apresentação inicial ALIVE ao master."""
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

    def should_register_temporary_worker(self) -> bool:
        """Indica se o worker precisa anunciar que foi emprestado."""
        return (self.master_host, self.master_port) != (self.original_master_host, self.original_master_port)

    def register_temporary_worker(self, sock: socket.socket, sock_file) -> bool:
        """Anuncia o worker ao novo master após um redirect."""
        try:
            message = {
                "type": "register_temporary_worker",
                "request_id": str(uuid.uuid4()),
                "payload": {
                    "worker_id": self.worker_uuid,
                    "original_master_address": f"{self.original_master_host}:{self.original_master_port}",
                },
            }
            if self.auth_token:
                message["AUTH_TOKEN"] = self.auth_token

            send_json(sock, message)
            response = recv_json_line(sock_file)

            if response and str(_ci(response, "type") or "").lower() == "response_accepted":
                logger.info("✓ Worker temporário registrado no novo master")
                return True

            logger.warning(f"Registro temporário rejeitado: {response}")
            return False
        except Exception as exc:
            logger.error(f"Erro ao registrar worker temporário: {exc}")
            return False

    def wait_heartbeat_response(self, sock_file) -> bool:
        """Aguarda a resposta do master após o ALIVE inicial."""
        try:
            response = recv_json_line(sock_file)

            if response is None:
                logger.warning("Conexão encerrada pelo Master")
                return False

            self.announce_election_leader(response)

            if str(_ci(response, "TASK") or "").upper() in {"HEARTBEAT", "NO_TASK", "REDIRECT"}:
                self.update_peer_registry(response)
                logger.info("✓ Conectado ao Master")
                return True

            logger.warning(f"Resposta inesperada: {response}")
            return False

        except Exception as exc:
            logger.error(f"Erro ao receber resposta inicial: {exc}")
            return False

    def request_task(self, sock: socket.socket) -> bool:
        """Solicita uma nova tarefa ao master usando o mesmo envelope ALIVE."""
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

            candidates.append({
                "WORKER_UUID": worker_uuid,
                "HOST": host,
                "FREE_DISK_BYTES": int(free_disk or 0),
                "SERVER_UUID": info.get("SERVER_UUID") or self.server_uuid,
            })

        if not candidates:
            return {
                "WORKER_UUID": self.worker_uuid,
                "HOST": self.master_host,
                "FREE_DISK_BYTES": self.get_free_disk_bytes(),
                "SERVER_UUID": self.server_uuid,
            }

        candidates.sort(key=lambda item: (-item["FREE_DISK_BYTES"], item["WORKER_UUID"]))
        return candidates[0]

    def _run_election(self, timeout: float = 1.0) -> dict:
        """Dispara uma eleição START e aguarda os VOTEs, retornando o vencedor escolhido."""
        # Monta a mensagem START; a função de construção gera o request_id
        msg = build_election_message_spec("START", {"SOURCE_WORKER_UUID": self.worker_uuid, "SOURCE_SERVER_UUID": self.server_uuid})
        request_id = msg.get("REQUEST_ID")

        # prepara o coletor
        _prepare_election_request(request_id)

        # Para testes no mesmo processo, coleta votos diretamente dos clientes registrados
        with _ELECTION_COND:
            for c in list(_ELECTION_CLIENTS):
                vote = {
                    "WORKER_UUID": c.worker_uuid,
                    "HOST": c.master_host,
                    "FREE_DISK_BYTES": c.get_free_disk_bytes(),
                    "SERVER_UUID": c.server_uuid,
                }
                _ELECTION_RESPONSES.setdefault(request_id, []).append(vote)

        responses = _wait_for_election_responses(request_id, timeout)
        # normaliza: as respostas já vêm como dicionários de payload
        winner = compute_winner(responses or [])

        # announce result (best-effort)
        try:
            result_msg = build_election_message_spec("RESULT", {"WINNER": winner}, request_id=request_id)
            _ELECTION_SOCKET.sendto(json.dumps(result_msg).encode("utf-8"), (_ELECTION_BROADCAST_ADDR, _ELECTION_PORT))
        except Exception:
            pass

        return winner

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
        """Sobe um Master local usando o server_uuid original."""
        from master import MasterServer

        # Election lock: try to bind a deterministic port so only one process can promote.
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
            logger.info(f"Bloqueio de eleição indisponível na porta {election_lock_port}; outro nó provavelmente já foi promovido. Adotando líder.")
            # Adota o vencedor pendente, se conhecido
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

        logger.info(f"Promoted Master iniciado com UUID {promoted_server_uuid} (escutando em 0.0.0.0:{self.master_port})")

        peers_env = os.getenv('MASTER_PEERS')
        if peers_env:
            for part in peers_env.split(','):
                try:
                    h, p, u = part.split(':')
                    p = int(p)
                except Exception:
                    continue

                try:
                    sock = socket.create_connection((h, p), timeout=5)
                    sock.settimeout(5)
                    sf = sock.makefile('r', encoding='utf-8')
                    state = self.request_state_from_peer(h, p, promoted_server_uuid)
                    if state:
                        logger.info(f"Estado recebido de peer {h}:{p}, carregando...")
                        server.task_manager.load_state_dict(state)
                        server.task_manager.save_state(promoted_server_uuid)
                        break
                except Exception:
                    continue

        monitor = threading.Thread(target=self.monitor_original_master_return, daemon=True)
        monitor.start()

    def request_state_from_peer(self, peer_host: str, peer_port: int, target_server: str) -> Optional[dict]:
        """Solicita o estado persistido de um master peer usando o envelope PDF."""
        request_id = str(uuid.uuid4())
        payload = {
            "target_server": target_server,
            "from_worker": self.worker_uuid,
        }
        if self.auth_token:
            payload["AUTH_TOKEN"] = self.auth_token

        envelope = build_master_envelope_spec("request_state", payload, request_id=request_id)

        sock = socket.create_connection((peer_host, peer_port), timeout=5)
        try:
            sock.settimeout(5)
            sock_file = sock.makefile("r", encoding="utf-8")
            send_json(sock, envelope)
            response = recv_json_line(sock_file)
            parsed = parse_master_envelope_spec(response or {})

            if parsed.get("type") == "response_state":
                payload = parsed.get("payload") or {}
                if payload.get("found"):
                    return payload.get("state")
            return None
        finally:
            try:
                sock.close()
            except Exception:
                pass

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

        winner = self.choose_election_winner()
        winner_uuid = winner.get("WORKER_UUID")
        winner_host = winner.get("HOST") or self.master_host
        self.election_pending_winner = winner
        settle_seconds = self.election_settle_seconds + self._election_backoff_delay()

        logger.warning(
            f"Eleição disparada após {self.master_failure_count} falhas. "
            f"Vencedor: {winner_uuid} ({winner.get('FREE_DISK_BYTES', 0)} bytes livres)"
        )

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

            self.announce_election_leader(response)
            
            task_type = str(_ci(response, "TASK") or "").upper()
            response_type = str(_ci(response, "type") or "").lower()
            
            if task_type == "NO_TASK":
                self.update_peer_registry(response)
                # se o master indicar um retry_after, respeitar
                retry_after = response.get("RETRY_AFTER")
                if isinstance(retry_after, (int, float)):
                    self._server_retry_after = float(retry_after)
                logger.debug("Nenhuma tarefa disponível")
                return None
            
            elif task_type == "QUERY":
                user = _ci(response, "USER")

                if user is not None:
                    self.update_peer_registry(response)
                    logger.info("→ Tarefa QUERY recebida")
                    return {
                        "TASK": "QUERY",
                        "USER": user,
                    }

                logger.error(f"Tarefa incompleta: {response}")
                return None
            
            elif task_type == "ERROR":
                logger.error(f"Erro do Master: {_ci(response, 'MESSAGE')}")
                return None

            elif task_type == "REDIRECT":
                self.update_peer_registry(response)
                return self.handle_redirect(response)

            elif response_type == "command_redirect":
                return self.handle_redirect(response)

            elif task_type == "RELEASE" or response_type == "command_release":
                self.update_peer_registry(response)
                return self.handle_release(response)
            
            else:
                logger.warning(f"Resposta desconhecida: {response}")
                return None
        
        except Exception as exc:
            logger.error(f"Erro ao receber tarefa: {exc}")
            return None

    def handle_redirect(self, message: dict) -> dict:
        """Aplica um comando de redirecionamento para outro master."""
        payload = _ci(message, "payload") if isinstance(_ci(message, "payload"), dict) else message

        new_address = _ci(payload, "new_master_address")
        target_host = _ci(payload, "TARGET_HOST")
        target_port = _ci(payload, "TARGET_PORT")

        if new_address and not (target_host and target_port):
            try:
                target_host, target_port_text = new_address.rsplit(":", 1)
                target_port = int(target_port_text)
            except Exception:
                logger.warning(f"Formato inválido de redirecionamento: {new_address}")
                return {"REDIRECT": {"HOST": self.master_host, "PORT": self.master_port, "SERVER_UUID": self.server_uuid}}

        if target_host:
            self.master_host = target_host
        if target_port:
            self.master_port = int(target_port)

        logger.info(
            f"↪ Redirecionamento recebido: {self.master_host}:{self.master_port} (server={self.server_uuid})"
        )

        return {
            "REDIRECT": {
                "HOST": self.master_host,
                "PORT": self.master_port,
                "SERVER_UUID": self.server_uuid,
            }
        }

    def handle_release(self, message: dict) -> dict:
        """Processa a devolução do worker ao master original."""
        payload = _ci(message, "payload") if isinstance(_ci(message, "payload"), dict) else message

        target_host = _ci(payload, "TARGET_HOST") or self.original_master_host
        target_port = _ci(payload, "TARGET_PORT") or self.original_master_port
        target_server = _ci(payload, "TARGET_SERVER_UUID") or self.original_server_uuid

        self.master_host = target_host
        self.master_port = int(target_port)
        self.server_uuid = target_server

        logger.info(
            f"↩ Retorno liberado: {self.master_host}:{self.master_port} (server={self.server_uuid})"
        )

        return {
            "REDIRECT": {
                "HOST": self.master_host,
                "PORT": self.master_port,
                "SERVER_UUID": self.server_uuid,
            }
        }

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
        task_name = task.get("TASK") or "QUERY"
        user_payload = task.get("USER")
        operation = task.get("OPERATION")
        values = task.get("VALUES")
        
        try:
            # Executar tarefa conforme o payload recebido do master.
            if user_payload is not None:
                result = execute_task({"user": user_payload})
            else:
                result = execute_task({
                    "operation": operation,
                    "values": values
                })
            
            # Reportar sucesso (não enviar TASK_ID no wire)
            message = {
                "STATUS": "OK",
                "TASK": task_name,
                "WORKER_UUID": self.worker_uuid,
            }
            
            send_json(sock, message)
            logger.info(f"✓ Tarefa {task_name} completada (resultado: {result})")
            
            # Aguardar ACK
            return self.wait_ack(sock_file)
        
        except Exception as exc:
            logger.warning(f"Erro ao executar tarefa {task_name}: {exc}")
            
            # Reportar falha (não enviar TASK_ID no wire)
            message = {
                "STATUS": "NOK",
                "TASK": task_name,
                "WORKER_UUID": self.worker_uuid,
            }
            
            send_json(sock, message)
            logger.debug(f"Falha reportada ao master")
            
            # Aguardar ACK mesmo na falha
            return self.wait_ack(sock_file)
    
    def wait_ack(self, sock_file) -> bool:
        """Aguarda confirmação ACK do Master."""
        try:
            response = recv_json_line(sock_file)
            
            if response and str(_ci(response, "STATUS") or "").upper() == "ACK":
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
                    
                    logger.info(f"✓ Conectado ao Master")
                    self.master_failure_count = 0

                    if self.should_register_temporary_worker():
                        if not self.register_temporary_worker(sock, sock_file):
                            break
                    
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
                        # Evita flood: aguarda intervalo configurado antes de solicitar nova tarefa
                        time.sleep(TASK_REQUEST_INTERVAL)

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
                            logger.info(f"↪ Atualizando master alvo para {self.master_host}:{self.master_port} (server={self.server_uuid}) and reconnecting")
                            # Force reconnection by breaking inner loop
                            break
                        
                        if task is None:
                            # Sem tarefa: aguarda o retry sugerido pelo master ou fallback
                            retry = self._server_retry_after or HEARTBEAT_INTERVAL
                            # resetar indicação do master
                            self._server_retry_after = None
                            time.sleep(retry)
                            continue
                        
                        # Executar tarefa e reportar
                        self.execute_and_report(sock, sock_file, task)
                        
                        # Aguarda antes de solicitar a próxima tarefa para evitar flood no master
                        time.sleep(TASK_REQUEST_INTERVAL)
            
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
