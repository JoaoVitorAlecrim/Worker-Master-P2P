"""
Master Server - Gerenciador de Workers e Distribuidor de Tarefas.
Implementa protocolo P2P com detecção de falhas e remande automático.
"""

import socket
import threading
import logging
import time
import json
from typing import Optional, Dict
from common.protocol import send_json, recv_json_line, build_master_envelope_spec, parse_master_envelope_spec
from common.task_manager import TaskManager
from common.models import TaskStatus, WorkerStatus
import uuid

import os

# Configuração (podem ser sobrescritas via env para testes)
HOST = os.getenv("MASTER_HOST", "0.0.0.0")
PORT = int(os.getenv("MASTER_PORT", "5000"))
SERVER_UUID = os.getenv("SERVER_UUID", "Master_A")
MASTER_AUTH_TOKEN = os.getenv("MASTER_AUTH_TOKEN")
SOCKET_TIMEOUT = 15
HEARTBEAT_TIMEOUT = 15  # Segundos até considerar worker offline
TASK_TIMEOUT = 30  # Segundos até considerar tarefa expirada
WORKER_CHECK_INTERVAL = 5  # Segundos entre checks de workers
CAPACITY = 100  # Número máximo de tarefas pendentes
HELP_REQUEST_COOLDOWN = float(os.getenv("HELP_REQUEST_COOLDOWN", "5"))  # seconds between peer help attempts
# Lista de masters pares (lab config). Pode ser passada via env MASTER_PEERS como
# "host:port:uuid,host2:port2:uuid2"
PEER_MASTERS = []
logger = logging.getLogger(__name__)


def _ci(data: dict, key: str, default=None):
    from common.protocol import get_ci_value

    return get_ci_value(data, key, default)
peers_env = os.getenv("MASTER_PEERS")
if peers_env:
    for part in peers_env.split(','):
        try:
            h, p, u = part.split(':')
            PEER_MASTERS.append((h, int(p), u))
        except Exception:
            logger.warning(f"MASTER_PEERS inválido: {part}")

# Endereço (ip:porta) de cada Master vizinho, indexado por master_id — conforme o PDF
# ("Cada Master deve possuir... endereço de socket (ip:porta) conhecido pelos Masters
# vizinhos"), o request_help do exemplo do PDF NÃO inclui host/porta do solicitante;
# quem responde já deve conhecer o endereço de seus vizinhos a partir de MASTER_PEERS.
PEER_ADDRESS_BY_ID = {peer_uuid: (peer_host, peer_port) for peer_host, peer_port, peer_uuid in PEER_MASTERS}

# Logging
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] [MASTER] %(levelname)s: %(message)s'
)
logger = logging.getLogger(__name__)


class MasterServer:
    """Servidor Master que gerencia workers e distribui tarefas."""
    
    def __init__(self, server_uuid: str = SERVER_UUID):
        self.server_uuid = server_uuid
        self.task_manager = TaskManager()
        self.worker_connections: Dict[str, socket.socket] = {}  # worker_uuid -> socket
        self.lock = threading.RLock()
        self.running = True
        self.failback_target = None
        self.election_leader_info = None
        # Rate-limit peer help requests to avoid flooding peers when many workers poll
        self._last_help_request_at = 0.0
        # Per-worker poll timestamps to avoid a single worker flooding the master
        self._worker_last_request: Dict[str, float] = {}
        WORKER_POLL_COOLDOWN = float(os.getenv("WORKER_POLL_COOLDOWN", "1"))
        self._worker_poll_cooldown = WORKER_POLL_COOLDOWN
        # Sprint 4: rastreamento para o monitor de métricas
        self.lent_workers: Dict[str, str] = {}   # worker_uuid -> peer_uuid (direction "out")
        self._peer_last_seen: Dict[str, float] = {}  # peer_uuid -> timestamp último ping/contato
        self.peer_masters = PEER_MASTERS          # acessível pelo common/monitor.py
        self._capacity = CAPACITY                 # acessível pelo common/monitor.py

    # ============ INICIALIZAÇÃO ============
    
    def load_initial_tasks(self, num_tasks: int = 60) -> None:
        """Carrega tarefas iniciais para teste."""
        logger.info(f"Carregando {num_tasks} tarefas iniciais...")
        
        for i in range(num_tasks):
            if i % 3 == 0:
                self.task_manager.create_task("soma", [i + 1, i + 2])
            elif i % 3 == 1:
                self.task_manager.create_task("multiplicacao", [i + 2, i + 3])
            else:
                self.task_manager.create_task("sleep", [1])
        
        logger.info(f"✓ {num_tasks} tarefas carregadas")

    def _attach_election_info(self, response: dict) -> dict:
        """Anexa informação de líder eleito quando este master foi promovido."""
        if self.election_leader_info:
            response = dict(response)
            response["ELECTION"] = dict(self.election_leader_info)
        return response
    
    # ============ PROTOCOLO - APRESENTAÇÃO (ALIVE) ============
    
    def handle_worker_alive(self, data: dict, worker_addr: tuple) -> dict:
        """
        Processa apresentação de worker (ALIVE).
        
        Esperado:
        {
            "WORKER": "ALIVE",
            "WORKER_UUID": "Worker_1",
            "SERVER_UUID": "Master_A"  # opcional, se emprestado
        }
        """
        worker_uuid = _ci(data, "WORKER_UUID")
        original_master = _ci(data, "SERVER_UUID", self.server_uuid)
        free_disk_bytes = _ci(data, "FREE_DISK_BYTES")
        # Autenticação (opcional)
        if MASTER_AUTH_TOKEN:
            if _ci(data, "AUTH_TOKEN") != MASTER_AUTH_TOKEN:
                logger.warning(f"Auth failed for worker at {worker_addr}")
                return {"TASK": "ERROR", "MESSAGE": "AUTH_FAILED"}
        
        if not worker_uuid:
            logger.warning(f"Worker em {worker_addr} sem WORKER_UUID")
            return {
                "TASK": "ERROR",
                "MESSAGE": "WORKER_UUID obrigatório"
            }
        
        # Registrar/atualizar worker
        worker = self.task_manager.register_worker(
            worker_uuid,
            original_master,
            host=worker_addr[0],
            free_disk_bytes=free_disk_bytes,
        )
        self.task_manager.update_worker_heartbeat(worker_uuid)
        
        logger.info(f"✓ Worker {worker_uuid} conectado")

        if self.failback_target:
            return self._attach_election_info(self._build_failback_redirect())
        
        # Responder com HEARTBEAT ALIVE
        return self._attach_election_info({
            "SERVER_UUID": self.server_uuid,
            "TASK": "HEARTBEAT",
            "RESPONSE": "ALIVE",
        })

    def handle_temporary_worker_registration(self, data: dict, worker_addr: tuple) -> dict:
        """Registra worker emprestado após command_redirect."""
        request_id = _ci(data, "request_id")
        payload = _ci(data, "payload") or {}
        worker_uuid = _ci(payload, "worker_id")
        original_master_address = _ci(payload, "original_master_address")
        free_disk_bytes = _ci(payload, "FREE_DISK_BYTES")

        if MASTER_AUTH_TOKEN:
            if _ci(data, "AUTH_TOKEN") != MASTER_AUTH_TOKEN:
                logger.warning(f"Auth failed for temporary worker at {worker_addr}")
                return build_master_envelope_spec("response_rejected", {"reason": "auth_failed"}, request_id=request_id)

        if not worker_uuid or not original_master_address:
            return build_master_envelope_spec(
                "response_rejected",
                {"reason": "worker_id_or_original_master_address_missing"},
                request_id=request_id,
            )

        worker = self.task_manager.register_temporary_worker(
            worker_uuid=worker_uuid,
            original_master_address=original_master_address,
            current_master_uuid=self.server_uuid,
            host=worker_addr[0],
            free_disk_bytes=free_disk_bytes,
        )

        logger.info(
            f"✓ Worker temporário {worker_uuid} registrado (origem={original_master_address}, master={self.server_uuid})"
        )

        return build_master_envelope_spec(
            "response_accepted",
            {
                "worker_id": worker.worker_uuid,
                "original_master_address": worker.original_master_address,
                "temporary": True,
            },
            request_id=request_id,
        )
    
    # ============ PROTOCOLO - DISTRIBUIÇÃO DE TAREFAS ============
    
    def handle_request_task(self, data: dict, worker_addr: tuple) -> dict:
        """
        Processa solicitação de tarefa (worker aguardando trabalho).
        
        Esperado:
        {
            "WORKER": "ALIVE",
            "WORKER_UUID": "Worker_1",
            "SERVER_UUID": "Master_A"  # opcional
        }
        """
        worker_uuid = _ci(data, "WORKER_UUID")
        
        if not worker_uuid:
            return {
                "TASK": "ERROR",
                "MESSAGE": "WORKER_UUID obrigatório"
            }

        # Atualizar heartbeat
        self.task_manager.update_worker_heartbeat(worker_uuid)

        # Per-worker poll cooldown: evita que um mesmo worker dispare requests em loop
        now = time.time()
        last = self._worker_last_request.get(worker_uuid, 0.0)
        if (now - last) < self._worker_poll_cooldown:
            retry_after = max(0.0, self._worker_poll_cooldown - (now - last))
            return self._attach_election_info({"TASK": "NO_TASK", "RETRY_AFTER": retry_after})
        self._worker_last_request[worker_uuid] = now

        if self.failback_target:
            return self._attach_election_info(self._build_failback_redirect())

        # Obter próxima tarefa
        task_id = self.task_manager.get_pending_task()

        if not task_id:
            logger.debug(f"Nenhuma tarefa para {worker_uuid}")
            # Per o protocolo (PDF Sprint 3), pedir ajuda a peers é responsabilidade do
            # master quando ele está SATURADO (ver `_check_saturation_and_request_help`,
            # disparado pelo `worker_monitor_thread`) — não quando está ocioso. Um master
            # sem tarefas simplesmente informa NO_TASK ao worker.
            return self._attach_election_info({"TASK": "NO_TASK"})
        
        # Atribuir tarefa ao worker
        task = self.task_manager.get_task(task_id)
        if not task:
            return self._attach_election_info({"TASK": "NO_TASK"})
        
        self.task_manager.assign_task(task_id, worker_uuid)
        logger.info(f"➔ {task.operation} atribuída a {worker_uuid}")
        
        # Enviar tarefa
        return self._attach_election_info({
            "TASK": "QUERY",
            "USER": getattr(task, "user", None) or json.dumps({"operation": task.operation, "values": task.values}),
        })

    def _build_failback_redirect(self) -> dict:
        """Constrói resposta de redirecionamento para o master original quando ele retornar."""
        target_host = self.failback_target.get("TARGET_HOST")
        target_port = self.failback_target.get("TARGET_PORT")
        target_server = self.failback_target.get("TARGET_SERVER_UUID")

        return self._attach_election_info({
            "TASK": "REDIRECT",
            "TARGET_HOST": target_host,
            "TARGET_PORT": target_port,
            "TARGET_SERVER_UUID": target_server,
            "WORKERS": self.task_manager.get_online_worker_snapshot(),
        })
    
    # ============ PROTOCOLO - REPORTE DE STATUS ============
    
    def handle_task_result(self, data: dict, worker_addr: tuple) -> dict:
        """
        Processa resultado de execução de tarefa.
        
        Esperado:
        {
            "STATUS": "OK" ou "NOK",
            "TASK": "QUERY",
            "WORKER_UUID": "Worker_1",
        }
        """
        task_name = _ci(data, "TASK")
        worker_uuid = _ci(data, "WORKER_UUID")
        status = _ci(data, "STATUS")
        task_id = None
        if worker_uuid:
            task_id = self.task_manager.worker_tasks.get(worker_uuid)

        if not task_id and worker_uuid:
            for candidate_id, candidate_task in self.task_manager.tasks.items():
                if candidate_task.assigned_worker == worker_uuid:
                    task_id = candidate_id
                    break

        if not worker_uuid or not status or not task_name:
            logger.warning(f"Reporte incompleto de {worker_uuid}")
            return {"STATUS": "ACK"}

        if not task_id:
            logger.warning(f"Tarefa associada a {worker_uuid} não encontrada")
            return {"STATUS": "ACK"}

        task = self.task_manager.get_task(task_id)
        if not task:
            logger.warning(f"Tarefa {task_id} não encontrada")
            return {"STATUS": "ACK"}

        if status == "OK":
            self.task_manager.complete_task(task_id, None)
            logger.info(f"✓ {task_id[:8]} completada por {worker_uuid}")
        else:
            self.task_manager.fail_task(task_id, "Worker reported NOK")
            logger.warning(f"✗ {task_id[:8]} falhou")
        
        # Responder com ACK estrito conforme o PDF.
        return {"STATUS": "ACK"}

    # ============ PROTOCOLO - MASTER-TO-MASTER ============

    def handle_master_request(self, data: dict, addr: tuple) -> dict:
        """
        Processa mensagens vindas de outros masters (REQUEST_HELP / RESPONSE_*)
        """
        parsed = parse_master_envelope_spec(data)

        if parsed.get("error"):
            # Compatibilidade transitória com o formato legado.
            mtype = str(_ci(data, "MASTER") or "")

            if MASTER_AUTH_TOKEN:
                if _ci(data, "AUTH_TOKEN") != MASTER_AUTH_TOKEN:
                    logger.warning(f"Auth failed for master at {addr}")
                    return {"MASTER": "ERROR", "MESSAGE": "AUTH_FAILED"}

            if mtype == "REQUEST_HELP":
                requested = data.get("REQUESTED", 0)
                from_server = data.get("FROM_SERVER")

                stats = self.task_manager.get_statistics()
                current_load = stats["tasks"]["pending"] + stats["tasks"]["in_progress"]

                threshold = int(CAPACITY * 0.7)
                can_accept = current_load + requested <= threshold

                logger.info(f"Master request from {from_server}: requested={requested}, load={current_load}, accept={can_accept}")

                return {
                    "MASTER": "RESPONSE_HELP",
                    "FROM_SERVER": self.server_uuid,
                    "ACCEPT": can_accept,
                    "AVAILABLE": max(0, threshold - current_load)
                }

            if mtype == "REQUEST_STATE":
                target = data.get("TARGET_SERVER")
                if not target:
                    return {"MASTER": "ERROR", "MESSAGE": "TARGET_SERVER faltando"}

                try:
                    path = self.task_manager._state_path(target)
                    if not path or not os.path.exists(path):
                        return {"MASTER": "RESPONSE_STATE", "FOUND": False}

                    with open(path, 'r', encoding='utf-8') as f:
                        state = json.load(f)

                    return {"MASTER": "RESPONSE_STATE", "FOUND": True, "TARGET_SERVER": target, "STATE": state}

                except Exception as exc:
                    logger.warning(f"Erro ao fornecer estado para {addr}: {exc}")
                    return {"MASTER": "ERROR", "MESSAGE": str(exc)}

            logger.warning(f"Mensagem MASTER desconhecida de {addr}: {data}")
            return {"MASTER": "ERROR", "MESSAGE": "Tipo MASTER desconhecido"}

        mtype = parsed.get("type")
        request_id = parsed.get("request_id")
        payload = parsed.get("payload") or {}

        # Autenticação entre masters (opcional)
        if MASTER_AUTH_TOKEN:
            if _ci(payload, "AUTH_TOKEN") != MASTER_AUTH_TOKEN:
                logger.warning(f"Auth failed for master at {addr}")
                return build_master_envelope_spec("response_rejected", {"reason": "auth_failed"}, request_id=request_id)

        if mtype == "request_help":
            requester_master_id = _ci(payload, "master_id")
            workers_needed = int(_ci(payload, "workers_needed") or 1)

            # O exemplo de request_help do PDF traz apenas master_id/current_load/
            # capacity/workers_needed — sem host/porta do solicitante. O endereço dos
            # Masters vizinhos já deve ser conhecido de antemão (MASTER_PEERS), então
            # resolvemos por master_id primeiro. master_host/master_port no payload
            # (campo extra, não previsto no PDF) servem apenas como fallback.
            requester_host, requester_port = PEER_ADDRESS_BY_ID.get(requester_master_id, (None, None))

            if not (requester_host and requester_port):
                requester_host = requester_host or _ci(payload, "master_host")
                requester_port_value = _ci(payload, "master_port")
                try:
                    requester_port = requester_port or (int(requester_port_value) if requester_port_value else None)
                except Exception:
                    pass

            stats = self.task_manager.get_statistics()
            current_load = stats["tasks"]["pending"] + stats["tasks"]["in_progress"]
            # "capacity" no PDF é o próprio threshold de saturação (current_load > capacity).
            # Usamos a mesma definição para decidir se TAMBÉM estamos saturados e não
            # podemos emprestar (reason="high_load").
            saturation_threshold = CAPACITY

            # Só podemos emprestar workers locais (não temporários) que estejam ociosos
            # (sem tarefa atribuída no momento) — emprestar um worker ocupado interromperia
            # uma tarefa em andamento, e reemprestar um worker já emprestado criaria cadeias.
            idle_workers = [
                worker for worker in self.task_manager.get_all_workers()
                if worker.is_alive(HEARTBEAT_TIMEOUT)
                and worker.status != WorkerStatus.OFFLINE
                and not worker.is_temporary
                and worker.current_task_id is None
            ]

            if current_load > saturation_threshold or not idle_workers:
                reason = "high_load" if current_load > saturation_threshold else "no_workers_available"
                logger.info(
                    f"Recusando ajuda a {requester_master_id}: reason={reason} "
                    f"(local_load={current_load}, idle_workers={len(idle_workers)})"
                )
                return build_master_envelope_spec(
                    "response_rejected",
                    {"reason": reason},
                    request_id=request_id,
                )

            chosen = idle_workers[:workers_needed]
            worker_details = []
            for worker in chosen:
                worker_details.append({"id": worker.worker_uuid, "address": worker.host or self.server_uuid})

                if requester_host and requester_port:
                    conn = None
                    with self.lock:
                        conn = self.worker_connections.get(worker.worker_uuid)
                    if conn:
                        envelope = build_master_envelope_spec(
                            "command_redirect",
                            {"new_master_address": f"{requester_host}:{requester_port}"},
                            request_id=str(uuid.uuid4()),
                        )
                        try:
                            send_json(conn, envelope)
                            logger.info(f"↪ command_redirect enviado a {worker.worker_uuid} -> {requester_host}:{requester_port}")
                            with self.lock:
                                self.lent_workers[worker.worker_uuid] = requester_master_id
                        except Exception:
                            logger.warning(f"Falha ao enviar command_redirect para worker {worker.worker_uuid}")

            logger.info(
                f"Master request from {requester_master_id}: requested={workers_needed}, "
                f"local_load={current_load}, lending={len(worker_details)}"
            )

            return build_master_envelope_spec(
                "response_accepted",
                {"workers_offered": len(worker_details), "worker_details": worker_details},
                request_id=request_id,
            )

        if mtype == "notify_worker_returned":
            worker_id = _ci(payload, "worker_id")
            if worker_id:
                worker = self.task_manager.get_worker(worker_id)
                if worker:
                    worker.server_uuid = self.server_uuid
                    worker.mark_online()
                with self.lock:
                    self.lent_workers.pop(worker_id, None)
            return build_master_envelope_spec("response_accepted", {"worker_id": worker_id}, request_id=request_id)

        if mtype == "request_state":
            target_server = _ci(payload, "target_server")
            if not target_server:
                return build_master_envelope_spec("response_rejected", {"reason": "target_server_missing"}, request_id=request_id)

            try:
                path = self.task_manager._state_path(target_server)
                if not path or not os.path.exists(path):
                    return build_master_envelope_spec(
                        "response_state",
                        {"found": False, "target_server": target_server},
                        request_id=request_id,
                    )

                with open(path, "r", encoding="utf-8") as f:
                    state = json.load(f)

                return build_master_envelope_spec(
                    "response_state",
                    {"found": True, "target_server": target_server, "state": state},
                    request_id=request_id,
                )
            except Exception as exc:
                logger.warning(f"Erro ao fornecer estado para {addr}: {exc}")
                return build_master_envelope_spec("response_rejected", {"reason": str(exc)}, request_id=request_id)

        if mtype == "ping":
            return build_master_envelope_spec("pong", {}, request_id=request_id)

        logger.warning(f"Mensagem MASTER desconhecida de {addr}: {data}")
        return build_master_envelope_spec("response_rejected", {"reason": "unsupported_message"}, request_id=request_id)

    def request_help_to_peer(self, peer_host: str, peer_port: int, requested: int = 1, timeout: int = 5) -> dict:
        """Envia uma solicitação de ajuda (REQUEST_HELP) a um peer master e devolve a resposta."""
        request_id = str(uuid.uuid4())
        stats = self.task_manager.get_statistics()
        # Payload conforme o exemplo literal do PDF (request_help): apenas master_id,
        # current_load, capacity e workers_needed — sem host/porta do solicitante.
        # O endereço dos vizinhos já deve ser conhecido de antemão (ver MASTER_PEERS /
        # PEER_ADDRESS_BY_ID), evitando depender de campos extras (ex.: HOST="0.0.0.0")
        # que outra implementação pode nem reconhecer.
        payload = {
            "master_id": self.server_uuid,
            "current_load": stats["tasks"]["pending"] + stats["tasks"]["in_progress"],
            "capacity": CAPACITY,
            "workers_needed": requested,
        }
        envelope = build_master_envelope_spec("request_help", payload, request_id=request_id)

        try:
            sock = socket.create_connection((peer_host, peer_port), timeout=timeout)
            sock.settimeout(timeout)
            sock_file = sock.makefile("r", encoding="utf-8")

            send_json(sock, envelope)
            response = recv_json_line(sock_file)

            try:
                sock.close()
            except:
                pass

            return parse_master_envelope_spec(response or {})

        except Exception as exc:
            logger.warning(f"Não foi possível contatar peer {peer_host}:{peer_port}: {exc}")
            return build_master_envelope_spec("response_rejected", {"reason": str(exc)}, request_id=request_id)

    def _send_command_release(self, worker_id: str, original_master_address: str) -> None:
        """Sends a command_release to a local worker and notifies the original master."""
        # Send command_release to the worker socket if connected
        conn = None
        try:
            with self.lock:
                conn = self.worker_connections.get(worker_id)
        except Exception:
            conn = None

        payload = {
            "original_master_address": original_master_address,
        }

        if conn:
            try:
                envelope = build_master_envelope_spec("command_release", payload, request_id=str(uuid.uuid4()))
                send_json(conn, envelope)
            except Exception as exc:
                logger.warning(f"Erro ao enviar command_release para {worker_id}: {exc}")

        # After instructing the worker, also notify the original master via master-to-master message
        try:
            # original_master_address expected as 'host:port'
            host, port_text = original_master_address.rsplit(":", 1)
            port = int(port_text)
            self._notify_worker_returned(host, port, worker_id)
        except Exception as exc:
            logger.warning(f"Falha ao notificar master de origem ({original_master_address}): {exc}")

        # Mark worker offline locally and remove connection
        try:
            with self.lock:
                if worker_id in self.worker_connections:
                    try:
                        self.worker_connections[worker_id].close()
                    except Exception:
                        pass
                    try:
                        del self.worker_connections[worker_id]
                    except Exception:
                        pass
        except Exception:
            pass

        try:
            w = self.task_manager.get_worker(worker_id)
            if w:
                w.mark_offline()
                w.is_temporary = False
        except Exception:
            pass

    def _notify_worker_returned(self, peer_host: str, peer_port: int, worker_id: str) -> bool:
        """Notify the original master that a worker was returned."""
        request_id = str(uuid.uuid4())
        envelope = build_master_envelope_spec("notify_worker_returned", {"worker_id": worker_id}, request_id=request_id)
        try:
            sock = socket.create_connection((peer_host, peer_port), timeout=5)
            try:
                sock.settimeout(5)
                sock_file = sock.makefile("r", encoding="utf-8")
                send_json(sock, envelope)
                recv_json_line(sock_file)
                return True
            finally:
                try:
                    sock.close()
                except Exception:
                    pass
        except Exception as exc:
            logger.warning(f"Não foi possível notificar master {peer_host}:{peer_port}: {exc}")
            return False
    
    # ============ GERENCIAMENTO DE CONEX├âO ============
    
    def handle_client(self, conn: socket.socket, addr: tuple) -> None:
        """
        Processa conex├úo de um worker.
        Implementa ciclo de: apresenta├º├úo ÔåÆ distribui├º├úo ÔåÆ execu├º├úo ÔåÆ reporte.
        """
        logger.info(f"Conex├úo recebida de {addr}")
        worker_uuid = None
        
        try:
            conn.settimeout(SOCKET_TIMEOUT)
            sock_file = conn.makefile("r", encoding="utf-8")
            
            while self.running:
                # Ler mensagem JSON
                data = recv_json_line(sock_file)
                
                if data is None:
                    logger.info(f"Worker desconectado: {addr}")
                    break
                
                logger.info(f"[{addr}] Recebido: {data.get('WORKER', data.get('STATUS', 'UNKNOWN'))}")
                
                # Determinar tipo de mensagem e processar
                message_worker = str(_ci(data, "WORKER") or "").upper()
                message_type = str(_ci(data, "type") or "").lower()
                message_status = str(_ci(data, "STATUS") or "").upper()

                if message_worker == "ALIVE":
                    # Pode ser apresenta├º├úo ou solicita├º├úo de tarefa
                    worker_uuid = _ci(data, "WORKER_UUID")
                    
                    # Verificar se ├® primeira vez (apresenta├º├úo) ou solicita├º├úo de tarefa
                    worker = self.task_manager.get_worker(worker_uuid) if worker_uuid else None
                    
                    if worker is None:
                        # Primeira apresenta├º├úo
                        response = self.handle_worker_alive(data, addr)
                    else:
                        # Worker conhecido, ├® solicita├º├úo de tarefa
                        response = self.handle_request_task(data, addr)

                elif message_type == "register_temporary_worker":
                    response = self.handle_temporary_worker_registration(data, addr)
                
                elif message_status in ["OK", "NOK"]:
                    # Reporte de resultado
                    response = self.handle_task_result(data, addr)
                    worker_uuid = _ci(data, "WORKER_UUID")

                elif _ci(data, "MASTER") or message_type in {"request_help", "response_accepted", "response_rejected", "command_redirect", "register_temporary_worker", "command_release", "notify_worker_returned", "request_state"}:
                    # Mensagem vinda de outro Master
                    response = self.handle_master_request(data, addr)
                
                else:
                    logger.warning(f"Mensagem desconhecida de {addr}: {data}")
                    response = {
                        "TASK": "ERROR",
                        "MESSAGE": "Tipo de mensagem desconhecido"
                    }
                
                # Store active connection for this worker so master can send commands later
                try:
                    if worker_uuid:
                        with self.lock:
                            self.worker_connections[worker_uuid] = conn
                except Exception:
                    pass

                # Enviar resposta
                send_json(conn, response)
                logger.debug(f"Enviado para {addr}: {response}")
        
        except socket.timeout:
            logger.warning(f"Timeout com {addr}")
        except ConnectionResetError:
            logger.warning(f"Conex├úo resetada por {addr}")
        except Exception as exc:
            logger.error(f"Erro ao processar {addr}: {exc}")
        
        finally:
            try:
                conn.close()
            except:
                pass
            
            # Se worker era conhecido, detectar tarefas em execu├º├úo
            if worker_uuid:
                self._handle_worker_disconnect(worker_uuid)
    
    def _handle_worker_disconnect(self, worker_uuid: str) -> None:
        """Processa desconex├úo de um worker."""
        logger.warning(f"⚠ Worker {worker_uuid} desconectado!")
        
        # Detectar e remande de tarefas
        reassigned = self.task_manager.detect_and_reassign_dead_worker(worker_uuid)
        
        if reassigned:
            logger.warning(f"⚠ Remandadas {len(reassigned)} tarefas de {worker_uuid}:")
            for task_id in reassigned:
                logger.warning(f"  - {task_id}")
        
        # Atualizar status do worker
        worker = self.task_manager.get_worker(worker_uuid)
        if worker:
            worker.mark_offline()
        # Remove stored socket reference if present
        try:
            with self.lock:
                if worker_uuid in self.worker_connections:
                    try:
                        del self.worker_connections[worker_uuid]
                    except Exception:
                        pass
        except Exception:
            pass
    
    # ============ MONITORAMENTO ============

    def _check_saturation_and_request_help(self, stats: dict) -> None:
        """Detecta saturação (current_load > capacity) e solicita Workers emprestados a peers.

        Conforme o PDF (Sprint 3, Tarefa 02): "capacity" É o threshold de saturação
        (ex.: capacity = 100) e o disparo ocorre quando current_load > capacity. É o
        master SATURADO quem envia `request_help` — diferente do master ocioso, que
        apenas responde NO_TASK aos seus workers.
        """
        if not PEER_MASTERS:
            return

        current_load = stats["tasks"]["pending"] + stats["tasks"]["in_progress"]

        if current_load <= CAPACITY:
            return

        now = time.time()
        if (now - self._last_help_request_at) < HELP_REQUEST_COOLDOWN:
            return
        self._last_help_request_at = now

        excess = current_load - CAPACITY
        workers_needed = max(1, (excess + 9) // 10)  # ~1 worker emprestado a cada 10 tarefas excedentes

        logger.info(f"⚠ Saturado (load={current_load} > capacity={CAPACITY}). Solicitando {workers_needed} worker(s) a peers...")

        for peer in PEER_MASTERS:
            try:
                peer_host, peer_port, peer_uuid = peer
            except Exception:
                continue

            resp = self.request_help_to_peer(peer_host, peer_port, requested=workers_needed)
            payload = (resp or {}).get("payload") or {}

            if resp and resp.get("type") == "response_accepted":
                offered = int(payload.get("workers_offered") or 0)
                if offered > 0:
                    logger.info(f"✓ Peer {peer_uuid} emprestará {offered} worker(s); aguardando register_temporary_worker.")
                    return
            elif resp and resp.get("type") == "response_rejected":
                logger.info(f"Peer {peer_uuid} recusou ajuda: reason={payload.get('reason')}")
            else:
                logger.warning(f"Sem resposta válida do peer {peer_uuid} para request_help.")

    def worker_monitor_thread(self) -> None:
        """Thread que monitora saúde de workers e detecta falhas."""
        logger.info("Worker monitor iniciado")
        
        while self.running:
            try:
                time.sleep(WORKER_CHECK_INTERVAL)
                
                # Verificar tarefas expiradas
                expired = self.task_manager.check_expired_tasks(TASK_TIMEOUT)
                if expired:
                    logger.warning(f"⚠ {len(expired)} tarefas expiradas detectadas e remandadas")
                
                # Verificar workers offline
                for worker in self.task_manager.get_all_workers():
                    if not worker.is_alive(HEARTBEAT_TIMEOUT):
                        if worker.status != WorkerStatus.OFFLINE:
                            logger.warning(f"⚠ Worker {worker.worker_uuid} offline (sem heartbeat)")
                            self._handle_worker_disconnect(worker.worker_uuid)
                
                # Estatísticas
                stats = self.task_manager.get_statistics()
                logger.info(
                    f"Stats - Pending: {stats['tasks']['pending']}, "
                    f"In Progress: {stats['tasks']['in_progress']}, "
                    f"Completed: {stats['tasks']['completed']}, "
                    f"Workers: {stats['workers']['online']}/{stats['workers']['total']}"
                )
                # Saturado? Pedir ajuda a peers (PDF Sprint 3: request_help é responsabilidade
                # do master saturado, não do master ocioso).
                self._check_saturation_and_request_help(stats)

                # Check for potential release of borrowed workers when load normalizes.
                # PDF (Sprint 3, Tarefa 02 / Nota 35): o threshold de liberação deve ser
                # MENOR que o de saturação (capacity) para gerar histerese e evitar o
                # efeito ping-pong de empréstimo/devolução imediatos (ex.: 60% da capacidade).
                try:
                    current_load = stats['tasks']['pending'] + stats['tasks']['in_progress']
                    release_threshold = int(CAPACITY * 0.6)
                    if current_load <= release_threshold:
                        # Find temporary workers to release
                        for worker in self.task_manager.get_all_workers():
                            if getattr(worker, 'is_temporary', False):
                                orig = getattr(worker, 'original_master_address', None)
                                if orig:
                                    logger.info(f"Liberando worker temporário {worker.worker_uuid} de volta a {orig}")
                                    try:
                                        self._send_command_release(worker.worker_uuid, orig)
                                    except Exception as exc:
                                        logger.warning(f"Falha ao liberar worker {worker.worker_uuid}: {exc}")
                except Exception:
                    pass
            
            except Exception as exc:
                logger.error(f"Erro no monitor: {exc}")
    
    # ============ SERVIDOR ============
    
    def start(self) -> None:
        """Inicia o servidor Master."""
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.settimeout(1.0)
        
        try:
            server.bind((HOST, PORT))
            server.listen(5)
            logger.info(f"🚀 Master Server iniciado em {HOST}:{PORT}")
            logger.info(f"   UUID: {self.server_uuid}")
            
            # Configurar persistência e (opcional) carregar estado salvo quando disponível
            try:
                self.task_manager.set_persistence(self.server_uuid)
                # Permite controlar via env var se queremos carregar o estado persistido
                load_state_flag = os.getenv("LOAD_STATE", "1")
                if str(load_state_flag).lower() in ("1", "true", "yes", "y"):
                    self.task_manager.load_state(self.server_uuid)
                else:
                    logger.info("LOAD_STATE desabilitado — iniciando sem carregar estado persistido")
            except Exception:
                pass

            # Se não havia estado salvo, carregar tarefas iniciais (config via env INITIAL_TASKS)
            if not self.task_manager.tasks:
                try:
                    num_tasks = int(os.getenv("INITIAL_TASKS", "60"))
                except Exception:
                    num_tasks = 60
                if num_tasks > 0:
                    self.load_initial_tasks(num_tasks)
            
            # Iniciar thread de monitoramento de workers
            monitor_thread = threading.Thread(
                target=self.worker_monitor_thread,
                daemon=True
            )
            monitor_thread.start()

            # Sprint 4: envio de métricas ao supervisor e ping M2M
            from common.monitor import start_monitor_thread, start_peer_ping_thread
            start_monitor_thread(self)
            start_peer_ping_thread(self)

            # Aceitar conexões
            while self.running:
                try:
                    conn, addr = server.accept()
                except socket.timeout:
                    continue

                logger.info(f"📨 Conexão recebida de {addr}")

                worker_thread = threading.Thread(
                    target=self.handle_client,
                    args=(conn, addr),
                    daemon=True
                )
                worker_thread.start()
        
        except KeyboardInterrupt:
            logger.info("\n🛑 Master encerrado pelo usuário")
        except Exception as exc:
            logger.error(f"Erro fatal: {exc}")
        
        finally:
            self.running = False
            server.close()
            logger.info("Socket fechado")


if __name__ == "__main__":
    master = MasterServer(SERVER_UUID)
    master.start()
