"""
Master Server - Gerenciador de Workers e Distribuidor de Tarefas.
Implementa protocolo P2P com detecção de falhas e remande automático.
"""

import socket
import threading
import logging
import json
from typing import Dict
from common.protocol import (
    send_json,
    recv_json_line,
    build_master_envelope,
    parse_master_envelope,
    build_master_envelope_spec,
    parse_master_envelope_spec,
)
from common.task_manager import TaskManager
from common.models import WorkerStatus

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
# Lista de masters pares (lab config). Pode ser passada via env MASTER_PEERS como
# "host:port:uuid,host2:port2:uuid2"
PEER_MASTERS = []
peers_env = os.getenv("MASTER_PEERS")
if peers_env:
    for part in peers_env.split(","):
        try:
            h, p, u = part.split(":")
            PEER_MASTERS.append((h, int(p), u))
        except Exception:
            # Logging not yet configured here; fallback to print
            print(f"MASTER_PEERS inválido: {part}")

# Logging
logging.basicConfig(level=logging.INFO, format="[%(asctime)s] [MASTER] %(levelname)s: %(message)s")
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
        worker_uuid = data.get("WORKER_UUID")
        original_master = data.get("SERVER_UUID", self.server_uuid)
        free_disk_bytes = data.get("FREE_DISK_BYTES")
        # Note: authentication tokens removed from wire per spec; accept ALIVE without AUTH_TOKEN

        if not worker_uuid:
            logger.warning(f"Worker em {worker_addr} sem WORKER_UUID")
            return {"TASK": "ERROR", "MESSAGE": "WORKER_UUID obrigatório"}

        # Registrar/atualizar worker
        _ = self.task_manager.register_worker(
            worker_uuid,
            original_master,
            host=worker_addr[0],
            free_disk_bytes=free_disk_bytes,
        )
        self.task_manager.update_worker_heartbeat(worker_uuid)

        logger.info(f"✓ Worker {worker_uuid} conectado")

        if self.failback_target:
            return self._build_failback_redirect()
        # Responder com HEARTBEAT ALIVE
        return {
            "SERVER_UUID": self.server_uuid,
            "TASK": "HEARTBEAT",
            "RESPONSE": "ALIVE",
        }

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
        worker_uuid = data.get("WORKER_UUID")

        if not worker_uuid:
            return {"TASK": "ERROR", "MESSAGE": "WORKER_UUID obrigatório"}

        # Atualizar heartbeat
        self.task_manager.update_worker_heartbeat(worker_uuid)

        if self.failback_target:
            return self._build_failback_redirect()

        # Obter próxima tarefa
        task_id = self.task_manager.get_pending_task()

        if not task_id:
            logger.debug(f"Nenhuma tarefa para {worker_uuid}")

            # Tentar pedir ajuda a peers configurados
            for peer in PEER_MASTERS:
                try:
                    peer_host, peer_port, peer_uuid = peer
                except Exception:
                    continue

                # Request help using PDF envelope: type/request_id/payload
                resp = self.request_help_to_peer(peer_host, peer_port, requested=1)

                # resp is normalized: {"type": ..., "request_id": ..., "payload": {...}}
                if resp and resp.get("type") == "response_accepted":
                    payload = resp.get("payload") or {}
                    # If peer accepted and offered workers, redirect
                    if payload.get("workers_offered", 0) > 0:
                        logger.debug(f"Redirecionando para {peer_uuid}")
                        return {
                            "TASK": "REDIRECT",
                            "TARGET_HOST": peer_host,
                            "TARGET_PORT": peer_port,
                            "TARGET_SERVER_UUID": peer_uuid,
                        }

            return {"TASK": "NO_TASK"}

        # Atribuir tarefa ao worker
        task = self.task_manager.get_task(task_id)
        if not task:
            return {"TASK": "NO_TASK"}

        self.task_manager.assign_task(task_id, worker_uuid)
        logger.info(f"➔ {task.operation} atribuída a {worker_uuid}")

        # Construir payload `USER` conforme contrato de rede. Se a tarefa
        # interna possui `user`, reutilizamos; caso contrário serializamos
        # um JSON com `operation`/`values` dentro do campo `USER`.
        try:
            user_payload = (
                task.user
                if getattr(task, "user", None)
                else json.dumps({"operation": task.operation, "values": task.values})
            )
        except Exception:
            user_payload = json.dumps({"operation": task.operation, "values": task.values})

        # Enviar tarefa respeitando o contrato: somente `TASK` e `USER` são obrigatórios
        return {
            "TASK": "QUERY",
            "USER": user_payload,
        }

    def _build_failback_redirect(self) -> dict:
        """Constrói resposta de redirecionamento para o master original quando ele retornar."""
        target_host = self.failback_target.get("TARGET_HOST")
        target_port = self.failback_target.get("TARGET_PORT")
        target_server = self.failback_target.get("TARGET_SERVER_UUID")

        return {
            "TASK": "REDIRECT",
            "TARGET_HOST": target_host,
            "TARGET_PORT": target_port,
            "TARGET_SERVER_UUID": target_server,
        }

    # ============ PROTOCOLO - REPORTE DE STATUS ============

    def handle_task_result(self, data: dict, worker_addr: tuple) -> dict:
        """
        Processa resultado de execução de tarefa.

        Esperado:
        {
            "STATUS": "OK" ou "NOK",
            "TASK_ID": "uuid-1234",
            "WORKER_UUID": "Worker_1",
            "RESULT": <resultado>,
            "ERROR": "mensagem de erro (se NOK)"
        }
        """
        # Em nova especificação de wire, workers reportam apenas STATUS e WORKER_UUID
        task_id = data.get("TASK_ID")
        worker_uuid = data.get("WORKER_UUID")
        status = data.get("STATUS")
        result = data.get("RESULT")
        error = data.get("ERROR")

        # Se TASK_ID não for fornecido, tentamos recuperar pela atribuição do worker
        if not task_id and worker_uuid:
            task_id = self.task_manager.worker_tasks.get(worker_uuid)

        if not worker_uuid or not status or not task_id:
            logger.warning(f"Reporte incompleto de {worker_uuid}")
            return {"STATUS": "ERROR", "MESSAGE": "Campos obrigatórios faltando"}

        task = self.task_manager.get_task(task_id)
        if not task:
            logger.warning(f"Tarefa {task_id} não encontrada")
            return {"STATUS": "ERROR", "MESSAGE": "TASK_ID desconhecido"}

        # Processar resultado: resultado pode não ser enviado pelo wire (policy: mark complete without payload)
        if status == "OK":
            self.task_manager.complete_task(task_id, result)
            logger.info(f"✓ {task_id[:8]} completada por {worker_uuid}")
        else:
            error_msg = error or "Erro desconhecido"
            self.task_manager.fail_task(task_id, error_msg)
            logger.warning(f"✗ {task_id[:8]} falhou: {error_msg}")

        # Responder com ACK (conforme spec: minimal)
        return {"STATUS": "ACK"}

    # ============ PROTOCOLO - MASTER-TO-MASTER ============

    def handle_master_request(self, data: dict, addr: tuple) -> dict:
        """
        Processa mensagens vindas de outros masters (REQUEST_HELP / RESPONSE_*)
        """
        # Normalizar envelope: primeiro tentar o formato PDF (spec),
        # se não válido, cair para o formato legacy/novo.
        env = parse_master_envelope_spec(data)
        if isinstance(env, dict) and env.get("error"):
            env = parse_master_envelope(data)
        mtype = env.get("type")
        payload = env.get("payload") or {}

        # Autenticação entre masters (opcional)
        if MASTER_AUTH_TOKEN:
            if payload.get("AUTH_TOKEN") != MASTER_AUTH_TOKEN:
                logger.warning(f"Auth failed for master at {addr}")
                return build_master_envelope("error", {"message": "AUTH_FAILED"}, request_id=env.get("request_id"))

        if mtype == "request_help":
            # Outro master pede ajuda (emprestar workers / aceitar tarefas)
            requested = payload.get("workers_needed", 0)
            from_server = payload.get("master_id") or payload.get("from_server")

            stats = self.task_manager.get_statistics()
            current_load = stats["tasks"]["pending"] + stats["tasks"]["in_progress"]

            # Simples política: aceitar se carga atual + requested <= 70% da capacidade
            threshold = int(CAPACITY * 0.7)
            can_accept = current_load + requested <= threshold

            logger.info(
                f"Master request from {from_server}: requested={requested}, load={current_load}, accept={can_accept}"
            )

            if can_accept:
                # Offer workers (simplified: offer up to requested)
                offer = min(requested, max(0, threshold - current_load))
                return build_master_envelope_spec(
                    "response_accepted",
                    {"workers_offered": offer, "worker_details": []},
                    request_id=env.get("request_id"),
                )
            else:
                return build_master_envelope_spec(
                    "response_rejected",
                    {"reason": "high_load"},
                    request_id=env.get("request_id"),
                )

        if mtype == "request_state":
            # Outro master/worker solicita o estado salvo de um server_uuid
            target = payload.get("TARGET_SERVER") or payload.get("TARGET_SERVER")
            if not target:
                return build_master_envelope(
                    "error", {"message": "TARGET_SERVER faltando"}, request_id=env.get("request_id")
                )

            # caminho do estado
            try:
                path = self.task_manager._state_path(target)
                if not path or not os.path.exists(path):
                    return build_master_envelope("response_state", {"FOUND": False}, request_id=env.get("request_id"))

                with open(path, "r", encoding="utf-8") as f:
                    state = json.load(f)

                return build_master_envelope(
                    "response_state",
                    {"FOUND": True, "TARGET_SERVER": target, "STATE": state},
                    request_id=env.get("request_id"),
                )

            except Exception as exc:
                logger.warning(f"Erro ao fornecer estado para {addr}: {exc}")
                return build_master_envelope("error", {"message": str(exc)}, request_id=env.get("request_id"))

        else:
            logger.warning(f"Mensagem MASTER desconhecida de {addr}: {data}")
            return build_master_envelope(
                "error", {"message": "Tipo MASTER desconhecido"}, request_id=env.get("request_id")
            )

    def request_help_to_peer(self, peer_host: str, peer_port: int, requested: int = 1, timeout: int = 5) -> dict:
        """Envia uma solicitação de ajuda (REQUEST_HELP) a um peer master e devolve a resposta."""
        payload = {
            "master_id": self.server_uuid,
            "workers_needed": requested,
            "current_load": self.task_manager.get_statistics(),
        }

        envelope = build_master_envelope_spec("request_help", payload)

        try:
            sock = socket.create_connection((peer_host, peer_port), timeout=timeout)
            sock.settimeout(timeout)
            sock_file = sock.makefile("r", encoding="utf-8")

            send_json(sock, envelope)
            response = recv_json_line(sock_file)

            # Parse normalized envelope (prefer spec parser)
            if response and isinstance(response, dict):
                parsed = None
                try:
                    parsed = parse_master_envelope_spec(response)
                except Exception:
                    parsed = parse_master_envelope(response)

                try:
                    sock.close()
                except Exception:
                    pass

                return parsed or {}

            try:
                sock.close()
            except Exception:
                pass

            return {}

        except Exception as exc:
            logger.warning(f"Não foi possível contatar peer {peer_host}:{peer_port}: {exc}")
            return {"type": "error", "message": str(exc)}

    # ============ GERENCIAMENTO DE CONEXÃO ============

    def handle_client(self, conn: socket.socket, addr: tuple) -> None:
        """
        Processa conexão de um worker.
        Implementa ciclo de: apresentação → distribuição → execução → reporte.
        """
        logger.info(f"Conexão recebida de {addr}")
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
                if data.get("WORKER") == "ALIVE":
                    # Pode ser apresentação ou solicitação de tarefa
                    worker_uuid = data.get("WORKER_UUID")

                    # Verificar se é primeira vez (apresentação) ou solicitação de tarefa
                    worker = self.task_manager.get_worker(worker_uuid) if worker_uuid else None

                    if worker is None:
                        # Primeira apresentação
                        self.handle_worker_alive(data, addr)
                    else:
                        # Worker conhecido, é solicitação de tarefa
                        self.handle_request_task(data, addr)

                elif data.get("STATUS") in ["OK", "NOK"]:
                    # Reporte de resultado
                    self.handle_task_result(data, addr)
                    worker_uuid = data.get("WORKER_UUID")

                worker_uuid = data.get("WORKER_UUID")

                if not worker_uuid:
                    return {"TASK": "ERROR", "MESSAGE": "WORKER_UUID obrigatório"}

                # Atualizar heartbeat
                self.task_manager.update_worker_heartbeat(worker_uuid)

                if self.failback_target:
                    return self._build_failback_redirect()

                # Obter próxima tarefa
                task_id = self.task_manager.get_pending_task()

                if not task_id:
                    logger.debug(f"Nenhuma tarefa para {worker_uuid}")

                    # Tentar pedir ajuda a peers configurados
                    for peer in PEER_MASTERS:
                        try:
                            peer_host, peer_port, peer_uuid = peer
                        except Exception:
                            continue

                        resp = self.request_help_to_peer(peer_host, peer_port, requested=1)

                        if resp and resp.get("type") == "response_accepted":
                            payload = resp.get("payload") or {}
                            if payload.get("workers_offered", 0) > 0:
                                logger.debug(f"Redirecionando para {peer_uuid}")
                                return {
                                    "TASK": "REDIRECT",
                                    "TARGET_HOST": peer_host,
                                    "TARGET_PORT": peer_port,
                                    "TARGET_SERVER_UUID": peer_uuid,
                                }

                    return {"TASK": "NO_TASK"}

                # Atribuir tarefa ao worker
                task = self.task_manager.get_task(task_id)
                if not task:
                    return {"TASK": "NO_TASK"}

                self.task_manager.assign_task(task_id, worker_uuid)
                logger.info(f"➔ {task.operation} atribuída a {worker_uuid}")

                # Construir payload `USER` conforme contrato de rede.
                try:
                    user_payload = (
                        task.user
                        if getattr(task, "user", None)
                        else json.dumps({"operation": task.operation, "values": task.values})
                    )
                except Exception:
                    user_payload = json.dumps({"operation": task.operation, "values": task.values})

                # Enviar tarefa respeitando o contrato: somente `TASK` e `USER` são obrigatórios
                return {"TASK": "QUERY", "USER": user_payload}
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

            # Iniciar thread de monitoramento
            monitor_thread = threading.Thread(target=self.worker_monitor_thread, daemon=True)
            monitor_thread.start()

            # Aceitar conexões
            while self.running:
                try:
                    conn, addr = server.accept()
                except socket.timeout:
                    continue

                logger.info(f"📨 Conexão recebida de {addr}")

                worker_thread = threading.Thread(target=self.handle_client, args=(conn, addr), daemon=True)
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
