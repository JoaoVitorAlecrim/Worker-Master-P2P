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
from common.protocol import send_json, recv_json_line
from common.task_manager import TaskManager
from common.models import TaskStatus, WorkerStatus
import uuid

import os

# Configuração (podem ser sobrescritas via env para testes)
HOST = os.getenv("MASTER_HOST", "0.0.0.0")
PORT = int(os.getenv("MASTER_PORT", "5000"))
MASTER_PEER_PORT = int(os.getenv("MASTER_PEER_PORT", os.getenv("MASTER_PORT", "5000")))
SERVER_UUID = os.getenv("SERVER_UUID", "Master_A")
MASTER_AUTH_TOKEN = os.getenv("MASTER_AUTH_TOKEN")
SOCKET_TIMEOUT = 15
HEARTBEAT_TIMEOUT = 15  # Segundos até considerar worker offline
TASK_TIMEOUT = 30  # Segundos até considerar tarefa expirada
WORKER_CHECK_INTERVAL = 5  # Segundos entre checks de workers
CAPACITY = 100  # Número máximo de tarefas pendentes
FAILBACK_GRACE_SECONDS = int(os.getenv("FAILBACK_GRACE_SECONDS", "5"))
# Lista de masters pares (lab config). Pode ser passada via env MASTER_PEERS como
# "host:port:uuid,host2:port2:uuid2"
PEER_MASTERS = []
logger = logging.getLogger(__name__)
peers_env = os.getenv("MASTER_PEERS")
if peers_env:
    for part in peers_env.split(','):
        try:
            parts = part.split(':')
            if len(parts) == 3:
                h, p, u = parts
                p = int(p)
            elif len(parts) == 2:
                h, u = parts
                p = MASTER_PEER_PORT
            else:
                raise ValueError("Formato incorreto")

            PEER_MASTERS.append((h, int(p), u))
        except Exception:
            logger.warning(f"MASTER_PEERS inválido: {part}")

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
        self.failback_initiated_at = None
        self.election_leader_info = None
    
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
        worker_uuid = data.get("WORKER_UUID")
        original_master = data.get("SERVER_UUID", self.server_uuid)
        free_disk_bytes = data.get("FREE_DISK_BYTES")
        # Autenticação (opcional)
        if MASTER_AUTH_TOKEN:
            if data.get("AUTH_TOKEN") != MASTER_AUTH_TOKEN:
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
            "WORKERS": self.task_manager.get_online_worker_snapshot(),
        })
    
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
            return {
                "TASK": "ERROR",
                "MESSAGE": "WORKER_UUID obrigatório"
            }

        # Atualizar heartbeat
        self.task_manager.update_worker_heartbeat(worker_uuid)

        if self.failback_target:
            return self._attach_election_info(self._build_failback_redirect())

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

                if resp and resp.get("MASTER") == "RESPONSE_HELP" and resp.get("ACCEPT"):
                    logger.debug(f"Redirecionando para {peer_uuid}")
                    return self._attach_election_info({
                        "TASK": "REDIRECT",
                        "TARGET_HOST": peer_host,
                        "TARGET_PORT": peer_port,
                        "TARGET_SERVER_UUID": peer_uuid,
                        "WORKERS": self.task_manager.get_online_worker_snapshot(),
                    })

            return self._attach_election_info({"TASK": "NO_TASK", "WORKERS": self.task_manager.get_online_worker_snapshot()})
        
        # Atribuir tarefa ao worker
        task = self.task_manager.get_task(task_id)
        if not task:
            return self._attach_election_info({"TASK": "NO_TASK", "WORKERS": self.task_manager.get_online_worker_snapshot()})
        
        self.task_manager.assign_task(task_id, worker_uuid)
        logger.info(f"➔ {task.operation} atribuída a {worker_uuid}")
        
        # Enviar tarefa
        return self._attach_election_info({
            "TASK": "QUERY",
            "TASK_ID": task_id,
            "OPERATION": task.operation,
            "VALUES": task.values,
            "WORKERS": self.task_manager.get_online_worker_snapshot(),
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
            "TASK_ID": "uuid-1234",
            "WORKER_UUID": "Worker_1",
            "RESULT": <resultado>,
            "ERROR": "mensagem de erro (se NOK)"
        }
        """
        task_id = data.get("TASK_ID")
        worker_uuid = data.get("WORKER_UUID")
        status = data.get("STATUS")
        result = data.get("RESULT")
        error = data.get("ERROR")
        
        if not task_id or not worker_uuid or not status:
            logger.warning(f"Reporte incompleto de {worker_uuid}")
            return {
                "STATUS": "ERROR",
                "MESSAGE": "Campos obrigatórios faltando"
            }
        
        task = self.task_manager.get_task(task_id)
        if not task:
            logger.warning(f"Tarefa {task_id} não encontrada")
            return {
                "STATUS": "ERROR",
                "MESSAGE": "TASK_ID desconhecido"
            }
        
        # Processar resultado
        if status == "OK":
            self.task_manager.complete_task(task_id, result)
            logger.info(f"✓ {task_id[:8]} completada por {worker_uuid}")
        else:
            error_msg = error or "Erro desconhecido"
            self.task_manager.fail_task(task_id, error_msg)
            logger.warning(f"✗ {task_id[:8]} falhou: {error_msg}")
        
        # Responder com ACK
        return self._attach_election_info({
            "STATUS": "ACK",
            "TASK_ID": task_id,
            "WORKER_UUID": worker_uuid
        })

    # ============ PROTOCOLO - MASTER-TO-MASTER ============

    def handle_master_request(self, data: dict, addr: tuple) -> dict:
        """
        Processa mensagens vindas de outros masters (REQUEST_HELP / RESPONSE_*)
        """
        # Autenticação entre masters (opcional)
        if MASTER_AUTH_TOKEN:
            if data.get("AUTH_TOKEN") != MASTER_AUTH_TOKEN:
                logger.warning(f"Auth failed for master at {addr}")
                return {"MASTER": "ERROR", "MESSAGE": "AUTH_FAILED"}

        mtype = data.get("MASTER")

        if mtype == "REQUEST_HELP":
            # Outro master pede ajuda (emprestar workers / aceitar tarefas)
            requested = data.get("REQUESTED", 0)
            from_server = data.get("FROM_SERVER")

            stats = self.task_manager.get_statistics()
            current_load = stats["tasks"]["pending"] + stats["tasks"]["in_progress"]

            # Simples política: aceitar se carga atual < 70% da capacidade
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
            # Outro master/worker solicita o estado salvo de um server_uuid
            target = data.get("TARGET_SERVER")
            if not target:
                return {"MASTER": "ERROR", "MESSAGE": "TARGET_SERVER faltando"}

            # caminho do estado
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

        else:
            logger.warning(f"Mensagem MASTER desconhecida de {addr}: {data}")
            return {"MASTER": "ERROR", "MESSAGE": "Tipo MASTER desconhecido"}

    def request_help_to_peer(self, peer_host: str, peer_port: int, requested: int = 1, timeout: int = 5) -> dict:
        """Envia uma solicitação de ajuda (REQUEST_HELP) a um peer master e devolve a resposta."""
        payload = {
            "MASTER": "REQUEST_HELP",
            "REQUEST_ID": str(uuid.uuid4()),
            "FROM_SERVER": self.server_uuid,
            "REQUESTED": requested,
            "LOAD": self.task_manager.get_statistics()
        }

        try:
            sock = socket.create_connection((peer_host, peer_port), timeout=timeout)
            sock.settimeout(timeout)
            sock_file = sock.makefile("r", encoding="utf-8")

            send_json(sock, payload)
            response = recv_json_line(sock_file)

            try:
                sock.close()
            except:
                pass

            return response or {}

        except Exception as exc:
            logger.warning(f"Não foi possível contatar peer {peer_host}:{peer_port}: {exc}")
            return {"MASTER": "ERROR", "MESSAGE": str(exc)}
    
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
                        response = self.handle_worker_alive(data, addr)
                    else:
                        # Worker conhecido, é solicitação de tarefa
                        response = self.handle_request_task(data, addr)
                
                elif data.get("STATUS") in ["OK", "NOK"]:
                    # Reporte de resultado
                    response = self.handle_task_result(data, addr)
                    worker_uuid = data.get("WORKER_UUID")

                elif data.get("MASTER"):
                    # Mensagem vinda de outro Master
                    response = self.handle_master_request(data, addr)
                
                else:
                    logger.warning(f"Mensagem desconhecida de {addr}: {data}")
                    response = {
                        "TASK": "ERROR",
                        "MESSAGE": "Tipo de mensagem desconhecido"
                    }
                
                # Enviar resposta
                send_json(conn, response)
                logger.debug(f"Enviado para {addr}: {response}")
        
        except socket.timeout:
            logger.warning(f"Timeout com {addr}")
        except ConnectionResetError:
            logger.warning(f"Conexão resetada por {addr}")
        except Exception as exc:
            logger.error(f"Erro ao processar {addr}: {exc}")
        
        finally:
            try:
                conn.close()
            except:
                pass
            
            # Se worker era conhecido, detectar tarefas em execução
            if worker_uuid:
                self._handle_worker_disconnect(worker_uuid)
    
    def _handle_worker_disconnect(self, worker_uuid: str) -> None:
        """Processa desconexão de um worker."""
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
    
    # ============ MONITORAMENTO ============
    
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

                # Se um failback target foi definido (master original voltou), iniciar contagem regressiva
                if self.failback_target:
                    if self.failback_initiated_at is None:
                        self.failback_initiated_at = time.time()
                        logger.info(f"Failback iniciado; encerrarei este master em {FAILBACK_GRACE_SECONDS}s se os workers forem redirecionados")
                    else:
                        elapsed = time.time() - self.failback_initiated_at
                        if elapsed >= FAILBACK_GRACE_SECONDS:
                            logger.info("Failback grace elapsed — encerrando master promovido para permitir retorno do master original")
                            # salvar estado antes de desligar
                            try:
                                if self.server_uuid:
                                    self.task_manager.save_state(self.server_uuid)
                            except Exception:
                                pass
                            self.running = False
                            break
                
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
            monitor_thread = threading.Thread(
                target=self.worker_monitor_thread,
                daemon=True
            )
            monitor_thread.start()
            
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