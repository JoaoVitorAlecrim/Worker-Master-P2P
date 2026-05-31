"""
TaskManager: Gerenciador central de tarefas.
Responsável por rastreamento, atribuição e remande de tarefas.
"""

import threading
import json
import os
from typing import Dict, List, Optional, Any
from collections import defaultdict
from queue import Queue
from common.models import Task, TaskStatus, Worker, WorkerStatus, TaskLog
import time


class TaskManager:
    """Gerencia o ciclo de vida completo das tarefas."""
    
    def __init__(self):
        self.tasks: Dict[str, Task] = {}  # task_id -> Task
        self.workers: Dict[str, Worker] = {}  # worker_uuid -> Worker
        self.task_logs: List[TaskLog] = []  # Histórico de eventos
        
        # Índices para acesso rápido
        self.pending_queue: Queue = Queue()  # Tarefas pendentes
        self.worker_tasks: Dict[str, Optional[str]] = defaultdict(lambda: None)  # worker_uuid -> task_id
        
        # Thread safety
        self.lock = threading.RLock()
        self.persistence_server_uuid: Optional[str] = None

    # ============ PERSISTÊNCIA DE ESTADO ============

    def _state_path(self, server_uuid: str) -> str:
        base = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'data')
        os.makedirs(base, exist_ok=True)
        return os.path.join(base, f'tasks_{server_uuid}.json')

    def save_state(self, server_uuid: str) -> None:
        """Salva estado atual (tasks, workers, logs) em arquivo JSON."""
        with self.lock:
            data = {
                'tasks': {tid: t.to_dict() for tid, t in self.tasks.items()},
                'workers': {wid: w.to_dict() for wid, w in self.workers.items()},
                'logs': [l.to_dict() for l in self.task_logs]
            }
            path = self._state_path(server_uuid)
            with open(path, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)

    def load_state(self, server_uuid: str) -> None:
        """Carrega estado salvo de disco se existir."""
        path = self._state_path(server_uuid)
        if not os.path.exists(path):
            return

        try:
            with open(path, 'r', encoding='utf-8') as f:
                data = json.load(f)

            with self.lock:
                # Reconstituir tasks
                for tid, td in (data.get('tasks') or {}).items():
                    task = Task(operation=td.get('operation', ''), values=td.get('values', []))
                    # overwrite fields
                    for k, v in td.items():
                        setattr(task, k, v)
                    # ensure enum types
                    try:
                        task.status = TaskStatus(td.get('status'))
                    except Exception:
                        pass
                    self.tasks[tid] = task
                    if task.status in (TaskStatus.PENDING, TaskStatus.REASSIGNED):
                        self.pending_queue.put(tid)

                # Reconstituir workers
                for wid, wd in (data.get('workers') or {}).items():
                    worker = Worker(worker_uuid=wd.get('worker_uuid'), server_uuid=wd.get('server_uuid'))
                    for k, v in wd.items():
                        setattr(worker, k, v)
                    try:
                        worker.status = WorkerStatus(wd.get('status'))
                    except Exception:
                        pass
                    self.workers[wid] = worker

                # Logs
                self.task_logs = []
                for ld in (data.get('logs') or []):
                    log = TaskLog(task_id=ld.get('task_id'), timestamp=ld.get('timestamp', 0), event_type=ld.get('event_type', ''), worker_uuid=ld.get('worker_uuid'), details=ld.get('details', {}))
                    self.task_logs.append(log)

        except Exception:
            # Falha ao carregar — ignorar e começar do zero
            return

    def load_state_dict(self, data: dict) -> None:
        """Carrega estado a partir de um dicionário (sem usar disco)."""
        if not data:
            return

        try:
            with self.lock:
                # Reconstituir tasks
                self.tasks = {}
                self.pending_queue = Queue()
                for tid, td in (data.get('tasks') or {}).items():
                    task = Task(operation=td.get('operation', ''), values=td.get('values', []))
                    for k, v in td.items():
                        setattr(task, k, v)
                    try:
                        task.status = TaskStatus(td.get('status'))
                    except Exception:
                        pass
                    self.tasks[tid] = task
                    if task.status in (TaskStatus.PENDING, TaskStatus.REASSIGNED):
                        self.pending_queue.put(tid)

                # Reconstituir workers
                self.workers = {}
                for wid, wd in (data.get('workers') or {}).items():
                    worker = Worker(worker_uuid=wd.get('worker_uuid'), server_uuid=wd.get('server_uuid'))
                    for k, v in wd.items():
                        setattr(worker, k, v)
                    try:
                        worker.status = WorkerStatus(wd.get('status'))
                    except Exception:
                        pass
                    self.workers[wid] = worker

                # Logs
                self.task_logs = []
                for ld in (data.get('logs') or []):
                    log = TaskLog(task_id=ld.get('task_id'), timestamp=ld.get('timestamp', 0), event_type=ld.get('event_type', ''), worker_uuid=ld.get('worker_uuid'), details=ld.get('details', {}))
                    self.task_logs.append(log)

        except Exception:
            return
    
    # ============ MÉTODOS DE TAREFA ============
    
    def create_task(self, operation: str, values: List[Any]) -> Task:
        """Cria nova tarefa e a adiciona à fila."""
        with self.lock:
            task = Task(operation=operation, values=values)
            self.tasks[task.task_id] = task
            self.pending_queue.put(task.task_id)
            
            self._log_event(task.task_id, "created", details={
                "operation": operation,
                "values": values
            })
            try:
                if self.persistence_server_uuid:
                    self.save_state(self.persistence_server_uuid)
            except Exception:
                pass

            return task

    def create_task_from_user(self, user: str) -> Task:
        """Cria nova tarefa a partir do payload externo `USER`.

        Este método mantém compatibilidade com o contrato de rede onde o
        master envia somente `USER`. Internamente a tarefa conterá o
        campo `user` e os campos ricos (`operation`/`values`) podem ser
        populados por um parser interno posteriormente.
        """
        with self.lock:
            task = Task(operation="", values=[], user=user)
            self.tasks[task.task_id] = task
            self.pending_queue.put(task.task_id)

            self._log_event(task.task_id, "created", details={
                "user": user
            })
            try:
                if self.persistence_server_uuid:
                    self.save_state(self.persistence_server_uuid)
            except Exception:
                pass

            return task

    # Trigger save hooks on state-changing operations
    
    def get_task(self, task_id: str) -> Optional[Task]:
        """Recupera tarefa pelo ID."""
        with self.lock:
            return self.tasks.get(task_id)
    
    def get_pending_task(self, block: bool = False) -> Optional[str]:
        """Obtém próxima tarefa pendente (retorna task_id ou None)."""
        try:
            if block:
                task_id = self.pending_queue.get(timeout=1)
            else:
                task_id = self.pending_queue.get_nowait()
            return task_id
        except:
            return None
    
    def assign_task(self, task_id: str, worker_uuid: str) -> bool:
        """Atribui tarefa a um worker."""
        with self.lock:
            task = self.tasks.get(task_id)
            if not task:
                return False
            
            if task.status != TaskStatus.PENDING and task.status != TaskStatus.REASSIGNED:
                return False
            
            task.mark_in_progress(worker_uuid)
            self.worker_tasks[worker_uuid] = task_id
            
            self._log_event(task_id, "assigned", worker_uuid=worker_uuid, details={
                "worker": worker_uuid,
                "retry": task.retries
            })
            try:
                if self.persistence_server_uuid:
                    self.save_state(self.persistence_server_uuid)
            except Exception:
                pass

            return True
    
    def complete_task(self, task_id: str, result: Any) -> bool:
        """Marca tarefa como completada."""
        with self.lock:
            task = self.tasks.get(task_id)
            if not task:
                return False
            
            task.mark_completed(result)
            
            if task.assigned_worker:
                self.worker_tasks[task.assigned_worker] = None
                worker = self.workers.get(task.assigned_worker)
                if worker:
                    worker.completed_tasks += 1
            
            self._log_event(task_id, "completed", 
                          worker_uuid=task.assigned_worker,
                          details={"result": str(result)[:100]})
            try:
                if self.persistence_server_uuid:
                    self.save_state(self.persistence_server_uuid)
            except Exception:
                pass

            return True
    
    def fail_task(self, task_id: str, error_message: str) -> bool:
        """Marca tarefa como falhada."""
        with self.lock:
            task = self.tasks.get(task_id)
            if not task:
                return False
            
            task.mark_failed(error_message)
            
            if task.assigned_worker:
                self.worker_tasks[task.assigned_worker] = None
                worker = self.workers.get(task.assigned_worker)
                if worker:
                    worker.failed_tasks += 1
            
            self._log_event(task_id, "failed",
                          worker_uuid=task.assigned_worker,
                          details={"error": error_message})
            try:
                if self.persistence_server_uuid:
                    self.save_state(self.persistence_server_uuid)
            except Exception:
                pass

            return True
    
    def reassign_task(self, task_id: str) -> bool:
        """Remande tarefa para outro worker."""
        with self.lock:
            task = self.tasks.get(task_id)
            if not task:
                return False
            
            if not task.can_retry():
                task.mark_failed(f"Máximo de tentativas ({task.max_retries}) atingido")
                return False
            
            old_worker = task.assigned_worker
            task.mark_reassigned()
            self.pending_queue.put(task.task_id)
            
            if old_worker:
                self.worker_tasks[old_worker] = None
            
            self._log_event(task_id, "reassigned",
                          worker_uuid=old_worker,
                          details={"old_worker": old_worker, "retry": task.retries})
            try:
                if self.persistence_server_uuid:
                    self.save_state(self.persistence_server_uuid)
            except Exception:
                pass

            return True
    
    def get_tasks_by_worker(self, worker_uuid: str) -> List[Task]:
        """Retorna todas as tarefas de um worker."""
        with self.lock:
            return [task for task in self.tasks.values()
                   if task.assigned_worker == worker_uuid]
    
    def get_tasks_by_status(self, status: TaskStatus) -> List[Task]:
        """Retorna tarefas com status específico."""
        with self.lock:
            return [task for task in self.tasks.values()
                   if task.status == status]
    
    def detect_and_reassign_dead_worker(self, worker_uuid: str) -> List[str]:
        """
        Detecta tarefas em execução de um worker que caiu.
        Remanda automaticamente.
        Retorna lista de task_ids remandadas.
        """
        with self.lock:
            reassigned_tasks = []
            in_progress_tasks = self.get_tasks_by_status(TaskStatus.IN_PROGRESS)
            
            for task in in_progress_tasks:
                if task.assigned_worker == worker_uuid:
                    if self.reassign_task(task.task_id):
                        reassigned_tasks.append(task.task_id)
            
            if worker_uuid in self.worker_tasks:
                self.worker_tasks[worker_uuid] = None
            
            return reassigned_tasks
    
    # ============ MÉTODOS DE WORKER ============
    
    def register_worker(self, worker_uuid: str, server_uuid: str, host: Optional[str] = None, free_disk_bytes: Optional[int] = None) -> Worker:
        """Registra novo worker no sistema."""
        with self.lock:
            if worker_uuid not in self.workers:
                worker = Worker(worker_uuid=worker_uuid, server_uuid=server_uuid)
                self.workers[worker_uuid] = worker
            
            worker = self.workers[worker_uuid]
            if host is not None:
                worker.host = host
            if free_disk_bytes is not None:
                worker.free_disk_bytes = free_disk_bytes
            worker.mark_online()
            worker.update_heartbeat()
            try:
                if self.persistence_server_uuid:
                    self.save_state(self.persistence_server_uuid)
            except Exception:
                pass

            return worker

    def set_persistence(self, server_uuid: str) -> None:
        """Configura o server_uuid para persistência automática de estado."""
        with self.lock:
            self.persistence_server_uuid = server_uuid
    
    def get_worker(self, worker_uuid: str) -> Optional[Worker]:
        """Recupera worker pelo UUID."""
        with self.lock:
            return self.workers.get(worker_uuid)
    
    def update_worker_heartbeat(self, worker_uuid: str) -> bool:
        """Atualiza heartbeat de um worker."""
        with self.lock:
            worker = self.workers.get(worker_uuid)
            if not worker:
                return False
            
            worker.update_heartbeat()
            return True
    
    def get_available_workers(self) -> List[Worker]:
        """Retorna workers online e não ocupados."""
        with self.lock:
            available = []
            for worker in self.workers.values():
                if worker.is_alive() and worker.worker_tasks is None:
                    available.append(worker)
            return available
    
    def get_all_workers(self) -> List[Worker]:
        """Retorna todos os workers."""
        with self.lock:
            return list(self.workers.values())

    def get_online_worker_snapshot(self) -> List[dict]:
        """Retorna um snapshot leve dos workers online para gossip/elegibilidade."""
        with self.lock:
            snapshot = []
            for worker in self.workers.values():
                if worker.is_alive() and worker.status != WorkerStatus.OFFLINE:
                    snapshot.append({
                        "WORKER_UUID": worker.worker_uuid,
                        "SERVER_UUID": worker.server_uuid,
                        "HOST": worker.host,
                        "FREE_DISK_BYTES": worker.free_disk_bytes,
                        "STATUS": worker.status.value,
                        "CURRENT_TASK_ID": worker.current_task_id,
                    })
            return snapshot
    
    # ============ MÉTODOS DE MONITORAMENTO ============
    
    def get_statistics(self) -> dict:
        """Retorna estatísticas do sistema."""
        with self.lock:
            all_tasks = list(self.tasks.values())
            all_workers = list(self.workers.values())
            
            pending = len(self.get_tasks_by_status(TaskStatus.PENDING))
            in_progress = len(self.get_tasks_by_status(TaskStatus.IN_PROGRESS))
            completed = len(self.get_tasks_by_status(TaskStatus.COMPLETED))
            failed = len(self.get_tasks_by_status(TaskStatus.FAILED))
            
            online_workers = sum(1 for w in all_workers if w.is_alive())
            offline_workers = len(all_workers) - online_workers
            
            return {
                "tasks": {
                    "pending": pending,
                    "in_progress": in_progress,
                    "completed": completed,
                    "failed": failed,
                    "total": len(all_tasks)
                },
                "workers": {
                    "online": online_workers,
                    "offline": offline_workers,
                    "total": len(all_workers)
                },
                "queue_size": self.pending_queue.qsize()
            }
    
    def get_task_history(self, task_id: str) -> List[TaskLog]:
        """Retorna histórico de eventos de uma tarefa."""
        with self.lock:
            return [log for log in self.task_logs if log.task_id == task_id]
    
    def check_expired_tasks(self, timeout_seconds: int = 30) -> List[str]:
        """
        Detecta tarefas que expiraram (sem resposta do worker).
        Remanda automaticamente.
        """
        with self.lock:
            expired_tasks = []
            in_progress_tasks = self.get_tasks_by_status(TaskStatus.IN_PROGRESS)
            
            for task in in_progress_tasks:
                if task.is_expired(timeout_seconds):
                    worker_uuid = task.assigned_worker
                    if self.reassign_task(task.task_id):
                        expired_tasks.append((task.task_id, worker_uuid))
            
            return expired_tasks
    
    # ============ MÉTODOS PRIVADOS ============
    
    def _log_event(self, task_id: str, event_type: str, 
                   worker_uuid: Optional[str] = None,
                   details: Optional[dict] = None) -> None:
        """Registra evento em log."""
        log = TaskLog(
            task_id=task_id,
            event_type=event_type,
            worker_uuid=worker_uuid,
            details=details or {}
        )
        self.task_logs.append(log)
    
    def clear_logs(self) -> None:
        """Limpa logs antigos (manter últimos 1000)."""
        with self.lock:
            if len(self.task_logs) > 1000:
                self.task_logs = self.task_logs[-1000:]


# Teste rápido
if __name__ == "__main__":
    tm = TaskManager()
    
    # Criar tarefas
    task1 = tm.create_task("soma", [1, 2])
    task2 = tm.create_task("multiplicacao", [3, 4])
    print(f"✓ Criadas 2 tarefas")
    
    # Registrar worker
    worker = tm.register_worker("Worker_1", "Master_A")
    print(f"✓ Worker registrado: {worker.worker_uuid}")
    
    # Atribuir tarefa
    tm.assign_task(task1.task_id, "Worker_1")
    print(f"✓ Tarefa atribuída")
    
    # Completar tarefa
    tm.complete_task(task1.task_id, 3)
    print(f"✓ Tarefa completada")
    
    # Estatísticas
    stats = tm.get_statistics()
    print(f"\nEstatísticas:")
    print(f"  Pending: {stats['tasks']['pending']}")
    print(f"  Completed: {stats['tasks']['completed']}")
    print(f"  Workers online: {stats['workers']['online']}")
