"""
Models para o sistema P2P Master-Worker.
Define estruturas de dados para tarefas, workers e logs.
"""

import uuid
import time
from enum import Enum
from dataclasses import dataclass, field, asdict
from typing import Any, Optional, List


class TaskStatus(str, Enum):
    """Estados possíveis de uma tarefa."""

    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"
    FAILED = "failed"
    REASSIGNED = "reassigned"


class WorkerStatus(str, Enum):
    """Estados possíveis de um worker."""

    ONLINE = "online"
    OFFLINE = "offline"
    BUSY = "busy"
    IDLE = "idle"


@dataclass
class Task:
    """Representa uma tarefa no sistema."""

    task_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    operation: str = ""  # soma, multiplicacao, sleep
    values: List[Any] = field(default_factory=list)
    # Campo externo (NETWORK): o payload recebido do master deve fornecer
    # apenas o campo `user`. Internamente mantemos `operation`/`values`.
    user: Optional[str] = None

    # Rastreamento
    status: TaskStatus = TaskStatus.PENDING
    assigned_worker: Optional[str] = None  # WORKER_UUID
    assigned_timestamp: Optional[float] = None

    # Execução
    start_time: Optional[float] = None
    end_time: Optional[float] = None
    result: Optional[Any] = None
    error_message: Optional[str] = None

    # Metadados
    created_at: float = field(default_factory=time.time)
    updated_at: float = field(default_factory=time.time)
    retries: int = 0
    max_retries: int = 3

    def to_dict(self) -> dict:
        """Converte tarefa para dicionário."""
        return asdict(self)

    def is_expired(self, timeout_seconds: int = 30) -> bool:
        """Verifica se a tarefa expirou (sem resposta)."""
        if self.status != TaskStatus.IN_PROGRESS:
            return False

        if self.assigned_timestamp is None:
            return False

        elapsed = time.time() - self.assigned_timestamp
        return elapsed > timeout_seconds

    def mark_in_progress(self, worker_uuid: str) -> None:
        """Marca tarefa como em execução."""
        self.status = TaskStatus.IN_PROGRESS
        self.assigned_worker = worker_uuid
        self.assigned_timestamp = time.time()
        self.start_time = time.time()
        self.updated_at = time.time()

    def mark_completed(self, result: Any) -> None:
        """Marca tarefa como completada."""
        self.status = TaskStatus.COMPLETED
        self.result = result
        self.end_time = time.time()
        self.updated_at = time.time()

    def mark_failed(self, error_message: str) -> None:
        """Marca tarefa como falhada."""
        self.status = TaskStatus.FAILED
        self.error_message = error_message
        self.end_time = time.time()
        self.updated_at = time.time()

    def mark_reassigned(self) -> None:
        """Marca tarefa como remandada."""
        self.status = TaskStatus.REASSIGNED
        self.assigned_worker = None
        self.assigned_timestamp = None
        self.retries += 1
        self.updated_at = time.time()

    def can_retry(self) -> bool:
        """Verifica se tarefa pode ser retentada."""
        return self.retries < self.max_retries


@dataclass
class Worker:
    """Representa um worker no sistema."""

    worker_uuid: str  # UUID único do worker
    server_uuid: str  # Master que o worker pertence originalmente
    host: Optional[str] = None  # Endereço observado pelo master
    free_disk_bytes: Optional[int] = None  # Espaço livre em disco
    status: WorkerStatus = WorkerStatus.OFFLINE

    # Conexão
    last_heartbeat: float = field(default_factory=time.time)
    connection_failures: int = 0

    # Tarefas
    current_task_id: Optional[str] = None
    completed_tasks: int = 0
    failed_tasks: int = 0

    # Metadados
    created_at: float = field(default_factory=time.time)
    updated_at: float = field(default_factory=time.time)

    def to_dict(self) -> dict:
        """Converte worker para dicionário."""
        return asdict(self)

    def is_alive(self, heartbeat_timeout: int = 15) -> bool:
        """Verifica se worker está vivo (baseado em heartbeat)."""
        elapsed = time.time() - self.last_heartbeat
        return elapsed < heartbeat_timeout

    def update_heartbeat(self) -> None:
        """Atualiza timestamp do último heartbeat."""
        self.last_heartbeat = time.time()
        self.connection_failures = 0
        self.updated_at = time.time()

    def record_failure(self) -> None:
        """Registra falha de conexão."""
        self.connection_failures += 1
        self.updated_at = time.time()

    def mark_offline(self) -> None:
        """Marca worker como offline."""
        self.status = WorkerStatus.OFFLINE
        self.updated_at = time.time()

    def mark_online(self) -> None:
        """Marca worker como online."""
        self.status = WorkerStatus.ONLINE
        self.updated_at = time.time()
        self.connection_failures = 0


@dataclass
class TaskLog:
    """Log de eventos de uma tarefa."""

    task_id: str
    timestamp: float = field(default_factory=time.time)
    event_type: str = ""  # created, assigned, started, completed, failed, reassigned
    worker_uuid: Optional[str] = None
    details: dict = field(default_factory=dict)

    def to_dict(self) -> dict:
        """Converte log para dicionário."""
        return asdict(self)


# Código de teste rápido
if __name__ == "__main__":
    # Teste de Task
    task = Task(operation="soma", values=[1, 2])
    print(f"✓ Task criada: {task.task_id}")
    print(f"  Status: {task.status}")

    task.mark_in_progress("Worker_1")
    print(f"  Atualizado: {task.status}")

    task.mark_completed(3)
    print(f"  Resultado: {task.result}")

    # Teste de Worker
    worker = Worker(worker_uuid="Worker_1", server_uuid="Master_A")
    print(f"\n✓ Worker criado: {worker.worker_uuid}")
    print(f"  Alive: {worker.is_alive()}")

    worker.mark_online()
    print(f"  Status: {worker.status}")
