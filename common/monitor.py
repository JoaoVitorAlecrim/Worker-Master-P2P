"""Sprint 4: envio periódico de métricas ao supervisor do professor."""
import datetime
import json
import logging
import os
import socket
import ssl
import threading
import time
import uuid

try:
    import psutil
    _HAS_PSUTIL = True
except ImportError:
    _HAS_PSUTIL = False

logger = logging.getLogger(__name__)

SUPERVISOR_HOST = "nuted-ia.dev"
SUPERVISOR_PORT = 443
NEIGHBOR_STALE_SECONDS = 60


def collect_system_metrics() -> dict:
    """Retorna métricas reais do sistema via psutil."""
    if not _HAS_PSUTIL:
        return {
            "uptime_seconds": 0,
            "load_average_1m": 0.0,
            "load_average_5m": 0.0,
            "cpu": {"usage_percent": 0.0, "count_logical": 1, "count_physical": 1},
            "memory": {"total_mb": 0, "available_mb": 0, "percent_used": 0.0, "memory_used": 0},
            "disk": {"total_gb": 0.0, "free_gb": 0.0, "percent_used": 0.0},
        }

    cpu_pct = psutil.cpu_percent(interval=None)
    cpu_logical = psutil.cpu_count(logical=True) or 1
    cpu_physical = psutil.cpu_count(logical=False) or 1
    mem = psutil.virtual_memory()
    disk_path = "C:\\" if os.name == "nt" else "/"
    disk = psutil.disk_usage(disk_path)
    uptime = int(time.time() - psutil.boot_time())

    try:
        load_1m, load_5m, _ = psutil.getloadavg()
    except (AttributeError, OSError):
        load_1m, load_5m = 0.0, 0.0

    return {
        "uptime_seconds": uptime,
        "load_average_1m": round(load_1m, 2),
        "load_average_5m": round(load_5m, 2),
        "cpu": {
            "usage_percent": round(cpu_pct, 2),
            "count_logical": cpu_logical,
            "count_physical": cpu_physical,
        },
        "memory": {
            "total_mb": mem.total // (1024 * 1024),
            "available_mb": mem.available // (1024 * 1024),
            "percent_used": round(mem.percent, 2),
            "memory_used": mem.used // (1024 * 1024),
        },
        "disk": {
            "total_gb": round(disk.total / (1024 ** 3), 1),
            "free_gb": round(disk.free / (1024 ** 3), 1),
            "percent_used": round(disk.percent, 1),
        },
    }


def _build_farm_state(master) -> dict:
    from common.models import WorkerStatus, TaskStatus

    all_workers = master.task_manager.get_all_workers()
    received = [w for w in all_workers if w.is_temporary]
    alive = [w for w in all_workers if w.status != WorkerStatus.OFFLINE]
    busy = [w for w in all_workers if w.current_task_id is not None]
    failed = [w for w in all_workers if w.status == WorkerStatus.OFFLINE]
    idle = [w for w in alive if w.current_task_id is None]
    home = [w for w in all_workers if not w.is_temporary]
    lent = getattr(master, "lent_workers", {})

    borrowed_workers = []
    for w in received:
        borrowed_workers.append({"direction": "in", "peer_uuid": w.server_uuid})
    for peer_uuid in lent.values():
        borrowed_workers.append({"direction": "out", "peer_uuid": peer_uuid})

    stats = master.task_manager.get_statistics()
    tasks_pending = stats["tasks"]["pending"]
    tasks_running = stats["tasks"]["in_progress"]
    tasks_completed = stats["tasks"]["completed"]
    tasks_failed = stats["tasks"]["failed"]

    oldest_age = 0
    try:
        pending_tasks = master.task_manager.get_tasks_by_status(TaskStatus.PENDING)
        if pending_tasks:
            oldest_age = int(time.time() - min(t.created_at for t in pending_tasks))
    except Exception:
        pass

    return {
        "workers": {
            "total_registered": len(all_workers),
            "workers_utilization": len(busy),
            "workers_alive": len(alive),
            "workers_idle": len(idle),
            "workers_borrowed": len(lent),
            "workers_received": len(received),
            "workers_failed": len(failed),
            "workers_home": len(home),
            "workers_available_capacity": len(idle),
            "borrowed_workers": borrowed_workers,
        },
        "tasks": {
            "tasks_pending": tasks_pending,
            "tasks_running": tasks_running,
            "tasks_completed": tasks_completed,
            "tasks_failed": tasks_failed,
            "oldest_task_age_s": oldest_age,
        },
    }


def _build_neighbors(master) -> list:
    now = time.time()
    last_seen = getattr(master, "_peer_last_seen", {})
    neighbors = []
    for _host, _port, peer_uuid in getattr(master, "peer_masters", []):
        seen_at = last_seen.get(peer_uuid)
        if seen_at and (now - seen_at) < NEIGHBOR_STALE_SECONDS:
            status = "available"
            last_hb = datetime.datetime.fromtimestamp(seen_at, tz=datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        else:
            status = "unavailable"
            last_hb = ""
        neighbors.append({"server_uuid": peer_uuid, "status": status, "last_heartbeat": last_hb})
    return neighbors


def build_performance_report(master) -> dict:
    """Monta o payload completo conforme spec do professor."""
    hostname = socket.gethostname()
    capacity = getattr(master, "_capacity", 100)

    try:
        system = collect_system_metrics()
    except Exception as exc:
        logger.warning(f"[monitor] falha ao coletar métricas de sistema: {exc}")
        system = {
            "uptime_seconds": 0, "load_average_1m": 0.0, "load_average_5m": 0.0,
            "cpu": {"usage_percent": 0.0, "count_logical": 1, "count_physical": 1},
            "memory": {"total_mb": 0, "available_mb": 0, "percent_used": 0.0, "memory_used": 0},
            "disk": {"total_gb": 0.0, "free_gb": 0.0, "percent_used": 0.0},
        }

    return {
        "server_uuid": master.server_uuid,
        "hostname": hostname,
        "role": "master",
        "task": "performance_report",
        "timestamp": datetime.datetime.now(tz=datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "message_id": str(uuid.uuid4()),
        "payload_version": "sprint4-monitor",
        "performance": {
            "system": system,
            "farm_state": _build_farm_state(master),
            "config_thresholds": {
                "max_task": capacity,
                "warn_cpu_percent": 85,
                "warn_memory_percent": 85,
                "release_task": int(capacity * 0.6),
            },
            "neighbors": _build_neighbors(master),
        },
    }


def send_to_supervisor(payload: dict) -> None:
    """Envia o payload ao supervisor via TLS/TCP (fire-and-forget, sem recv)."""
    data = (json.dumps(payload) + "\n").encode("utf-8")
    try:
        ctx = ssl.create_default_context()
        with socket.create_connection((SUPERVISOR_HOST, SUPERVISOR_PORT), timeout=5.0) as raw:
            with ctx.wrap_socket(raw, server_hostname=SUPERVISOR_HOST) as tls:
                tls.sendall(data)
        return
    except ssl.SSLError as exc:
        logger.warning(f"[monitor] TLS verificado falhou ({exc}); tentando sem verificação.")
    # Fallback: TLS sem verificação de certificado (ambiente de sala de aula).
    ctx = ssl._create_unverified_context()
    with socket.create_connection((SUPERVISOR_HOST, SUPERVISOR_PORT), timeout=5.0) as raw:
        with ctx.wrap_socket(raw, server_hostname=SUPERVISOR_HOST) as tls:
            tls.sendall(data)


def _monitor_loop(master, interval: int) -> None:
    while True:
        try:
            payload = build_performance_report(master)
            send_to_supervisor(payload)
            logger.info(f"[monitor] métricas enviadas (message_id={payload['message_id']})")
        except Exception as exc:
            logger.warning(f"[monitor] falha ao enviar métricas: {exc}")
        time.sleep(interval)


def _peer_ping_loop(master, interval: int) -> None:
    from common.protocol import build_master_envelope_spec, send_json

    while True:
        time.sleep(interval)
        for peer_host, peer_port, peer_uuid in getattr(master, "peer_masters", []):
            try:
                with socket.create_connection((peer_host, peer_port), timeout=3.0) as conn:
                    send_json(conn, build_master_envelope_spec(
                        "ping", {}, request_id=str(uuid.uuid4())
                    ))
                master._peer_last_seen[peer_uuid] = time.time()
            except Exception:
                pass


def start_monitor_thread(master, interval: int = 10) -> None:
    """Inicia thread daemon de envio de métricas ao supervisor."""
    t = threading.Thread(
        target=_monitor_loop, args=(master, interval),
        daemon=True, name="monitor-metrics"
    )
    t.start()
    logger.info(f"[monitor] thread de métricas iniciada (intervalo={interval}s)")


def start_peer_ping_thread(master, interval: int = 30) -> None:
    """Inicia thread daemon de ping M2M para rastrear status dos vizinhos."""
    if not getattr(master, "peer_masters", []):
        return
    t = threading.Thread(
        target=_peer_ping_loop, args=(master, interval),
        daemon=True, name="monitor-peer-ping"
    )
    t.start()
    logger.info(f"[monitor] thread de ping M2M iniciada (intervalo={interval}s)")
