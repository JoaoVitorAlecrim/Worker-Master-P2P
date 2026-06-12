import time
import unittest
from unittest.mock import MagicMock

from common.monitor import build_performance_report, collect_system_metrics


def _make_mock_master(
    server_uuid="Test_A",
    lent_workers=None,
    peer_last_seen=None,
    peer_masters=None,
    capacity=100,
    is_temporary_workers=None,
):
    master = MagicMock()
    master.server_uuid = server_uuid
    master._capacity = capacity
    master.lent_workers = lent_workers or {}
    master._peer_last_seen = peer_last_seen or {}
    master.peer_masters = peer_masters or []

    def make_worker(uuid, temporary=False, server_uuid_w="Test_A", task_id=None, offline=False):
        w = MagicMock()
        w.worker_uuid = uuid
        w.server_uuid = server_uuid_w
        w.is_temporary = temporary
        w.current_task_id = task_id
        from common.models import WorkerStatus
        w.status = WorkerStatus.OFFLINE if offline else WorkerStatus.IDLE
        return w

    workers = []
    for spec in (is_temporary_workers or []):
        workers.append(make_worker(**spec))
    if not workers:
        workers = [make_worker("W1"), make_worker("W2")]

    master.task_manager.get_all_workers.return_value = workers
    master.task_manager.get_statistics.return_value = {
        "tasks": {"pending": 5, "in_progress": 2, "completed": 10, "failed": 1},
        "workers": {"online": len(workers), "offline": 0, "total": len(workers)},
    }

    from common.models import TaskStatus
    mock_task = MagicMock()
    mock_task.created_at = time.time() - 30
    master.task_manager.get_tasks_by_status.return_value = [mock_task]

    return master


class TestCollectSystemMetrics(unittest.TestCase):
    def test_returns_required_keys(self):
        metrics = collect_system_metrics()
        for key in ["uptime_seconds", "load_average_1m", "load_average_5m", "cpu", "memory", "disk"]:
            self.assertIn(key, metrics)

    def test_cpu_keys(self):
        cpu = collect_system_metrics()["cpu"]
        for key in ["usage_percent", "count_logical", "count_physical"]:
            self.assertIn(key, cpu)

    def test_memory_keys(self):
        mem = collect_system_metrics()["memory"]
        for key in ["total_mb", "available_mb", "percent_used", "memory_used"]:
            self.assertIn(key, mem)

    def test_disk_keys(self):
        disk = collect_system_metrics()["disk"]
        for key in ["total_gb", "free_gb", "percent_used"]:
            self.assertIn(key, disk)

    def test_uptime_is_positive_int(self):
        metrics = collect_system_metrics()
        self.assertIsInstance(metrics["uptime_seconds"], int)
        self.assertGreater(metrics["uptime_seconds"], 0)


class TestBuildPerformanceReport(unittest.TestCase):
    def test_top_level_fields_present(self):
        master = _make_mock_master()
        report = build_performance_report(master)
        for field in ["server_uuid", "hostname", "role", "task", "timestamp",
                      "message_id", "payload_version", "performance"]:
            self.assertIn(field, report)

    def test_role_and_task_values(self):
        master = _make_mock_master()
        report = build_performance_report(master)
        self.assertEqual(report["role"], "master")
        self.assertEqual(report["task"], "performance_report")
        self.assertEqual(report["payload_version"], "sprint4-monitor")

    def test_server_uuid_matches_master(self):
        master = _make_mock_master(server_uuid="My_Master")
        report = build_performance_report(master)
        self.assertEqual(report["server_uuid"], "My_Master")

    def test_performance_sections_present(self):
        master = _make_mock_master()
        perf = build_performance_report(master)["performance"]
        for section in ["system", "farm_state", "config_thresholds", "neighbors"]:
            self.assertIn(section, perf)

    def test_workers_borrowed_out(self):
        master = _make_mock_master(lent_workers={"W5": "Master_B"})
        report = build_performance_report(master)
        borrowed = report["performance"]["farm_state"]["workers"]["borrowed_workers"]
        out_entries = [e for e in borrowed if e["direction"] == "out"]
        self.assertEqual(len(out_entries), 1)
        self.assertEqual(out_entries[0]["peer_uuid"], "Master_B")

    def test_workers_received_in(self):
        workers_spec = [
            {"uuid": "W_ext", "temporary": True, "server_uuid_w": "Master_B"},
            {"uuid": "W_local"},
        ]
        master = _make_mock_master(is_temporary_workers=workers_spec)
        report = build_performance_report(master)
        borrowed = report["performance"]["farm_state"]["workers"]["borrowed_workers"]
        in_entries = [e for e in borrowed if e["direction"] == "in"]
        self.assertEqual(len(in_entries), 1)
        self.assertEqual(in_entries[0]["peer_uuid"], "Master_B")

    def test_neighbor_available_when_recently_seen(self):
        peer_masters = [("127.0.0.1", 5001, "Master_B")]
        peer_last_seen = {"Master_B": time.time() - 10}
        master = _make_mock_master(peer_masters=peer_masters, peer_last_seen=peer_last_seen)
        neighbors = build_performance_report(master)["performance"]["neighbors"]
        self.assertEqual(len(neighbors), 1)
        self.assertEqual(neighbors[0]["server_uuid"], "Master_B")
        self.assertEqual(neighbors[0]["status"], "available")

    def test_neighbor_unavailable_when_not_seen(self):
        peer_masters = [("127.0.0.1", 5001, "Master_B")]
        master = _make_mock_master(peer_masters=peer_masters, peer_last_seen={})
        neighbors = build_performance_report(master)["performance"]["neighbors"]
        self.assertEqual(neighbors[0]["status"], "unavailable")

    def test_config_thresholds(self):
        master = _make_mock_master(capacity=100)
        thresholds = build_performance_report(master)["performance"]["config_thresholds"]
        self.assertEqual(thresholds["max_task"], 100)
        self.assertEqual(thresholds["release_task"], 60)
        self.assertEqual(thresholds["warn_cpu_percent"], 85)
        self.assertEqual(thresholds["warn_memory_percent"], 85)

    def test_oldest_task_age_s_is_non_negative(self):
        master = _make_mock_master()
        age = build_performance_report(master)["performance"]["farm_state"]["tasks"]["oldest_task_age_s"]
        self.assertGreaterEqual(age, 0)


if __name__ == "__main__":
    unittest.main()
