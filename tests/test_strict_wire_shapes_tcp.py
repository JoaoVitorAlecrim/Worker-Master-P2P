import unittest
from master import MasterServer


class TestStrictWireShapesTCP(unittest.TestCase):
    def test_master_query_contains_only_user_and_no_disallowed_keys(self):
        ms = MasterServer(server_uuid="Master_Test")
        # ensure there is at least one task to assign
        ms.task_manager.tasks = {}
        ms.task_manager.create_task("soma", [1, 2])

        data = {"WORKER": "ALIVE", "WORKER_UUID": "W-1"}
        resp = ms.handle_request_task(data, ("127.0.0.1", 12345))

        self.assertEqual(resp.get("TASK"), "QUERY")
        self.assertIn("USER", resp)
        # Forbidden keys
        for forbidden in ("TASK_ID", "WORKERS", "RESULT", "AUTH_TOKEN"):
            self.assertNotIn(forbidden, resp)

    def test_handle_worker_alive_heartbeat_has_no_workers_list(self):
        ms = MasterServer(server_uuid="Master_Test")
        data = {"WORKER": "ALIVE", "WORKER_UUID": "W-2"}
        resp = ms.handle_worker_alive(data, ("127.0.0.1", 12345))

        self.assertEqual(resp.get("TASK"), "HEARTBEAT")
        self.assertIn("SERVER_UUID", resp)
        self.assertNotIn("WORKERS", resp)


if __name__ == "__main__":
    unittest.main()
