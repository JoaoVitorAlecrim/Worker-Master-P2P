import unittest
from unittest.mock import patch

from master import MasterServer
from worker import WorkerClient


class TestSprint3Protocol(unittest.TestCase):
    def test_master_request_help_uses_pdf_envelope(self):
        master = MasterServer(server_uuid="Master_A")

        response = master.handle_master_request(
            {
                "type": "request_help",
                "request_id": "RID-1",
                "payload": {
                    "master_id": "A",
                    "current_load": 150,
                    "capacity": 100,
                    "workers_needed": 2,
                },
            },
            ("127.0.0.1", 5001),
        )

        self.assertEqual(response["type"], "response_accepted")
        self.assertEqual(response["request_id"], "RID-1")
        self.assertIn("payload", response)

    def test_master_request_state_uses_pdf_envelope(self):
        master = MasterServer(server_uuid="Master_A")

        response = master.handle_master_request(
            {
                "type": "request_state",
                "request_id": "RID-3",
                "payload": {
                    "target_server": "Master_A",
                    "from_worker": "Worker_1",
                },
            },
            ("127.0.0.1", 5002),
        )

        self.assertEqual(response["type"], "response_state")
        self.assertEqual(response["request_id"], "RID-3")
        self.assertIn("payload", response)

    def test_master_register_temporary_worker_uses_pdf_envelope(self):
        master = MasterServer(server_uuid="Master_A")

        response = master.handle_temporary_worker_registration(
            {
                "type": "register_temporary_worker",
                "request_id": "RID-7",
                "payload": {
                    "worker_id": "Worker_1",
                    "original_master_address": "127.0.0.1:5101",
                },
            },
            ("127.0.0.1", 5003),
        )

        self.assertEqual(response["type"], "response_accepted")
        self.assertEqual(response["request_id"], "RID-7")
        self.assertEqual(response["payload"]["worker_id"], "Worker_1")
        self.assertTrue(master.task_manager.get_worker("Worker_1").is_temporary)
        self.assertEqual(master.task_manager.get_worker("Worker_1").original_master_address, "127.0.0.1:5101")

    def test_worker_handles_command_redirect(self):
        worker = WorkerClient(worker_uuid="Worker_1", server_uuid="Master_A")

        worker.handle_redirect(
            {
                "type": "command_redirect",
                "request_id": "RID-2",
                "payload": {"new_master_address": "127.0.0.1:5100"},
            }
        )

        self.assertEqual(worker.master_host, "127.0.0.1")
        self.assertEqual(worker.master_port, 5100)
        self.assertEqual(worker.server_uuid, "Master_A")

    def test_worker_registers_temporary_worker(self):
        worker = WorkerClient(worker_uuid="Worker_1", server_uuid="Master_A")
        worker.master_host = "127.0.0.1"
        worker.master_port = 5100
        sent_messages = []

        class FakeSocket:
            def settimeout(self, timeout):
                return None

            def makefile(self, mode, encoding=None):
                return object()

            def close(self):
                return None

        def fake_create_connection(*args, **kwargs):
            return FakeSocket()

        with patch("worker.socket.create_connection", side_effect=fake_create_connection), \
                patch("worker.send_json", side_effect=lambda sock, data: sent_messages.append(data)), \
                patch("worker.recv_json_line", return_value={"type": "response_accepted", "request_id": "RID-8", "payload": {}}):
            ok = worker.register_temporary_worker(FakeSocket(), object())

        self.assertTrue(ok)
        self.assertEqual(sent_messages[0]["type"], "register_temporary_worker")
        self.assertEqual(sent_messages[0]["payload"]["worker_id"], "Worker_1")
        self.assertEqual(sent_messages[0]["payload"]["original_master_address"], "127.0.0.1:5000")

    def test_worker_handles_command_release(self):
        worker = WorkerClient(worker_uuid="Worker_1", server_uuid="Master_A")
        sent_messages = []

        class FakeSocket:
            def settimeout(self, timeout):
                return None

            def makefile(self, mode, encoding=None):
                return object()

            def close(self):
                return None

        def fake_create_connection(*args, **kwargs):
            return FakeSocket()

        with patch("worker.socket.create_connection", side_effect=fake_create_connection), \
                patch("worker.send_json", side_effect=lambda sock, data: sent_messages.append(data)), \
                patch("worker.recv_json_line", return_value={"type": "response_accepted", "request_id": "RID-6", "payload": {}}):
            worker.handle_redirect(
                {
                    "type": "command_redirect",
                    "request_id": "RID-5",
                    "payload": {"new_master_address": "127.0.0.1:5100", "TARGET_SERVER_UUID": "Master_B"},
                }
            )
            worker.handle_release(
                {
                    "type": "command_release",
                    "request_id": "RID-6",
                    "payload": {"TARGET_HOST": "127.0.0.1", "TARGET_PORT": 5000, "TARGET_SERVER_UUID": "Master_A"},
                }
            )

        self.assertEqual(worker.master_host, "127.0.0.1")
        self.assertEqual(worker.master_port, 5000)
        self.assertEqual(worker.server_uuid, "Master_A")
        self.assertEqual(sent_messages[0]["type"], "notify_worker_returned")
        self.assertEqual(sent_messages[0]["payload"]["worker_id"], "Worker_1")

    def test_worker_requests_state_with_pdf_envelope(self):
        worker = WorkerClient(worker_uuid="Worker_1", server_uuid="Master_A")
        sent_messages = []

        class FakeSocket:
            def settimeout(self, timeout):
                return None

            def makefile(self, mode, encoding=None):
                return object()

            def close(self):
                return None

        def fake_create_connection(*args, **kwargs):
            return FakeSocket()

        with patch("worker.socket.create_connection", side_effect=fake_create_connection), \
                patch("worker.send_json", side_effect=lambda sock, data: sent_messages.append(data)), \
                patch("worker.recv_json_line", return_value={
                    "type": "response_state",
                    "request_id": "RID-4",
                    "payload": {
                        "found": True,
                        "target_server": "Master_A",
                        "state": {"tasks": {}, "workers": {}, "logs": []},
                    },
                }):
            state = worker.request_state_from_peer("127.0.0.1", 5101, "Master_A")

        self.assertEqual(sent_messages[0]["type"], "request_state")
        self.assertEqual(sent_messages[0]["payload"]["target_server"], "Master_A")
        self.assertEqual(state, {"tasks": {}, "workers": {}, "logs": []})


if __name__ == "__main__":
    unittest.main()