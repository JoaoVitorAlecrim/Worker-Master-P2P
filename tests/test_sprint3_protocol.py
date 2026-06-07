import unittest
from unittest.mock import patch

from master import MasterServer
from worker import WorkerClient


class TestSprint3Protocol(unittest.TestCase):
    def test_master_request_help_lends_idle_worker_via_command_redirect(self):
        """Per o PDF, quem recebe request_help empresta SEUS PRÓPRIOS workers ociosos,
        enviando command_redirect a eles — não apenas devolve um envelope vazio."""
        master = MasterServer(server_uuid="Master_A")
        master.task_manager.register_worker("Worker_B1", "Master_A", host="127.0.0.1")

        sent_messages = []
        master.worker_connections["Worker_B1"] = object()

        with patch("master.send_json", side_effect=lambda sock, data: sent_messages.append(data)):
            response = master.handle_master_request(
                {
                    "type": "request_help",
                    "request_id": "RID-1",
                    "payload": {
                        "master_id": "Master_X",
                        "master_host": "127.0.0.1",
                        "master_port": 5100,
                        "current_load": 150,
                        "capacity": 100,
                        "workers_needed": 1,
                    },
                },
                ("127.0.0.1", 5001),
            )

        self.assertEqual(response["type"], "response_accepted")
        self.assertEqual(response["request_id"], "RID-1")
        self.assertEqual(response["payload"]["workers_offered"], 1)
        self.assertEqual(response["payload"]["worker_details"][0]["id"], "Worker_B1")

        redirects = [m for m in sent_messages if m.get("type") == "command_redirect"]
        self.assertEqual(len(redirects), 1)
        self.assertEqual(redirects[0]["payload"]["new_master_address"], "127.0.0.1:5100")

    def test_master_request_help_resolves_redirect_address_from_known_peers(self):
        """O exemplo de request_help do PDF traz só master_id/current_load/capacity/
        workers_needed — sem host/porta do solicitante ("endereço de socket conhecido
        pelos Masters vizinhos"). O redirecionamento deve funcionar mesmo sem esses
        campos extras, resolvendo o endereço a partir dos peers conhecidos (MASTER_PEERS)."""
        master = MasterServer(server_uuid="Master_A")
        master.task_manager.register_worker("Worker_B1", "Master_A", host="127.0.0.1")
        master.worker_connections["Worker_B1"] = object()

        sent_messages = []

        with patch("master.PEER_ADDRESS_BY_ID", {"Master_X": ("127.0.0.1", 5100)}), \
                patch("master.send_json", side_effect=lambda sock, data: sent_messages.append(data)):
            response = master.handle_master_request(
                {
                    "type": "request_help",
                    "request_id": "RID-9",
                    "payload": {
                        "master_id": "Master_X",
                        "current_load": 150,
                        "capacity": 100,
                        "workers_needed": 1,
                    },
                },
                ("127.0.0.1", 5001),
            )

        self.assertEqual(response["type"], "response_accepted")
        redirects = [m for m in sent_messages if m.get("type") == "command_redirect"]
        self.assertEqual(len(redirects), 1)
        self.assertEqual(redirects[0]["payload"]["new_master_address"], "127.0.0.1:5100")

    def test_master_request_help_rejects_when_no_idle_workers(self):
        master = MasterServer(server_uuid="Master_A")

        response = master.handle_master_request(
            {
                "type": "request_help",
                "request_id": "RID-2",
                "payload": {"master_id": "Master_X", "workers_needed": 1},
            },
            ("127.0.0.1", 5001),
        )

        self.assertEqual(response["type"], "response_rejected")
        self.assertEqual(response["request_id"], "RID-2")
        self.assertEqual(response["payload"]["reason"], "no_workers_available")

    def test_master_with_no_pending_tasks_does_not_redirect_its_own_worker(self):
        """Um master ocioso (sem tarefas) apenas responde NO_TASK — quem pede ajuda a
        peers e empresta workers é responsabilidade do master saturado/do master com
        capacidade ociosa, não do worker que está fazendo polling."""
        master = MasterServer(server_uuid="Master_A")

        with patch.object(master.task_manager, "get_pending_task", return_value=None), \
                patch("master.PEER_MASTERS", [("127.0.0.1", 5101, "Master_B")]), \
                patch.object(master, "request_help_to_peer") as mock_request_help:
            response = master.handle_request_task(
                {
                    "WORKER": "ALIVE",
                    "WORKER_UUID": "Worker_1",
                    "SERVER_UUID": "Master_A",
                },
                ("127.0.0.1", 5000),
            )

        self.assertEqual(response["TASK"], "NO_TASK")
        mock_request_help.assert_not_called()

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
        # notify_worker_returned é estritamente Master-to-Master no PDF (Master que
        # libera -> master de origem). O worker apenas atualiza seu alvo e reconecta;
        # quem notifica é o master liberador (master._send_command_release ->
        # _notify_worker_returned), não o próprio worker.
        self.assertEqual(sent_messages, [])

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