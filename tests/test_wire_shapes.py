import unittest
from master import MasterServer


class TestWireShapes(unittest.TestCase):
    def test_master_query_user_only_no_task_id_or_workers(self):
        ms = MasterServer(server_uuid="Master_Test")
        # ensure no tasks
        ms.task_manager.tasks = {}
        # create a task for pending
        ms.task_manager.create_task("soma", [1, 2])

        data = {"WORKER": "ALIVE", "WORKER_UUID": "W-1"}
        resp = ms.handle_request_task(data, ("127.0.0.1", 12345))

        # Expect QUERY with USER only (no TASK_ID, no WORKERS)
        self.assertEqual(resp.get("TASK"), "QUERY")
        self.assertIn("USER", resp)
        self.assertNotIn("TASK_ID", resp)
        self.assertNotIn("WORKERS", resp)

    def test_worker_report_without_task_id(self):
        from worker import WorkerClient
        import worker as worker_module

        wc = WorkerClient(worker_uuid="W-rt", server_uuid="Master_Test")

        sent = []

        def fake_send_json(sock, data):
            sent.append(data.copy())

        # monkeypatch send_json in worker module (send_json was imported there)
        worker_send_json_orig = worker_module.send_json
        worker_module.send_json = fake_send_json

        original_wait_ack = wc.wait_ack

        try:
            # simulate task assigned with the PDF-shaped request payload
            task = {"TASK": "QUERY", "USER": "{\"operation\": \"soma\", \"values\": [1, 2]}"}

            wc.wait_ack = lambda sock_file: True

            # call execute_and_report which should send STATUS with TASK and no RESULT/ERROR
            wc.execute_and_report(sock=None, sock_file=None, task=task)

            self.assertTrue(len(sent) >= 1)
            msg = sent[-1]
            self.assertEqual(msg, {"STATUS": "OK", "TASK": "QUERY", "WORKER_UUID": "W-rt"})
        finally:
            wc.wait_ack = original_wait_ack
            worker_module.send_json = worker_send_json_orig

    def test_worker_wait_task_response_returns_pdf_query_shape(self):
        from worker import WorkerClient
        import worker as worker_module

        wc = WorkerClient(worker_uuid="W-rt", server_uuid="Master_Test")
        wc.announce_election_leader = lambda response: None
        wc.update_peer_registry = lambda response: None

        original_recv_json_line = worker_module.recv_json_line
        worker_module.recv_json_line = lambda sock_file: {"TASK": "QUERY", "USER": "Michel"}

        try:
            task = wc.wait_task_response(sock_file=None)
            self.assertEqual(task, {"TASK": "QUERY", "USER": "Michel"})
        finally:
            worker_module.recv_json_line = original_recv_json_line

    def test_master_accepts_pdf_status_report_without_task_id(self):
        ms = MasterServer(server_uuid="Master_Test")
        task = ms.task_manager.create_task("soma", [1, 2])
        ms.task_manager.assign_task(task.task_id, "W-1")

        resp = ms.handle_task_result(
            {"STATUS": "OK", "TASK": "QUERY", "WORKER_UUID": "W-1"},
            ("127.0.0.1", 12345),
        )

        self.assertEqual(resp.get("STATUS"), "ACK")
        self.assertEqual(resp.get("WORKER_UUID"), "W-1")
        self.assertEqual(ms.task_manager.get_task(task.task_id).status.value, "completed")


if __name__ == "__main__":
    unittest.main()
