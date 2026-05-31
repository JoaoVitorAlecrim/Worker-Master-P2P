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

        try:
            # simulate task assigned
            task = {"TASK_ID": "ignored", "OPERATION": "soma", "VALUES": [1, 2]}
            # call execute_and_report which should send STATUS without TASK_ID
            wc.execute_and_report(sock=None, sock_file=None, task=task)

            self.assertTrue(len(sent) >= 1)
            # last sent message should be the STATUS report
            msg = sent[-1]
            self.assertIn("STATUS", msg)
            self.assertEqual(msg.get("TASK"), None)
            # ensure TASK_ID not present in report
            self.assertNotIn("TASK_ID", msg)
        finally:
            worker_module.send_json = worker_send_json_orig


if __name__ == "__main__":
    unittest.main()
