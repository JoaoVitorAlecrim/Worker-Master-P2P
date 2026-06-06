import unittest

from common.protocol import get_ci_value
from master import MasterServer
from worker import WorkerClient


class TestProtocolNormalization(unittest.TestCase):
    def test_get_ci_value_reads_mixed_case_keys(self):
        message = {
            "TYPE": "REQUEST_HELP",
            "request_id": "RID-101",
            "PAYLOAD": {
                "MASTER_ID": "Master_A",
                "CURRENT_LOAD": 150,
            },
        }

        self.assertEqual(get_ci_value(message, "type"), "REQUEST_HELP")
        self.assertEqual(get_ci_value(message, "request_id"), "RID-101")
        self.assertEqual(get_ci_value(message, "payload"), {"MASTER_ID": "Master_A", "CURRENT_LOAD": 150})
        self.assertIsNone(get_ci_value(message, "missing"))

    def test_master_handles_mixed_case_request_help_envelope(self):
        master = MasterServer(server_uuid="Master_A")

        response = master.handle_master_request(
            {
                "TYPE": "REQUEST_HELP",
                "REQUEST_ID": "RID-202",
                "PAYLOAD": {
                    "MASTER_ID": "Master_B",
                    "CURRENT_LOAD": 150,
                    "CAPACITY": 100,
                    "WORKERS_NEEDED": 2,
                },
            },
            ("127.0.0.1", 5001),
        )

        self.assertEqual(response["type"], "response_accepted")
        self.assertEqual(response["request_id"], "RID-202")

    def test_worker_handles_mixed_case_redirect_payload(self):
        worker = WorkerClient(worker_uuid="Worker_1", server_uuid="Master_A")

        result = worker.handle_redirect(
            {
                "TYPE": "COMMAND_REDIRECT",
                "REQUEST_ID": "RID-303",
                "PAYLOAD": {
                    "NEW_MASTER_ADDRESS": "127.0.0.1:5100",
                    "TARGET_SERVER_UUID": "Master_B",
                },
            }
        )

        self.assertEqual(worker.master_host, "127.0.0.1")
        self.assertEqual(worker.master_port, 5100)
        self.assertEqual(result["REDIRECT"]["HOST"], "127.0.0.1")
        self.assertEqual(result["REDIRECT"]["SERVER_UUID"], "Master_A")


if __name__ == "__main__":
    unittest.main()