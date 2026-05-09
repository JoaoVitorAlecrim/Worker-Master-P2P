from p2p_farm.master import MasterFarm
from p2p_farm.worker import WorkerClient


def test_worker_returns_to_original_master_after_release():
    original_master = MasterFarm(master_id="A", address=("127.0.0.1", 0), saturation_threshold=10, release_threshold=5)
    temporary_master = MasterFarm(master_id="B", address=("127.0.0.1", 0), saturation_threshold=10, release_threshold=5)
    original_master.start()
    temporary_master.start()

    worker = WorkerClient(worker_id="W-999", server_address=original_master.listen_address, server_uuid="Master-A")

    try:
        worker.connect()
        worker.handle_control_message(
            {
                "type": "command_redirect",
                "request_id": "abc",
                "payload": {"new_master_address": f"{temporary_master.listen_address[0]}:{temporary_master.listen_address[1]}"},
            }
        )

        response = worker.handle_control_message(
            {
                "type": "command_release",
                "request_id": "def",
                "payload": {"original_master_address": f"{original_master.listen_address[0]}:{original_master.listen_address[1]}"},
            }
        )

        assert response["type"] == "response_accepted"
        assert worker.server_address == original_master.listen_address
    finally:
        worker.close()
        original_master.stop()
        temporary_master.stop()