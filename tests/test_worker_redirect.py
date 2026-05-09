from p2p_farm.master import MasterFarm
from p2p_farm.worker import WorkerClient


def test_worker_redirects_to_new_master_and_registers_temporarily():
    original_master = MasterFarm(master_id="A", address=("127.0.0.1", 0), saturation_threshold=10, release_threshold=5)
    new_master = MasterFarm(master_id="B", address=("127.0.0.1", 0), saturation_threshold=10, release_threshold=5)
    original_master.start()
    new_master.start()

    worker = WorkerClient(worker_id="W-999", server_address=original_master.listen_address, server_uuid="Master-A")

    try:
        worker.connect()
        response = worker.handle_control_message(
            {
                "type": "command_redirect",
                "request_id": "abc",
                "payload": {"new_master_address": f"{new_master.listen_address[0]}:{new_master.listen_address[1]}"},
            }
        )

        assert response["type"] == "response_accepted"
        assert worker.server_address == new_master.listen_address
    finally:
        worker.close()
        original_master.stop()
        new_master.stop()