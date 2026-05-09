from p2p_farm.master import MasterFarm
from p2p_farm.worker import WorkerClient


def test_worker_can_register_as_temporary_on_new_master():
    farm = MasterFarm(master_id="B", address=("127.0.0.1", 0), saturation_threshold=10, release_threshold=5)
    farm.start()

    worker = WorkerClient(worker_id="W-999", server_address=farm.listen_address, server_uuid="Master-B")

    try:
        worker.connect()
        response = worker.register_temporary_worker(original_master_address=("127.0.0.1", 6000))

        assert response["type"] == "response_accepted"
        assert response["request_id"]
    finally:
        worker.close()
        farm.stop()