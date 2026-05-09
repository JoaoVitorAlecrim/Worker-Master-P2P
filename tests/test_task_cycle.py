from p2p_farm.master import MasterFarm
from p2p_farm.worker import WorkerClient


def test_master_assigns_task_and_acknowledges_completion():
    farm = MasterFarm(master_id="A", address=("127.0.0.1", 0), saturation_threshold=10, release_threshold=5)
    farm.enqueue_task({"TASK": "QUERY", "USER": "Michel"})
    farm.start()

    worker = WorkerClient(worker_id="W-123", server_address=farm.listen_address, server_uuid="Master_A")

    try:
        worker.connect()

        task = worker.request_task_once()
        assert task == {"TASK": "QUERY", "USER": "Michel"}

        ack = worker.submit_task_result(task, status="OK")
        assert ack == {"STATUS": "ACK", "WORKER_UUID": "W-123"}
    finally:
        worker.close()
        farm.stop()