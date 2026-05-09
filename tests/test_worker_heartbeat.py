from p2p_farm.worker import WorkerClient


def test_worker_heartbeat_message_uses_server_uuid_and_task_heartbeat():
    worker = WorkerClient(worker_id="W-123", server_address=("127.0.0.1", 5000), server_uuid="Master_A")

    assert worker.heartbeat_message() == {"SERVER_UUID": "Master_A", "TASK": "HEARTBEAT"}