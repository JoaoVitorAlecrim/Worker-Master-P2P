from p2p_farm.master import MasterFarm


def test_master_rejects_help_when_saturated():
    neighbor = MasterFarm(master_id="B", address=("127.0.0.1", 0), saturation_threshold=0, release_threshold=0)
    neighbor.enqueue_task({"TASK": "QUERY", "USER": "Michel"})
    neighbor.start()

    requester = MasterFarm(master_id="A", address=("127.0.0.1", 0), saturation_threshold=0, release_threshold=0)

    try:
        response = requester.request_help(
            neighbor.listen_address,
            current_load=150,
            capacity=100,
            workers_needed=2,
        )

        assert response["type"] == "response_rejected"
        assert response["payload"]["reason"] == "high_load"
    finally:
        neighbor.stop()