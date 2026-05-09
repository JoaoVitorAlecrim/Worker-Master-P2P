from p2p_farm.master import MasterFarm


def test_master_can_request_help_from_neighbor():
    neighbor = MasterFarm(master_id="B", address=("127.0.0.1", 0), saturation_threshold=10, release_threshold=5)
    neighbor.start()

    requester = MasterFarm(master_id="A", address=("127.0.0.1", 0), saturation_threshold=0, release_threshold=0)

    try:
        response = requester.request_help(
            neighbor.listen_address,
            current_load=150,
            capacity=100,
            workers_needed=2,
        )

        assert response["type"] == "response_accepted"
        assert response["payload"]["workers_offered"] == 2
    finally:
        neighbor.stop()