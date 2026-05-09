from p2p_farm.master import MasterFarm


def test_master_reports_saturation_when_queue_exceeds_threshold():
    farm = MasterFarm(master_id="A", address=("127.0.0.1", 5001), saturation_threshold=1, release_threshold=0)
    farm.enqueue_task({"TASK": "QUERY", "USER": "Michel"})
    farm.enqueue_task({"TASK": "QUERY", "USER": "Ana"})

    assert farm.is_saturated() is True