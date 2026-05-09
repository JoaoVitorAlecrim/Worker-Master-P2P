import socket

from p2p_farm.master import MasterFarm
from p2p_farm.session import JsonSession


def test_master_answers_worker_heartbeat_over_tcp():
    farm = MasterFarm(master_id="A", address=("127.0.0.1", 0), saturation_threshold=10, release_threshold=5)
    farm.start()

    try:
        connection = socket.create_connection(farm.listen_address)
        session = JsonSession(connection)
        response = session.request({"SERVER_UUID": "Master_A", "TASK": "HEARTBEAT"})

        assert response == {"SERVER_UUID": "Master_A", "TASK": "HEARTBEAT", "RESPONSE": "ALIVE"}
    finally:
        session.close()
        farm.stop()