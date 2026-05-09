import socket
import threading

from p2p_farm.session import JsonSession


def test_session_request_response_roundtrip():
    left, right = socket.socketpair()
    try:
        client = JsonSession(left)
        server = JsonSession(right)

        def reply() -> None:
            message = server.receive()
            server.send({"TASK": message["TASK"], "SERVER_UUID": message["SERVER_UUID"], "RESPONSE": "ALIVE"})

        thread = threading.Thread(target=reply)
        thread.start()

        assert client.request({"TASK": "HEARTBEAT", "SERVER_UUID": "Master_A"}) == {
            "TASK": "HEARTBEAT",
            "SERVER_UUID": "Master_A",
            "RESPONSE": "ALIVE",
        }

        thread.join(timeout=1)
    finally:
        left.close()
        right.close()