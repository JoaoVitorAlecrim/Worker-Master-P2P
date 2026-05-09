import socket

from p2p_farm.transport import JsonLineSocket


def test_json_line_socket_reads_one_message():
    left, right = socket.socketpair()
    try:
        sender = JsonLineSocket(left)
        receiver = JsonLineSocket(right)

        sender.send({"TASK": "HEARTBEAT", "SERVER_UUID": "Master_A"})

        assert receiver.receive() == {"TASK": "HEARTBEAT", "SERVER_UUID": "Master_A"}
    finally:
        left.close()
        right.close()