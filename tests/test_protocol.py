from p2p_farm.protocol import decode_message


def test_decode_message_ignores_unknown_fields():
    raw = '{"TASK":"HEARTBEAT","SERVER_UUID":"Master_A","EXTRA":"ignored"}\n'

    message = decode_message(raw)

    assert message == {"TASK": "HEARTBEAT", "SERVER_UUID": "Master_A", "EXTRA": "ignored"}