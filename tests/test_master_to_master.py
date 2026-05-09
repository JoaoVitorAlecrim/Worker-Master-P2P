from p2p_farm.master import build_request_help_message


def test_request_help_message_uses_shared_envelope():
    message = build_request_help_message(master_id="A", current_load=150, capacity=100, workers_needed=2)

    assert message["type"] == "request_help"
    assert message["payload"]["workers_needed"] == 2