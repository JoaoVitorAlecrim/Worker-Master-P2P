import unittest
from common.protocol import build_master_envelope_spec, parse_master_envelope_spec


class TestMasterEnvelopeSpec(unittest.TestCase):
    def test_build_spec_envelope_keys_and_payload(self):
        payload = {"FROM_SERVER": "Master_A", "REQUESTED": 1}
        env = build_master_envelope_spec("request_help", payload, request_id="RID-123")

        # Exact spec keys must be present
        self.assertIn("type", env)
        self.assertIn("request_id", env)
        self.assertIn("payload", env)

        # Values must be exactly as expected
        self.assertEqual(env["type"], "request_help")
        self.assertEqual(env["request_id"], "RID-123")
        self.assertEqual(env["payload"], payload)

    def test_parse_spec_valid_and_missing_fields(self):
        payload = {"FROM_SERVER": "Master_A"}
        env = {"type": "response_help", "request_id": "RID-2", "payload": payload}

        parsed = parse_master_envelope_spec(env)
        self.assertEqual(parsed.get("type"), "response_help")
        self.assertEqual(parsed.get("request_id"), "RID-2")
        self.assertEqual(parsed.get("payload"), payload)

        # Missing fields => error with missing list
        bad = {"type": "response_help", "payload": payload}
        parsed_bad = parse_master_envelope_spec(bad)
        self.assertIn("error", parsed_bad)
        self.assertEqual(parsed_bad.get("error"), "MISSING_FIELDS")
        self.assertIn("request_id", parsed_bad.get("missing"))


if __name__ == "__main__":
    unittest.main()
