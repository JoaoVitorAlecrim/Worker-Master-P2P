import unittest
from common.protocol import build_master_envelope_spec, parse_master_envelope_spec


class TestMasterEnvelopeSpec(unittest.TestCase):
    def test_build_spec_envelope_keys_and_payload(self):
        payload = {"FROM_SERVER": "Master_A", "REQUESTED": 1}
        env = build_master_envelope_spec("request_help", payload, request_id="RID-123")

        # Exact spec keys must be present
        self.assertIn("MASTER", env)
        self.assertIn("REQUEST_ID", env)
        self.assertIn("PAYLOAD", env)

        # Values must be exactly as expected
        self.assertEqual(env["MASTER"], "REQUEST_HELP")
        self.assertEqual(env["REQUEST_ID"], "RID-123")
        self.assertEqual(env["PAYLOAD"], payload)

    def test_parse_spec_valid_and_missing_fields(self):
        payload = {"FROM_SERVER": "Master_A"}
        env = {"MASTER": "response_help", "REQUEST_ID": "RID-2", "PAYLOAD": payload}

        parsed = parse_master_envelope_spec(env)
        self.assertEqual(parsed.get("type"), "response_help")
        self.assertEqual(parsed.get("request_id"), "RID-2")
        self.assertEqual(parsed.get("payload"), payload)

        # Missing fields => error with missing list
        bad = {"MASTER": "response_help", "PAYLOAD": payload}
        parsed_bad = parse_master_envelope_spec(bad)
        self.assertIn("error", parsed_bad)
        self.assertEqual(parsed_bad.get("error"), "MISSING_FIELDS")
        self.assertIn("REQUEST_ID", parsed_bad.get("missing"))


if __name__ == "__main__":
    unittest.main()
