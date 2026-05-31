import unittest
from common.election import build_election_message_spec, parse_election_message_spec


class TestElectionSpecShape(unittest.TestCase):
    def test_build_and_parse_spec_message(self):
        payload = {"CANDIDATE": {"WORKER_UUID": "W-1", "FREE_DISK_BYTES": 100}}
        msg = build_election_message_spec("start", payload, request_id="RID-1")

        self.assertIn("ELECTION", msg)
        self.assertIn("REQUEST_ID", msg)
        self.assertIn("PAYLOAD", msg)
        self.assertEqual(msg["REQUEST_ID"], "RID-1")

        parsed = parse_election_message_spec(msg)
        self.assertEqual(parsed.get("type"), "start")
        self.assertEqual(parsed.get("request_id"), "RID-1")
        self.assertEqual(parsed.get("payload"), payload)

    def test_parse_missing_fields_error(self):
        bad = {"ELECTION": "start", "PAYLOAD": {}}
        parsed = parse_election_message_spec(bad)
        self.assertIn("error", parsed)
        self.assertEqual(parsed.get("error"), "MISSING_FIELDS")


if __name__ == "__main__":
    unittest.main()
