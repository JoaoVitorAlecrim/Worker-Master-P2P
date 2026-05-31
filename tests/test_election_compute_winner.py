import unittest
from common.election import compute_winner


class TestElectionComputeWinner(unittest.TestCase):
    def test_choose_highest_disk(self):
        candidates = [
            {"WORKER_UUID": "A", "FREE_DISK_BYTES": 100},
            {"WORKER_UUID": "B", "FREE_DISK_BYTES": 200},
            {"WORKER_UUID": "C", "FREE_DISK_BYTES": 50},
        ]
        winner = compute_winner(candidates)
        self.assertEqual(winner["WORKER_UUID"], "B")

    def test_tie_breaker_uuid(self):
        candidates = [
            {"WORKER_UUID": "b", "FREE_DISK_BYTES": 100},
            {"WORKER_UUID": "a", "FREE_DISK_BYTES": 100},
        ]
        winner = compute_winner(candidates)
        self.assertEqual(winner["WORKER_UUID"], "a")


if __name__ == "__main__":
    unittest.main()
