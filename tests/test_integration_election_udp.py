import os
import time
import unittest
import random
from worker import WorkerClient


class TestIntegrationElectionUDP(unittest.TestCase):
    def test_udp_election_among_workers(self):
        # Use loopback and a random high port to avoid conflicts
        port = 54000 + random.randint(0, 1000)
        os.environ['ELECTION_PORT'] = str(port)
        os.environ['ELECTION_BROADCAST_ADDR'] = '127.0.0.1'

        # Create three worker clients with distinct UUIDs
        clients = []
        try:
            for i in range(3):
                wc = WorkerClient(worker_uuid=f'W-test-{i}', server_uuid='Master_Test', master_host='127.0.0.1', master_port=5000)
                clients.append(wc)

            # give listeners a moment to bind
            time.sleep(0.2)

            # Trigger election from first client
            winner = clients[0]._run_election(timeout=1.0)
            self.assertIsInstance(winner, dict)
            self.assertIn('WORKER_UUID', winner)
            self.assertTrue(any(c.worker_uuid == winner.get('WORKER_UUID') for c in clients))

        finally:
            # Stop clients' threads
            for c in clients:
                try:
                    c.running = False
                except Exception:
                    pass


if __name__ == '__main__':
    unittest.main()
