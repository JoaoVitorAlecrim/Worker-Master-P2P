import os
import sys

repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if repo_root not in sys.path:
    sys.path.insert(0, repo_root)

from worker import WorkerClient


def make_worker(worker_uuid: str, free_disk_bytes: int) -> WorkerClient:
    worker = WorkerClient(worker_uuid=worker_uuid, server_uuid='Master_A')
    worker.get_free_disk_bytes = lambda: free_disk_bytes
    return worker


def main() -> int:
    worker_a = make_worker('Worker_A', 100)
    worker_b = make_worker('Worker_B', 250)

    shared_registry = {
        'Worker_A': {
            'WORKER_UUID': 'Worker_A',
            'HOST': '127.0.0.1',
            'FREE_DISK_BYTES': 100,
            'SERVER_UUID': 'Master_A',
            'STATUS': 'online',
        },
        'Worker_B': {
            'WORKER_UUID': 'Worker_B',
            'HOST': '127.0.0.1',
            'FREE_DISK_BYTES': 250,
            'SERVER_UUID': 'Master_A',
            'STATUS': 'online',
        },
    }

    worker_a.peer_registry = dict(shared_registry)
    worker_b.peer_registry = dict(shared_registry)

    winner_a = worker_a.choose_election_winner()
    winner_b = worker_b.choose_election_winner()

    if winner_a['WORKER_UUID'] != 'Worker_B' or winner_b['WORKER_UUID'] != 'Worker_B':
        print('Election failed: expected Worker_B to win by higher free disk')
        print('winner_a =', winner_a)
        print('winner_b =', winner_b)
        return 2

    worker_c = make_worker('Worker_C', 250)
    worker_c.peer_registry = {
        'Worker_B': {
            'WORKER_UUID': 'Worker_B',
            'HOST': '127.0.0.1',
            'FREE_DISK_BYTES': 250,
            'SERVER_UUID': 'Master_A',
            'STATUS': 'online',
        },
        'Worker_C': {
            'WORKER_UUID': 'Worker_C',
            'HOST': '127.0.0.1',
            'FREE_DISK_BYTES': 250,
            'SERVER_UUID': 'Master_A',
            'STATUS': 'online',
        },
    }

    winner_c = worker_c.choose_election_winner()
    if winner_c['WORKER_UUID'] != 'Worker_B':
        print('Election failed: tie should prefer lexicographically smaller worker UUID')
        print('winner_c =', winner_c)
        return 3

    print('Election rule verified: highest disk wins, tie breaks by worker UUID.')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())