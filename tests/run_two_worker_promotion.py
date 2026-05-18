import subprocess
import os
import time

repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
worker_path = os.path.join(repo_root, 'worker.py')

env_base = os.environ.copy()
# Use a port we assume is free for the fake master
TEST_PORT = '5200'

env = env_base.copy()
env['PROMOTE_THRESHOLD'] = '2'
env['RECONNECT_DELAY'] = '1'
env['MASTER_HOST'] = '127.0.0.1'
env['MASTER_PORT'] = TEST_PORT

workers = ['SimWorkerA', 'SimWorkerB']
procs = {}

print('Starting two worker subprocesses...')
for w in workers:
    p = subprocess.Popen(['python', worker_path, w], stdout=subprocess.PIPE, stderr=subprocess.STDOUT, env=env, text=True)
    procs[w] = p

start = time.time()
outputs = {w: [] for w in workers}
promoted = {w: False for w in workers}
announced = {w: False for w in workers}

try:
    while time.time() - start < 20:
        for name, p in procs.items():
            if p.stdout is None:
                continue
            line = p.stdout.readline()
            if not line:
                continue
            print(f"[{name}] {line}", end='')
            outputs[name].append(line)
            if 'Promoted Master iniciado' in line or 'Promoted Master iniciado' in line:
                promoted[name] = True
            if 'Líder da eleição anunciado' in line or '↪ Líder da eleição anunciado' in line:
                announced[name] = True
        if any(promoted.values()) and (sum(promoted.values()) >= 1) and any(announced.values()):
            break
        time.sleep(0.05)
finally:
    for p in procs.values():
        try:
            p.kill()
        except:
            pass

print('\nSummary:')
print('Promoted:', promoted)
print('Announced leader seen:', announced)

# Simple verdict
num_promoted = sum(1 for v in promoted.values() if v)
if num_promoted == 1:
    print('RESULT: PASS - single promoted master')
    exit(0)
else:
    print('RESULT: FAIL - promoted count =', num_promoted)
    exit(2)
