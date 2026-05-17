import subprocess
import sys
import time
import os

# Run worker with env overrides to trigger promotion quickly
env = os.environ.copy()
env['PROMOTE_THRESHOLD'] = '2'
env['RECONNECT_DELAY'] = '1'
env['MASTER_HOST'] = '127.0.0.1'
env['MASTER_PORT'] = '5100'

worker_uuid = 'TestWorker_Promote'
repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
worker_path = os.path.join(repo_root, 'worker.py')
script = ['python', worker_path, worker_uuid]

print('Starting worker subprocess...')
proc = subprocess.Popen(script, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, env=env, text=True)

start = time.time()
promoted = False

try:
    while True:
        line = proc.stdout.readline()
        if not line and proc.poll() is not None:
            break
        if line:
            print(line, end='')
            if 'Promoted Master iniciado' in line or 'Promovendo' in line or 'Promoted' in line:
                promoted = True
                break
        if time.time() - start > 20:
            break
finally:
    try:
        proc.kill()
    except:
        pass

print('\nPromotion observed:' , promoted)
sys.exit(0 if promoted else 2)
