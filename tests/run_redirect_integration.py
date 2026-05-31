import subprocess
import os
import time

repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
master_path = os.path.join(repo_root, "master.py")
worker_path = os.path.join(repo_root, "worker.py")

env_base = os.environ.copy()

# Start Master B (has tasks)
env_b = env_base.copy()
env_b["MASTER_PORT"] = "5101"
env_b["SERVER_UUID"] = "Master_B"

proc_b = subprocess.Popen(
    ["python", master_path], stdout=subprocess.PIPE, stderr=subprocess.STDOUT, env=env_b, text=True
)
print("Started Master B (5101)")

# Wait briefly for B to start
time.sleep(1.0)

# Start Master A (no tasks expected to redirect). Set peer to Master B
env_a = env_base.copy()
env_a["MASTER_PORT"] = "5100"
env_a["SERVER_UUID"] = "Master_A"
env_a["MASTER_PEERS"] = "127.0.0.1:5101:Master_B"
env_a["INITIAL_TASKS"] = "0"

proc_a = subprocess.Popen(
    ["python", master_path], stdout=subprocess.PIPE, stderr=subprocess.STDOUT, env=env_a, text=True
)
print("Started Master A (5100) with peer Master_B")

# Wait briefly
time.sleep(1.0)

# Start a worker pointing to Master A (should be redirected to Master B)
env_w = env_base.copy()
env_w["MASTER_HOST"] = "127.0.0.1"
env_w["MASTER_PORT"] = "5100"
worker_uuid = "TestWorker_Redirect"
proc_w = subprocess.Popen(
    ["python", worker_path, worker_uuid], stdout=subprocess.PIPE, stderr=subprocess.STDOUT, env=env_w, text=True
)
print("Started Worker pointing to Master A (5100)")

start = time.time()
redirected = False
completed = False

try:
    procs = {"A": proc_a, "B": proc_b, "W": proc_w}
    while time.time() - start < 20:
        for name, p in procs.items():
            if p.stdout is None:
                continue
            line = p.stdout.readline()
            if not line:
                continue
            print(f"[{name}] {line}", end="")
            if (
                name == "W"
                and "Redirecionamento recebido" in line
                or "↪ Redirecionamento" in line
                or "↪ Redirecionamento recebido" in line
                or "↪ Redirecionamento" in line
            ):
                redirected = True
            if "Tarefa" in line and "atribuída" in line and "Master_B" in line:
                completed = True
        if redirected and completed:
            break
        time.sleep(0.05)
finally:
    for p in [proc_w, proc_a, proc_b]:
        try:
            p.kill()
        except Exception:
            pass

print("\nRedirected:", redirected, "Task assigned to Master_B observed:", completed)
