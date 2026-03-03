import subprocess
import sys
import time
from pathlib import Path

Path("logs").mkdir(exist_ok=True)

PROCESSES = [
    ("producer",          [sys.executable, "-u", "processes/producer.py"]),
    ("producer_consumer", [sys.executable, "-u", "processes/producer_consumer.py"]),
    ("audit",             [sys.executable, "-u", "processes/audit_consumer.py"]),
    ("audit_dlq",         [sys.executable, "-u", "processes/audit_dlq.py"]),
    ("consumer",          [sys.executable, "-u", "processes/consumer.py"]),
    ("dashboard",         [sys.executable, "-m", "streamlit", "run", "processes/dashboard.py"]),
]

procs = []

print("Starting services...\n")
for name, cmd in PROCESSES:
    out = open(f"logs/{name}.out", "w", encoding="utf-8")
    err = open(f"logs/{name}.err", "w", encoding="utf-8")
    p = subprocess.Popen(cmd, stdout=out, stderr=err)
    procs.append((name, p))
    print(f"✔ {name} started (pid={p.pid})")

print("\nRunning. Press CTRL+C to stop.\n")

try:
    while True:
        for name, p in procs:
            rc = p.poll()
            if rc is not None:
                print(f"✖ {name} exited with code {rc}. Check logs/{name}.err")
        time.sleep(0.001)
except KeyboardInterrupt:
    print("\nStopping...")
    for name, p in procs:
        p.terminate()
        print(f"✖ {name} stopped")