# Minimal structured logger for notebooks
from datetime import datetime

def log(msg: str, **kwargs):
    ts = datetime.utcnow().isoformat()
    ks = " ".join([f"{k}={v}" for k, v in kwargs.items()])
    print(f"[{ts}] {msg} {ks}".strip())
