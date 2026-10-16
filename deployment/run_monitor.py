"""Keep the local Streamlit dashboard running under Windows Task Scheduler."""

from __future__ import annotations

import json
import logging
from logging.handlers import RotatingFileHandler
import msvcrt
import os
from pathlib import Path
import socket
import subprocess
import sys
import time


ROOT = Path(__file__).resolve().parents[1]
RUNTIME = ROOT / ".runtime"
STATE = RUNTIME / "monitor-state.json"


def main() -> int:
    RUNTIME.mkdir(exist_ok=True)
    # An OS lock is released even when Windows forcibly terminates this process.
    with (RUNTIME / "monitor.lock").open("a+b") as lock:
        lock.seek(0)
        lock.write(b"0")
        lock.flush()
        lock.seek(0)
        try:
            msvcrt.locking(lock.fileno(), msvcrt.LK_NBLCK, 1)
        except OSError:
            return 0

        logger = logging.getLogger("monitor")
        logger.setLevel(logging.INFO)
        handler = RotatingFileHandler(
            RUNTIME / "monitor.log", maxBytes=10_000_000, backupCount=3,
            encoding="utf-8",
        )
        handler.setFormatter(logging.Formatter("%(asctime)s %(message)s"))
        logger.addHandler(handler)
        env = dict(os.environ, PYTHONUTF8="1", PYTHONUNBUFFERED="1",
                   APPDATA=str(RUNTIME / "appdata"))
        (RUNTIME / "appdata").mkdir(exist_ok=True)
        command = [
            str(ROOT / ".venv" / "Scripts" / "python.exe"), "-u", "-m",
            "streamlit", "run", str(ROOT / "app.py"),
            "--server.address", "127.0.0.1", "--server.port", "8510",
            "--server.headless", "true", "--browser.gatherUsageStats", "false",
        ]
        logger.info("Supervisor started, pid=%s", os.getpid())
        while True:
            # Never repeatedly launch against a port owned by another service.
            with socket.socket() as probe:
                probe.settimeout(1)
                occupied = probe.connect_ex(("127.0.0.1", 8510)) == 0
            if occupied:
                logger.warning("Port 8510 is already in use; retrying in 30 seconds")
                time.sleep(30)
                continue
            child = None
            try:
                child = subprocess.Popen(
                    command, cwd=ROOT, env=env, stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT, text=True, encoding="utf-8",
                    errors="replace", creationflags=subprocess.CREATE_NO_WINDOW,
                )
                state = {"supervisor_pid": os.getpid(), "launcher_pid": child.pid,
                         "started_at": time.strftime("%Y-%m-%d %H:%M:%S"),
                         "url": "http://127.0.0.1:8510"}
                temporary = STATE.with_suffix(".tmp")
                temporary.write_text(json.dumps(state, indent=2), encoding="utf-8")
                temporary.replace(STATE)
                logger.info("Streamlit started, launcher pid=%s", child.pid)
                assert child.stdout is not None
                for line in child.stdout:
                    logger.info("%s", line.rstrip())
                logger.warning("Streamlit exited, code=%s; restarting in 5 seconds",
                               child.wait())
            except Exception:
                logger.exception("Streamlit launch failed; retrying in 5 seconds")
            finally:
                if child is not None and child.poll() is None:
                    subprocess.run(["taskkill", "/PID", str(child.pid), "/T", "/F"],
                                   creationflags=subprocess.CREATE_NO_WINDOW,
                                   capture_output=True, check=False)
            time.sleep(5)


if __name__ == "__main__":
    sys.exit(main())
