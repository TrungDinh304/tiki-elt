"""Block until Airflow metadata DB is ready (CLI `airflow dags list` succeeds).

Cross-platform replacement for a bash `until ... done` loop in Make recipes —
GNU Make on Windows falls back to cmd.exe, which doesn't understand bash
control flow. Python is always on PATH when `make up` runs from the project
venv, so this is the most portable option.

Exit 0 once Airflow responds, exit 1 after the timeout.
"""
from __future__ import annotations

import subprocess
import sys
import time

TIMEOUT_SECONDS = 300  # 5 min
POLL_INTERVAL = 3


def main() -> int:
    deadline = time.monotonic() + TIMEOUT_SECONDS
    attempts = 0
    while time.monotonic() < deadline:
        attempts += 1
        result = subprocess.run(
            ["docker", "compose", "exec", "-T", "airflow", "airflow", "dags", "list"],
            capture_output=True,
        )
        if result.returncode == 0:
            print(f"Airflow ready after {attempts} probe(s).")
            return 0
        time.sleep(POLL_INTERVAL)

    print(
        f"Airflow did not become ready within {TIMEOUT_SECONDS}s. "
        "Check `docker compose logs airflow` for boot errors.",
        file=sys.stderr,
    )
    return 1


if __name__ == "__main__":
    sys.exit(main())
