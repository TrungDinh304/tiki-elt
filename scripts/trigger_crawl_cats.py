"""Trigger `tiki_lakehouse_daily_pipeline` với category override.

Wrap `airflow dags trigger -c '{"category_ids":"..."}'` để né cmd.exe
JSON-quoting nightmare khi `make crawl-cats` chạy trên Windows. subprocess
truyền list args → quoting do CreateProcess lo, JSON tới Airflow CLI nguyên vẹn.

Validate input dạng CSV digits trước khi trigger — match pattern Param đã set
trong DAG (xem dags/tiki_lakehouse_dag.py). Fail-fast tại đây tránh DAG trigger
xong mới reject ở task render.
"""
from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys

PATTERN = re.compile(r"^\s*\d+(\s*,\s*\d+)*\s*$")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument(
        "ids",
        help="Comma-separated leaf category IDs, ví dụ 8322,316",
    )
    ap.add_argument(
        "--dag",
        default="tiki_lakehouse_daily_pipeline",
        help="DAG id để trigger (default: tiki_lakehouse_daily_pipeline)",
    )
    args = ap.parse_args()

    if not PATTERN.match(args.ids):
        print(
            f"[trigger_crawl_cats] invalid IDS={args.ids!r} — "
            "phải là CSV chỉ chứa digits, ví dụ '8322,316'",
            file=sys.stderr,
        )
        return 2

    normalized = ",".join(part.strip() for part in args.ids.split(","))
    conf = json.dumps({"category_ids": normalized})

    cmd = [
        "docker", "compose", "exec", "-T", "airflow",
        "airflow", "dags", "trigger", args.dag,
        "--conf", conf,
    ]
    print("[trigger_crawl_cats]", " ".join(cmd))
    return subprocess.call(cmd)


if __name__ == "__main__":
    sys.exit(main())
