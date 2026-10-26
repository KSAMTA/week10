"""Run one pipeline job and record its telemetry in Postgres (obs.task_metrics).

    python instrument.py --task silver_clean --ds 2026-10-21 --run-id <airflow run_id> --stage silver \
           -- python /opt/airflow/spark_jobs/02_silver.py

Records: duration_seconds, exit_code, peak_rss_mb (approx., includes child processes) and - when --stage is
given - every row count the job wrote to data/reports/stage_metrics.json (rows, quarantined_rows, ...).
The job's own exit code is returned unchanged, so Airflow behaves exactly as before.
"""
import argparse
import os
import resource
import subprocess
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import obs  # noqa: E402
import paths as P  # noqa: E402
from common import read_json  # noqa: E402


def _is_num(v):
    return isinstance(v, (int, float)) and not isinstance(v, bool)


def flatten_stage(data):
    """stage_metrics.json section -> [(table, metric, value)].
    {"customers": {"rows": 3}}  -> ("customers","rows",3);  {"dim_x": 5} -> ("dim_x","rows",5)."""
    out = []
    for key, val in (data or {}).items():
        if key == "tables" and isinstance(val, dict):
            out += flatten_stage(val)
        elif isinstance(val, dict):
            out += [(key, m, v) for m, v in val.items() if _is_num(v)]
        elif _is_num(val):
            out.append((key, "rows", val))
    return out


def main():
    argv = sys.argv[1:]
    cut = argv.index("--") if "--" in argv else len(argv)
    cmd = argv[cut + 1:]
    ap = argparse.ArgumentParser()
    ap.add_argument("--task", required=True)
    ap.add_argument("--ds", default="")
    ap.add_argument("--run-id", default="manual")
    ap.add_argument("--stage", default=None)
    args = ap.parse_args(argv[:cut])
    if not cmd:
        sys.exit("usage: instrument.py --task T --run-id R [--stage S] -- <command ...>")

    t0 = time.time()
    rc = subprocess.call(cmd)
    duration = time.time() - t0

    items = [(None, "duration_seconds", round(duration, 2)), (None, "exit_code", rc),
             (None, "peak_rss_mb", round(resource.getrusage(resource.RUSAGE_CHILDREN).ru_maxrss / 1024, 1))]
    if rc == 0 and args.stage:
        items += flatten_stage((read_json(P.METRICS_PATH, {}) or {}).get(args.stage, {}))
    obs.clear_task_metrics(args.run_id, args.task)
    obs.record_metrics(args.run_id, args.ds, args.task, items)
    print(f"[instrument] {args.task}: exit={rc} duration={duration:.1f}s", flush=True)
    sys.exit(rc)


if __name__ == "__main__":
    main()
