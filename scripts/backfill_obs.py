"""Seed the metrics history from existing data/reports/run_report_<date>.json files, so anomaly checks
(volume, drift) have a baseline right away.

    docker compose exec scheduler python /opt/airflow/scripts/backfill_obs.py
"""
import glob
import json
import os
import sys

sys.path.insert(0, "/opt/airflow/include")
import obs  # noqa: E402

reports = sorted(glob.glob("/opt/airflow/data/reports/run_report_*.json"))
for path in reports:
    rep = json.load(open(path, encoding="utf-8"))
    ds = rep["ds"]
    run_id = f"backfill__{ds}"
    ts = f"{ds}T02:00:00+00:00"
    for task, key in (("bronze_ingest", "bronze_rows"), ("silver_clean", "silver_rows")):
        obs.clear_task_metrics(run_id, task)
        obs.record_metrics(run_id, ds, task, [(t, "rows", m[key]) for t, m in rep["tables"].items()], recorded_at=ts)
    obs.clear_task_metrics(run_id, "gold_facts")
    obs.record_metrics(run_id, ds, "gold_facts", [(t, "rows", n) for t, n in rep["gold_tables"].items()], recorded_at=ts)
    print("seeded", ds, "from", os.path.basename(path))
print(f"done - {len(reports)} report(s)")
