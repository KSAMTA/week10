"""Create the observability schema (obs.*) in Postgres. Safe to run any time.

    docker compose exec scheduler python /opt/airflow/scripts/init_obs.py
"""
import logging
import sys

sys.path.insert(0, "/opt/airflow/include")
import obs  # noqa: E402

logging.basicConfig(level=logging.INFO)
obs.ensure_schema()
print("obs schema ready: task_metrics, dq_results, alerts, run_summary")
