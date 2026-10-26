"""
### Pipeline watchdog - catches what in-pipeline alerts cannot

Runs hourly, independent of the pipeline DAG:
* **Staleness**: no successful `bank_medallion_pipeline` run within WATCHDOG_MAX_HOURS_SINCE_SUCCESS hours
  (pipeline paused, scheduler stuck, schedule broken...) -> CRITICAL alert.
* **Observability store**: Postgres `obs` schema reachable -> WARNING alert if not.
Repeat alerts are throttled by ALERT_COOLDOWN_MINUTES.
If the whole scheduler is down this DAG can't run either - that case is covered by the Prometheus rule
`SchedulerHeartbeatMissing` (see monitoring/prometheus/alerts.yml).
"""
import os
import sys
from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.models import DagRun
from airflow.operators.python import PythonOperator
from airflow.utils.state import DagRunState

sys.path.insert(0, "/opt/airflow/include")
import alerting  # noqa: E402
import obs  # noqa: E402

TARGET = "bank_medallion_pipeline"
MAX_HOURS = float(os.environ.get("WATCHDOG_MAX_HOURS_SINCE_SUCCESS", "30"))
COOLDOWN = int(os.environ.get("ALERT_COOLDOWN_MINUTES", "360"))


def check_staleness():
    ok = [r.end_date for r in DagRun.find(dag_id=TARGET, state=DagRunState.SUCCESS) if r.end_date]
    last = max(ok) if ok else None
    age_h = (datetime.now(timezone.utc) - last).total_seconds() / 3600 if last else None
    print(f"last successful run: {last} ({age_h if age_h is None else round(age_h, 1)} h ago)")
    if last is None or age_h > MAX_HOURS:
        alerting.send_alert("CRITICAL", f"{TARGET} has no recent successful run",
                            [f"Last success: {last or 'never'}" + (f" ({age_h:.1f} h ago)" if age_h else ""),
                             f"Allowed: {MAX_HOURS:g} h. Check that the DAG is un-paused and the scheduler is healthy."],
                            task_id="check_staleness", fingerprint_key=f"stale:{TARGET}", cooldown_minutes=COOLDOWN)


def check_obs_store():
    try:
        obs.query("SELECT 1")
    except Exception as exc:  # noqa: BLE001
        alerting.send_alert("WARNING", "Observability store (Postgres obs schema) unreachable",
                            [str(exc)[:300], "Pipeline still runs; metrics and DQ history are not being recorded."],
                            task_id="check_obs_store", fingerprint_key="obs-down", cooldown_minutes=COOLDOWN)


with DAG(dag_id="pipeline_watchdog", description="Hourly freshness / health watchdog for the medallion pipeline",
         start_date=datetime(2026, 10, 1), schedule="15 * * * *", catchup=False, max_active_runs=1,
         default_args={"owner": "data-engineering", "retries": 0, "execution_timeout": timedelta(minutes=5)},
         tags=["monitoring", "watchdog"], doc_md=__doc__) as dag:
    PythonOperator(task_id="check_staleness", python_callable=check_staleness)
    PythonOperator(task_id="check_obs_store", python_callable=check_obs_store)
