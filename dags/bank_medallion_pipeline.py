"""
### Bank medallion pipeline (Bronze -> Silver -> Gold star schema) - with monitoring, quality gates and alerting

```
download_source_files -> check_source_files -> bronze_ingest -> dq_bronze (gate 1) -> silver_clean -> dq_silver (gate 2)
   -> gold_dimensions -> gold_facts -> dq_gold (gate 3) -> table_health -> notify_success
                      (all tasks) -> finalize_run  (records run summary, checks the SLA, fails the run if anything failed)
```
* Every Spark task runs through `instrument.py`, which records duration, exit code, peak memory and row counts.
* A CRITICAL data-quality failure stops the pipeline BEFORE the next layer is built and raises an alert.
* WARNING failures do not stop the run; they are sent as one digest (e-mail + Teams).
* Dashboards: Grafana http://localhost:3000 - Metrics: Prometheus http://localhost:9090
"""
import json
import os
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.exceptions import AirflowFailException
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule

sys.path.insert(0, "/opt/airflow/include")
sys.path.insert(0, "/opt/airflow/spark_jobs")
import alerting  # noqa: E402
import obs  # noqa: E402
import schemas as S  # noqa: E402
from notify import build_success_html, send_mail  # noqa: E402

JOBS = "/opt/airflow/spark_jobs"
DATA = "/opt/airflow/data"
AIRFLOW_URL = os.environ.get("AIRFLOW_BASE_URL", "http://localhost:8080")
GRAFANA_URL = os.environ.get("GRAFANA_URL", "http://localhost:3000")
SLA_MINUTES = int(os.environ.get("PIPELINE_SLA_MINUTES", "30"))

default_args = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "execution_timeout": timedelta(minutes=45),
    "on_failure_callback": alerting.task_failure_callback,
    "on_retry_callback": alerting.task_retry_callback,
}


def download_source_files(run_id=None, ds=None, **_):
    """Pull the 5 CSV files straight from the Kaggle dataset (include/fetch_data.py)."""
    import fetch_data
    print(fetch_data.fetch(run_id or "manual", ds))


def check_source_files():
    missing = [S.SOURCES[t]["file"] for t in S.SOURCES if not S.find_file(f"{DATA}/raw", t)]
    if missing:
        raise AirflowFailException(f"Missing file(s) in {DATA}/raw: {missing}. "
                                   "Copy all 5 Kaggle CSV files into data/raw.")
    print("All 5 source files found.")


def spark_job(task_id, script, stage=None, args="", **kw):
    """BashOperator that runs a Spark job through instrument.py (duration / memory / row-count telemetry)."""
    cmd = (f"python {JOBS}/instrument.py --task {task_id} --ds {{{{ ds }}}} --run-id '{{{{ run_id }}}}'"
           + (f" --stage {stage}" if stage else "") + f" -- python {JOBS}/{script} {args}")
    return BashOperator(task_id=task_id, bash_command=cmd, **kw)


def send_success(ds, run_id, **_):
    with open(f"{DATA}/reports/run_report.json", encoding="utf-8") as f:
        report = json.load(f)
    run_url = f"{AIRFLOW_URL}/dags/bank_medallion_pipeline/grid?dag_run_id={run_id}"
    warnings = obs.dq_failures(run_id, severity="WARNING")
    durations = obs.task_durations(run_id)
    if warnings:       # succeeded, but something deserves a look -> WARNING alert (e-mail + Teams) instead of plain success
        lines = [f"Run {ds} finished, but {len(warnings)} data-quality warning(s) need a look:"]
        lines += [alerting._fmt_dq(w) for w in warnings[:15]]
        lines.append(f"Dashboard: {GRAFANA_URL}")
        alerting.send_alert("WARNING", f"{len(warnings)} DQ warning(s) in pipeline run {ds}", lines,
                            task_id="notify_success", run_id=run_id, link=run_url)
        return
    extra = "".join(f"<tr><td style='padding:2px 12px 2px 0;color:#555'>{t}</td><td>{s:.0f}s</td></tr>"
                    for t, s in durations)
    html = (build_success_html(report, ds, run_url)
            + f"<h3>Task durations</h3><table>{extra}</table><p><a href='{GRAFANA_URL}'>Open Grafana dashboard</a></p>")
    send_mail(f"[SUCCESS] Bank medallion pipeline - {ds}", html,
              text=f"Pipeline succeeded for {ds}: {report['tables']}")


def finalize_run(ds, run_id, dag_run, **_):
    """Always runs. Stores the run summary, checks the SLA, and fails the DAG run if any task failed
    (otherwise Airflow would mark the run 'success' because this last task succeeded)."""
    tis = [ti for ti in dag_run.get_task_instances() if ti.task_id != "finalize_run"]
    failed = [ti.task_id for ti in tis if str(ti.state) in ("failed", "upstream_failed")]
    started = dag_run.start_date
    duration = (datetime.now(started.tzinfo) - started).total_seconds() if started else 0.0
    counts = obs.run_dq_counts(run_id)
    status = "FAILED" if failed else "SUCCESS"
    obs.upsert_run_summary(run_id, ds, status, started, duration, counts, failed)
    obs.clear_task_metrics(run_id, "pipeline")
    obs.record_metrics(run_id, ds, "pipeline", [(None, "duration_seconds", duration)])
    if not failed and duration > SLA_MINUTES * 60:
        alerting.send_alert("WARNING", f"Pipeline SLA breached ({duration / 60:.0f} min > {SLA_MINUTES} min)",
                            [f"Run {ds} took {duration / 60:.1f} minutes; SLA is {SLA_MINUTES} minutes.",
                             f"Task durations: {', '.join(f'{t}={s:.0f}s' for t, s in obs.task_durations(run_id))}"],
                            task_id="finalize_run", run_id=run_id)
    if failed:
        raise AirflowFailException(f"Pipeline run failed - task(s): {', '.join(failed)}")


with DAG(
    dag_id="bank_medallion_pipeline",
    description="Bronze -> Silver -> Gold on Delta Lake with Spark, quality gates, monitoring and alerting",
    start_date=datetime(2026, 10, 1),
    schedule="0 2 * * *",          # every day at 02:00 (Airflow timezone, see .env)
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=2),
    default_args=default_args,
    tags=["medallion", "delta", "spark", "bank", "monitored"],
    doc_md=__doc__,
) as dag:

    download = PythonOperator(task_id="download_source_files", python_callable=download_source_files)
    check_source = PythonOperator(task_id="check_source_files", python_callable=check_source_files)

    bronze = spark_job("bronze_ingest", "01_bronze.py", stage="bronze",
                       args="--ds {{ ds }} --batch-id '{{ run_id }}'")
    dq_bronze = spark_job("dq_bronze", "dq_bronze.py", args="--ds {{ ds }} --run-id '{{ run_id }}'", retries=0)
    silver = spark_job("silver_clean", "02_silver.py", stage="silver")
    dq_silver = spark_job("dq_silver", "dq_silver.py", args="--ds {{ ds }} --run-id '{{ run_id }}'", retries=0)
    gold_dims = spark_job("gold_dimensions", "03_gold_dimensions.py", stage="gold_dimensions")
    gold_fact = spark_job("gold_facts", "04_gold_fact.py", stage="gold_fact")
    dq_gold = spark_job("dq_gold", "05_quality_checks.py", args="--ds {{ ds }} --run-id '{{ run_id }}'", retries=0)
    health = spark_job("table_health", "06_table_health.py", args="--ds {{ ds }} --run-id '{{ run_id }}'")
    notify = PythonOperator(task_id="notify_success", python_callable=send_success)
    finalize = PythonOperator(task_id="finalize_run", python_callable=finalize_run,
                              trigger_rule=TriggerRule.ALL_DONE, retries=0, on_failure_callback=None)

    (download >> check_source >> bronze >> dq_bronze >> silver >> dq_silver >> gold_dims >> gold_fact >> dq_gold
     >> health >> notify)
    for t in (download, check_source, bronze, dq_bronze, silver, dq_silver, gold_dims, gold_fact, dq_gold, health, notify):
        t >> finalize
