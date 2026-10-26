# Bank Medallion Pipeline - Airflow + Spark + Delta Lake

Scheduled **Bronze -> Silver -> Gold (star schema)** batch pipeline for the 5 Kaggle bank files
(`salikhussaini49/bank-dataset`). Runs in Docker: Airflow 2.10, PySpark 3.5 (local mode), Delta Lake 3.3, Postgres.

```
data/raw/*.csv (5 files)
   v  check_source_files
BRONZE   1 Delta table per file, all strings + lineage (_row_id, _ingest_date, _batch_id ...)
   v
SILVER   typed, trimmed, standardised, de-duplicated  -->  quarantine/<table> (bad rows + _dq_reason)
   v
GOLD     dim_date, dim_customer, dim_branch, dim_account, dim_transaction_type, dim_transaction_media
         fact_transactions (grain: 1 transaction)   fact_loans (grain: 1 loan)
   v
quality_checks (reconciliation, unique keys, no NULL FKs, referential integrity, sums) -> notify_success
```
Schedule: daily 02:00, 2 retries (2 min apart), no catch-up. Alerts: e-mail and/or Teams (see `.env.example`).

## Monitoring, quality gates & alerting (added)
Quality gates after Bronze, Silver and Gold; run/task metrics in Postgres; Grafana dashboard (http://localhost:3000);
Prometheus rules; e-mail + Microsoft Teams alerts; hourly watchdog DAG. **Full guide: [MONITORING.md](MONITORING.md)**.

```
check_source -> bronze -> [dq_bronze] -> silver -> [dq_silver] -> gold_dims -> gold_facts -> [dq_gold] -> table_health -> notify
```
Run drills with `python scripts/inject_bad_data.py list`. Tests: `python -m unittest discover -s tests -v`.

## Star schema
fact_transactions -> dim_date, dim_account, dim_customer, dim_branch, dim_transaction_type, dim_transaction_media
fact_loans        -> dim_customer, dim_branch
Surrogate keys = `xxhash64(natural id)`, so reruns give identical keys. A transaction's customer/branch come via its account.

## Important data quirk
In every Kaggle file each ID appears **3 times with different content** (e.g. `C00001` = 3 different people).
Silver therefore: keeps the **last occurrence** of each ID for customers / branches / accounts / loans, and keeps **all**
transaction rows (they are distinct events; only exact duplicates are dropped). Counts: bronze 3,000 customers -> 1,000 in Silver.

## Run
```
docker compose build
docker compose up airflow-init
docker compose up -d
docker compose exec scheduler python /opt/airflow/spark_jobs/00_profile.py
```
Open http://localhost:8080 (admin / admin), un-pause and trigger `bank_medallion_pipeline`.
Results: `data/lakehouse/{bronze,silver,quarantine,gold}`, report `data/reports/run_report.json`.
Demo queries + Delta history / time travel: `docker compose exec scheduler python /opt/airflow/spark_jobs/99_demo_queries.py`

## Troubleshooting
| Symptom | Fix |
|---|---|
| `expected columns ... but file has ...` in bronze | the CSV header differs from `spark_jobs/schemas.py` |
| Containers killed / out of memory | set `SPARK_DRIVER_MEMORY=1g` in `.env`, restart |
| `.env` changes ignored | `docker compose up -d` again |
| Gmail "Username and Password not accepted" | use an App Password (needs 2-Step Verification) |
| Slow Delta writes on a Windows drive | normal for bind mounts; the data here is small |
