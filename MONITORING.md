# Monitoring, data-quality gates & alerting - bank medallion pipeline

Mini project: *instrument an existing pipeline with full monitoring, DQ checks and alerting.*
The existing pipeline (Airflow + Spark + Delta, Bronze -> Silver -> Gold) is unchanged in what it computes;
this layer adds **observability**, **quality gates** and **alerting** around it.

## 1. Architecture

```
                       +------------------ Airflow (scheduler) ------------------+
 data/raw/*.csv        |  check_source -> bronze_ingest -> [dq_bronze] -> silver_clean -> [dq_silver]
                       |   -> gold_dimensions -> gold_facts -> [dq_gold] -> table_health -> notify_success
                       |                      (all tasks) -> finalize_run (summary + SLA + fails run if needed)
                       +--------------------------------------------------------------------------------+
   every Spark task runs via spark_jobs/instrument.py  --> duration, exit code, peak memory, row counts
   every DQ gate (DQRunner)                            --> obs.dq_results (check, severity, observed, limit)
                                    |                                         |
                          Postgres schema `obs`  <---------------- include/obs.py (never breaks the pipeline)
                                    |
   Airflow StatsD -> statsd-exporter -> Prometheus (rules)          include/alerting.py
                                    \-----> Grafana <-----/          CRITICAL / WARNING -> e-mail + Teams
                                                                      + obs.alerts   + pipeline_watchdog DAG
```

| Pillar | What was added | Where |
|---|---|---|
| **Monitoring** | per-task duration / exit code / peak memory; row counts per table & layer; quarantine %; Delta table size & file count; end-to-end run duration; Airflow internals (scheduler heartbeat, task failures, DAG import errors); Grafana dashboard | `spark_jobs/instrument.py`, `06_table_health.py`, `include/obs.py`, `monitoring/` |
| **Data quality** | 3 gates (Bronze, Silver, Gold) with CRITICAL / WARNING / INFO severities; history-based volume & drift checks; results stored per run | `spark_jobs/dq_*.py`, `05_quality_checks.py`, `dq_rules.py`, `dq_framework.py` |
| **Alerting** | severity routing to e-mail + Microsoft Teams; failure alerts that list the failing DQ checks; WARNING digest; SLA alert; hourly watchdog for stale pipelines; Prometheus rules; alert log | `include/alerting.py`, `dags/pipeline_watchdog.py`, `monitoring/prometheus/alerts.yml` |

## 2. Quality gates - what is checked, and what happens on failure

Key change vs. the original: the only check used to run **after** Gold was built. Now bad data is stopped **before the next layer**.

| Gate | Task | CRITICAL (stops the pipeline) | WARNING (run continues, alert digest) |
|---|---|---|---|
| 1 Bronze | `dq_bronze` | table has rows; **bronze rows = source-file rows** (completeness) | volume vs recent runs (+/-30%); blank primary keys; source-file freshness (if enabled) |
| 2 Silver | `dq_silver` | rows exist; key not null; key unique (master data); **bronze = silver + quarantine + duplicates**; **quarantine share <= 5%**; **referential integrity** (accounts->customers/branches, loans->customers/branches, transactions->accounts); dates not in the future | null rate per column (> 1%); numeric ranges; unknown categories (account status/type, transaction type/media); ID & phone formats; mean-amount **drift** (z-score vs history); volume vs history |
| 3 Gold | `dq_gold` | the original 41 checks (unique keys, no NULL FKs, FK integrity, sums silver = gold, row reconciliation) | -- |

Volume severity: deviation >= 30% from the median of recent runs -> WARNING, >= 60% -> CRITICAL.
These checks need history: the first runs say "no baseline yet" (INFO). `scripts/backfill_obs.py` seeds history from
your existing `run_report_*.json` files.

Notes
* **Transaction IDs repeat in the Kaggle file on purpose** (15,000 rows, 5,000 distinct IDs - distinct events), so uniqueness is
  checked for master data only, matching the Silver rules.
* DQ tasks have `retries=0`: re-running a deterministic data check can't fix bad data. Infrastructure tasks keep 2 retries.
* All thresholds are environment variables (`.env`): `MAX_QUARANTINE_PCT`, `MAX_NULL_PCT`, `ORPHAN_MAX_PCT`,
  `VOLUME_WARN_PCT`, `VOLUME_FAIL_PCT`, `DRIFT_Z_THRESHOLD`, `FRESHNESS_MAX_HOURS` (0 = off).

## 3. Alerting rules

| Event | Severity | Channels | Notes |
|---|---|---|---|
| Any task fails after retries | CRITICAL | e-mail + Teams | includes error, attempt, log link; for `dq_*` tasks also lists the failing checks |
| Task retry | INFO | stored only | visible in the dashboard's "Recent alerts" |
| Run succeeded but DQ warnings exist | WARNING | e-mail + Teams | one digest instead of a plain success mail |
| Run duration > `PIPELINE_SLA_MINUTES` | WARNING | e-mail + Teams | (Airflow's built-in SLA feature is deprecated; this is checked in `finalize_run`) |
| No successful run for `WATCHDOG_MAX_HOURS_SINCE_SUCCESS` h | CRITICAL | e-mail + Teams | hourly `pipeline_watchdog` DAG; throttled by `ALERT_COOLDOWN_MINUTES` |
| Obs store unreachable | WARNING | e-mail + Teams | pipeline keeps running |
| Scheduler heartbeat missing 5 min / DAG import errors | critical | Prometheus **Alerts** page | covers "Airflow itself is down" (no Alertmanager is wired up - see Limitations) |
| Run succeeded, no warnings | - | e-mail | success mail now also lists task durations + dashboard link |

`ALERT_CHANNELS` (default `email,teams`) picks channels; a channel with no configuration is skipped silently.
`ALERT_MIN_SEVERITY` (default WARNING) sets what is sent at all. Every alert is stored in `obs.alerts`.

**Teams:** in a channel or chat: *Workflows -> "Post to a channel when a webhook request is received"* -> copy the URL into
`TEAMS_WEBHOOK_URL` in `.env`. (Webhook-based alerts need no password; e-mail still uses whatever `include/notify.py` does.)

## 4. Run it

Unzip this project **over your existing project folder** (it adds/overwrites `dags/`, `include/`, `spark_jobs/`,
`scripts/`, `docker-compose.yml`, `monitoring/`, docs). It does not touch your `.env` or `data/`. Then, from the project folder:

```powershell
# 1. (optional) add the new settings from .env.example to your .env  - everything has a default, so this can wait
# 2. pull the 3 new images, recreate containers (no rebuild needed - code is bind-mounted)
docker compose pull
docker compose up airflow-init          # also creates the obs.* tables
docker compose up -d
docker compose ps                       # postgres, statsd-exporter, scheduler, webserver, prometheus, grafana

# 3. seed history from your earlier runs (gives the volume/drift checks a baseline)
docker compose exec scheduler python /opt/airflow/scripts/backfill_obs.py

# 4. test the alert channels
docker compose exec scheduler python /opt/airflow/scripts/test_alerts.py

# 5. run the pipeline: Airflow UI http://localhost:8080 (admin/admin) -> un-pause + trigger bank_medallion_pipeline
#    also un-pause pipeline_watchdog
```

| URL | What |
|---|---|
| http://localhost:3000 (admin / admin) | **Grafana** - dashboard "Bank medallion pipeline - health, quality & alerts" opens as home |
| http://localhost:9090 | Prometheus (`/alerts` shows the rules) |
| http://localhost:8080 | Airflow |

Memory: Grafana + Prometheus are capped at 384 MB each. If Spark gets OOM-killed on your 8 GB machine, either give WSL more
memory (`%UserProfile%\.wslconfig` -> `[wsl2]` / `memory=5GB`, then `wsl --shutdown`) or stop them while running the
pipeline: `docker compose stop grafana prometheus`.

## 5. Failure drills (do these for the demo / report)

`scripts/inject_bad_data.py` breaks the raw data on purpose (originals are backed up; run it on your machine from the project folder).

```powershell
python scripts/inject_bad_data.py list
python scripts/inject_bad_data.py orphans         # then trigger the DAG
python scripts/inject_bad_data.py restore
```

| Scenario | Expected |
|---|---|
| `orphans` | **dq_silver fails** (transactions -> accounts integrity). `gold_*` tasks are *upstream_failed* - Gold is untouched. CRITICAL alert lists the check. |
| `blank_ids` | dq_silver fails: quarantine share 10% > 5% (reason `missing_customer_id`) |
| `future_dates` | dq_silver fails: transaction_date out of range |
| `schema_change` | bronze_ingest fails fast (column contract), CRITICAL alert |
| `volume_drop` | dq_bronze fails: transactions -65% vs median (needs >= 3 earlier runs - run once normally after the backfill) |
| `bad_category` | run **succeeds** with a WARNING digest ("unexpected: ['Frozen']") |
| `extreme_amounts` | WARNING: amounts above range; drift warning once >= 5 runs of history exist |

After each drill: check Grafana (failed checks table, run status, alerts), your mailbox and Teams, then `restore` and re-run - the next run should be green again.

## 6. Unit tests

`python -m unittest discover -s tests -v` (22 tests: volume/drift decisions, severity & exit behaviour of the DQ runner,
alert routing/cool-down, metric flattening, "observability never breaks the pipeline"). No Docker needed.

## 7. Design decisions & limitations (be upfront about these)

* **Postgres as the metrics store**, next to Prometheus for infrastructure metrics. Batch-job business metrics (row counts,
  DQ results) don't fit Prometheus' scrape model well, and Postgres gives history for the anomaly checks. In production you'd
  use a separate database/role (Grafana here reads with the Airflow DB user - fine for a lab, not for production).
* **Alerts come from Airflow, not Alertmanager.** Prometheus rules are visible/evaluated but not routed anywhere; adding
  Alertmanager (webhook receiver for Teams) is the natural next step.
* **Anomaly checks are simple on purpose** (median % deviation, z-score) - explainable and cheap; they stay quiet until enough history exists.
* **Peak memory** (`peak_rss_mb`) is approximate (process + children via `getrusage`).
* **Source freshness** is off by default because this Kaggle dataset is static; set `FRESHNESS_MAX_HOURS` when real daily files arrive.
* Dashboards and Spark/Airflow code were written without being able to run Docker in the authoring environment: Python was
  compiled and unit-tested, YAML/JSON validated, and DAG command strings rendered - but the first real run is the true test.
  If a Grafana panel shows "relation does not exist", run the pipeline once (or `scripts/init_obs.py`).

## 8. Source data straight from Kaggle (task `download_source_files`)

The DAG now starts with `download_source_files`, which pulls the dataset
`salikhussaini49/bank-dataset` from Kaggle (code: `include/fetch_data.py`) - no manual copying of CSV files.

* Downloads into a temp folder, **validates** all 5 files (present, non-empty, header matches `schemas.py`), then copies them
  into `data/raw` atomically. A bad or partial download can never reach Bronze.
* Credentials live only in `.env` (`KAGGLE_USERNAME` + `KAGGLE_KEY`, or `KAGGLE_API_TOKEN`) - never in code or git.
* Download failure -> task retries twice, then a CRITICAL alert. If the 5 files already exist locally, the run continues on
  them and a WARNING alert is sent (`ALLOW_LOCAL_FALLBACK=true`). `DATA_SOURCE=local` turns downloading off.
* Metrics recorded per run: `download_ok`, `download_seconds`, `download_mb`, `source_rows` per table.
