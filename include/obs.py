"""Observability store: run metrics, data-quality results, alerts and run summaries in Postgres (schema `obs`).

Design rule: observability must NEVER break the pipeline. Every public function swallows its own errors
(logs them) and returns a safe default, so a Postgres hiccup can't fail a data job.

Connection: OBS_DB_DSN (set in docker-compose.yml, defaults to the Airflow Postgres).
"""
import logging
import os
from contextlib import contextmanager
from functools import wraps

log = logging.getLogger("obs")

DAG_ID = os.environ.get("PIPELINE_DAG_ID", "bank_medallion_pipeline")

DDL = """
SELECT pg_advisory_xact_lock(727272);
CREATE SCHEMA IF NOT EXISTS obs;

CREATE TABLE IF NOT EXISTS obs.task_metrics (
    id          BIGSERIAL PRIMARY KEY,
    recorded_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    dag_id      TEXT NOT NULL,
    run_id      TEXT NOT NULL,
    ds          TEXT,
    task_id     TEXT NOT NULL,
    table_name  TEXT NOT NULL DEFAULT '',
    metric      TEXT NOT NULL,
    value       DOUBLE PRECISION
);
CREATE INDEX IF NOT EXISTS ix_task_metrics_lookup ON obs.task_metrics (task_id, table_name, metric, recorded_at DESC);
CREATE INDEX IF NOT EXISTS ix_task_metrics_run ON obs.task_metrics (run_id);

CREATE TABLE IF NOT EXISTS obs.dq_results (
    id          BIGSERIAL PRIMARY KEY,
    recorded_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    dag_id      TEXT NOT NULL,
    run_id      TEXT NOT NULL,
    ds          TEXT,
    layer       TEXT NOT NULL,
    table_name  TEXT NOT NULL DEFAULT '',
    check_name  TEXT NOT NULL,
    severity    TEXT NOT NULL,
    passed      BOOLEAN NOT NULL,
    observed    TEXT,
    threshold   TEXT
);
CREATE INDEX IF NOT EXISTS ix_dq_results_run ON obs.dq_results (run_id, layer);

CREATE TABLE IF NOT EXISTS obs.alerts (
    id          BIGSERIAL PRIMARY KEY,
    recorded_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    dag_id      TEXT,
    run_id      TEXT,
    task_id     TEXT,
    severity    TEXT NOT NULL,
    title       TEXT NOT NULL,
    body        TEXT,
    delivered   TEXT,
    fingerprint TEXT
);
CREATE INDEX IF NOT EXISTS ix_alerts_fp ON obs.alerts (fingerprint, recorded_at DESC);

CREATE TABLE IF NOT EXISTS obs.run_summary (
    run_id            TEXT PRIMARY KEY,
    dag_id            TEXT NOT NULL,
    ds                TEXT,
    status            TEXT NOT NULL,
    started_at        TIMESTAMPTZ,
    finished_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    duration_seconds  DOUBLE PRECISION,
    checks_total      INTEGER,
    checks_passed     INTEGER,
    critical_failures INTEGER,
    warnings          INTEGER,
    failed_tasks      TEXT
);
"""

_schema_ready = False


def _dsn():
    return os.environ.get("OBS_DB_DSN", "").strip()


@contextmanager
def _conn():
    global _schema_ready
    import psycopg2  # imported lazily so unit tests / non-Airflow shells don't need it

    dsn = _dsn()
    if not dsn:
        raise RuntimeError("OBS_DB_DSN is not set")
    conn = psycopg2.connect(dsn, connect_timeout=10)
    try:
        if not _schema_ready:
            with conn.cursor() as cur:
                cur.execute(DDL)
            conn.commit()
            _schema_ready = True
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def execute(sql, params=None, many=False):
    with _conn() as conn, conn.cursor() as cur:
        if many:
            cur.executemany(sql, params or [])
        else:
            cur.execute(sql, params)


def query(sql, params=None):
    with _conn() as conn, conn.cursor() as cur:
        cur.execute(sql, params)
        return cur.fetchall()


def _safe(default=None):
    def deco(fn):
        @wraps(fn)
        def wrapper(*a, **kw):
            try:
                return fn(*a, **kw)
            except Exception as exc:  # noqa: BLE001
                log.warning("obs.%s skipped: %s", fn.__name__, exc)
                return default() if callable(default) else default
        return wrapper
    return deco


@_safe()
def ensure_schema():
    with _conn():
        pass
    log.info("obs schema ready")


# ----------------------------------------------------------------------------- metrics
@_safe()
def clear_task_metrics(run_id, task_id):
    """Make reruns/retries idempotent: one set of metrics per (run, task)."""
    execute("DELETE FROM obs.task_metrics WHERE run_id=%s AND task_id=%s", (run_id, task_id))


@_safe()
def record_metrics(run_id, ds, task_id, items, recorded_at=None):
    """items: iterable of (table_name or None, metric, numeric value)."""
    rows = [(DAG_ID, run_id, ds, task_id, t or "", m, float(v), recorded_at)
            for t, m, v in items if v is not None]
    if rows:
        execute("INSERT INTO obs.task_metrics (dag_id, run_id, ds, task_id, table_name, metric, value, recorded_at) "
                "VALUES (%s,%s,%s,%s,%s,%s,%s, COALESCE(%s, now()))", rows, many=True)


@_safe(default=list)
def history(task_id, table_name, metric, n=7, exclude_run_id=None):
    """Last n values (newest first) of a metric from OTHER runs - the baseline for anomaly checks."""
    rows = query("SELECT value FROM obs.task_metrics WHERE task_id=%s AND table_name=%s AND metric=%s "
                 "AND run_id <> %s AND value IS NOT NULL ORDER BY recorded_at DESC LIMIT %s",
                 (task_id, table_name or "", metric, exclude_run_id or "", n))
    return [float(r[0]) for r in rows]


@_safe(default=list)
def task_durations(run_id):
    rows = query("SELECT task_id, value FROM obs.task_metrics WHERE run_id=%s AND metric='duration_seconds' "
                 "ORDER BY recorded_at", (run_id,))
    return [(r[0], float(r[1])) for r in rows]


# ----------------------------------------------------------------------------- data quality
@_safe()
def clear_dq(run_id, layer):
    execute("DELETE FROM obs.dq_results WHERE run_id=%s AND layer=%s", (run_id, layer))


@_safe()
def record_dq(run_id, ds, layer, results):
    rows = [(DAG_ID, run_id, ds, layer, r.get("table") or "", r["name"], r["severity"], bool(r["passed"]),
             str(r.get("observed", ""))[:500], str(r.get("threshold", ""))[:200]) for r in results]
    if rows:
        execute("INSERT INTO obs.dq_results (dag_id, run_id, ds, layer, table_name, check_name, severity, passed, "
                "observed, threshold) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)", rows, many=True)


@_safe(default=list)
def dq_failures(run_id, layer=None, severity=None):
    sql = ("SELECT layer, table_name, check_name, severity, observed, threshold FROM obs.dq_results "
           "WHERE run_id=%s AND NOT passed")
    params = [run_id]
    if layer:
        sql += " AND layer=%s"
        params.append(layer)
    if severity:
        sql += " AND severity=%s"
        params.append(severity)
    rows = query(sql + " ORDER BY id", params)
    return [dict(layer=r[0], table=r[1], name=r[2], severity=r[3], observed=r[4], threshold=r[5]) for r in rows]


@_safe(default=lambda: {"total": 0, "passed": 0, "critical": 0, "warnings": 0})
def run_dq_counts(run_id):
    r = query("SELECT count(*), count(*) FILTER (WHERE passed), "
              "count(*) FILTER (WHERE NOT passed AND severity='CRITICAL'), "
              "count(*) FILTER (WHERE NOT passed AND severity='WARNING') "
              "FROM obs.dq_results WHERE run_id=%s", (run_id,))[0]
    return {"total": r[0], "passed": r[1], "critical": r[2], "warnings": r[3]}


# ----------------------------------------------------------------------------- alerts + run summary
@_safe()
def record_alert(severity, title, body, delivered, fingerprint=None, run_id=None, task_id=None):
    execute("INSERT INTO obs.alerts (dag_id, run_id, task_id, severity, title, body, delivered, fingerprint) "
            "VALUES (%s,%s,%s,%s,%s,%s,%s,%s)",
            (DAG_ID, run_id, task_id, severity, title[:300], (body or "")[:2000], delivered, fingerprint))


@_safe(default=False)
def recent_alert_exists(fingerprint, minutes):
    r = query("SELECT 1 FROM obs.alerts WHERE fingerprint=%s AND recorded_at > now() - (%s * interval '1 minute') "
              "LIMIT 1", (fingerprint, minutes))
    return bool(r)


@_safe()
def upsert_run_summary(run_id, ds, status, started_at, duration_seconds, counts, failed_tasks):
    execute("INSERT INTO obs.run_summary (run_id, dag_id, ds, status, started_at, finished_at, duration_seconds, "
            "checks_total, checks_passed, critical_failures, warnings, failed_tasks) "
            "VALUES (%s,%s,%s,%s,%s, now(), %s,%s,%s,%s,%s,%s) "
            "ON CONFLICT (run_id) DO UPDATE SET status=EXCLUDED.status, finished_at=now(), "
            "duration_seconds=EXCLUDED.duration_seconds, checks_total=EXCLUDED.checks_total, "
            "checks_passed=EXCLUDED.checks_passed, critical_failures=EXCLUDED.critical_failures, "
            "warnings=EXCLUDED.warnings, failed_tasks=EXCLUDED.failed_tasks",
            (run_id, DAG_ID, ds, status, started_at, duration_seconds, counts["total"], counts["passed"],
             counts["critical"], counts["warnings"], ", ".join(failed_tasks)))
