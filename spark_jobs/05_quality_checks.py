"""QUALITY GATE 3 - Gold. Reconciliation, keys, referential integrity, sums + run report.

Every check is also stored in Postgres (obs.dq_results) via DQRunner. A failed check exits non-zero
(-> Airflow task fails -> CRITICAL alert).
"""
import argparse
import os
import sys
from datetime import datetime, timezone

from pyspark.sql import functions as F

import paths as P
import schemas as S
from common import get_spark, log, read_json, update_metrics, write_json
from dq_framework import DQRunner

MAX_QUARANTINE_PCT = float(os.environ.get("MAX_QUARANTINE_PCT", "5.0"))
FACT_SOURCE = {"fact_transactions": "transactions", "fact_loans": "loans"}
DIM_SOURCE = {"dim_customer": ("customers", "customer_id"), "dim_branch": ("branches", "branch_id"),
              "dim_account": ("accounts", "account_id"),
              "dim_transaction_type": ("transactions", "transaction_type"),
              "dim_transaction_media": ("transactions", "transaction_media")}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--ds", default=datetime.now(timezone.utc).strftime("%Y-%m-%d"))
    ap.add_argument("--run-id", default=None)
    args = ap.parse_args()

    spark = get_spark("dq_gold")
    runner = DQRunner("gold", args.ds, args.run_id)
    silver_m = (read_json(P.METRICS_PATH, {}) or {}).get("silver", {})
    rd = lambda p: spark.read.format("delta").load(p)  # noqa: E731
    checks = []

    def check(name, passed, detail):
        checks.append({"name": name, "passed": bool(passed), "detail": detail})
        runner.check(name, passed, observed=detail, severity="CRITICAL", table=name.split(":")[0].split(" ")[0])

    tables, silver = {}, {}
    for t in S.SOURCES:
        b_all = rd(P.bronze(t))
        latest = b_all.agg(F.max("_ingest_date")).first()[0]
        b = b_all.filter(F.col("_ingest_date") == latest).count()
        silver[t] = rd(P.silver(t))
        s, q = silver[t].count(), rd(P.quarantine(t)).count()
        d = int(silver_m.get(t, {}).get("duplicates_removed", 0))
        tables[t] = {"bronze_rows": b, "silver_rows": s, "quarantined_rows": q, "duplicates_removed": d}
        check(f"{t}: bronze and silver have rows", b > 0 and s > 0, f"bronze {b:,}, silver {s:,}")
        check(f"{t}: bronze = silver + quarantine + duplicates", b == s + q + d, f"{b:,} = {s:,} + {q:,} + {d:,}")
        pct = 100.0 * q / b if b else 0.0
        check(f"{t}: quarantine share <= {MAX_QUARANTINE_PCT}%", pct <= MAX_QUARANTINE_PCT, f"{pct:.2f}%")

    gold_tables = {}
    for dim, key in S.DIM_KEYS.items():
        d = rd(P.gold(dim))
        n = d.count()
        gold_tables[dim] = n
        check(f"{dim}: {key} is unique", d.select(key).distinct().count() == n, f"{n:,} rows")
        if dim in DIM_SOURCE:
            st, nat = DIM_SOURCE[dim]
            exp = silver[st].select(nat).distinct().count()
            check(f"{dim}: one row per distinct {nat} in silver", n == exp, f"{n:,} vs {exp:,}")

    for fname, spec in S.FACTS.items():
        f = rd(P.gold(fname))
        n = f.count()
        gold_tables[fname] = n
        src = silver[FACT_SOURCE[fname]]
        check(f"{fname}: rows = silver {FACT_SOURCE[fname]}", n == src.count(), f"{n:,} vs {src.count():,}")
        check(f"{fname}: {spec['pk']} is unique", f.select(spec["pk"]).distinct().count() == n, f"{n:,} rows")
        nulls = f.select([F.sum(F.col(c).isNull().cast("int")).alias(c) for c in spec["fks"]]).first().asDict()
        check(f"{fname}: no NULL foreign keys", all((v or 0) == 0 for v in nulls.values()),
              ", ".join(f"{k}={v or 0}" for k, v in nulls.items()))
        for fk, dim in spec["fks"].items():
            orphans = f.join(rd(P.gold(dim)).select(fk), on=fk, how="left_anti").count()
            check(f"{fname} -> {dim}: referential integrity", orphans == 0, f"{orphans:,} orphan rows")
        m = spec["measure"]
        s_tot = src.agg(F.sum(m)).first()[0] or 0.0
        f_tot = f.agg(F.sum(m)).first()[0] or 0.0
        check(f"{fname}: sum({m}) silver = fact", abs(s_tot - f_tot) < 0.01, f"{s_tot:,.2f} vs {f_tot:,.2f}")
        tables[FACT_SOURCE[fname]]["total_" + m] = round(f_tot, 2)

    runner.volume_check("fact_transactions", gold_tables["fact_transactions"], history_task="gold_facts",
                        label="gold rows")
    runner.volume_check("fact_loans", gold_tables["fact_loans"], history_task="gold_facts", label="gold rows")

    rng = silver["transactions"].agg(F.min("transaction_date").alias("mn"),
                                     F.max("transaction_date").alias("mx")).first()
    status = "PASSED" if all(c["passed"] for c in checks) else "FAILED"
    report = {"ds": args.ds, "status": status,
              "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
              "transaction_date_range": f"{rng['mn']} to {rng['mx']}",
              "tables": tables, "gold_tables": gold_tables, "checks": checks}
    write_json(os.path.join(P.REPORTS_DIR, "run_report.json"), report)
    write_json(os.path.join(P.REPORTS_DIR, f"run_report_{args.ds}.json"), report)
    update_metrics("quality", {"status": status})
    spark.stop()
    runner.finish()                       # stores results in Postgres; exits non-zero on CRITICAL failures
    if status != "PASSED":
        sys.exit("Data-quality checks failed - see data/reports/run_report.json")


if __name__ == "__main__":
    main()
