"""QUALITY GATE 2 - Silver. Runs before any Gold table is (re)built, so bad data never reaches the star schema.

CRITICAL: rows exist | business key not null + unique (master data) | bronze = silver + quarantine + duplicates
          quarantine share <= MAX_QUARANTINE_PCT | referential integrity between tables | dates not in the future
WARNING : null rate per column | numeric ranges | allowed categories | ID/phone formats | volume vs history
          mean-amount drift (z-score vs history)
Thresholds come from environment variables (see dq_rules.py / .env.example).
"""
import argparse
from datetime import date, datetime, timezone

from pyspark.sql import functions as F

import dq_rules as R
import paths as P
import schemas as S
from common import get_spark, read_json
from dq_framework import DQRunner

TODAY = date.today()
# table -> [(column, min, max, severity)]
RANGES = {
    "customers": [("dob", date(1900, 1, 1), TODAY, "CRITICAL")],
    "accounts": [("opening_balance", 0, 10_000_000, "WARNING"),
                 ("account_open_date", date(1990, 1, 1), TODAY, "CRITICAL")],
    "loans": [("loan_amount", 1, 50_000_000, "WARNING")],
    "transactions": [("transaction_amount", 0.01, 1_000_000, "WARNING"),
                     ("transaction_date", date(1990, 1, 1), TODAY, "CRITICAL")],
}
# table -> {column: allowed values}  (new category appearing upstream -> WARNING)
DOMAINS = {
    "accounts": {"account_type": ["Checking", "Savings", "Fixed Deposit"],
                 "account_status": ["Active", "Closed", "Pending"]},
    "transactions": {"transaction_type": ["Deposit", "Transfer", "Purchase", "Withdrawal"],
                     "transaction_media": ["Cash", "Credit Card", "Check", "Debit Card"]},
}
# table -> {column: regex}
FORMATS = {
    "customers": {"customer_id": r"^C\d{5}$", "phone_number": r"^\d{10}$"},
    "branches": {"branch_id": r"^B\d{5}$"},
    "accounts": {"account_id": r"^A\d{5}$"},
    "loans": {"loan_id": r"^L\d{5}$"},
    "transactions": {"transaction_id": r"^T\d{5}$"},
}
# (child table, child column, parent table, parent column)
REFERENCES = [("accounts", "customer_id", "customers", "customer_id"),
              ("accounts", "branch_id", "branches", "branch_id"),
              ("loans", "customer_id", "customers", "customer_id"),
              ("loans", "branch_id", "branches", "branch_id"),
              ("transactions", "account_id", "accounts", "account_id")]
DRIFT = {"transactions": "transaction_amount", "loans": "loan_amount", "accounts": "opening_balance"}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--ds", default=datetime.now(timezone.utc).strftime("%Y-%m-%d"))
    ap.add_argument("--run-id", default=None)
    args = ap.parse_args()

    spark = get_spark("dq_silver")
    r = DQRunner("silver", args.ds, args.run_id)
    rd = lambda p: spark.read.format("delta").load(p)  # noqa: E731
    silver_m = (read_json(P.METRICS_PATH, {}) or {}).get("silver", {})
    silver = {t: rd(P.silver(t)) for t in S.SOURCES}

    for t, spec in S.SOURCES.items():
        df, q = silver[t], rd(P.quarantine(t))
        n, nq = df.count(), q.count()
        m = silver_m.get(t, {})
        b, d = int(m.get("bronze_rows", 0)), int(m.get("duplicates_removed", 0))
        pk = spec["pk"].replace("transcation", "transaction")

        r.check(f"{t}: silver has rows", n > 0, f"{n:,}", "> 0", "CRITICAL", t)
        nulls = df.filter(F.col(pk).isNull()).count()
        r.check(f"{t}: {pk} not null", nulls == 0, f"{nulls:,} nulls", "0", "CRITICAL", t)
        if t != "transactions":     # transaction ids repeat in the source on purpose (distinct events)
            dup = n - df.select(pk).distinct().count()
            r.check(f"{t}: {pk} unique", dup == 0, f"{dup:,} duplicate keys", "0", "CRITICAL", t)
        r.check(f"{t}: bronze = silver + quarantine + duplicates", b == n + nq + d,
                f"{b:,} = {n:,} + {nq:,} + {d:,}", "equal", "CRITICAL", t)

        qpct = R.pct(nq, b)
        top = ", ".join(f"{x['_dq_reason']}={x['count']}" for x in
                        q.groupBy("_dq_reason").count().orderBy(F.desc("count")).limit(3).collect())
        r.check(f"{t}: quarantine share <= {R.MAX_QUARANTINE_PCT:g}%", qpct <= R.MAX_QUARANTINE_PCT,
                f"{qpct:.2f}%" + (f" (top reasons: {top})" if top else ""), f"<= {R.MAX_QUARANTINE_PCT:g}%",
                "CRITICAL", t)
        r.metric(t, "quarantine_pct", qpct)

        cols = [c for c in df.columns if not c.startswith("_")]
        nc = df.select([F.sum(F.col(c).isNull().cast("int")).alias(c) for c in cols]).first().asDict()
        for c in cols:
            p = R.pct(nc[c] or 0, n)
            r.check(f"{t}.{c}: null rate <= {R.MAX_NULL_PCT:g}%", p <= R.MAX_NULL_PCT, f"{p:.2f}%",
                    f"<= {R.MAX_NULL_PCT:g}%", "WARNING", t)

        for col, lo, hi, sev in RANGES.get(t, []):
            bad = df.filter((F.col(col) < F.lit(lo)) | (F.col(col) > F.lit(hi))).count()
            r.check(f"{t}.{col}: within [{lo}, {hi}]", bad == 0, f"{bad:,} out of range", "0", sev, t)
        for col, allowed in DOMAINS.get(t, {}).items():
            bad = [x[0] for x in df.filter(~F.col(col).isin(*allowed)).select(col).distinct().limit(5).collect()]
            r.check(f"{t}.{col}: only known categories", not bad,
                    f"unexpected: {bad}" if bad else "all known", ", ".join(allowed), "WARNING", t)
        for col, rx in FORMATS.get(t, {}).items():
            bad = df.filter(~F.col(col).rlike(rx)).count()
            r.check(f"{t}.{col}: matches format {rx}", bad == 0, f"{bad:,} malformed", "0", "WARNING", t)

        r.volume_check(t, n, history_task="silver_clean", label="silver rows")
        if t in DRIFT:
            avg = df.agg(F.avg(DRIFT[t])).first()[0]
            if avg is not None:
                r.drift_check(t, f"avg_{DRIFT[t]}", float(avg), history_task="dq_silver_stats")

    for child, ccol, parent, pcol in REFERENCES:
        c = silver[child]
        total = c.count()
        parent_keys = silver[parent].select(F.col(pcol).alias("_pk")).distinct()
        orphans = c.join(parent_keys, c[ccol] == F.col("_pk"), "left_anti").count()
        p = R.pct(orphans, total)
        r.check(f"{child}.{ccol} -> {parent}.{pcol}: referential integrity", p <= R.ORPHAN_MAX_PCT,
                f"{orphans:,} orphan rows ({p:.2f}%)", f"<= {R.ORPHAN_MAX_PCT:g}%", "CRITICAL", child)

    spark.stop()
    r.finish()


if __name__ == "__main__":
    main()
