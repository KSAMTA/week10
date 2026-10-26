"""QUALITY GATE 1 - Bronze. Runs right after ingestion, before Silver is touched.

CRITICAL: each table has rows | bronze row count == source file row count (completeness)
WARNING : volume vs recent runs | blank primary keys | source file freshness (if FRESHNESS_MAX_HOURS > 0)
"""
import argparse
import csv
import os
import time
from datetime import datetime, timezone

from pyspark.sql import functions as F

import dq_rules as R
import paths as P
import schemas as S
from common import get_spark, log
from dq_framework import DQRunner


def source_rows(path):
    with open(path, encoding="utf-8-sig", newline="") as f:
        return max(sum(1 for row in csv.reader(f) if row) - 1, 0)      # minus header


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--ds", default=datetime.now(timezone.utc).strftime("%Y-%m-%d"))
    ap.add_argument("--run-id", default=None)
    args = ap.parse_args()

    spark = get_spark("dq_bronze")
    r = DQRunner("bronze", args.ds, args.run_id)

    for t, spec in S.SOURCES.items():
        df = spark.read.format("delta").load(P.bronze(t)).filter(F.col("_ingest_date") == args.ds)
        n = df.count()
        r.check(f"{t}: bronze batch has rows", n > 0, f"{n:,}", "> 0", "CRITICAL", t)

        src = S.find_file(P.RAW_DIR, t)
        if src:
            expected = source_rows(src)
            r.check(f"{t}: bronze rows = source file rows (completeness)", n == expected,
                    f"{n:,} vs {expected:,}", "equal", "CRITICAL", t)
            if R.FRESHNESS_MAX_HOURS > 0:
                age_h = (time.time() - os.path.getmtime(src)) / 3600
                r.check(f"{t}: source file fresher than {R.FRESHNESS_MAX_HOURS:g}h", age_h <= R.FRESHNESS_MAX_HOURS,
                        f"{age_h:.1f}h old", f"<= {R.FRESHNESS_MAX_HOURS:g}h", "WARNING", t)

        pk = spec["pk"]
        blank = df.filter(F.col(pk).isNull() | (F.trim(F.col(pk)) == "")).count()
        blank_pct = R.pct(blank, n)
        r.check(f"{t}: blank {pk} <= {R.MAX_NULL_PCT:g}%", blank_pct <= R.MAX_NULL_PCT,
                f"{blank:,} ({blank_pct:.2f}%)", f"<= {R.MAX_NULL_PCT:g}%", "WARNING", t)

        r.volume_check(t, n, history_task="bronze_ingest", label="bronze rows")
        r.metric(t, "blank_pk_pct", blank_pct)

    spark.stop()
    r.finish()


if __name__ == "__main__":
    main()
