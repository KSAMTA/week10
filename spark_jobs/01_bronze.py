"""BRONZE: land each of the 5 CSV files as its own Delta table, untouched (all columns strings).

Added only: Delta-safe snake_case column names and lineage columns
(_row_id = original file order, _ingest_date, _ingested_at, _source_file, _batch_id).
Re-running the same date replaces just that date's partition (idempotent); other days stay as history.
"""
import argparse
import os
import sys
from datetime import datetime, timezone

from pyspark.sql import functions as F

import paths as P
import schemas as S
from common import get_spark, log, update_metrics


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--ds", default=datetime.now(timezone.utc).strftime("%Y-%m-%d"))
    ap.add_argument("--batch-id", default=None)
    args = ap.parse_args()
    batch_id = args.batch_id or f"manual__{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S')}"

    spark = get_spark("bronze_ingest")
    summary = {}
    for table, spec in S.SOURCES.items():
        src = S.find_file(P.RAW_DIR, table)
        if not src:
            sys.exit(f"{table}: file {spec['file']} not found in {P.RAW_DIR}")
        raw = (spark.read.option("header", "true").option("inferSchema", "false")
               .option("escape", '"').option("mode", "PERMISSIVE").csv(src))
        cols = S.unique_names([S.snake(c) for c in raw.columns])
        if sorted(cols) != sorted(spec["columns"]):
            sys.exit(f"{table}: expected columns {spec['columns']} but {os.path.basename(src)} has {cols}")

        df = (raw.toDF(*cols)
              .withColumn("_row_id", F.monotonically_increasing_id())
              .withColumn("_ingest_date", F.lit(args.ds))
              .withColumn("_ingested_at", F.current_timestamp())
              .withColumn("_source_file", F.lit(os.path.basename(src)))
              .withColumn("_batch_id", F.lit(batch_id)))
        (df.write.format("delta").mode("overwrite")
           .option("replaceWhere", f"_ingest_date = '{args.ds}'")
           .partitionBy("_ingest_date").save(P.bronze(table)))

        rows = (spark.read.format("delta").load(P.bronze(table))
                .filter(F.col("_ingest_date") == args.ds).count())
        log(f"BRONZE {table}: {rows:,} rows from {os.path.basename(src)}")
        summary[table] = {"rows": rows, "source_file": os.path.basename(src)}

    update_metrics("bronze", {"ingest_date": args.ds, "tables": summary})
    spark.stop()


if __name__ == "__main__":
    main()
