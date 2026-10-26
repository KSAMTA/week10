"""SILVER: typed, cleaned, de-duplicated tables. Bad rows go to a quarantine table, not the bin.

Rules per table (see SPECS):
  * trim text, blank -> NULL; column names "transcation_*" are renamed to "transaction_*"
  * dates parsed (yyyy-MM-dd), numbers parsed to double; invalid / missing / non-positive -> quarantine (+reason)
  * categorical text standardised (Title Case, "_" -> space), missing -> 'Unknown'
  * KNOWN SOURCE QUIRK: in the Kaggle files every ID appears 3 times with DIFFERENT content.
      - master data (customers, branches, accounts, loans): keep the LAST occurrence in file order
      - transactions: every row is a distinct event -> only exact duplicate rows are removed
Reconciliation (checked in 05): bronze rows = silver rows + quarantined rows + duplicates removed.
"""
import argparse

from pyspark.sql import Window
from pyspark.sql import functions as F

import paths as P
import schemas as S
from common import get_spark, log, update_metrics

SPECS = {
    "customers": {"required": ["customer_id"], "dates": ["dob"], "numbers": [], "positive": [],
                  "title": ["first_name", "last_name", "city", "occupation"], "latest_by": ["customer_id"]},
    "branches": {"required": ["branch_id"], "dates": [], "numbers": [], "positive": [],
                 "title": ["branch_state"], "latest_by": ["branch_id"]},
    "accounts": {"required": ["account_id", "customer_id", "branch_id"], "dates": ["account_open_date"],
                 "numbers": ["opening_balance"], "positive": [], "non_negative": ["opening_balance"],
                 "title": ["account_type", "account_status"], "latest_by": ["account_id"]},
    "loans": {"required": ["loan_id", "customer_id", "branch_id"], "dates": [], "numbers": ["loan_amount"],
              "positive": ["loan_amount"], "title": [], "latest_by": ["loan_id"]},
    "transactions": {"required": ["transaction_id", "account_id"], "dates": ["transaction_date"],
                     "numbers": ["transaction_amount"], "positive": ["transaction_amount"],
                     "title": ["transaction_type", "transaction_media"], "latest_by": None},
}


def clean_table(spark, table):
    spec = SPECS[table]
    bronze_all = spark.read.format("delta").load(P.bronze(table))
    latest = bronze_all.agg(F.max("_ingest_date")).first()[0]
    df = bronze_all.filter(F.col("_ingest_date") == latest)
    df = df.toDF(*[c.replace("transcation", "transaction") for c in df.columns])
    biz = [c for c in df.columns if not c.startswith("_")]
    bronze_rows = df.count()

    for c in biz:                                   # trim, blank -> NULL
        t = F.trim(F.col(c))
        df = df.withColumn(c, F.when(t == "", F.lit(None)).otherwise(t))
    for c in spec["dates"]:
        df = df.withColumn("_p_" + c, F.to_date(F.col(c), "yyyy-MM-dd"))
    for c in spec["numbers"]:
        df = df.withColumn("_p_" + c, F.regexp_replace(F.col(c), r"[^0-9.\-]", "").cast("double"))

    conds = [(F.col(c).isNull(), f"missing_{c}") for c in spec["required"]]
    conds += [(F.col("_p_" + c).isNull(), f"invalid_or_missing_{c}") for c in spec["dates"] + spec["numbers"]]
    conds += [(F.col("_p_" + c) <= 0, f"non_positive_{c}") for c in spec["positive"]]
    conds += [(F.col("_p_" + c) < 0, f"negative_{c}") for c in spec.get("non_negative", [])]
    reason = None
    for cond, label in conds:
        reason = F.when(cond, F.lit(label)) if reason is None else reason.when(cond, F.lit(label))
    df = df.withColumn("_dq_reason", reason)

    p_cols = ["_p_" + c for c in spec["dates"] + spec["numbers"]]
    quarantine = df.filter(F.col("_dq_reason").isNotNull()).drop(*p_cols)
    good = df.filter(F.col("_dq_reason").isNull())
    for c in spec["dates"] + spec["numbers"]:
        good = good.withColumn(c, F.col("_p_" + c))
    good = good.drop(*p_cols, "_dq_reason")
    for c in spec["title"]:
        good = good.withColumn(c, F.coalesce(F.initcap(F.regexp_replace(F.col(c), "_", " ")), F.lit("Unknown")))

    good_rows = good.count()
    if spec["latest_by"]:                           # master data: last occurrence in file wins
        w = Window.partitionBy(*spec["latest_by"]).orderBy(F.col("_row_id").desc())
        deduped = good.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")
    else:                                           # transactions: only exact duplicate rows
        deduped = good.dropDuplicates(biz)
    silver = (deduped.drop("_row_id", "_ingest_date", "_ingested_at", "_source_file")
                     .withColumn("_silver_processed_at", F.current_timestamp()))

    silver.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(P.silver(table))
    quarantine.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(P.quarantine(table))

    silver_rows = spark.read.format("delta").load(P.silver(table)).count()
    quarantined = spark.read.format("delta").load(P.quarantine(table)).count()
    dups = good_rows - silver_rows
    log(f"SILVER {table}: bronze {bronze_rows:,} -> silver {silver_rows:,} | quarantined {quarantined:,} "
        f"| duplicates removed {dups:,}")
    return {"bronze_rows": bronze_rows, "rows": silver_rows, "quarantined_rows": quarantined,
            "duplicates_removed": dups}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--ds", default=None)   # accepted for symmetry; Silver always reads the latest Bronze batch
    ap.parse_args()
    spark = get_spark("silver_clean")
    result = {t: clean_table(spark, t) for t in S.SOURCES}
    update_metrics("silver", result)
    spark.stop()


if __name__ == "__main__":
    main()
