"""GOLD (1/2): dimension tables of the star schema (keys come from schemas.DIM_KEYS).

Surrogate keys are deterministic xxhash64 of the natural value, so reruns give identical keys.
dim_date is a full calendar from Jan 1 of the first transaction year to Dec 31 of the last.
"""
import datetime
import sys

from pyspark.sql import functions as F

import paths as P
import schemas as S
from common import get_spark, log, update_metrics


def save(df, name):
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(P.gold(name))


def main():
    spark = get_spark("gold_dimensions")
    rd = lambda t: spark.read.format("delta").load(P.silver(t))  # noqa: E731
    K = S.DIM_KEYS

    txn = rd("transactions")
    b = txn.agg(F.min("transaction_date").alias("mn"), F.max("transaction_date").alias("mx")).first()
    if b["mn"] is None:
        sys.exit("silver.transactions is empty - nothing to build dim_date from")
    start, end = datetime.date(b["mn"].year, 1, 1), datetime.date(b["mx"].year, 12, 31)
    dim_date = (
        spark.range(1)
        .select(F.explode(F.sequence(F.lit(start), F.lit(end), F.expr("interval 1 day"))).alias("full_date"))
        .select(
            F.date_format("full_date", "yyyyMMdd").cast("int").alias(K["dim_date"]),
            "full_date",
            F.year("full_date").alias("year"),
            F.quarter("full_date").alias("quarter"),
            F.month("full_date").alias("month"),
            F.date_format("full_date", "MMMM").alias("month_name"),
            F.date_format("full_date", "yyyy-MM").alias("year_month"),
            F.dayofmonth("full_date").alias("day_of_month"),
            F.dayofweek("full_date").alias("day_of_week"),
            F.date_format("full_date", "EEEE").alias("day_name"),
            F.weekofyear("full_date").alias("week_of_year"),
            F.dayofweek("full_date").isin(1, 7).alias("is_weekend"),
        ))
    save(dim_date, "dim_date")

    cust = rd("customers")
    save(cust.select(
        F.xxhash64("customer_id").alias(K["dim_customer"]), "customer_id",
        F.concat_ws(" ", "first_name", "last_name").alias("full_name"),
        "first_name", "last_name", "city", "phone_number", "occupation", "dob",
        F.year("dob").alias("birth_year")), "dim_customer")

    save(rd("branches").select(F.xxhash64("branch_id").alias(K["dim_branch"]),
                               "branch_id", "branch_name", "branch_state"), "dim_branch")

    save(rd("accounts").select(F.xxhash64("account_id").alias(K["dim_account"]), "account_id",
                               "account_type", "account_status", "opening_balance", "account_open_date"),
         "dim_account")

    save(txn.select("transaction_type").distinct()
         .select(F.xxhash64("transaction_type").alias(K["dim_transaction_type"]), "transaction_type"),
         "dim_transaction_type")
    save(txn.select("transaction_media").distinct()
         .select(F.xxhash64("transaction_media").alias(K["dim_transaction_media"]), "transaction_media"),
         "dim_transaction_media")

    counts = {t: spark.read.format("delta").load(P.gold(t)).count() for t in K}
    for t, n in counts.items():
        log(f"{t}: {n:,} rows")
    update_metrics("gold_dimensions", counts)
    spark.stop()


if __name__ == "__main__":
    main()
