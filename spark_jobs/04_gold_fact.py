"""GOLD (2/2): fact tables.

fact_transactions  grain: one transaction | FKs: date, account, customer, branch, type, media
fact_loans         grain: one loan        | FKs: customer, branch
customer_key / branch_key of a transaction are derived through its account (silver.accounts).
"""
from pyspark.sql import functions as F

import paths as P
import schemas as S
from common import get_spark, log, update_metrics


def save(spark, df, name):
    """Write a Gold table and return its row count."""
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(P.gold(name))
    return spark.read.format("delta").load(P.gold(name)).count()


def main():
    spark = get_spark("gold_fact")
    rd = lambda t: spark.read.format("delta").load(P.silver(t))  # noqa: E731

    txn = rd("transactions")
    biz = [c for c in txn.columns if not c.startswith("_")]
    acc = rd("accounts").select("account_id", "customer_id", "branch_id")
    j = txn.join(F.broadcast(acc), on="account_id", how="left")
    fact_txn = j.select(
        F.xxhash64(*biz).alias("transaction_key"),
        F.col("transaction_id"),                                              # degenerate dimension
        F.date_format("transaction_date", "yyyyMMdd").cast("int").alias("date_key"),
        F.xxhash64("account_id").alias("account_key"),
        F.when(F.col("customer_id").isNotNull(), F.xxhash64("customer_id")).alias("customer_key"),
        F.when(F.col("branch_id").isNotNull(), F.xxhash64("branch_id")).alias("branch_key"),
        F.xxhash64("transaction_type").alias("transaction_type_key"),
        F.xxhash64("transaction_media").alias("transaction_media_key"),
        F.col("transaction_amount"),
        F.col("_batch_id"))
    n_txn = save(spark, fact_txn, "fact_transactions")

    loans = rd("loans")
    fact_loans = loans.select(
        F.xxhash64("loan_id").alias("loan_key"), "loan_id",
        F.xxhash64("customer_id").alias("customer_key"),
        F.xxhash64("branch_id").alias("branch_key"),
        "loan_amount", "_batch_id")
    n_loan = save(spark, fact_loans, "fact_loans")

    log(f"fact_transactions: {n_txn:,} rows | fact_loans: {n_loan:,} rows")
    update_metrics("gold_fact", {"fact_transactions": n_txn, "fact_loans": n_loan})
    spark.stop()


if __name__ == "__main__":
    main()
