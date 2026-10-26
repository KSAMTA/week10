"""Demo: query the star schema with SQL + Delta Lake history and time travel.

    docker compose exec scheduler python /opt/airflow/spark_jobs/99_demo_queries.py
"""
import paths as P
import schemas as S
from common import get_spark


def main():
    spark = get_spark("demo_queries")
    for t in list(S.DIM_KEYS) + list(S.FACTS):
        spark.read.format("delta").load(P.gold(t)).createOrReplaceTempView(t)

    def show(title, sql, n=20):
        print(f"\n=== {title} ===")
        spark.sql(sql).show(n, truncate=False)

    show("Monthly transaction volume (fact JOIN dim_date)", """
        SELECT d.year_month, COUNT(*) AS txns, ROUND(SUM(f.transaction_amount), 0) AS total_amount
        FROM fact_transactions f JOIN dim_date d ON f.date_key = d.date_key
        GROUP BY d.year_month ORDER BY d.year_month DESC""", 12)
    show("Amount by transaction type and media", """
        SELECT t.transaction_type, m.transaction_media, COUNT(*) AS txns,
               ROUND(SUM(f.transaction_amount), 0) AS total_amount
        FROM fact_transactions f
        JOIN dim_transaction_type t ON f.transaction_type_key = t.transaction_type_key
        JOIN dim_transaction_media m ON f.transaction_media_key = m.transaction_media_key
        GROUP BY t.transaction_type, m.transaction_media ORDER BY total_amount DESC""")
    show("Top 10 branches by transaction amount", """
        SELECT b.branch_id, b.branch_name, b.branch_state, ROUND(SUM(f.transaction_amount), 0) AS total_amount
        FROM fact_transactions f JOIN dim_branch b ON f.branch_key = b.branch_key
        GROUP BY b.branch_id, b.branch_name, b.branch_state ORDER BY total_amount DESC LIMIT 10""")
    show("Loan amount by state", """
        SELECT b.branch_state, COUNT(*) AS loans, ROUND(SUM(l.loan_amount), 0) AS total_loans
        FROM fact_loans l JOIN dim_branch b ON l.branch_key = b.branch_key
        GROUP BY b.branch_state ORDER BY total_loans DESC""", 10)
    show("Top 10 customers by transaction amount", """
        SELECT c.customer_id, c.full_name, ROUND(SUM(f.transaction_amount), 0) AS total_amount
        FROM fact_transactions f JOIN dim_customer c ON f.customer_key = c.customer_key
        GROUP BY c.customer_id, c.full_name ORDER BY total_amount DESC LIMIT 10""")

    print("\n=== Delta Lake history: silver.transactions ===")
    spark.sql(f"DESCRIBE HISTORY delta.`{P.silver('transactions')}`") \
        .select("version", "timestamp", "operation").show(truncate=False)
    print("=== Time travel: silver.transactions at version 0 ===")
    print("rows at version 0:", spark.read.format("delta").option("versionAsOf", 0)
          .load(P.silver("transactions")).count())
    spark.stop()


if __name__ == "__main__":
    main()
