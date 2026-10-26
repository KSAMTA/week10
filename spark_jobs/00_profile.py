"""Quick look at the 5 CSVs (no Spark, ~1 second): rows, repeated IDs, cross-file key matches.

    docker compose exec scheduler python /opt/airflow/spark_jobs/00_profile.py
"""
import csv
import sys

import paths as P
import schemas as S


def load(table):
    path = S.find_file(P.RAW_DIR, table)
    if not path:
        return None, None, []
    with open(path, newline="", encoding="utf-8-sig") as f:
        r = csv.reader(f)
        header = S.unique_names([S.snake(h) for h in next(r)])
        rows = [dict(zip(header, row)) for row in r]
    return path, header, rows


def main():
    data, problems = {}, 0
    for table, spec in S.SOURCES.items():
        path, header, rows = load(table)
        if path is None:
            print(f"[MISSING] {table}: {spec['file']} not found in {P.RAW_DIR}")
            problems += 1
            continue
        missing = [c for c in spec["columns"] if c not in header]
        ids = [r[spec["pk"]] for r in rows if spec["pk"] in r]
        data[table] = rows
        print(f"{table:<13} rows={len(rows):>6,}  unique {spec['pk']}={len(set(ids)):>6,}  "
              f"each id repeats ~{len(rows) / max(len(set(ids)), 1):.1f}x  columns_ok={not missing}")
        problems += bool(missing)
    if problems:
        sys.exit(f"{problems} problem(s) found - fix the files above first.")

    def ids(t, c):
        return {r[c] for r in data[t]}

    print("\nCross-file key matches (orphans should be 0):")
    pairs = [("transactions", "account_id", "accounts"), ("accounts", "customer_id", "customers"),
             ("accounts", "branch_id", "branches"), ("loans", "customer_id", "customers"),
             ("loans", "branch_id", "branches")]
    for child, col, parent in pairs:
        orphans = len(ids(child, col) - ids(parent, col))
        print(f"  {child}.{col} -> {parent}: {orphans} orphan id(s)")

    print("\nNOTE: every id repeats with DIFFERENT content (e.g. one customer id = 3 different people).")
    print("Silver keeps the last occurrence for master data and keeps all transaction rows.")


if __name__ == "__main__":
    main()
