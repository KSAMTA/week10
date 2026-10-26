"""Break the raw data on purpose to demonstrate the quality gates and alerts. Original files are backed up
in data/raw_backup/ the first time. Run on your machine (not in a container), from the project folder:

    python scripts/inject_bad_data.py list
    python scripts/inject_bad_data.py <scenario>      then trigger the DAG in Airflow
    python scripts/inject_bad_data.py restore         put the original files back

Scenario                What happens                                              Expected result
----------------------  --------------------------------------------------------  --------------------------------
blank_ids               10% of customer IDs blanked                               CRITICAL  dq_silver (quarantine > 5%)
orphans                 3% of transactions point to a non-existent account        CRITICAL  dq_silver (referential integrity)
future_dates            1% of transaction dates set to 2099                       CRITICAL  dq_silver (date range)
volume_drop             transactions cut to 35% of the rows                       CRITICAL  dq_bronze (needs >= 3 earlier runs)
schema_change           a column header is renamed                                FAIL      bronze_ingest (schema contract)
bad_category            some accounts get status "Frozen"                         WARNING   dq_silver (unknown category)
extreme_amounts         0.5% of transaction amounts multiplied x1000              WARNING   dq_silver (range) + drift
"""
import csv
import os
import random
import shutil
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RAW, BAK = os.path.join(ROOT, "data", "raw"), os.path.join(ROOT, "data", "raw_backup")
F = {"customers": "Bank_Customer_Data.csv", "accounts": "Bank_Account_Data.csv", "transactions": "Bank_Transacation_Data.csv"}
random.seed(7)


def load(name):
    with open(os.path.join(RAW, F[name]), encoding="utf-8-sig", newline="") as f:
        r = csv.reader(f)
        return next(r), list(r)


def save(name, header, rows):
    with open(os.path.join(RAW, F[name]), "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(header)
        w.writerows(rows)


def pick(rows, share):
    return random.sample(range(len(rows)), max(1, int(len(rows) * share)))


def blank_ids():
    h, rows = load("customers")
    for i in pick(rows, 0.10):
        rows[i][0] = ""
    save("customers", h, rows)


def orphans():
    h, rows = load("transactions")
    for i in pick(rows, 0.03):
        rows[i][1] = "A99999"
    save("transactions", h, rows)


def future_dates():
    h, rows = load("transactions")
    for i in pick(rows, 0.01):
        rows[i][2] = "2099-01-01"
    save("transactions", h, rows)


def volume_drop():
    h, rows = load("transactions")
    save("transactions", h, rows[: int(len(rows) * 0.35)])


def schema_change():
    h, rows = load("accounts")
    h[3] = "OPENING_BAL"
    save("accounts", h, rows)


def bad_category():
    h, rows = load("accounts")
    for i in pick(rows, 0.02):
        rows[i][6] = "Frozen"
    save("accounts", h, rows)


def extreme_amounts():
    h, rows = load("transactions")
    for i in pick(rows, 0.005):
        rows[i][5] = str(int(float(rows[i][5]) * 1000))
    save("transactions", h, rows)


SCENARIOS = {k: v for k, v in globals().items() if k in
             ("blank_ids", "orphans", "future_dates", "volume_drop", "schema_change", "bad_category", "extreme_amounts")}


def backup():
    os.makedirs(BAK, exist_ok=True)
    for fn in F.values():
        if not os.path.exists(os.path.join(BAK, fn)):
            shutil.copy2(os.path.join(RAW, fn), os.path.join(BAK, fn))


def restore():
    for fn in F.values():
        shutil.copy2(os.path.join(BAK, fn), os.path.join(RAW, fn))
    print("original files restored")


if __name__ == "__main__":
    cmd = sys.argv[1] if len(sys.argv) > 1 else "list"
    if cmd == "restore":
        restore()
    elif cmd in SCENARIOS:
        restore() if os.path.isdir(BAK) else None
        backup()
        SCENARIOS[cmd]()
        print(f"scenario '{cmd}' applied. Trigger the DAG, then run:  python scripts/inject_bad_data.py restore")
    else:
        print(__doc__)
        print("scenarios:", ", ".join(SCENARIOS))
