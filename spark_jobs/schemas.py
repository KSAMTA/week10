"""Source-file contracts and star-schema definition (pure python, no Spark import)."""
import glob
import os
import re


def snake(name):
    """'First_Name' -> 'first_name', 'TRANSCATION_ID' -> 'transcation_id' (also strips a BOM)."""
    s = re.sub(r"[^0-9a-zA-Z]+", "_", str(name).strip())
    s = re.sub(r"(?<=[a-z0-9])(?=[A-Z])", "_", s)
    s = re.sub(r"(?<=[A-Z])(?=[A-Z][a-z])", "_", s)
    s = re.sub(r"_+", "_", s).strip("_").lower()
    return s or "col"


def unique_names(names):
    seen, out = {}, []
    for n in names:
        if n in seen:
            seen[n] += 1
            out.append(f"{n}_{seen[n]}")
        else:
            seen[n] = 1
            out.append(n)
    return out


# table -> expected file + columns (snake_case, exactly as spelled in the Kaggle files)
SOURCES = {
    "customers": {
        "file": "Bank_Customer_Data.csv", "keyword": "customer", "pk": "customer_id",
        "columns": ["customer_id", "first_name", "last_name", "city", "phone_number", "occupation", "dob"],
    },
    "branches": {
        "file": "Bank_Branch_Data.csv", "keyword": "branch", "pk": "branch_id",
        "columns": ["branch_id", "branch_name", "branch_state"],
    },
    "accounts": {
        "file": "Bank_Account_Data.csv", "keyword": "account", "pk": "account_id",
        "columns": ["account_id", "customer_id", "branch_id", "opening_balance", "account_open_date",
                    "account_type", "account_status"],
    },
    "loans": {
        "file": "Bank_Loan_Data.csv", "keyword": "loan", "pk": "loan_id",
        "columns": ["loan_id", "customer_id", "branch_id", "loan_amount"],
    },
    "transactions": {
        # NOTE: the source files really are spelled "TRANSCATION"; Silver renames to "transaction".
        "file": "Bank_Transacation_Data.csv", "keyword": "transac", "pk": "transcation_id",
        "columns": ["transcation_id", "account_id", "transcation_date", "transcation_media",
                    "transcation_type", "transcation_amount"],
    },
}


def find_file(raw_dir, table):
    spec = SOURCES[table]
    files = glob.glob(os.path.join(raw_dir, "*.csv"))
    for f in files:
        if os.path.basename(f).lower() == spec["file"].lower():
            return f
    cand = sorted(f for f in files if spec["keyword"] in os.path.basename(f).lower())
    return cand[0] if cand else None


# Gold: dimension table -> surrogate key column
DIM_KEYS = {
    "dim_date": "date_key",
    "dim_customer": "customer_key",
    "dim_branch": "branch_key",
    "dim_account": "account_key",
    "dim_transaction_type": "transaction_type_key",
    "dim_transaction_media": "transaction_media_key",
}

# Gold: fact table -> primary key and foreign keys (FK column name == key column of the dimension)
FACTS = {
    "fact_transactions": {
        "pk": "transaction_key",
        "fks": {"date_key": "dim_date", "account_key": "dim_account", "customer_key": "dim_customer",
                "branch_key": "dim_branch", "transaction_type_key": "dim_transaction_type",
                "transaction_media_key": "dim_transaction_media"},
        "measure": "transaction_amount",
    },
    "fact_loans": {
        "pk": "loan_key",
        "fks": {"customer_key": "dim_customer", "branch_key": "dim_branch"},
        "measure": "loan_amount",
    },
}
