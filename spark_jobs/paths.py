"""Single place for every path used by the pipeline (pure python, no Spark import)."""
import os

DATA_DIR = os.environ.get("DATA_DIR", "/opt/airflow/data")
RAW_DIR = os.path.join(DATA_DIR, "raw")
LAKE_DIR = os.path.join(DATA_DIR, "lakehouse")
REPORTS_DIR = os.path.join(DATA_DIR, "reports")
METRICS_PATH = os.path.join(REPORTS_DIR, "stage_metrics.json")


def bronze(table):
    return os.path.join(LAKE_DIR, "bronze", table)


def silver(table):
    return os.path.join(LAKE_DIR, "silver", table)


def quarantine(table):
    return os.path.join(LAKE_DIR, "quarantine", table)


def gold(table):
    return os.path.join(LAKE_DIR, "gold", table)
