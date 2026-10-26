"""Download the 5 source CSV files straight from Kaggle into data/raw (first task of the DAG).

Dataset: https://www.kaggle.com/datasets/salikhussaini49/bank-dataset  (env KAGGLE_DATASET)

Safety rules - a half-downloaded or wrong file must never reach the pipeline:
  1. download + unzip into a temporary folder first
  2. validate every file (all 5 present, non-empty, header columns match the contract in schemas.py)
  3. only then copy into data/raw (atomic replace per file)
If the download fails but all 5 files already exist locally, the run continues on them and a WARNING alert is
sent (ALLOW_LOCAL_FALLBACK=true, the default). DATA_SOURCE=local skips downloading completely.

Credentials (never commit them): KAGGLE_USERNAME + KAGGLE_KEY in .env, or KAGGLE_API_TOKEN for the newer token style.
"""
import csv
import logging
import os
import shutil
import tempfile
import time

import obs
import schemas as S

log = logging.getLogger("fetch_data")


def _raw_dir():
    return os.path.join(os.environ.get("DATA_DIR", "/opt/airflow/data"), "raw")


def have_credentials():
    env = os.environ
    return bool((env.get("KAGGLE_USERNAME") and env.get("KAGGLE_KEY")) or env.get("KAGGLE_API_TOKEN")
                or os.path.exists(os.path.expanduser("~/.kaggle/kaggle.json")))


def local_files_complete(raw_dir=None):
    raw_dir = raw_dir or _raw_dir()
    return all(S.find_file(raw_dir, t) for t in S.SOURCES)


def locate_files(src_dir):
    """table -> path of the matching CSV inside the downloaded folder (searches sub-folders too)."""
    found, missing = {}, []
    all_dirs = [root for root, _, _ in os.walk(src_dir)]
    for table in S.SOURCES:
        path = next((p for d in all_dirs if (p := S.find_file(d, table))), None)
        (found.__setitem__(table, path) if path else missing.append(S.SOURCES[table]["file"]))
    if missing:
        raise RuntimeError(f"Kaggle dataset is missing expected file(s): {missing}")
    return found


def validate(found):
    """Raise if a file is empty or its header doesn't match the source contract. Returns {table: data_rows}."""
    rows = {}
    for table, path in found.items():
        with open(path, encoding="utf-8-sig", newline="") as f:
            reader = csv.reader(f)
            header = next(reader, None)
            if not header:
                raise RuntimeError(f"{os.path.basename(path)} is empty")
            cols = S.unique_names([S.snake(c) for c in header])
            if sorted(cols) != sorted(S.SOURCES[table]["columns"]):
                raise RuntimeError(f"{os.path.basename(path)}: unexpected columns {cols}")
            rows[table] = sum(1 for r in reader if r)
        if rows[table] == 0:
            raise RuntimeError(f"{os.path.basename(path)} has a header but no data rows")
    return rows


def install(found, raw_dir=None):
    """Copy validated files into data/raw under their canonical names (atomic per file)."""
    raw_dir = raw_dir or _raw_dir()
    os.makedirs(raw_dir, exist_ok=True)
    for table, path in found.items():
        final = os.path.join(raw_dir, S.SOURCES[table]["file"])
        tmp = final + ".part"
        shutil.copyfile(path, tmp)
        os.replace(tmp, final)


def download_from_kaggle(dest):
    if not have_credentials():
        raise RuntimeError("No Kaggle credentials. Set KAGGLE_USERNAME and KAGGLE_KEY (or KAGGLE_API_TOKEN) in .env "
                           "and run: docker compose up -d")
    from kaggle.api.kaggle_api_extended import KaggleApi  # imported lazily: only needed when downloading

    api = KaggleApi()
    api.authenticate()
    dataset = os.environ.get("KAGGLE_DATASET", "salikhussaini49/bank-dataset")
    log.info("Downloading Kaggle dataset %s ...", dataset)
    api.dataset_download_files(dataset, path=dest, unzip=True, quiet=True)


def fetch(run_id="manual", ds=None):
    """Task entry point. Returns a short status string."""
    if os.environ.get("DATA_SOURCE", "kaggle").strip().lower() == "local":
        log.info("DATA_SOURCE=local - not downloading")
        return "skipped (DATA_SOURCE=local)"
    started = time.time()
    try:
        with tempfile.TemporaryDirectory(prefix="kaggle_") as tmp:
            download_from_kaggle(tmp)
            found = locate_files(tmp)
            rows = validate(found)
            size = sum(os.path.getsize(p) for p in found.values())
            install(found)
    except Exception as exc:  # noqa: BLE001
        allow = os.environ.get("ALLOW_LOCAL_FALLBACK", "true").strip().lower() == "true"
        if allow and local_files_complete():
            import alerting  # local import: keeps this module importable without notify/SMTP set up
            alerting.send_alert("WARNING", "Kaggle download failed - pipeline continues on existing local files",
                                [f"Reason: {str(exc)[:400]}", "Data may be stale. Fix the Kaggle credentials/network, "
                                 "or set DATA_SOURCE=local to stop trying."], task_id="download_source_files",
                                run_id=run_id)
            obs.record_metrics(run_id, ds, "download_source_files", [(None, "download_ok", 0)])
            return "fallback to local files"
        raise
    items = [(None, "download_ok", 1), (None, "download_seconds", round(time.time() - started, 2)),
             (None, "download_mb", round(size / 1048576, 3))] + [(t, "source_rows", n) for t, n in rows.items()]
    obs.clear_task_metrics(run_id, "download_source_files")
    obs.record_metrics(run_id, ds, "download_source_files", items)
    log.info("Downloaded and validated: %s", rows)
    return f"downloaded {sum(rows.values()):,} rows"
