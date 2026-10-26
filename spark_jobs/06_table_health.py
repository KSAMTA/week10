"""TABLE HEALTH: size, file count and Delta version of every lakehouse table -> obs.task_metrics.

Small-file build-up and runaway table growth are early warnings that the dashboard can show.
"""
import argparse
from datetime import datetime, timezone

from delta.tables import DeltaTable

import obs
import paths as P
import schemas as S
from common import get_spark, log


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--ds", default=datetime.now(timezone.utc).strftime("%Y-%m-%d"))
    ap.add_argument("--run-id", default=None)
    args = ap.parse_args()
    run_id = args.run_id or f"manual__{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S')}"

    spark = get_spark("table_health")
    tables = ([("bronze", t, P.bronze(t)) for t in S.SOURCES] + [("silver", t, P.silver(t)) for t in S.SOURCES]
              + [("quarantine", t, P.quarantine(t)) for t in S.SOURCES]
              + [("gold", t, P.gold(t)) for t in list(S.DIM_KEYS) + list(S.FACTS)])
    items = []
    for layer, name, path in tables:
        try:
            dt = DeltaTable.forPath(spark, path)
            d = dt.detail().select("numFiles", "sizeInBytes").first()
            version = dt.history(1).select("version").first()[0]
        except Exception as exc:  # noqa: BLE001  - a missing table must not fail the whole job
            log(f"HEALTH {layer}.{name}: unavailable ({str(exc)[:80]})")
            continue
        key = f"{layer}.{name}"
        items += [(key, "num_files", d["numFiles"]), (key, "size_mb", round(d["sizeInBytes"] / 1048576, 3)),
                  (key, "delta_version", version)]
        log(f"HEALTH {key}: {d['numFiles']} file(s), {d['sizeInBytes'] / 1048576:.2f} MB, version {version}")
    obs.clear_task_metrics(run_id, "table_health_stats")
    obs.record_metrics(run_id, args.ds, "table_health_stats", items)
    spark.stop()


if __name__ == "__main__":
    main()
