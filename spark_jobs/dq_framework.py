"""Tiny data-quality framework shared by the bronze / silver / gold quality gates.

    runner = DQRunner("silver", ds, run_id)
    runner.check("customers: rows > 0", n > 0, observed=n, threshold="> 0", table="customers")
    runner.check("amount in range", bad == 0, observed=bad, severity="WARNING")
    runner.finish()      # stores results in Postgres + JSON, exits non-zero if any CRITICAL check failed

Severity semantics:
  CRITICAL  blocks the pipeline (the task fails, downstream layers are not built, a CRITICAL alert is sent)
  WARNING   does not block; included in the run digest / Teams alert
  INFO      informational (e.g. "no baseline yet")
"""
import os
import sys
from datetime import datetime, timezone

import dq_rules as R
import obs
import paths as P
from common import log, write_json


class DQRunner:
    def __init__(self, layer, ds, run_id=None):
        self.layer, self.ds = layer, ds
        self.run_id = run_id or f"manual__{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S')}"
        self.task_id = f"dq_{layer}_stats"   # NOT dq_<layer>: instrument.py owns that task_id and clears it
        self.checks, self.metrics = [], []

    # -- recording -------------------------------------------------------------------------------
    def check(self, name, passed, observed="", threshold="", severity="CRITICAL", table=None):
        passed = bool(passed)
        self.checks.append({"name": name, "passed": passed, "severity": severity, "observed": observed,
                            "threshold": threshold, "table": table})
        tag = "PASS" if passed else ("FAIL" if severity == "CRITICAL" else "WARN")
        log(f"{tag}  [{severity}] {name}: {observed}" + (f" (limit {threshold})" if threshold != "" else ""))

    def metric(self, table, name, value):
        self.metrics.append((table, name, value))

    # -- reusable check patterns -----------------------------------------------------------------
    def volume_check(self, table, current, history_task, metric="rows", label="rows"):
        """Row count vs the median of recent runs (WARNING at +/-VOLUME_WARN_PCT, CRITICAL at +/-VOLUME_FAIL_PCT)."""
        hist = obs.history(history_task, table, metric, n=10, exclude_run_id=self.run_id)
        status, dev, base = R.volume_status(current, hist)
        name = f"{table}: {label} within {R.WARN_PCT:g}% of recent median"
        if status == "no_baseline":
            self.check(name, True, f"{current:,} (no baseline yet, {len(hist)} earlier run(s))", "", "INFO", table)
        else:
            sev = "CRITICAL" if status == "fail" else "WARNING"
            self.check(name, status == "ok", f"{current:,} vs median {base:,.0f} ({dev:+.1f}%)",
                       f"warn {R.WARN_PCT:g}% / fail {R.FAIL_PCT:g}%", sev, table)

    def drift_check(self, table, metric_name, current, history_task):
        hist = obs.history(history_task, table, metric_name, n=14, exclude_run_id=self.run_id)
        status, z, mean = R.zscore_status(current, hist)
        name = f"{table}: {metric_name} drift (z-score)"
        if status == "no_baseline":
            self.check(name, True, f"{current:,.2f} (no baseline yet, {len(hist)} earlier run(s))", "", "INFO", table)
        else:
            self.check(name, status == "ok", f"{current:,.2f} vs mean {mean:,.2f} (z={z:.1f})",
                       f"|z| <= {R.Z_THRESHOLD:g}", "WARNING", table)
        self.metric(table, metric_name, current)

    # -- finish ----------------------------------------------------------------------------------
    def finish(self):
        obs.clear_dq(self.run_id, self.layer)
        obs.record_dq(self.run_id, self.ds, self.layer, self.checks)
        obs.clear_task_metrics(self.run_id, self.task_id)
        obs.record_metrics(self.run_id, self.ds, self.task_id, self.metrics)
        n_fail = [c for c in self.checks if not c["passed"]]
        crit = [c for c in n_fail if c["severity"] == "CRITICAL"]
        warn = [c for c in n_fail if c["severity"] == "WARNING"]
        write_json(os.path.join(P.REPORTS_DIR, f"dq_{self.layer}_{self.ds}.json"),
                   {"layer": self.layer, "ds": self.ds, "run_id": self.run_id,
                    "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
                    "checks": self.checks})
        log(f"DQ {self.layer.upper()}: {len(self.checks) - len(n_fail)}/{len(self.checks)} passed | "
            f"{len(crit)} critical | {len(warn)} warning")
        if crit:
            sys.exit(f"{self.layer} quality gate FAILED - {len(crit)} critical check(s): "
                     + "; ".join(c["name"] for c in crit[:5]))
