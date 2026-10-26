"""Pure-python decision logic for data-quality checks (no Spark, no DB) so it can be unit-tested."""
import math
import os
import statistics

WARN_PCT = float(os.environ.get("VOLUME_WARN_PCT", "30"))
FAIL_PCT = float(os.environ.get("VOLUME_FAIL_PCT", "60"))
VOL_MIN_HISTORY = int(os.environ.get("VOLUME_MIN_HISTORY", "3"))
Z_THRESHOLD = float(os.environ.get("DRIFT_Z_THRESHOLD", "3"))
DRIFT_MIN_HISTORY = int(os.environ.get("DRIFT_MIN_HISTORY", "5"))
MAX_NULL_PCT = float(os.environ.get("MAX_NULL_PCT", "1.0"))
MAX_QUARANTINE_PCT = float(os.environ.get("MAX_QUARANTINE_PCT", "5.0"))
ORPHAN_MAX_PCT = float(os.environ.get("ORPHAN_MAX_PCT", "0"))
FRESHNESS_MAX_HOURS = float(os.environ.get("FRESHNESS_MAX_HOURS", "0"))   # 0 = check disabled


def pct(part, whole):
    return 0.0 if not whole else 100.0 * part / whole


def volume_status(current, history, warn_pct=WARN_PCT, fail_pct=FAIL_PCT, min_history=VOL_MIN_HISTORY):
    """Compare today's row count with the median of recent runs.
    Returns (status, deviation_pct, baseline); status in ok | warn | fail | no_baseline."""
    if len(history) < min_history:
        return "no_baseline", 0.0, None
    baseline = statistics.median(history)
    if baseline == 0:
        return "no_baseline", 0.0, baseline
    dev = 100.0 * (current - baseline) / baseline
    if abs(dev) >= fail_pct:
        return "fail", dev, baseline
    if abs(dev) >= warn_pct:
        return "warn", dev, baseline
    return "ok", dev, baseline


def zscore_status(current, history, z_threshold=Z_THRESHOLD, min_history=DRIFT_MIN_HISTORY):
    """Distribution-drift check on a summary statistic (e.g. mean amount).
    Returns (status, z, mean); status in ok | warn | no_baseline."""
    if len(history) < min_history:
        return "no_baseline", 0.0, None
    mean = statistics.fmean(history)
    sd = statistics.pstdev(history)
    if sd == 0:                                  # identical history: any real change is notable
        same = math.isclose(current, mean, rel_tol=1e-9, abs_tol=1e-9)
        return ("ok", 0.0, mean) if same else ("warn", float("inf"), mean)
    z = (current - mean) / sd
    return ("warn" if abs(z) > z_threshold else "ok"), z, mean


def worst_severity(checks):
    """Highest severity among FAILED checks, or None."""
    order = {"INFO": 0, "WARNING": 1, "CRITICAL": 2}
    failed = [c["severity"] for c in checks if not c["passed"]]
    return max(failed, key=lambda s: order.get(s, 0)) if failed else None
