"""Severity-based alert router: e-mail (via notify.send_mail) + Microsoft Teams webhook.

Severities: INFO (log + obs.alerts only) < WARNING < CRITICAL.
Every alert is stored in obs.alerts (shown on the Grafana dashboard) whether or not a channel is configured.
Sending never raises - an alerting problem must not hide the real pipeline problem.

Env: ALERT_CHANNELS (default "email,teams"), ALERT_MIN_SEVERITY (default WARNING), TEAMS_WEBHOOK_URL,
     AIRFLOW_BASE_URL, GRAFANA_URL, plus the SMTP settings used by notify.py.
"""
import hashlib
import json
import logging
import os
import urllib.request
from html import escape

import obs
from notify import send_mail

log = logging.getLogger("alerting")

LEVEL = {"INFO": 0, "WARNING": 1, "CRITICAL": 2}
COLOR = {"INFO": "#1a7f37", "WARNING": "#bf8700", "CRITICAL": "#cf222e"}
DQ_LAYER_OF_TASK = {"dq_bronze": "bronze", "dq_silver": "silver", "dq_gold": "gold"}


def _channels():
    return [c.strip().lower() for c in os.environ.get("ALERT_CHANNELS", "email,teams").split(",") if c.strip()]


def _min_level():
    return LEVEL.get(os.environ.get("ALERT_MIN_SEVERITY", "WARNING").strip().upper(), 1)


def fingerprint(*parts):
    return hashlib.sha1("|".join(str(p) for p in parts).encode()).hexdigest()[:16]


def post_teams(title, severity, lines, link=None):
    """Post an Adaptive Card to a Teams 'Workflows' webhook. Returns True on success."""
    url = os.environ.get("TEAMS_WEBHOOK_URL", "").strip()
    if not url:
        return False
    body = [{"type": "TextBlock", "text": title, "weight": "Bolder", "size": "Medium", "wrap": True,
             "color": "Attention" if severity == "CRITICAL" else "Warning" if severity == "WARNING" else "Good"}]
    body += [{"type": "TextBlock", "text": str(line)[:400], "wrap": True, "spacing": "None"} for line in lines[:15]]
    card = {"type": "AdaptiveCard", "version": "1.4", "body": body,
            "$schema": "http://adaptivecards.io/schemas/adaptive-card.json"}
    if link:
        card["actions"] = [{"type": "Action.OpenUrl", "title": "Open in Airflow", "url": link}]
    payload = {"type": "message", "attachments": [{"contentType": "application/vnd.microsoft.card.adaptive",
                                                   "contentUrl": None, "content": card}]}
    try:
        req = urllib.request.Request(url, data=json.dumps(payload).encode(), method="POST",
                                     headers={"Content-Type": "application/json"})
        with urllib.request.urlopen(req, timeout=15) as resp:  # noqa: S310
            return 200 <= resp.status < 300
    except Exception as exc:  # noqa: BLE001
        log.error("Teams alert failed: %s", exc)
        return False


def _html(severity, title, lines, link):
    items = "".join(f"<li>{escape(str(x))}</li>" for x in lines)
    a = f"<p><a href='{escape(link)}'>Open in Airflow</a></p>" if link else ""
    return (f"<div style=\"font-family:Segoe UI,Arial,sans-serif;font-size:14px\">"
            f"<h2 style=\"color:{COLOR[severity]}\">[{severity}] {escape(title)}</h2><ul>{items}</ul>{a}</div>")


def send_alert(severity, title, lines, *, task_id=None, run_id=None, link=None, fingerprint_key=None,
               cooldown_minutes=0):
    """Route one alert. Returns the list of channels it was actually delivered to."""
    severity = severity.upper()
    lines = [str(x) for x in (lines or [])]
    fp = fingerprint_key and fingerprint(fingerprint_key)
    if cooldown_minutes and fp and obs.recent_alert_exists(fp, cooldown_minutes):
        log.info("Alert suppressed (cooldown %s min): %s", cooldown_minutes, title)
        return []
    delivered = []
    if LEVEL.get(severity, 0) >= _min_level():
        chans = _channels()
        if "email" in chans and send_mail(f"[{severity}] {title}", _html(severity, title, lines, link),
                                          text=f"[{severity}] {title}\n" + "\n".join(lines)):
            delivered.append("email")
        if "teams" in chans and post_teams(f"[{severity}] {title}", severity, lines, link):
            delivered.append("teams")
    log.warning("ALERT %s %s -> %s", severity, title, delivered or "log only")
    obs.record_alert(severity, title, "\n".join(lines), ",".join(delivered) or "none", fp, run_id, task_id)
    return delivered


# ----------------------------------------------------------------------------- Airflow callbacks
def _fmt_dq(f):
    return f"[{f['severity']}] {f['layer']}/{f['table'] or '-'}: {f['name']} -> {f['observed']} (limit {f['threshold']})"


def task_failure_callback(context):
    """on_failure_callback: CRITICAL alert with the error, log link and (for dq_* tasks) the failing checks."""
    ti = context["task_instance"]
    run_id = context.get("run_id")
    lines = [f"DAG {ti.dag_id} / task {ti.task_id} / run date {context.get('ds')} / attempt {ti.try_number}",
             f"Error: {str(context.get('exception'))[:600]}"]
    layer = DQ_LAYER_OF_TASK.get(ti.task_id)
    if layer:
        fails = obs.dq_failures(run_id, layer=layer, severity="CRITICAL")
        lines.append(f"{len(fails)} critical data-quality check(s) failed - downstream layers were NOT built:")
        lines += [_fmt_dq(f) for f in fails[:10]]
    send_alert("CRITICAL", f"{ti.dag_id} / {ti.task_id} failed ({context.get('ds')})", lines,
               task_id=ti.task_id, run_id=run_id, link=ti.log_url)


def task_retry_callback(context):
    """on_retry_callback: recorded as INFO (visible on the dashboard) but not sent - retries are self-healing."""
    ti = context["task_instance"]
    send_alert("INFO", f"{ti.dag_id} / {ti.task_id} will retry (attempt {ti.try_number})",
               [str(context.get("exception"))[:400]], task_id=ti.task_id, run_id=context.get("run_id"),
               link=ti.log_url)
