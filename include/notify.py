"""E-mail notifications over plain SMTP (works with a Gmail App Password).

Configured only through environment variables (set in .env -> docker-compose.yml):
NOTIFY_EMAIL, SMTP_HOST, SMTP_PORT, SMTP_USER, SMTP_PASSWORD, SMTP_MAIL_FROM
"""
import logging
import os
import smtplib
import ssl
from email.message import EmailMessage
from html import escape

log = logging.getLogger(__name__)


def _config():
    user = os.environ.get("SMTP_USER", "").strip()
    return {
        "host": os.environ.get("SMTP_HOST", "smtp.gmail.com").strip(),
        "port": int(os.environ.get("SMTP_PORT", "587") or 587),
        "user": user,
        "password": os.environ.get("SMTP_PASSWORD", "").replace(" ", ""),
        "sender": (os.environ.get("SMTP_MAIL_FROM") or user).strip(),
        "to": [a.strip() for a in os.environ.get("NOTIFY_EMAIL", "").split(",") if a.strip()],
    }


def send_mail(subject, html, text=None, to=None, raise_errors=False):
    """Send one e-mail. Returns True on success. Never raises unless raise_errors=True,
    so a mail problem can never hide the real pipeline failure."""
    cfg = _config()
    recipients = to or cfg["to"]
    try:
        if not (cfg["user"] and cfg["password"] and recipients):
            raise RuntimeError("Email not configured: set NOTIFY_EMAIL, SMTP_USER and SMTP_PASSWORD in .env "
                               "and restart the containers (docker compose up -d).")
        msg = EmailMessage()
        msg["Subject"] = subject
        msg["From"] = cfg["sender"]
        msg["To"] = ", ".join(recipients)
        msg.set_content(text or "This message is HTML. Open it in an HTML-capable mail client.")
        msg.add_alternative(html, subtype="html")

        ctx = ssl.create_default_context()
        if cfg["port"] == 465:
            server = smtplib.SMTP_SSL(cfg["host"], cfg["port"], timeout=30, context=ctx)
        else:
            server = smtplib.SMTP(cfg["host"], cfg["port"], timeout=30)
            server.ehlo()
            server.starttls(context=ctx)
            server.ehlo()
        try:
            server.login(cfg["user"], cfg["password"])
            server.send_message(msg)
        finally:
            server.quit()
        log.info("Email sent to %s: %s", recipients, subject)
        return True
    except Exception as exc:  # noqa: BLE001
        log.error("Could not send email '%s': %s", subject, exc)
        if raise_errors:
            raise
        return False


def _row(label, value):
    return (f"<tr><td style='padding:4px 12px 4px 0;color:#555'>{escape(str(label))}</td>"
            f"<td style='padding:4px 0'><b>{escape(str(value))}</b></td></tr>")


def build_success_html(report, ds, run_url=None):
    tables = "".join(
        _row(name, f"bronze {m['bronze_rows']:,} -> silver {m['silver_rows']:,} "
                   f"(quarantined {m['quarantined_rows']:,}, older duplicates {m['duplicates_removed']:,})")
        for name, m in report.get("tables", {}).items())
    gold = "".join(_row(t, f"{n:,}") for t, n in report.get("gold_tables", {}).items())
    checks = "".join(
        f"<li>{'&#9989;' if c['passed'] else '&#10060;'} {escape(c['name'])} "
        f"<span style='color:#777'>({escape(str(c['detail']))})</span></li>"
        for c in report.get("checks", []))
    link = f"<p><a href='{escape(run_url)}'>Open in Airflow</a></p>" if run_url else ""
    return f"""
<div style="font-family:Segoe UI,Arial,sans-serif;font-size:14px">
  <h2 style="color:#1a7f37">Bank medallion pipeline succeeded</h2>
  <p>Run date: <b>{escape(str(ds))}</b> &middot; Status: <b>{escape(str(report.get('status')))}</b>
     &middot; Transactions: {escape(str(report.get('transaction_date_range')))}</p>
  <h3>Source tables</h3><table>{tables}</table>
  <h3>Gold tables (rows)</h3><table>{gold}</table>
  <h3>Quality checks</h3><ul>{checks}</ul>
  {link}
</div>"""


def failure_callback(context):
    """Airflow on_failure_callback: mail the failing task, error and log link."""
    ti = context["task_instance"]
    exc = context.get("exception")
    subject = f"[FAILED] {ti.dag_id} / {ti.task_id} ({context.get('ds')})"
    html = f"""
<div style="font-family:Segoe UI,Arial,sans-serif;font-size:14px">
  <h2 style="color:#cf222e">Pipeline task failed</h2>
  <table>
    {_row('DAG', ti.dag_id)}{_row('Task', ti.task_id)}{_row('Run date', context.get('ds'))}
    {_row('Attempt', f"{ti.try_number}")}
  </table>
  <p><b>Error:</b> <code>{escape(str(exc))[:1500]}</code></p>
  <p><a href="{escape(ti.log_url)}">Open the task log in Airflow</a></p>
</div>"""
    send_mail(subject, html, text=f"{subject}\nError: {exc}\nLog: {ti.log_url}")
