"""Send one test alert through every configured channel (e-mail + Teams) and store it in obs.alerts.

    docker compose exec scheduler python /opt/airflow/scripts/test_alerts.py
"""
import sys

sys.path.insert(0, "/opt/airflow/include")
import alerting  # noqa: E402

for sev in ("WARNING", "CRITICAL"):
    sent = alerting.send_alert(sev, f"TEST {sev.lower()} alert from the bank medallion pipeline",
                               ["This is only a test.", "If you can read this, the channel works."])
    print(f"{sev}: delivered to {sent or 'NO channel (check SMTP_* / TEAMS_WEBHOOK_URL in .env)'}")
