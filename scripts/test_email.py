"""Send a test email with the same code the pipeline uses.

    docker compose exec scheduler python /opt/airflow/scripts/test_email.py
"""
import sys

sys.path.insert(0, "/opt/airflow/include")
from notify import send_mail  # noqa: E402

send_mail(
    "[TEST] Bank medallion pipeline - email works",
    "<h3>It works &#127881;</h3><p>Pipeline success and failure alerts will arrive at this address.</p>",
    raise_errors=True,
)
print("Test email sent. Check your inbox (and spam folder).")
