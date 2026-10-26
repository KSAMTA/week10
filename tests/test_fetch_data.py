"""Tests for the Kaggle download task (no network needed - the Kaggle call is mocked)."""
import os
import sys
import tempfile
import unittest
from unittest import mock

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path[:0] = [os.path.join(ROOT, "include"), os.path.join(ROOT, "spark_jobs")]

import fetch_data  # noqa: E402
import obs  # noqa: E402
import schemas as S  # noqa: E402

HEADERS = {"customers": "CUSTOMER_ID,First_Name,Last_Name,City,Phone_Number,Occupation,DOB",
           "branches": "BRANCH_ID,BRANCH_NAME,BRANCH_STATE",
           "accounts": "ACCOUNT_ID,CUSTOMER_ID,BRANCH_ID,OPENING_BALANCE,ACCOUNT_OPEN_DATE,ACCOUNT_TYPE,ACCOUNT_STATUS",
           "loans": "LOAN_ID,CUSTOMER_ID,BRANCH_ID,LOAN_AMOUNT",
           "transactions": "TRANSCATION_ID,ACCOUNT_ID,TRANSCATION_DATE,TRANSCATION_MEDIA,TRANSCATION_TYPE,TRANSCATION_AMOUNT"}


def write_dataset(folder, bad_table=None, empty_table=None):
    os.makedirs(folder, exist_ok=True)
    for t, h in HEADERS.items():
        header = "WRONG,COLUMNS" if t == bad_table else h
        body = "" if t == empty_table else "\n" + ",".join(["x"] * (header.count(",") + 1))
        with open(os.path.join(folder, S.SOURCES[t]["file"]), "w", encoding="utf-8") as f:
            f.write(header + body + "\n")


class Fetch(unittest.TestCase):
    def setUp(self):
        self.data = tempfile.mkdtemp()
        self.src = tempfile.mkdtemp()
        env = mock.patch.dict(os.environ, {"DATA_DIR": self.data, "DATA_SOURCE": "kaggle", "ALLOW_LOCAL_FALLBACK": "true"})
        env.start()
        self.addCleanup(env.stop)
        for name in ("record_metrics", "clear_task_metrics"):
            p = mock.patch.object(obs, name)
            p.start()
            self.addCleanup(p.stop)
        self.raw = os.path.join(self.data, "raw")

    def fake_download(self, **kw):
        def _dl(dest):
            write_dataset(os.path.join(dest, "nested"), **kw)   # files inside a sub-folder, like some zips
        return mock.patch.object(fetch_data, "download_from_kaggle", side_effect=_dl)

    def test_success_installs_all_files_atomically(self):
        with self.fake_download():
            self.assertIn("downloaded", fetch_data.fetch("r1", "2026-10-22"))
        self.assertTrue(fetch_data.local_files_complete(self.raw))
        self.assertFalse([f for f in os.listdir(self.raw) if f.endswith(".part")])

    def test_bad_header_is_rejected_and_nothing_installed(self):
        with self.fake_download(bad_table="accounts"), self.assertRaises(RuntimeError):
            fetch_data.fetch("r2", "d")
        self.assertFalse(os.path.exists(self.raw) and os.listdir(self.raw))

    def test_empty_file_is_rejected(self):
        with self.fake_download(empty_table="loans"), self.assertRaises(RuntimeError):
            fetch_data.fetch("r3", "d")

    def test_failure_without_local_files_raises(self):
        with mock.patch.object(fetch_data, "download_from_kaggle", side_effect=RuntimeError("no credentials")), \
                self.assertRaises(RuntimeError):
            fetch_data.fetch("r4", "d")

    def test_failure_with_local_files_falls_back_and_warns(self):
        write_dataset(self.raw)
        with mock.patch.object(fetch_data, "download_from_kaggle", side_effect=RuntimeError("network down")), \
                mock.patch("alerting.send_alert") as alert:
            self.assertEqual(fetch_data.fetch("r5", "d"), "fallback to local files")
        self.assertEqual(alert.call_args[0][0], "WARNING")

    def test_fallback_can_be_disabled(self):
        write_dataset(self.raw)
        with mock.patch.dict(os.environ, {"ALLOW_LOCAL_FALLBACK": "false"}), \
                mock.patch.object(fetch_data, "download_from_kaggle", side_effect=RuntimeError("x")), \
                self.assertRaises(RuntimeError):
            fetch_data.fetch("r6", "d")

    def test_local_mode_skips_download(self):
        with mock.patch.dict(os.environ, {"DATA_SOURCE": "local"}), \
                mock.patch.object(fetch_data, "download_from_kaggle") as dl:
            self.assertIn("skipped", fetch_data.fetch("r7", "d"))
        dl.assert_not_called()

    def test_missing_credentials_message(self):
        with mock.patch.dict(os.environ, {"KAGGLE_USERNAME": "", "KAGGLE_KEY": "", "KAGGLE_API_TOKEN": ""}), \
                mock.patch("os.path.exists", return_value=False), self.assertRaises(RuntimeError) as cm:
            fetch_data.download_from_kaggle(self.src)
        self.assertIn("KAGGLE_USERNAME", str(cm.exception))


if __name__ == "__main__":
    unittest.main()
