"""Idempotent upload tests: optional client-supplied global batch id.

Semantics under test:

* same id + same UTC day + byte-identical CSV  -> replay the first
  registration's receipt (file name + rows), no new parquet, no new rows
* same id + different day or different content -> 409 conflict
* different ids with identical content         -> each appends its own file
* no id                                        -> legacy append behaviour
* racing first uploads of one id               -> exactly one registration
* a failed attempt (bad CSV / write error)     -> never holds the id
* receipts survive restarts and compaction     -> retry never resurrects
  the reclaimed file nor adds rows
"""

from __future__ import annotations

import sys
import threading
import time
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.main import create_app  # noqa: E402

DAY = "2026-11-18"
DAY2 = "2026-11-19"

CSV_A = (
    "buoy_id,ts_utc,salinity\n"
    "B001,2026-11-18T00:01:00Z,35.1\n"
    "B002,2026-11-18T00:02:00Z,35.2\n"
)
CSV_B = "buoy_id,ts_utc,salinity\nB003,2026-11-18T01:02:00Z,34.9\n"
CSV_A_MODIFIED = CSV_A.replace("35.1", "35.9")  # same shape, different bytes
CSV_BAD = "buoy_id,ts_utc,salinity\nB1,not-a-time,35\n"


def make_client(tmp_path, **kwargs) -> TestClient:
    return TestClient(create_app(data_dir=str(tmp_path / "data"), **kwargs))


def upload(client: TestClient, csv_text: str, day: str = DAY, batch_id: str | None = None):
    params = {} if batch_id is None else {"batch_id": batch_id}
    return client.post(
        f"/days/{day}/upload",
        content=csv_text.encode(),
        headers={"Content-Type": "text/csv"},
        params=params,
    )


def wait_job(client: TestClient, day: str, job_id: int, timeout: float = 15.0) -> dict:
    deadline = time.time() + timeout
    while time.time() < deadline:
        r = client.get(f"/days/{day}/compactions/{job_id}")
        assert r.status_code == 200, r.text
        job = r.json()
        if job["status"] in ("done", "failed"):
            return job
        time.sleep(0.05)
    pytest.fail(f"compaction job {job_id} did not finish in {timeout}s")


# ---------------------------------------------------------------------------
# replay / conflict semantics
# ---------------------------------------------------------------------------

def test_retry_same_batch_replays_original_receipt(tmp_path):
    with make_client(tmp_path) as c:
        r1 = upload(c, CSV_A, batch_id="batch-1")
        assert r1.status_code == 200, r1.text
        first = r1.json()
        assert first["batch_id"] == "batch-1"
        assert first["deduplicated"] is False
        assert first["rows"] == 2 and first["file_count"] == 1

        r2 = upload(c, CSV_A, batch_id="batch-1")
        assert r2.status_code == 200, r2.text
        replay = r2.json()
        assert replay["deduplicated"] is True
        assert replay["file"] == first["file"]  # first registration's receipt
        assert replay["rows"] == first["rows"]

        # no new parquet registered, no rows appended
        files = c.get(f"/days/{DAY}/files").json()
        assert files["file_count"] == 1
        assert files["files"][0]["file"] == first["file"]
        assert c.get(f"/days/{DAY}/records").json()["row_count"] == 2


def test_same_batch_id_different_content_conflicts(tmp_path):
    with make_client(tmp_path) as c:
        assert upload(c, CSV_A, batch_id="b1").status_code == 200
        r = upload(c, CSV_A_MODIFIED, batch_id="b1")
        assert r.status_code == 409, r.text
        # the conflicting attempt appended nothing
        body = c.get(f"/days/{DAY}/records").json()
        assert body["file_count"] == 1 and body["row_count"] == 2


def test_same_batch_id_different_day_conflicts(tmp_path):
    with make_client(tmp_path) as c:
        assert upload(c, CSV_A, day=DAY, batch_id="b1").status_code == 200
        r = upload(c, CSV_A, day=DAY2, batch_id="b1")
        assert r.status_code == 409, r.text
        assert c.get(f"/days/{DAY2}/files").json()["file_count"] == 0


def test_different_batch_ids_same_content_append_independently(tmp_path):
    with make_client(tmp_path) as c:
        r1 = upload(c, CSV_A, batch_id="b1").json()
        r2 = upload(c, CSV_A, batch_id="b2").json()
        assert r1["deduplicated"] is False and r2["deduplicated"] is False
        assert r1["file"] != r2["file"]
        body = c.get(f"/days/{DAY}/records").json()
        assert body["file_count"] == 2 and body["row_count"] == 4


def test_no_batch_id_keeps_legacy_append_semantics(tmp_path):
    with make_client(tmp_path) as c:
        upload(c, CSV_A)
        upload(c, CSV_A)
        body = c.get(f"/days/{DAY}/records").json()
        assert body["file_count"] == 2 and body["row_count"] == 4


def test_batch_id_via_header(tmp_path):
    with make_client(tmp_path) as c:
        r1 = c.post(
            f"/days/{DAY}/upload",
            content=CSV_A.encode(),
            headers={"Content-Type": "text/csv", "X-Batch-Id": "hdr-1"},
        )
        assert r1.status_code == 200, r1.text
        assert r1.json()["batch_id"] == "hdr-1"
        r2 = c.post(
            f"/days/{DAY}/upload",
            content=CSV_A.encode(),
            headers={"Content-Type": "text/csv", "X-Batch-Id": "hdr-1"},
        )
        assert r2.json()["deduplicated"] is True
        assert c.get(f"/days/{DAY}/files").json()["file_count"] == 1


def test_batch_id_with_multipart_upload(tmp_path):
    with make_client(tmp_path) as c:
        r1 = c.post(
            f"/days/{DAY}/upload?batch_id=mp-1",
            files={"file": ("obs.csv", CSV_A.encode(), "text/csv")},
        )
        assert r1.status_code == 200, r1.text
        r2 = c.post(
            f"/days/{DAY}/upload?batch_id=mp-1",
            files={"file": ("obs.csv", CSV_A.encode(), "text/csv")},
        )
        assert r2.json()["deduplicated"] is True
        assert r2.json()["file"] == r1.json()["file"]
        assert c.get(f"/days/{DAY}/files").json()["file_count"] == 1


def test_invalid_batch_id_rejected(tmp_path):
    with make_client(tmp_path) as c:
        assert upload(c, CSV_A, batch_id="has spaces").status_code == 400
        assert upload(c, CSV_A, batch_id="x" * 200).status_code == 400
        assert upload(c, CSV_A, batch_id="").status_code == 400
        assert c.get(f"/days/{DAY}/files").json()["file_count"] == 0


# ---------------------------------------------------------------------------
# concurrency: exactly one registration per id
# ---------------------------------------------------------------------------

def test_concurrent_same_batch_id_registers_exactly_once(tmp_path):
    with make_client(tmp_path) as c:
        responses = []

        def worker():
            responses.append(upload(c, CSV_A, batch_id="race"))

        threads = [threading.Thread(target=worker) for _ in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert all(r.status_code == 200 for r in responses), [r.text for r in responses]
        payloads = [r.json() for r in responses]
        # every caller gets the same single registration's receipt
        assert {p["file"] for p in payloads} == {payloads[0]["file"]}
        assert sum(1 for p in payloads if not p["deduplicated"]) == 1
        body = c.get(f"/days/{DAY}/records").json()
        assert body["file_count"] == 1 and body["row_count"] == 2


# ---------------------------------------------------------------------------
# failures never hold a batch id
# ---------------------------------------------------------------------------

def test_invalid_csv_does_not_hold_batch_id(tmp_path):
    with make_client(tmp_path) as c:
        assert upload(c, CSV_BAD, batch_id="b1").status_code == 422
        ok = upload(c, CSV_A, batch_id="b1")
        assert ok.status_code == 200, ok.text
        assert ok.json()["deduplicated"] is False
        assert c.get(f"/days/{DAY}/records").json()["row_count"] == 2


def test_failed_write_does_not_hold_batch_id(tmp_path, monkeypatch):
    import app.lake as lake_mod

    app = create_app(data_dir=str(tmp_path / "data"))
    with TestClient(app, raise_server_exceptions=False) as c:
        original = lake_mod.write_parquet_atomic

        def boom(*args, **kwargs):
            raise IOError("disk full (simulated)")

        monkeypatch.setattr(lake_mod, "write_parquet_atomic", boom)
        assert upload(c, CSV_A, batch_id="b1").status_code == 500
        monkeypatch.setattr(lake_mod, "write_parquet_atomic", original)

        ok = upload(c, CSV_A, batch_id="b1")  # id is free again
        assert ok.status_code == 200, ok.text
        assert ok.json()["deduplicated"] is False
        assert c.get(f"/days/{DAY}/records").json()["row_count"] == 2


# ---------------------------------------------------------------------------
# durability across restarts and compaction
# ---------------------------------------------------------------------------

def test_batch_id_recognized_after_restart(tmp_path):
    with make_client(tmp_path) as c:
        first = upload(c, CSV_A, batch_id="b1").json()
        assert first["deduplicated"] is False

    # "process restart": a brand-new app over the same data directory
    with make_client(tmp_path) as c2:
        replay = upload(c2, CSV_A, batch_id="b1").json()
        assert replay["deduplicated"] is True
        assert replay["file"] == first["file"]
        assert replay["rows"] == first["rows"]
        assert c2.get(f"/days/{DAY}/files").json()["file_count"] == 1
        # conflicts are still detected after the restart
        assert upload(c2, CSV_A_MODIFIED, batch_id="b1").status_code == 409


def test_receipt_survives_compaction_and_gc(tmp_path):
    """After compaction reclaims the original parquet, the batch receipt
    still names it; a retry must not resurrect the file or add rows."""
    with make_client(tmp_path, gc_interval_sec=0.1) as c:
        first = upload(c, CSV_A, batch_id="b1").json()
        upload(c, CSV_B, batch_id="b2")

        job_id = c.post(f"/days/{DAY}/compactions").json()["job_id"]
        assert wait_job(c, DAY, job_id)["status"] == "done"

        day_dir = tmp_path / "data" / f"dt={DAY}"
        deadline = time.time() + 10
        while time.time() < deadline:
            if not (day_dir / first["file"]).exists():
                break
            time.sleep(0.1)
        else:
            pytest.fail("retired upload file was not reclaimed")

        replay = upload(c, CSV_A, batch_id="b1").json()
        assert replay["deduplicated"] is True
        assert replay["file"] == first["file"]  # receipt still points at the batch
        assert replay["rows"] == first["rows"]
        assert not (day_dir / first["file"]).exists()  # not resurrected

        body = c.get(f"/days/{DAY}/records").json()
        assert body["file_count"] == 1  # only the compacted output
        assert body["row_count"] == 3  # nothing appended by the retry
