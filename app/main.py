"""FastAPI surface for the buoy data-lake compaction service.

Endpoints
---------
POST   /days/{day}/upload                 append a CSV as one parquet file
GET    /days/{day}/records                all records (consistent snapshot)
GET    /days/{day}/files                  active file list / count
POST   /days/{day}/compactions            start background compaction (202)
GET    /days/{day}/compactions            list jobs for the day
GET    /days/{day}/compactions/{job_id}   job status
GET    /health                            liveness
"""

from __future__ import annotations

import logging
import os
import re
import threading
from contextlib import asynccontextmanager
from datetime import date
from typing import Annotated

from fastapi import FastAPI, File, Header, HTTPException, Request, UploadFile
from fastapi.concurrency import run_in_threadpool

from .csvio import CsvError
from .lake import BatchConflictError, ConflictError, Lake, NotFoundError

log = logging.getLogger("buoy-lake")

DAY_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
BATCH_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$")


def _valid_day(day: str) -> str:
    if not DAY_RE.match(day):
        raise HTTPException(status_code=400, detail=f"invalid day {day!r}; expected YYYY-MM-DD")
    try:
        date.fromisoformat(day)
    except ValueError:
        raise HTTPException(status_code=400, detail=f"invalid calendar day {day!r}")
    return day


def _valid_batch_id(raw: str | None) -> str | None:
    if raw is None:
        return None
    bid = raw.strip()
    if not BATCH_ID_RE.match(bid):
        raise HTTPException(
            status_code=400,
            detail="invalid batch id: use 1-128 chars from [A-Za-z0-9._:-],"
            " starting with a letter or digit",
        )
    return bid


def create_app(
    data_dir: str | None = None,
    compaction_delay_sec: float | None = None,
    gc_interval_sec: float | None = None,
) -> FastAPI:
    data_dir = data_dir or os.environ.get("DATA_DIR", "./data")
    if compaction_delay_sec is None:
        compaction_delay_sec = float(os.environ.get("LAKE_COMPACTION_DELAY_SEC", "0"))
    if gc_interval_sec is None:
        gc_interval_sec = float(os.environ.get("LAKE_GC_INTERVAL_SEC", "2"))

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        lake = Lake(data_dir, compaction_delay_sec=compaction_delay_sec)
        stats = lake.recover()  # reconcile + resume interrupted compactions
        app.state.lake = lake
        app.state.recovery = stats

        stop = threading.Event()

        def gc_loop():
            while not stop.wait(gc_interval_sec):
                try:
                    lake.collect_garbage()
                except Exception:
                    log.exception("periodic garbage collection failed")

        gc_thread = threading.Thread(target=gc_loop, name="retired-file-gc", daemon=True)
        gc_thread.start()
        yield
        stop.set()
        gc_thread.join(timeout=5)

    app = FastAPI(title="Buoy Data Lake Compaction Service", version="1.0.0", lifespan=lifespan)

    def lake(request: Request) -> Lake:
        return request.app.state.lake

    @app.get("/health")
    def health():
        return {"status": "ok"}

    @app.get("/")
    def index():
        return {
            "service": "buoy-lake-compactor",
            "docs": "/docs",
            "endpoints": [
                "POST /days/{day}/upload",
                "GET /days/{day}/records",
                "GET /days/{day}/files",
                "POST /days/{day}/compactions",
                "GET /days/{day}/compactions",
                "GET /days/{day}/compactions/{job_id}",
            ],
        }

    @app.post("/days/{day}/upload")
    async def upload(
        day: str,
        request: Request,
        file: UploadFile | None = File(None),
        batch_id: str | None = None,
        x_batch_id: Annotated[str | None, Header()] = None,
    ):
        """Append a CSV (multipart file or raw ``text/csv`` body) as one parquet.

        Optional idempotency key via the ``batch_id`` query parameter or the
        ``X-Batch-Id`` header: a retry with the same id, same day and
        byte-identical CSV replays the first registration's receipt (no new
        file, no new rows); the same id with a different day or different
        content is rejected with 409.  Without an id every upload appends.
        """
        day = _valid_day(day)
        bid = _valid_batch_id(x_batch_id if x_batch_id is not None else batch_id)
        content = await file.read() if file is not None else await request.body()
        if not content:
            raise HTTPException(status_code=400, detail="empty CSV body")
        try:
            # parquet write + fsync are blocking -> run off the event loop
            return await run_in_threadpool(lake(request).upload, day, content, bid)
        except CsvError as exc:
            raise HTTPException(status_code=422, detail=str(exc))
        except BatchConflictError as exc:
            raise HTTPException(status_code=409, detail=str(exc))

    @app.get("/days/{day}/records")
    def records(day: str, request: Request):
        """All records for the day from one consistent snapshot + file count."""
        return lake(request).read_day(_valid_day(day))

    @app.get("/days/{day}/files")
    def files(day: str, request: Request):
        return lake(request).list_files(_valid_day(day))

    @app.post("/days/{day}/compactions", status_code=202)
    def start_compaction(day: str, request: Request):
        """Start a background compaction; 409 if one is already running."""
        try:
            return lake(request).start_compaction(_valid_day(day))
        except ConflictError as exc:
            raise HTTPException(status_code=409, detail=str(exc))

    @app.get("/days/{day}/compactions")
    def list_compactions(day: str, request: Request):
        day = _valid_day(day)
        return {"day": day, "jobs": lake(request).list_compactions(day)}

    @app.get("/days/{day}/compactions/{job_id}")
    def get_compaction(day: str, job_id: int, request: Request):
        day = _valid_day(day)
        try:
            job = lake(request).get_compaction(job_id)
        except NotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc))
        if job["day"] != day:
            raise HTTPException(status_code=404, detail=f"job {job_id} does not belong to day {day}")
        return job

    return app


app = create_app()
