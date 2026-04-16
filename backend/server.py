"""
FastAPI server for the FA Rule Converter web app.

Wraps convert_fa_rule.py and exposes:
  POST /api/login          — shared-password auth (returns a session cookie)
  POST /api/convert        — upload files, run a pipeline, stream progress via SSE
  GET  /api/progress/{id}  — SSE stream for a running job
  GET  /api/download/{id}  — download result zip

Static files (React build) are served from /frontend/build.
"""

import asyncio
import os
import shutil
import tempfile
import uuid
import zipfile
from pathlib import Path
from threading import Thread

from fastapi import FastAPI, UploadFile, File, Form, HTTPException, Cookie, Response
from fastapi.responses import FileResponse, StreamingResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware

import convert_fa_rule as engine

# ── Config ──────────────────────────────────────────────────
SHARED_PASSWORD = os.environ.get("APP_PASSWORD", "converter demo")
SESSION_TOKEN = os.environ.get("SESSION_TOKEN", uuid.uuid4().hex)
OUTPUT_ROOT = os.environ.get("OUTPUT_ROOT", tempfile.mkdtemp(prefix="fa_output_"))

app = FastAPI(title="FA Rule Converter")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── In-memory job store ─────────────────────────────────────
jobs: dict = {}   # job_id -> { status, progress_events, result_zip, error }


# ── Auth ────────────────────────────────────────────────────
@app.post("/api/login")
async def login(password: str = Form(...)):
    if password != SHARED_PASSWORD:
        raise HTTPException(status_code=401, detail="Wrong password")
    response = Response(content='{"ok":true}', media_type="application/json")
    response.set_cookie("session", SESSION_TOKEN, httponly=True, samesite="lax")
    return response


def _check_auth(session: str | None):
    if session != SESSION_TOKEN:
        raise HTTPException(status_code=401, detail="Not authenticated")


# ── Upload + run ────────────────────────────────────────────
@app.post("/api/convert")
async def start_conversion(
    mode: str = Form(...),
    action: str = Form(...),
    threshold: float = Form(0.01),
    key_file: UploadFile = File(...),
    fa_file: UploadFile = File(...),
    fa_mat_file: UploadFile | None = File(None),
    session: str | None = Cookie(None),
):
    _check_auth(session)

    if mode not in ("LTA", "FTA"):
        raise HTTPException(400, "mode must be LTA or FTA")
    if action not in ("convert", "audit", "group"):
        raise HTTPException(400, "action must be convert, audit, or group")
    if mode == "FTA" and fa_mat_file is None:
        raise HTTPException(400, "FTA mode requires fa_mat_file")

    # Save uploads to a temp dir
    job_id = uuid.uuid4().hex[:12]
    work_dir = tempfile.mkdtemp(prefix=f"fa_job_{job_id}_")
    out_dir = os.path.join(work_dir, "output")
    os.makedirs(out_dir)

    key_path = _save_upload(key_file, work_dir)
    fa_path = _save_upload(fa_file, work_dir)
    mat_path = _save_upload(fa_mat_file, work_dir) if fa_mat_file else None

    jobs[job_id] = {
        "status": "running",
        "progress_events": [],
        "result_zip": None,
        "error": None,
        "work_dir": work_dir,
    }

    # Run in background thread
    thread = Thread(
        target=_run_job,
        args=(job_id, key_path, fa_path, out_dir, mode, action, threshold, mat_path),
        daemon=True,
    )
    thread.start()

    return {"job_id": job_id}


def _save_upload(upload: UploadFile, dest_dir: str) -> str:
    path = os.path.join(dest_dir, upload.filename)
    with open(path, "wb") as f:
        f.write(upload.file.read())
    return path


def _run_job(job_id, key_path, fa_path, out_dir, mode, action, threshold, mat_path):
    """Execute the conversion pipeline in a background thread."""
    job = jobs[job_id]

    def progress_cb(stage, step, total):
        job["progress_events"].append({
            "stage": stage, "step": step, "total": total
        })

    try:
        if action == "convert":
            conv, _, _ = engine.workflow_convert(
                key_path, fa_path, out_dir, mode, mat_path,
                progress=progress_cb)
            summary = _format_convert(conv, mode)

        elif action == "audit":
            conv, audit = engine.workflow_convert_audit(
                key_path, fa_path, out_dir, mode, mat_path,
                progress=progress_cb)
            summary = _format_audit(conv, audit, mode)

        elif action == "group":
            conv, audit, grp = engine.workflow_convert_audit_group(
                key_path, fa_path, out_dir, mode, mat_path, threshold,
                progress=progress_cb)
            if grp is None:
                summary = (f"Audit: {audit.sheets_pass}/{audit.total_sheets} sheets pass.\n"
                           "Grouping requires all to pass. Check Audit_Detail.csv.")
            else:
                summary = _format_group(conv, audit, grp, mode, threshold)

        # Zip the output
        zip_path = os.path.join(job["work_dir"], f"{mode}_results.zip")
        with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
            for fname in os.listdir(out_dir):
                zf.write(os.path.join(out_dir, fname), fname)

        # Also save to persistent output root
        persist_dir = os.path.join(OUTPUT_ROOT, job_id)
        shutil.copytree(out_dir, persist_dir, dirs_exist_ok=True)

        job["result_zip"] = zip_path
        job["status"] = "done"
        job["summary"] = summary
        job["progress_events"].append({"stage": "Complete", "step": 1, "total": 1})

    except Exception as e:
        job["status"] = "error"
        job["error"] = str(e)
        job["progress_events"].append({"stage": f"Error: {e}", "step": 0, "total": 1})


# ── SSE progress stream ─────────────────────────────────────
@app.get("/api/progress/{job_id}")
async def progress_stream(job_id: str, session: str | None = Cookie(None)):
    _check_auth(session)
    if job_id not in jobs:
        raise HTTPException(404, "Job not found")

    async def event_generator():
        seen = 0
        while True:
            job = jobs[job_id]
            events = job["progress_events"]
            while seen < len(events):
                evt = events[seen]
                yield f"data: {_json_dumps(evt)}\n\n"
                seen += 1
            if job["status"] in ("done", "error"):
                final = {"status": job["status"]}
                if job["status"] == "done":
                    final["summary"] = job.get("summary", "")
                else:
                    final["error"] = job.get("error", "Unknown error")
                yield f"data: {_json_dumps(final)}\n\n"
                break
            await asyncio.sleep(0.3)

    return StreamingResponse(event_generator(), media_type="text/event-stream")


# ── Download ─────────────────────────────────────────────────
@app.get("/api/download/{job_id}")
async def download_zip(job_id: str, session: str | None = Cookie(None)):
    _check_auth(session)
    if job_id not in jobs:
        raise HTTPException(404, "Job not found")
    job = jobs[job_id]
    if job["status"] != "done" or not job["result_zip"]:
        raise HTTPException(400, "Job not ready")
    return FileResponse(
        job["result_zip"],
        media_type="application/zip",
        filename=os.path.basename(job["result_zip"]),
    )


# ── Helpers ──────────────────────────────────────────────────
def _json_dumps(obj):
    import json
    return json.dumps(obj)


def _format_convert(c, mode):
    lines = [f"{mode} Conversion Done!",
             f"Sheets processed: {c.sheets_processed}",
             f"Data rows: {c.data_row_count} | Value rows: {c.value_row_count}",
             f"Total sets: {c.total_sets}",
             f"Skipped: {c.skip_summary}"]
    if mode == "FTA":
        lines.insert(2, f"Combined: {c.combined_sets} | INC-only: {c.inc_only_sets} | MAT-only: {c.mat_only_sets}")
        lines.insert(3, f"Merge differences: {c.diff_count}")
    return "\n".join(lines)


def _format_audit(c, a, mode):
    lines = [_format_convert(c, mode), "",
             f"Audit: {a.sheets_pass}/{a.total_sheets} sheets pass"]
    if a.sheets_pass < a.total_sheets:
        lines.append("WARNING: Some sheets failed. See Audit_Detail.csv.")
    return "\n".join(lines)


def _format_group(c, a, g, mode, threshold):
    lines = [_format_audit(c, a, mode), "",
             f"Grouping (threshold={threshold}):",
             f"  Total sets: {g.total_sets}",
             f"  Cells: {g.original_cells} -> {g.grouped_cells} ({g.reduction_pct:.1f}% reduction)",
             f"  Sheets grouped: {g.sheets_grouped}"]
    return "\n".join(lines)


# ── Serve React build (static files) ────────────────────────
frontend_build = Path(__file__).parent.parent / "frontend" / "build"
if frontend_build.exists():
    app.mount("/static", StaticFiles(directory=str(frontend_build / "static")), name="static")

    @app.get("/{full_path:path}")
    async def serve_react(full_path: str):
        # Serve index.html for all non-API routes (React SPA routing)
        file_path = frontend_build / full_path
        if full_path and file_path.exists() and file_path.is_file():
            return FileResponse(str(file_path))
        return FileResponse(str(frontend_build / "index.html"))
