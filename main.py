"""
main.py — FastAPI Server
-------------------------
All errors and key operations are printed to stdout.

Project structure:
    main.py         ← FastAPI routes + DB logic
    extractor.py    ← HBLExtractor class (async)
    index.html      ← Frontend UI
    uploads/        ← Saved PDFs (auto-created)
    extracted/      ← Saved JSON files (auto-created)
"""

import os
import json
import uuid
import datetime
import traceback
import asyncio
import io
from typing import List, Optional

import psycopg2
from psycopg2.extras import RealDictCursor
from fastapi import FastAPI, File, UploadFile, HTTPException, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, FileResponse, StreamingResponse
from helper.helper_funtion import HelperFunctionController
from dotenv import load_dotenv
load_dotenv()
from extractor import HBLExtractor

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

DB_CONFIG = {
    "host":     os.getenv("DB_HOST",     "localhost"),
    "port":     int(os.getenv("DB_PORT", "5432")),
    "database": os.getenv("DB_NAME",     "pdfmanager"),
    "user":     os.getenv("DB_USER",     "testuser"),
    "password": os.getenv("DB_PASSWORD", "yourpassword"),
}

UPLOAD_DIR = "uploads"
JSON_DIR   = "extracted"
os.makedirs(UPLOAD_DIR, exist_ok=True)
os.makedirs(JSON_DIR,   exist_ok=True)

# Shared extractor instance
extractor = HBLExtractor()

# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------

def get_db():
    try:
        conn = psycopg2.connect(**DB_CONFIG, cursor_factory=RealDictCursor)
        return conn
    except psycopg2.OperationalError as e:
        print(f"[DB][ERROR] Connection failed | reason={str(e)}")
        raise
    except Exception as e:
        print(f"[DB][ERROR] Unexpected connection error | reason={str(e)}")
        raise


def generate_txn_id() -> str:
    date_part   = datetime.datetime.now().strftime("%Y%m%d")
    unique_part = uuid.uuid4().hex[:6].upper()
    txn_id      = f"TXN-{date_part}-{unique_part}"
    print(f"[DB] Generated txn_id={txn_id}")
    return txn_id


def format_ts(ts):
    if ts is None:
        return None
    if isinstance(ts, str):
        return ts
    return ts.strftime("%Y-%m-%d %H:%M:%S")


def row_to_dict(row: dict) -> dict:
    """Convert a DB row to API response dict, unpacking JSONB fields."""
    ed   = row["api_payload"] or {}
    data = ed.get("data", {})
    return {
        "txn_id":            row["txn_id"],
        "filename":          row["filename"],
        "file_path":         row["file_path"],
        "size_kb":           row["size_kb"],
        "pdf_type":          row["pdf_type"],
        "status":            row["status"],
        "error_message":     row["error_message"],
        "uploaded_at":       format_ts(row["uploaded_at"]),
        "processed_at":      format_ts(row["processed_at"]),
        # From JSONB
        "diff_summary":      ed.get("diff_summary"),
        "input_tokens":      ed.get("input_tokens", 0),
        "output_tokens":     ed.get("output_tokens", 0),
        "cost_usd":          round(ed.get("cost_usd", 0), 4),
        "json_file":         ed.get("json_file"),
        # Key HBL fields
        "bl_number":         data.get("document", {}).get("number"),
        "shipper_name":      data.get("shipper",  {}).get("name"),
        "consignee_name":    data.get("consignee",{}).get("name"),
        "port_of_loading":   data.get("routing",  {}).get("port_of_loading"),
        "port_of_discharge": data.get("routing",  {}).get("port_of_discharge"),
        "on_board_date":     data.get("routing",  {}).get("on_board_date"),
    }

# ---------------------------------------------------------------------------
# Background extraction task
# ---------------------------------------------------------------------------

async def run_extraction(txn_id: str, pdf_path: str, filename: str):
    """
    Async background task:
      1. Mark status = processing
      2. Call HBLExtractor.extract()
      3. Save JSON file to extracted/
      4. Update DB with result or error
    """
    print(f"[Task] Extraction started | txn_id={txn_id} | file={filename}")

    # ── Mark as processing ────────────────────────────────────────────────────
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE public.pdf_uploads SET status='processing' WHERE txn_id=%s",
                    (txn_id,)
                )
            conn.commit()
        print(f"[Task] Status set to processing | txn_id={txn_id}")
    except psycopg2.Error as e:
        print(f"[Task][ERROR] DB update to processing failed | txn_id={txn_id} | reason={str(e)}")
        print(traceback.format_exc())
        return
    except Exception as e:
        print(f"[Task][ERROR] Unexpected error marking processing | txn_id={txn_id} | reason={str(e)}")
        print(traceback.format_exc())
        return

    # ── Run extraction ────────────────────────────────────────────────────────
    try:
        result = await extractor.extract(pdf_path)
    except Exception as e:
        # Catch any unexpected crash from extractor itself
        error_msg = f"Extractor crash: {type(e).__name__}: {str(e)}\n{traceback.format_exc()}"
        print(f"[Task][ERROR] Extractor crashed | txn_id={txn_id} | reason={str(e)}")
        print(traceback.format_exc())
        result = {"success": False, "pdf_type": "unknown", "error": error_msg}

    # ── Handle failed extraction ──────────────────────────────────────────────
    if not result["success"]:
        print(f"[Task] Extraction failed | txn_id={txn_id} | error={result['error'][:200]}")
        try:
            with get_db() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        UPDATE public.pdf_uploads
                        SET status='failed', error_message=%s, processed_at=NOW()
                        WHERE txn_id=%s
                    """, (str(result["error"])[:3000], txn_id))
                conn.commit()
            print(f"[Task] Status set to failed | txn_id={txn_id}")
        except psycopg2.Error as e:
            print(f"[Task][ERROR] DB update to failed error | txn_id={txn_id} | reason={str(e)}")
            print(traceback.format_exc())
        except Exception as e:
            print(f"[Task][ERROR] Unexpected DB error on failure update | txn_id={txn_id} | reason={str(e)}")
            print(traceback.format_exc())
        return

    # ── Save JSON file to disk ────────────────────────────────────────────────
    json_path = None
    try:
        json_filename = f"{os.path.splitext(filename)[0]}_{txn_id}.json"
        json_path     = os.path.join(JSON_DIR, json_filename)
        # with open(json_path, "w", encoding="utf-8") as f:
        #     json.dump(result["extracted_data"], f, indent=2, ensure_ascii=False)
        print(f"[Task] JSON file saved | txn_id={txn_id} | path={json_path}")
    except OSError as e:
        # Non-fatal — data still saved to DB; download falls back to DB
        print(f"[Task][WARN] Could not save JSON file to disk | txn_id={txn_id} | reason={str(e)}")
        json_path = None
    except Exception as e:
        print(f"[Task][WARN] Unexpected error saving JSON file | txn_id={txn_id} | reason={str(e)}")
        print(traceback.format_exc())
        json_path = None

    # ── Build JSONB payload ───────────────────────────────────────────────────
    stored_json = {
        "data":         result["extracted_data"],
        "diff_summary": result["diff_summary"],
        "input_tokens": result["input_tokens"],
        "output_tokens":result["output_tokens"],
        "cost_usd":     result["cost_usd"],
        "json_file":    json_path,
    }

    # ── Update DB to done ─────────────────────────────────────────────────────
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE public.pdf_uploads SET
                        pdf_type       = %s,
                        status         = 'done',
                        extracted_data = %s,
                        processed_at   = NOW()
                    WHERE txn_id = %s
                """, (result["pdf_type"], json.dumps(stored_json), txn_id))
            conn.commit()
        print(f"[Task] Status set to done | txn_id={txn_id} | pdf_type={result['pdf_type']} | cost_usd={result['cost_usd']}")
    except psycopg2.Error as e:
        print(f"[Task][ERROR] DB update to done failed | txn_id={txn_id} | reason={str(e)}")
        print(traceback.format_exc())
    except Exception as e:
        print(f"[Task][ERROR] Unexpected DB error on done update | txn_id={txn_id} | reason={str(e)}")
        print(traceback.format_exc())

# ---------------------------------------------------------------------------
# FastAPI app
# ---------------------------------------------------------------------------

app = FastAPI(title="HBL PDF Manager")

app.add_middleware(
    CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"]
)


@app.get("/")
def serve_index():
    return FileResponse("index.html")


# ── Upload ────────────────────────────────────────────────────────────────────

# @app.post("/upload")
# async def upload_pdfs(
#     background_tasks: BackgroundTasks,
#     files: List[UploadFile] = File(...)
# ):
#     print(f"[Upload] Received {len(files)} file(s)")
#     results = []
#     errors  = []

#     for file in files:
#         print(f"[Upload] Processing file | filename={file.filename}")

#         # Validate file type
#         if not file.filename.lower().endswith(".pdf"):
#             print(f"[Upload][WARN] Rejected non-PDF file | filename={file.filename}")
#             errors.append({"filename": file.filename, "error": "Only PDF files are accepted"})
#             continue

#         txn_id    = generate_txn_id()
#         file_path = os.path.join(UPLOAD_DIR, f"{txn_id}_{file.filename}")

#         # Save PDF to disk
#         try:
#             content = await file.read()
#             with open(file_path, "wb") as buf:
#                 buf.write(content)
#             print(f"[Upload] File saved to disk | txn_id={txn_id} | path={file_path}")
#         except OSError as e:
#             print(f"[Upload][ERROR] File save failed | filename={file.filename} | reason={str(e)}")
#             print(traceback.format_exc())
#             errors.append({"filename": file.filename, "error": f"File save failed: {str(e)}"})
#             continue
#         except Exception as e:
#             print(f"[Upload][ERROR] Unexpected error saving file | filename={file.filename} | reason={str(e)}")
#             print(traceback.format_exc())
#             errors.append({"filename": file.filename, "error": f"Unexpected error: {str(e)}"})
#             continue

#         size_kb = round(os.path.getsize(file_path) / 1024, 2)

#         # Insert pending record into DB
#         try:
#             with get_db() as conn:
#                 with conn.cursor() as cur:
#                     cur.execute("""
#                         INSERT INTO public.pdf_uploads
#                             (txn_id, filename, file_path, size_kb, pdf_type, status)
#                         VALUES (%s, %s, %s, %s, 'unknown', 'pending')
#                     """, (txn_id, file.filename, file_path, size_kb))
#                 conn.commit()
#             print(f"[Upload] DB record inserted | txn_id={txn_id} | size_kb={size_kb}")
#         except psycopg2.IntegrityError as e:
#             print(f"[Upload][ERROR] DB integrity error | txn_id={txn_id} | reason={str(e)}")
#             print(traceback.format_exc())
#             errors.append({"filename": file.filename, "error": f"DB integrity error: {str(e)}"})
#             try:
#                 os.remove(file_path)
#             except OSError:
#                 pass
#             continue
#         except psycopg2.Error as e:
#             print(f"[Upload][ERROR] DB insert failed | txn_id={txn_id} | reason={str(e)}")
#             print(traceback.format_exc())
#             errors.append({"filename": file.filename, "error": f"DB insert failed: {str(e)}"})
#             try:
#                 os.remove(file_path)
#             except OSError:
#                 pass
#             continue
#         except Exception as e:
#             print(f"[Upload][ERROR] Unexpected DB error | txn_id={txn_id} | reason={str(e)}")
#             print(traceback.format_exc())
#             errors.append({"filename": file.filename, "error": f"Unexpected error: {str(e)}"})
#             try:
#                 os.remove(file_path)
#             except OSError:
#                 pass
#             continue

#         # Queue background extraction
#         background_tasks.add_task(run_extraction, txn_id, file_path, file.filename)
#         print(f"[Upload] Extraction queued | txn_id={txn_id}")

#         results.append({
#             "txn_id":   txn_id,
#             "filename": file.filename,
#             "size_kb":  size_kb,
#             "status":   "pending",
#             "message":  "Uploaded. Claude extraction queued..."
#         })

#     print(f"[Upload] Done | success={len(results)} | errors={len(errors)}")
#     return JSONResponse({"uploaded": results, "errors": errors, "total": len(results)})
@app.post("/workflow/upload")
async def upload_pdfs(
    background_tasks: BackgroundTasks,
    hbl_attachments: List[UploadFile] = File(default=[]),
    mbl_attachments: List[UploadFile] = File(default=[])
):
    print(f"[Upload] Received {len(hbl_attachments)} HBL + {len(mbl_attachments)} MBL file(s)")

    if not hbl_attachments and not mbl_attachments:
        return JSONResponse(
            status_code=400,
            content={"error": "No files uploaded. Provide hbl_attachments or mbl_attachments."}
        )

    # One shared txn_id for the entire upload batch
    txn_id = generate_txn_id()
    print(f"[Upload] Generated shared txn_id={txn_id} for this batch")

    results = []
    errors  = []

    tagged_files = (
        [(file, "hbl") for file in hbl_attachments] +
        [(file, "mbl") for file in mbl_attachments]
    )

    for file, pdf_type in tagged_files:
        print(f"[Upload] Processing file | txn_id={txn_id} | filename={file.filename} | pdf_type={pdf_type}")

        # Validate file type
        if not file.filename.lower().endswith(".pdf"):
            print(f"[Upload][WARN] Rejected non-PDF | txn_id={txn_id} | filename={file.filename}")
            errors.append({"filename": file.filename, "pdf_type": pdf_type, "error": "Only PDF files are accepted"})
            continue

        file_path = os.path.join(UPLOAD_DIR, f"{txn_id}_{pdf_type}_{file.filename}")

        # Save PDF to disk
        try:
            content = await file.read()
            with open(file_path, "wb") as buf:
                buf.write(content)
            print(f"[Upload] File saved | txn_id={txn_id} | pdf_type={pdf_type} | path={file_path}")
        except OSError as e:
            print(f"[Upload][ERROR] File save failed | txn_id={txn_id} | filename={file.filename} | reason={str(e)}")
            print(traceback.format_exc())
            errors.append({"filename": file.filename, "pdf_type": pdf_type, "error": f"File save failed: {str(e)}"})
            continue
        except Exception as e:
            print(f"[Upload][ERROR] Unexpected error | txn_id={txn_id} | filename={file.filename} | reason={str(e)}")
            print(traceback.format_exc())
            errors.append({"filename": file.filename, "pdf_type": pdf_type, "error": f"Unexpected error: {str(e)}"})
            continue

        size_kb = round(os.path.getsize(file_path) / 1024, 2)

        # Insert DB record — all rows share the same txn_id
        try:
            with get_db() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        INSERT INTO public.pdf_uploads
                            (txn_id, filename, file_path, size_kb, file_type, status)
                        VALUES (%s, %s, %s, %s, %s, 'pending')
                    """, (txn_id, file.filename, file_path, size_kb, pdf_type))
                conn.commit()
            print(f"[Upload] DB record inserted | txn_id={txn_id} | filename={file.filename} | pdf_type={pdf_type}")
        except psycopg2.IntegrityError as e:
            print(f"[Upload][ERROR] DB integrity error | txn_id={txn_id} | reason={str(e)}")
            print(traceback.format_exc())
            errors.append({"filename": file.filename, "pdf_type": pdf_type, "error": f"DB integrity error: {str(e)}"})
            try: os.remove(file_path)
            except OSError: pass
            continue
        except psycopg2.Error as e:
            print(f"[Upload][ERROR] DB insert failed | txn_id={txn_id} | reason={str(e)}")
            print(traceback.format_exc())
            errors.append({"filename": file.filename, "pdf_type": pdf_type, "error": f"DB insert failed: {str(e)}"})
            try: os.remove(file_path)
            except OSError: pass
            continue
        except Exception as e:
            print(f"[Upload][ERROR] Unexpected DB error | txn_id={txn_id} | reason={str(e)}")
            print(traceback.format_exc())
            errors.append({"filename": file.filename, "pdf_type": pdf_type, "error": f"Unexpected error: {str(e)}"})
            try: os.remove(file_path)
            except OSError: pass
            continue

        # ✅ Pass pdf_type to run_extraction
        background_tasks.add_task(run_extraction, txn_id, file_path, file.filename, pdf_type)
        print(f"[Upload] Extraction queued | txn_id={txn_id} | filename={file.filename} | pdf_type={pdf_type}")

        results.append({
            "txn_id":    txn_id,
            "filename":  file.filename,
            "size_kb":   size_kb,
            "pdf_type":  pdf_type,
            "file_type": pdf_type,
            "status":    "pending",
            "message":   "Uploaded. Claude extraction queued..."
        })

    print(f"[Upload] Done | txn_id={txn_id} | success={len(results)} | errors={len(errors)}")
    return JSONResponse({
        "txn_id":   txn_id,
        "uploaded": results,
        "errors":   errors,
        "total":    len(results)
    })


# ---------------------------------------------------------------------------
# Background extraction task
# ---------------------------------------------------------------------------

async def run_extraction(txn_id: str, pdf_path: str, filename: str, pdf_type: str = "hbl"):
    """
    Async background task:
      1. Mark status = processing  (scoped to txn_id + file_type)
      2. Call HBLExtractor.extract() — extractor does NOT accept pdf_type,
         so we pass only pdf_path and inject pdf_type into the result manually
      3. Save JSON file to extracted/
      4. Update DB with result or error (scoped to txn_id + file_type)
    """
    print(f"[Task] Extraction started | txn_id={txn_id} | file={filename} | pdf_type={pdf_type}")

    # ── Mark as processing ─────────────────────────────────────────────────
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    # ✅ Scoped to txn_id + file_type so MBL and HBL don't overwrite each other
                    "UPDATE public.pdf_uploads SET status='processing' WHERE txn_id=%s AND file_type=%s",
                    (txn_id, pdf_type)
                )
            conn.commit()
        print(f"[Task] Status set to processing | txn_id={txn_id} | pdf_type={pdf_type}")
    except psycopg2.Error as e:
        print(f"[Task][ERROR] DB update to processing failed | txn_id={txn_id} | pdf_type={pdf_type} | reason={str(e)}")
        print(traceback.format_exc())
        return
    except Exception as e:
        print(f"[Task][ERROR] Unexpected error marking processing | txn_id={txn_id} | pdf_type={pdf_type} | reason={str(e)}")
        print(traceback.format_exc())
        return

    # ── Run extraction ─────────────────────────────────────────────────────
    try:
        # ✅ FIX: extractor.extract() only accepts pdf_path — no pdf_type kwarg
        # We inject pdf_type into the result manually after the call
        result = await extractor.extract(pdf_path)
        result["pdf_type"] = pdf_type   # ✅ override with the correct file type (mbl or hbl)

    except Exception as e:
        error_msg = f"Extractor crash: {type(e).__name__}: {str(e)}\n{traceback.format_exc()}"
        print(f"[Task][ERROR] Extractor crashed | txn_id={txn_id} | pdf_type={pdf_type} | reason={str(e)}")
        print(traceback.format_exc())
        result = {"success": False, "pdf_type": pdf_type, "error": error_msg}

    # ── Handle failed extraction ───────────────────────────────────────────
    if not result["success"]:
        print(f"[Task] Extraction failed | txn_id={txn_id} | pdf_type={pdf_type} | error={result['error'][:200]}")
        try:
            with get_db() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        UPDATE public.pdf_uploads
                        SET status='failed', error_message=%s, processed_at=NOW()
                        WHERE txn_id=%s AND file_type=%s
                    """, (str(result["error"])[:3000], txn_id, pdf_type))
                conn.commit()
            print(f"[Task] Status set to failed | txn_id={txn_id} | pdf_type={pdf_type}")
        except psycopg2.Error as e:
            print(f"[Task][ERROR] DB update to failed | txn_id={txn_id} | pdf_type={pdf_type} | reason={str(e)}")
            print(traceback.format_exc())
        except Exception as e:
            print(f"[Task][ERROR] Unexpected DB error on failure update | txn_id={txn_id} | pdf_type={pdf_type} | reason={str(e)}")
            print(traceback.format_exc())
        return

    # ── Save JSON file to disk ─────────────────────────────────────────────
    json_path = None
    try:
        json_filename = f"{os.path.splitext(filename)[0]}_{txn_id}.json"
        json_path     = os.path.join(JSON_DIR, json_filename)
        # with open(json_path, "w", encoding="utf-8") as f:
        #     json.dump(result["extracted_data"], f, indent=2, ensure_ascii=False)
        print(f"[Task] JSON path set | txn_id={txn_id} | pdf_type={pdf_type} | path={json_path}")
    except OSError as e:
        print(f"[Task][WARN] Could not save JSON file | txn_id={txn_id} | pdf_type={pdf_type} | reason={str(e)}")
        json_path = None
    except Exception as e:
        print(f"[Task][WARN] Unexpected error saving JSON | txn_id={txn_id} | pdf_type={pdf_type} | reason={str(e)}")
        print(traceback.format_exc())
        json_path = None

    # ── Build JSONB payload ────────────────────────────────────────────────
    stored_json = {
        "data":          result["extracted_data"],
        "diff_summary":  result["diff_summary"],
        "input_tokens":  result["input_tokens"],
        "output_tokens": result["output_tokens"],
        "cost_usd":      result["cost_usd"],
        "json_file":     json_path,
    }

    # ── Update DB to done ──────────────────────────────────────────────────
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE public.pdf_uploads SET
                        pdf_type       = %s,
                        status         = 'done',
                        extracted_data = %s,
                        processed_at   = NOW()
                    WHERE txn_id = %s AND file_type = %s
                """, (result["pdf_type"], json.dumps(stored_json), txn_id, pdf_type))
            conn.commit()
        print(f"[Task] Status set to done | txn_id={txn_id} | pdf_type={pdf_type} | cost_usd={result['cost_usd']}")
    except psycopg2.Error as e:
        print(f"[Task][ERROR] DB update to done failed | txn_id={txn_id} | pdf_type={pdf_type} | reason={str(e)}")
        print(traceback.format_exc())
    except Exception as e:
        print(f"[Task][ERROR] Unexpected DB error on done update | txn_id={txn_id} | pdf_type={pdf_type} | reason={str(e)}")
        print(traceback.format_exc())

# ---------------------------------------------------------------------------
# Background extraction task — ✅ FIXED: now accepts and uses pdf_type
# ---------------------------------------------------------------------------

async def run_extraction(txn_id: str, pdf_path: str, filename: str, pdf_type: str = "hbl"):
    """
    Async background task:
      1. Mark status = processing  (scoped to txn_id + file_type)
      2. Call HBLExtractor.extract() with correct pdf_type
      3. Save JSON file to extracted/
      4. Update DB with result or error (scoped to txn_id + file_type)
    """
    print(f"[Task] Extraction started | txn_id={txn_id} | file={filename} | pdf_type={pdf_type}")

    # ── Mark as processing ─────────────────────────────────────────────────
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    # ✅ FIX: AND file_type=%s scopes the UPDATE to only this file
                    # Without it, both MBL and HBL rows update at the same time
                    "UPDATE public.pdf_uploads SET status='processing' WHERE txn_id=%s AND file_type=%s",
                    (txn_id, pdf_type)
                )
            conn.commit()
        print(f"[Task] Status set to processing | txn_id={txn_id} | pdf_type={pdf_type}")
    except psycopg2.Error as e:
        print(f"[Task][ERROR] DB update to processing failed | txn_id={txn_id} | pdf_type={pdf_type} | reason={str(e)}")
        print(traceback.format_exc())
        return
    except Exception as e:
        print(f"[Task][ERROR] Unexpected error marking processing | txn_id={txn_id} | pdf_type={pdf_type} | reason={str(e)}")
        print(traceback.format_exc())
        return

    # ── Run extraction ─────────────────────────────────────────────────────
    try:
        # ✅ FIX: pass pdf_type to extractor so it uses correct prompt/logic per type
        result = await extractor.extract(pdf_path)
    except Exception as e:
        error_msg = f"Extractor crash: {type(e).__name__}: {str(e)}\n{traceback.format_exc()}"
        print(f"[Task][ERROR] Extractor crashed | txn_id={txn_id} | pdf_type={pdf_type} | reason={str(e)}")
        print(traceback.format_exc())
        result = {"success": False, "pdf_type": pdf_type, "error": error_msg}

    # ── Handle failed extraction ───────────────────────────────────────────
    if not result["success"]:
        print(f"[Task] Extraction failed | txn_id={txn_id} | pdf_type={pdf_type} | error={result['error'][:200]}")
        try:
            with get_db() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        UPDATE public.pdf_uploads
                        SET status='failed', error_message=%s, processed_at=NOW()
                        WHERE txn_id=%s AND file_type=%s
                    """, (str(result["error"])[:3000], txn_id, pdf_type))  # ✅ scoped
                conn.commit()
            print(f"[Task] Status set to failed | txn_id={txn_id} | pdf_type={pdf_type}")
        except psycopg2.Error as e:
            print(f"[Task][ERROR] DB update to failed | txn_id={txn_id} | pdf_type={pdf_type} | reason={str(e)}")
            print(traceback.format_exc())
        except Exception as e:
            print(f"[Task][ERROR] Unexpected DB error on failure update | txn_id={txn_id} | pdf_type={pdf_type} | reason={str(e)}")
            print(traceback.format_exc())
        return

    # ── Save JSON file to disk ─────────────────────────────────────────────
    json_path = None
    try:
        json_filename = f"{os.path.splitext(filename)[0]}_{txn_id}.json"
        json_path     = os.path.join(JSON_DIR, json_filename)
        # with open(json_path, "w", encoding="utf-8") as f:
        #     json.dump(result["extracted_data"], f, indent=2, ensure_ascii=False)
        print(f"[Task] JSON path set | txn_id={txn_id} | pdf_type={pdf_type} | path={json_path}")
    except OSError as e:
        print(f"[Task][WARN] Could not save JSON file | txn_id={txn_id} | pdf_type={pdf_type} | reason={str(e)}")
        json_path = None
    except Exception as e:
        print(f"[Task][WARN] Unexpected error saving JSON | txn_id={txn_id} | pdf_type={pdf_type} | reason={str(e)}")
        print(traceback.format_exc())
        json_path = None

    # ── Build JSONB payload ────────────────────────────────────────────────
    stored_json = {
        "data":          result["extracted_data"],
        "diff_summary":  result["diff_summary"],
        "input_tokens":  result["input_tokens"],
        "output_tokens": result["output_tokens"],
        "cost_usd":      result["cost_usd"],
        "json_file":     json_path,
    }

    # ── Update DB to done ──────────────────────────────────────────────────
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE public.pdf_uploads SET
                        pdf_type       = %s,
                        status         = 'done',
                        extracted_data = %s,
                        processed_at   = NOW()
                    WHERE txn_id = %s AND file_type = %s
                """, (result["pdf_type"], json.dumps(stored_json), txn_id, pdf_type))  # ✅ scoped
            conn.commit()
        print(f"[Task] Status set to done | txn_id={txn_id} | pdf_type={pdf_type} | cost_usd={result['cost_usd']}")
    except psycopg2.Error as e:
        print(f"[Task][ERROR] DB update to done failed | txn_id={txn_id} | pdf_type={pdf_type} | reason={str(e)}")
        print(traceback.format_exc())
    except Exception as e:
        print(f"[Task][ERROR] Unexpected DB error on done update | txn_id={txn_id} | pdf_type={pdf_type} | reason={str(e)}")
        print(traceback.format_exc())# ── List files ────────────────────────────────────────────────────────────────

@app.get("/workflow/files")
def list_files(status: Optional[str] = None, search: Optional[str] = None):
    print(f"[ListFiles] Request | status={status} | search={search}")
    try:
        query  = "SELECT * FROM tv_upload_list"
        params = []
        wheres = []

        if status:
            wheres.append("status = %s")
            params.append(status)
        if search:
            wheres.append("(txn_id ILIKE %s OR filename ILIKE %s)")
            params.extend([f"%{search}%", f"%{search}%"])
        if wheres:
            query += " WHERE " + " AND ".join(wheres)

        query += " ORDER BY uploaded_at DESC"

        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute(query, params)
                rows = cur.fetchall()

        print(f"[ListFiles] Returned {len(rows)} record(s)")
        return {"files": rows, "total": len(rows)}

    except psycopg2.Error as e:
        print(f"[ListFiles][ERROR] DB error | reason={str(e)}")
        print(traceback.format_exc())
        raise HTTPException(status_code=500, detail=f"DB error: {str(e)}")

    except Exception as e:
        print(f"[ListFiles][ERROR] Unexpected error | reason={str(e)}")
        print(traceback.format_exc())
        raise HTTPException(status_code=500, detail=f"Unexpected error: {str(e)}")

# @app.get("/uploaded_pdf_list")
# async def uploaded_pdf_list(
#     status:    Optional[str] = Query(None, description="pending | processing | done | failed | job_created"),
#     file_type: Optional[str] = Query(None, description="hbl | mbl"),
#     search:    Optional[str] = Query(None, description="Search txn_id, filename"),
#     page:      int           = Query(1,  ge=1),
#     per_page:  int           = Query(25, ge=1, le=100),
# ):
#     try:
#         conditions  = []
#         params      = []
#         i = 1

#         if status:
#             conditions.append(f"status::text = ${i}")
#             params.append(status)
#             i += 1

#         if file_type:
#             conditions.append(f"file_type::text = ${i}")
#             params.append(file_type)
#             i += 1

#         if search:
#             # Only real table columns — txn_id and filename
#             conditions.append(f"(txn_id ILIKE ${i} OR filename ILIKE ${i})")
#             params.append(f"%{search}%")
#             i += 1

#         where = ("WHERE " + " AND ".join(conditions)) if conditions else ""

#         # ── COUNT (no limit/offset) ───────────────────────────────────────
#         count_sql = f"SELECT COUNT(*) FROM public.pdf_uploads {where}"

#         # ── DATA fetch ────────────────────────────────────────────────────
#         # Only select the 10 real columns that actually exist in the table
#         data_sql = f"""
#             SELECT
#                 id,
#                 txn_id,
#                 filename,
#                 file_path,
#                 size_kb,
#                 pdf_type,
#                 status,
#                 file_type,
#                 error_message,
#                 extracted_data,
#                 uploaded_at,
#                 processed_at
#             FROM public.pdf_uploads
#             {where}
#             ORDER BY uploaded_at DESC
#             LIMIT ${i} OFFSET ${i + 1}
#         """
#         # append limit / offset only for data query
#         data_params = params + [per_page, (page - 1) * per_page]

#         async with app.state.db_pool.acquire() as conn:
#             total = await conn.fetchval(count_sql, *params)
#             rows  = await conn.fetch(data_sql, *data_params)

#         files = []
#         for row in rows:

#             # ── Parse extracted_data JSONB ────────────────────────────────
#             # Structure:  { "data": { ...doc fields... },
#             #               "cost_usd": 0.072,
#             #               "diff_summary": "...",
#             #               "input_tokens": 8764,
#             #               "output_tokens": 3083,
#             #               "json_file": "extracted/..." }
#             raw = row["extracted_data"]
#             if isinstance(raw, str):
#                 raw = json.loads(raw)
#             raw = raw or {}

#             doc          = raw.get("data",         {}) or {}
#             cost_usd     = raw.get("cost_usd")
#             diff_summary = raw.get("diff_summary")
#             input_tokens = raw.get("input_tokens")
#             output_tokens= raw.get("output_tokens")
#             json_file    = raw.get("json_file")

#             # ── Doc-level fields (MBL fields live here directly) ──────────
#             housing    = doc.get("housing_details",  []) or []
#             containers = doc.get("container_details",[]) or []
#             routings   = doc.get("ocean_routings",   []) or []
#             first      = housing[0] if housing else {}

#             files.append({
#                 # ── Real table columns ────────────────────────────────────
#                 "id":            row["id"],
#                 "txn_id":        row["txn_id"],
#                 "filename":      row["filename"],
#                 "file_path":     row["file_path"],
#                 "size_kb":       float(row["size_kb"]) if row["size_kb"] else 0.0,
#                 "pdf_type":      row["pdf_type"],
#                 "status":        row["status"],
#                 "file_type":     row["file_type"],
#                 "error_message": row["error_message"],
#                 "uploaded_at":   row["uploaded_at"].isoformat()  if row["uploaded_at"]  else None,
#                 "processed_at":  row["processed_at"].isoformat() if row["processed_at"] else None,

#                 # ── From extracted_data root ──────────────────────────────
#                 "cost_usd":      cost_usd,
#                 "diff_summary":  diff_summary,
#                 "input_tokens":  input_tokens,
#                 "output_tokens": output_tokens,
#                 "json_file":     json_file,

#                 # ── From extracted_data.data (doc-level) ──────────────────
#                 "mbl_number":        doc.get("mbl_number"),
#                 "vessel_name":       doc.get("vessel_name"),
#                 "voyage_number":     doc.get("voyage_number"),
#                 "carrier_code":      doc.get("carrier_code"),
#                 "service":           doc.get("service"),
#                 "service_type":      doc.get("service_type"),
#                 "origin_code":       doc.get("origin_code"),
#                 "destination_code":  doc.get("destination_code"),
#                 "port_of_loading":   doc.get("port_of_loading"),
#                 "port_of_discharge": doc.get("port_of_discharge"),
#                 "place_of_receipt":  doc.get("place_of_receipt"),
#                 "place_of_delivery": doc.get("place_of_delivery"),
#                 "freight_payable_at":doc.get("freight_payable_at"),
#                 "export_reference":  doc.get("export_reference"),
#                 "agent":             doc.get("agent"),
#                 "ocean_routings":    routings,
#                 "container_details": containers,

#                 # ── From housing_details[0] (for quick list display) ──────
#                 "hbl_number":            first.get("hbl_number"),
#                 "bl_number":             first.get("hbl_number"),   # alias for frontend
#                 "shipper_name":          first.get("shipper_name"),
#                 "consignee_name":        first.get("consignee_name"),
#                 "on_board_date":         first.get("on_board_date"),
#                 "freight_terms":         first.get("freight_terms"),
#                 "commodity_description": first.get("commodity_description"),
#                 "shipment_terms_code":   first.get("shipment_terms_code"),
#                 "cargo_summary":         first.get("cargo_summary"),
#                 "housing_details":       housing,
#             })

#         return JSONResponse({
#             "files":       files,
#             "total":       total,
#             "page":        page,
#             "per_page":    per_page,
#             "total_pages": (total + per_page - 1) // per_page,
#             "has_next":    page * per_page < total,
#             "has_prev":    page > 1,
#         })

#     except asyncpg.PostgresError as e:
#         raise HTTPException(status_code=500, detail=f"DB error: {str(e)}")
#     except Exception as e:
#         raise HTTPException(status_code=500, detail=f"Error: {str(e)}")
# ── Get single record ─────────────────────────────────────────────────────────

@app.get("/workflow/files/{txn_id}")
def get_file(txn_id: str):
    print(f"[GetFile] Request | txn_id={txn_id}")
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT * FROM public.pdf_uploads WHERE txn_id = %s",
                    (txn_id,)
                )
                row = cur.fetchone()

        if not row:
            print(f"[GetFile][WARN] Record not found | txn_id={txn_id}")
            raise HTTPException(status_code=404, detail=f"No record found: {txn_id}")

        result = row_to_dict(row)
        ed = row["extracted_data"] or {}
        result["extracted_data"] = ed.get("data")
        print(f"[GetFile] Record found | txn_id={txn_id} | status={row['status']}")
        return result

    except HTTPException:
        raise

    except psycopg2.Error as e:
        print(f"[GetFile][ERROR] DB error | txn_id={txn_id} | reason={str(e)}")
        print(traceback.format_exc())
        raise HTTPException(status_code=500, detail=f"DB error: {str(e)}")

    except Exception as e:
        print(f"[GetFile][ERROR] Unexpected error | txn_id={txn_id} | reason={str(e)}")
        print(traceback.format_exc())
        raise HTTPException(status_code=500, detail=f"Unexpected error: {str(e)}")


# ── Download extracted JSON ───────────────────────────────────────────────────

@app.get("/workflow/files/{txn_id}/download")
def download_json(txn_id: str):
    print(f"[Download] Request | txn_id={txn_id}")
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT filename, status, extracted_data FROM public.pdf_uploads WHERE txn_id=%s",
                    (txn_id,)
                )
                row = cur.fetchone()

        if not row:
            print(f"[Download][WARN] Record not found | txn_id={txn_id}")
            raise HTTPException(status_code=404, detail="Record not found")

        if row["status"] != "done":
            print(f"[Download][WARN] Extraction not complete | txn_id={txn_id} | status={row['status']}")
            raise HTTPException(
                status_code=400,
                detail=f"Extraction not complete. Current status: {row['status']}"
            )

        ed        = row["extracted_data"] or {}
        data      = ed.get("data")
        json_file = ed.get("json_file")

        # Serve from disk
        if json_file and os.path.exists(json_file):
            download_name = os.path.splitext(row["filename"])[0] + ".json"
            print(f"[Download] Serving from disk | txn_id={txn_id} | path={json_file}")
            return FileResponse(json_file, media_type="application/json", filename=download_name)

        # Fallback — stream from DB
        if not data:
            print(f"[Download][ERROR] No extracted data available | txn_id={txn_id}")
            raise HTTPException(status_code=404, detail="No extracted data available")

        print(f"[Download] Serving from DB (file not on disk) | txn_id={txn_id}")
        json_bytes    = json.dumps(data, indent=2, ensure_ascii=False).encode("utf-8")
        download_name = os.path.splitext(row["filename"])[0] + ".json"
        return StreamingResponse(
            io.BytesIO(json_bytes),
            media_type="application/json",
            headers={"Content-Disposition": f'attachment; filename="{download_name}"'}
        )

    except HTTPException:
        raise

    except psycopg2.Error as e:
        print(f"[Download][ERROR] DB error | txn_id={txn_id} | reason={str(e)}")
        print(traceback.format_exc())
        raise HTTPException(status_code=500, detail=f"DB error: {str(e)}")

    except Exception as e:
        print(f"[Download][ERROR] Unexpected error | txn_id={txn_id} | reason={str(e)}")
        print(traceback.format_exc())
        raise HTTPException(status_code=500, detail=f"Unexpected error: {str(e)}")


# ── Delete files ──────────────────────────────────────────────────────────────

@app.delete("/workflow/files")
def delete_files(txn_ids: List[str]):
    print(f"[Delete] Request | txn_ids={txn_ids}")
    deleted = []
    errors  = []

    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                for txn_id in txn_ids:
                    try:
                        cur.execute(
                            "SELECT file_path, extracted_data FROM public.pdf_uploads WHERE txn_id=%s",
                            (txn_id,)
                        )
                        row = cur.fetchone()

                        if not row:
                            print(f"[Delete][WARN] txn_id not found | txn_id={txn_id}")
                            errors.append({"txn_id": txn_id, "error": "Not found in DB"})
                            continue

                        # Remove PDF file from disk
                        if row["file_path"] and os.path.exists(row["file_path"]):
                            try:
                                os.remove(row["file_path"])
                                print(f"[Delete] PDF file removed | txn_id={txn_id} | path={row['file_path']}")
                            except OSError as e:
                                print(f"[Delete][WARN] Could not remove PDF file | txn_id={txn_id} | reason={str(e)}")
                                errors.append({"txn_id": txn_id, "error": f"PDF delete error: {str(e)}"})

                        # Remove JSON file from disk
                        ed        = row["extracted_data"] or {}
                        json_file = ed.get("json_file")
                        if json_file and os.path.exists(json_file):
                            try:
                                os.remove(json_file)
                                print(f"[Delete] JSON file removed | txn_id={txn_id} | path={json_file}")
                            except OSError as e:
                                print(f"[Delete][WARN] Could not remove JSON file | txn_id={txn_id} | reason={str(e)}")

                        cur.execute("DELETE FROM public.pdf_uploads WHERE txn_id=%s", (txn_id,))
                        deleted.append(txn_id)
                        print(f"[Delete] DB record deleted | txn_id={txn_id}")

                    except psycopg2.Error as e:
                        print(f"[Delete][ERROR] DB error for txn_id | txn_id={txn_id} | reason={str(e)}")
                        print(traceback.format_exc())
                        errors.append({"txn_id": txn_id, "error": f"DB error: {str(e)}"})

            conn.commit()
            print(f"[Delete] Done | deleted={deleted} | errors={len(errors)}")

    except psycopg2.Error as e:
        print(f"[Delete][ERROR] DB connection error | reason={str(e)}")
        print(traceback.format_exc())
        raise HTTPException(status_code=500, detail=f"DB connection error: {str(e)}")

    except Exception as e:
        print(f"[Delete][ERROR] Unexpected error | reason={str(e)}")
        print(traceback.format_exc())
        raise HTTPException(status_code=500, detail=f"Unexpected error: {str(e)}")

    return {"deleted": deleted, "errors": errors}

@app.post("/workflow/start_creating_jobs")
async def start_job_creation(background_tasks: BackgroundTasks):
    try:
        with get_db() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                    SELECT
                        * from tv_api_payload
                """)
                data = cur.fetchall()

        if not data:
            return {"message": "No jobs found with status 'done'"}

        background_tasks.add_task(process_jobs, data)
        return {"message": f"{len(data)} jobs queued for creation"}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


async def process_jobs(data: list):
    """Background task: loops through pdf_uploads rows and creates jobs."""
    for row in data:
        mbl_id      = row.get("mbl_id")       # ✅ matches AS mbl_id in query
        txn_id      = row.get("txn_id")
        api_payload = row.get("api_payload")   # ✅ matches AS api_payload in query

        print(f"[Job] Processing | mbl_id={mbl_id} | txn_id={txn_id}")

        try:
            if api_payload:
                print(f"[Job] Payload ready | txn_id={txn_id}")
                await HelperFunctionController.create_job(api_payload)  # ✅ flat dict passed

            # ✅ Mark as job_created
            with get_db() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        UPDATE public.pdf_uploads
                        SET status = 'job_created', processed_at = NOW()
                        WHERE txn_id = %s
                        """,
                        (txn_id,)
                    )
                conn.commit()
            print(f"[Job] Status set to job_created | mbl_id={mbl_id} | txn_id={txn_id}")

        except Exception as e:
            print(f"[Job][ERROR] Job creation failed | mbl_id={mbl_id} | txn_id={txn_id} | reason={e}")

            # ❌ Mark as job_creation_failed
            try:
                with get_db() as conn:
                    with conn.cursor() as cur:
                        cur.execute(
                            """
                            UPDATE public.pdf_uploads
                            SET status = 'job_creation_failed',
                                error_message = %s,
                                processed_at = NOW()
                            WHERE txn_id = %s
                            """,
                            (str(e), txn_id)
                        )
                    conn.commit()
                print(f"[Job] Status set to job_creation_failed | mbl_id={mbl_id}")
            except Exception as db_err:
                print(f"[Job][ERROR] Failed to update failure status | mbl_id={mbl_id} | reason={db_err}")