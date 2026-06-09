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
from fastapi import FastAPI, File, Request, UploadFile, HTTPException, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, FileResponse, StreamingResponse
from helper.helper_funtion import HelperFunctionController
from dotenv import load_dotenv
load_dotenv()
from extractor import HBLExtractor
from routes.dropdown import router as dropdown_router
from Config import s3_storage

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
# JWT helpers
# ---------------------------------------------------------------------------

def _auth_token(request: Request) -> str:
    auth = request.headers.get("Authorization", "")
    if auth.startswith("Bearer "):
        return auth[len("Bearer "):]
    return ""


def _token_payload(request: Request) -> dict:
    """
    Decode the JWT from the Authorization header and return its JSON payload.
    Signature verification is skipped — trust the auth middleware for that.
    Returns an empty dict if the token is missing or malformed.
    """
    import base64
    try:
        token = _auth_token(request)
        if not token:
            return {}
        # JWT = header.payload.signature  — we only need the payload part
        payload_b64 = token.split(".")[1]
        # Restore base64 padding
        payload_b64 += "=" * (-len(payload_b64) % 4)
        payload =json.loads(base64.urlsafe_b64decode(payload_b64))
        print(f"[JWT] Decoding token | payload_b64={payload}")
        return payload
    except Exception as e:
        print(f"[JWT] Error decoding token | error={str(e)}")
        return {}


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


def _fetch_txn_file_urls(txn_ids: List[str]) -> dict:
    """Return {txn_id: {"hbl": url, "mbl": url}} from pdf_uploads."""
    if not txn_ids:
        return {}
    url_map = {}
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT txn_id, file_type, file_path FROM public.pdf_uploads WHERE txn_id = ANY(%s)",
                (txn_ids,),
            )
            for rec in cur.fetchall():
                txn = rec["txn_id"]
                ft = (rec["file_type"] or "").lower()
                url = s3_storage.normalize_file_url(rec["file_path"])
                if url:
                    url_map.setdefault(txn, {})[ft] = url
    return url_map


def _apply_file_urls(row: dict, url_map: dict) -> dict:
    r = dict(row)
    urls = url_map.get(r.get("txn_id"), {})
    r["hbl_file_url"] = urls.get("hbl")
    r["mbl_file_url"] = urls.get("mbl")
    r["file_url"] = (
        r.get("mbl_file_url")
        or r.get("hbl_file_url")
        or s3_storage.normalize_file_url(r.get("file_path"))
    )
    return r


def row_to_dict(row: dict) -> dict:
    """Convert a DB row to API response dict, unpacking JSONB fields."""
    ed   = row["api_payload"] or {}
    data = ed.get("data", {})
    return {
        "txn_id":            row["txn_id"],
        "filename":          row["filename"],
        "file_path":         row["file_path"],
        "file_url":          s3_storage.normalize_file_url(row.get("file_path")),
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

async def run_extraction(txn_id: str, pdf_path: str, filename: str, pdf_type: str = "hbl"):
    """
    Async background task:
      1. Mark status = processing  (scoped to txn_id + file_type)
      2. Call HBLExtractor.extract() with correct pdf_type
      3. Save JSON file to extracted/
      4. Update DB with result or error (scoped to txn_id + file_type)
    """
    print(f"[Task] Extraction started | txn_id={txn_id} | file={filename} | pdf_type={pdf_type}")

    try:
        # ── Mark as processing ─────────────────────────────────────────────────
        try:
            with get_db() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "UPDATE public.pdf_uploads SET status='processing', final_status='processing' WHERE txn_id=%s AND file_type=%s AND filename=%s",
                        (txn_id, pdf_type, filename)
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
            result = await extractor.extract(pdf_path, schema_type=pdf_type)
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
                        WHERE txn_id = %s AND file_type = %s and filename = %s
                    """, (result["pdf_type"], json.dumps(stored_json), txn_id, pdf_type, filename))
                conn.commit()
                with conn.cursor() as cur:
                    cur.execute("""
                        UPDATE pdf_uploads
                        SET final_status = 'done'
                        WHERE txn_id = %s
                          AND (
                              SELECT COUNT(*) FROM pdf_uploads WHERE txn_id = %s
                          ) = (
                              SELECT COUNT(*) FROM pdf_uploads WHERE txn_id = %s AND status = 'done'
                          )
                    """, (txn_id, txn_id, txn_id))
                conn.commit()
            print(f"[Task] Status set to done | txn_id={txn_id} | pdf_type={pdf_type} | cost_usd={result['cost_usd']}")
        except psycopg2.Error as e:
            print(f"[Task][ERROR] DB update to done failed | txn_id={txn_id} | pdf_type={pdf_type} | reason={str(e)}")
            print(traceback.format_exc())
        except Exception as e:
            print(f"[Task][ERROR] Unexpected DB error on done update | txn_id={txn_id} | pdf_type={pdf_type} | reason={str(e)}")
            print(traceback.format_exc())
    finally:
        if s3_storage.USE_S3 and pdf_path and os.path.exists(pdf_path):
            try:
                os.remove(pdf_path)
                print(f"[Task] Local temp file removed | txn_id={txn_id} | path={pdf_path}")
            except OSError as e:
                print(f"[Task][WARN] Could not remove local temp file | txn_id={txn_id} | reason={str(e)}")

# ---------------------------------------------------------------------------
# FastAPI app
# ---------------------------------------------------------------------------

app = FastAPI(title="HBL PDF Manager")

app.add_middleware(
    CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"]
)

app.include_router(dropdown_router)


@app.get("/")
def serve_index():
    return FileResponse("index.html")


# ── Upload ────────────────────────────────────────────────────────────────────

@app.post("/workflow/upload")
async def upload_pdfs(
    request: Request,
    background_tasks: BackgroundTasks,
    hbl_attachments: List[UploadFile] = File(default=[]),
    mbl_attachments: List[UploadFile] = File(default=[])
):
    print(f"[Upload] Received {len(hbl_attachments)} HBL + {len(mbl_attachments)} MBL file(s)")
    claims     = _token_payload(request)
    branch_id  = claims.get("branchId") or claims.get("branch_id")
    created_by = claims.get("user_id") or claims.get("userId") or claims.get("username") or "unknown"
    schema_name = claims.get("schema") or "public"

    if s3_storage.USE_S3:
        if not claims.get("schema"):
            user_data = await HelperFunctionController.get_user_details(request)
            schema_name = user_data.get("schema") or schema_name
            if not branch_id:
                branch_id = user_data.get("active_branch_id")
        print(f"[Upload] S3 enabled | schema={schema_name} | branch_id={branch_id}")

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
        # if not file.filename.lower().endswith(".pdf"):
        #     print(f"[Upload][WARN] Rejected non-PDF | txn_id={txn_id} | filename={file.filename}")
        #     errors.append({"filename": file.filename, "pdf_type": pdf_type, "error": "Only PDF files are accepted"})
        #     continue

        local_path = os.path.join(UPLOAD_DIR, f"{txn_id}_{pdf_type}_{file.filename}")
        db_file_path = local_path
        s3_key = None

        try:
            content = await file.read()
            with open(local_path, "wb") as buf:
                buf.write(content)
            print(f"[Upload] File saved locally | txn_id={txn_id} | pdf_type={pdf_type} | path={local_path}")

            if s3_storage.USE_S3:
                s3_key, db_file_path = s3_storage.upload_bytes(
                    content, file.filename, DB_CONFIG["database"], schema_name
                )
                print(f"[Upload] File uploaded to S3 | txn_id={txn_id} | url={db_file_path}")

            size_kb = round(len(content) / 1024, 2)
        except OSError as e:
            print(f"[Upload][ERROR] File save failed | txn_id={txn_id} | filename={file.filename} | reason={str(e)}")
            print(traceback.format_exc())
            errors.append({"filename": file.filename, "pdf_type": pdf_type, "error": f"File save failed: {str(e)}"})
            continue
        except Exception as e:
            print(f"[Upload][ERROR] Unexpected error | txn_id={txn_id} | filename={file.filename} | reason={str(e)}")
            print(traceback.format_exc())
            errors.append({"filename": file.filename, "pdf_type": pdf_type, "error": f"Unexpected error: {str(e)}"})
            if s3_key:
                try:
                    s3_storage.delete_object(s3_key)
                except Exception:
                    pass
            try:
                os.remove(local_path)
            except OSError:
                pass
            continue

        # Insert DB record — all rows share the same txn_id
        try:
            with get_db() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        INSERT INTO public.pdf_uploads
                            (txn_id, filename, file_path, size_kb, file_type, status, branch_id, created_by)
                        VALUES (%s, %s, %s, %s, %s, 'pending', %s, %s)
                    """, (txn_id, file.filename, db_file_path, size_kb, pdf_type, branch_id, created_by))
                conn.commit()
            print(f"[Upload] DB record inserted | txn_id={txn_id} | filename={file.filename} | pdf_type={pdf_type}")
        except psycopg2.IntegrityError as e:
            print(f"[Upload][ERROR] DB integrity error | txn_id={txn_id} | reason={str(e)}")
            print(traceback.format_exc())
            errors.append({"filename": file.filename, "pdf_type": pdf_type, "error": f"DB integrity error: {str(e)}"})
            if s3_key:
                try:
                    s3_storage.delete_object(s3_key)
                except Exception:
                    pass
            try:
                os.remove(local_path)
            except OSError:
                pass
            continue
        except psycopg2.Error as e:
            print(f"[Upload][ERROR] DB insert failed | txn_id={txn_id} | reason={str(e)}")
            print(traceback.format_exc())
            errors.append({"filename": file.filename, "pdf_type": pdf_type, "error": f"DB insert failed: {str(e)}"})
            if s3_key:
                try:
                    s3_storage.delete_object(s3_key)
                except Exception:
                    pass
            try:
                os.remove(local_path)
            except OSError:
                pass
            continue
        except Exception as e:
            print(f"[Upload][ERROR] Unexpected DB error | txn_id={txn_id} | reason={str(e)}")
            print(traceback.format_exc())
            errors.append({"filename": file.filename, "pdf_type": pdf_type, "error": f"Unexpected error: {str(e)}"})
            if s3_key:
                try:
                    s3_storage.delete_object(s3_key)
                except Exception:
                    pass
            try:
                os.remove(local_path)
            except OSError:
                pass
            continue

        background_tasks.add_task(run_extraction, txn_id, local_path, file.filename, pdf_type)
        print(f"[Upload] Extraction queued | txn_id={txn_id} | filename={file.filename} | pdf_type={pdf_type}")

        results.append({
            "txn_id":    txn_id,
            "filename":  file.filename,
            "size_kb":   size_kb,
            "pdf_type":  pdf_type,
            "file_type": pdf_type,
            "file_url":  s3_storage.normalize_file_url(db_file_path),
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


# ── List files ────────────────────────────────────────────────────────────────

@app.get("/workflow/files")
def list_files(request: Request, status: Optional[str] = None, search: Optional[str] = None, branch_id: Optional[str] = None):
    claims = _token_payload(request)
    # Use branch_id from query param; fall back to JWT claim
    effective_branch_id = branch_id or claims.get("branchId") or claims.get("branch_id")
    print(f"[ListFiles] Request | status={status} | search={search} | branch_id={effective_branch_id}")
    try:
        query  = "SELECT * FROM tv_upload_list"
        params = []
        wheres = []

        if effective_branch_id:
            wheres.append("branch_id = %s")
            params.append(effective_branch_id)
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
        txn_ids = list({row["txn_id"] for row in rows if row.get("txn_id")})
        url_map = _fetch_txn_file_urls(txn_ids)
        files = [_apply_file_urls(row, url_map) for row in rows]
        return {"files": files, "total": len(files)}

    except psycopg2.Error as e:
        print(f"[ListFiles][ERROR] DB error | reason={str(e)}")
        print(traceback.format_exc())
        raise HTTPException(status_code=500, detail=f"DB error: {str(e)}")

    except Exception as e:
        print(f"[ListFiles][ERROR] Unexpected error | reason={str(e)}")
        print(traceback.format_exc())
        raise HTTPException(status_code=500, detail=f"Unexpected error: {str(e)}")


# ── Get single record ─────────────────────────────────────────────────────────

@app.get("/workflow/files/{txn_id}")
def get_file(txn_id: str):
    print(f"[GetFile] Request | txn_id={txn_id}")
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT * FROM public.pdf_uploads WHERE txn_id = %s ORDER BY file_type",
                    (txn_id,),
                )
                rows = cur.fetchall()

        if not rows:
            print(f"[GetFile][WARN] Record not found | txn_id={txn_id}")
            raise HTTPException(status_code=404, detail=f"No record found: {txn_id}")

        row = next((r for r in rows if r["file_type"] == "mbl"), rows[0])
        result = row_to_dict(row)
        url_map = _fetch_txn_file_urls([txn_id])
        result = _apply_file_urls(result, url_map)
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

                        # Remove uploaded file (S3 or local disk)
                        stored = row["file_path"]
                        if s3_storage.USE_S3 and s3_storage.is_s3_stored_path(stored):
                            try:
                                key = s3_storage.s3_key_from_stored_path(stored)
                                if key:
                                    s3_storage.delete_object(key)
                                    print(f"[Delete] S3 file removed | txn_id={txn_id} | key={key}")
                            except Exception as e:
                                print(f"[Delete][WARN] Could not remove S3 file | txn_id={txn_id} | reason={str(e)}")
                                errors.append({"txn_id": txn_id, "error": f"S3 delete error: {str(e)}"})
                        elif stored and os.path.exists(stored):
                            try:
                                os.remove(stored)
                                print(f"[Delete] PDF file removed | txn_id={txn_id} | path={stored}")
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


# ── Start creating jobs ───────────────────────────────────────────────────────

@app.post("/workflow/start_creating_jobs")
async def start_job_creation(Request: Request, background_tasks: BackgroundTasks):

    try:
        body = await Request.json()
        ids = body.get("txn_ids", [])
        with get_db() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("SELECT * from tv_api_payload where txn_id in %s", (tuple(ids),))
                data = cur.fetchall()

        if not data:
            return {"message": "No jobs found with status 'done'"}

        background_tasks.add_task(process_jobs, data, Request)
        return {"message": f"{len(data)} jobs queued for creation"}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


async def process_jobs(data: list, Request):
    """Background task: loops through pdf_uploads rows and creates jobs."""
    for row in data:
        mbl_id      = row.get("mbl_id")
        txn_id      = row.get("txn_id")
        api_payload = row.get("api_payload")

        print(f"[Job] Processing | mbl_id={mbl_id} | txn_id={txn_id}")

        try:
            if api_payload:
                print(f"[Job] Payload ready | txn_id={txn_id}")
                
                origin_code = api_payload.get("origin_code")
                destination_code = api_payload.get("destination_code")
                carrier_code = api_payload.get("carrier_code")

                api_payload["origin_code"] = await HelperFunctionController.search_from_db("tv_port_code", "port_code", "port_code", origin_code, or_conditions=[("alias_code", origin_code)]) or origin_code
                api_payload["destination_code"] = await HelperFunctionController.search_from_db("tv_port_code", "port_code", "port_code", destination_code, or_conditions=[("alias_code", destination_code)]) or destination_code
                api_payload["carrier_code"] = await HelperFunctionController.search_from_db("tv_carrier_code", "carrier_code", "carrier_code", carrier_code, or_conditions=[("alias_code", carrier_code)]) or carrier_code

                for ho in api_payload.get("housing_details", []):
                    ho_origin = ho.get("origin_code")
                    ho_destination = ho.get("destination_code")
                    ho["origin_code"] = await HelperFunctionController.search_from_db("tv_port_code", "port_code", "port_code", ho_origin, or_conditions=[("alias_code", ho_origin)]) or ho_origin
                    ho["destination_code"] = await HelperFunctionController.search_from_db("tv_port_code", "port_code", "port_code", ho_destination, or_conditions=[("alias_code", ho_destination)]) or ho_destination


                # container types
                for cont in api_payload.get("container_details", []):
                    cont_type = cont.get("container_type_input")
                    code = await HelperFunctionController.search_from_db("tv_container_code", "container_code", "container_code", cont_type, or_conditions=[("alias_code", cont_type)])
                    print(f"[Job] Container code lookup | input={cont_type} | found={code}")
                    cont["container_type_input"] = code or cont_type


                





                await HelperFunctionController.create_job(Request, api_payload)

            with get_db() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        UPDATE public.pdf_uploads
                        SET status = 'job_created', processed_at = NOW(),final_status = 'JOB_CREATED'
                        WHERE txn_id = %s
                        """,
                        (txn_id,)
                    )
                conn.commit()
            print(f"[Job] Status set to job_created | mbl_id={mbl_id} | txn_id={txn_id}")

        except Exception as e:
            print(f"[Job][ERROR] Job creation failed | mbl_id={mbl_id} | txn_id={txn_id} | reason={e}")

            try:
                with get_db() as conn:
                    with conn.cursor() as cur:
                        cur.execute(
                            """
                            UPDATE public.pdf_uploads
                            SET status = 'job_creation_failed',
                                final_status = 'JOB_CREATION_FAILED',
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