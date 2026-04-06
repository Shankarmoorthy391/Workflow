"""
routes/dropdown.py — Dropdown data endpoints
---------------------------------------------
Provides lookup/autocomplete data for frontend dropdowns.
"""

import traceback
from typing import Optional

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel
from sqlalchemy import text

from Config.database import get_async_db

router = APIRouter(tags=["Dropdown"])


# ── Port Alias schemas ────────────────────────────────────────────────────────

class PortAliasCreate(BaseModel):
    alias_code: str
    port_code: str
    transport_mode: Optional[str] = None
    status: Optional[str] = "ACTIVE"


class CarrierAliasCreate(BaseModel):
    alias_code: str
    carrier_code: str
    transport_mode: Optional[str] = None
    status: Optional[str] = "ACTIVE"


class ContainerAliasCreate(BaseModel):
    alias_code: str
    container_code: str




# ── Port dropdown ─────────────────────────────────────────────────────────────

@router.get("/workflow/ports")
async def get_ports(search: Optional[str] = Query(default=None, description="Filter ports by name or code")):
    """
    Returns all columns and all rows from tv_port_code.
    Optionally filters by a search string (case-insensitive match on name or code).
    """
    print(f"[Dropdown] Ports request | search={search}")
    try:
        async with get_async_db() as conn:
            if search:
                sql = text("""
                    SELECT * FROM tv_port_code
                    WHERE LOWER(port_name) LIKE LOWER(:search) OR LOWER(port_code) LIKE LOWER(:search)
                    ORDER BY port_name ASC
                """)
                result = await conn.execute(sql, {"search": f"%{search}%"})
            else:
                sql = text("SELECT * FROM tv_port_code ORDER BY port_name ASC")
                result = await conn.execute(sql)

            rows = result.mappings().all()

        data = [dict(r) for r in rows]
        print(f"[Dropdown] Ports returned {len(data)} record(s)")
        return {"ports": data, "total": len(data)}

    except Exception as e:
        print(f"[Dropdown][ERROR] Unexpected error | reason={str(e)}")
        print(traceback.format_exc())
        raise HTTPException(status_code=500, detail=f"Unexpected error: {str(e)}")


# ── Port Alias ────────────────────────────────────────────────────────────────

@router.post("/workflow/port-aliases", status_code=201)
async def create_port_alias(payload: PortAliasCreate):
    """Insert a new port alias record."""
    print(f"[PortAlias] Create request | alias_code={payload.alias_code} | port_code={payload.port_code}")
    try:
        async with get_async_db() as conn:
            result = await conn.execute(
                text("""
                    INSERT INTO public.port_alias (alias_code, port_code, transport_mode, status)
                    VALUES (:alias_code, :port_code, :transport_mode, :status)
                    RETURNING *
                """),
                {
                    "alias_code":     payload.alias_code,
                    "port_code":      payload.port_code,
                    "transport_mode": payload.transport_mode,
                    "status":         payload.status or "ACTIVE",
                },
            )
            row = result.mappings().fetchone()

        print(f"[PortAlias] Created | id={row['id']}")
        return dict(row)

    except Exception as e:
        print(f"[PortAlias][ERROR] Create failed | reason={str(e)}")
        print(traceback.format_exc())
        raise HTTPException(status_code=500, detail=f"Unexpected error: {str(e)}")


# ── Carrier dropdown ──────────────────────────────────────────────────────────

@router.get("/workflow/carriers")
async def get_carriers(search: Optional[str] = Query(default=None, description="Filter carriers by name or code")):
    """
    Returns all columns from tv_carrier_code.
    Optionally filters by a search string (case-insensitive match on name or code).
    """
    print(f"[Dropdown] Carriers request | search={search}")
    try:
        async with get_async_db() as conn:
            if search:
                sql = text("""
                    SELECT * FROM public.tv_carrier_code
                    WHERE LOWER(carrier_name) LIKE LOWER(:search) OR LOWER(carrier_code) LIKE LOWER(:search)
                    ORDER BY carrier_name ASC
                """)
                result = await conn.execute(sql, {"search": f"%{search}%"})
            else:
                sql = text("SELECT * FROM public.tv_carrier_code ORDER BY carrier_name ASC")
                result = await conn.execute(sql)

            rows = result.mappings().all()

        data = [dict(r) for r in rows]
        print(f"[Dropdown] Carriers returned {len(data)} record(s)")
        return {"carriers": data, "total": len(data)}

    except Exception as e:
        print(f"[Dropdown][ERROR] Unexpected error | reason={str(e)}")
        print(traceback.format_exc())
        raise HTTPException(status_code=500, detail=f"Unexpected error: {str(e)}")


# ── Carrier Alias ─────────────────────────────────────────────────────────────

@router.post("/workflow/carrier-aliases", status_code=201)
async def create_carrier_alias(payload: CarrierAliasCreate):
    """Insert a new carrier alias record."""
    print(f"[CarrierAlias] Create request | alias_code={payload.alias_code} | carrier_code={payload.carrier_code}")
    try:
        async with get_async_db() as conn:
            result = await conn.execute(
                text("""
                    INSERT INTO public.carrier_alias (alias_code, carrier_code, transport_mode, status)
                    VALUES (:alias_code, :carrier_code, :transport_mode, :status)
                    RETURNING *
                """),
                {
                    "alias_code":     payload.alias_code,
                    "carrier_code":   payload.carrier_code,
                    "transport_mode": payload.transport_mode,
                    "status":         payload.status or "ACTIVE",
                },
            )
            row = result.mappings().fetchone()

        print(f"[CarrierAlias] Created | id={row['id']}")
        return dict(row)

    except Exception as e:
        print(f"[CarrierAlias][ERROR] Create failed | reason={str(e)}")
        print(traceback.format_exc())
        raise HTTPException(status_code=500, detail=f"Unexpected error: {str(e)}")


# ── Container Code dropdown ───────────────────────────────────────────────────

@router.get("/workflow/container-codes")
async def get_container_codes(search: Optional[str] = Query(default=None, description="Filter containers by name or code")):
    """
    Returns all columns from tv_container_code.
    Optionally filters by a search string (case-insensitive match on name or code).
    """
    print(f"[Dropdown] ContainerCodes request | search={search}")
    try:
        async with get_async_db() as conn:
            if search:
                sql = text("""
                    SELECT * FROM public.tv_container_code
                    WHERE LOWER(container_name) LIKE LOWER(:search) OR LOWER(container_code) LIKE LOWER(:search)
                    ORDER BY container_name ASC
                """)
                result = await conn.execute(sql, {"search": f"%{search}%"})
            else:
                sql = text("SELECT * FROM public.tv_container_code ORDER BY container_name ASC")
                result = await conn.execute(sql)

            rows = result.mappings().all()

        data = [dict(r) for r in rows]
        print(f"[Dropdown] ContainerCodes returned {len(data)} record(s)")
        return {"container_codes": data, "total": len(data)}

    except Exception as e:
        print(f"[Dropdown][ERROR] Unexpected error | reason={str(e)}")
        print(traceback.format_exc())
        raise HTTPException(status_code=500, detail=f"Unexpected error: {str(e)}")


# ── Container Code Alias ──────────────────────────────────────────────────────

@router.post("/workflow/container-type-aliases", status_code=201)
async def create_container_alias(payload: ContainerAliasCreate):
    """Insert a new container code alias record."""
    print(f"[ContainerAlias] Create request | alias_code={payload.alias_code} | container_code={payload.container_code}")
    try:
        async with get_async_db() as conn:
            result = await conn.execute(
                text("""
                    INSERT INTO public.container_code_alias (alias_code, container_code)
                    VALUES (:alias_code, :container_code)
                    RETURNING *
                """),
                {
                    "alias_code":      payload.alias_code,
                    "container_code":  payload.container_code,
                },
            )
            row = result.mappings().fetchone()

        print(f"[ContainerAlias] Created | alias_code={payload.alias_code}")
        return dict(row)

    except Exception as e:
        print(f"[ContainerAlias][ERROR] Create failed | reason={str(e)}")
        print(traceback.format_exc())
        raise HTTPException(status_code=500, detail=f"Unexpected error: {str(e)}")
