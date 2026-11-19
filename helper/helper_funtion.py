import httpx
import os
from typing import Dict, Any
from fastapi import HTTPException, Request
from dotenv import load_dotenv
load_dotenv()
import json
from sqlalchemy import text
from Config.database import get_async_db

class HelperFunctionController:

    @staticmethod
    async def get_user_details(request: Request) -> dict:
        """Fetch user details (including tenant schema) from the auth API."""
        url = f"{os.getenv('BASE_URL')}/api/user-details/"
        auth = request.headers.get("Authorization", "")
        headers = {"Authorization": auth} if auth else {}
        try:
            async with httpx.AsyncClient(timeout=30, follow_redirects=True) as client:
                response = await client.get(url, headers=headers)
            response.raise_for_status()
            body = response.json()
            return body.get("data") or {}
        except httpx.HTTPStatusError as e:
            print(f"HTTP error fetching user details: {e.response.status_code} - {e.response.text}")
            raise HTTPException(status_code=e.response.status_code, detail=e.response.text)
        except httpx.TimeoutException:
            raise HTTPException(status_code=504, detail="User details API timed out")
        except Exception as e:
            print(f"Unexpected error fetching user details: {e}")
            raise HTTPException(status_code=500, detail=str(e))

    @staticmethod
    def extract_job_id_from_response(response: Dict[str, Any] | None) -> int | None:
        """Pull job_details_id / id from POST /api/job-create/ success body."""
        if not isinstance(response, dict):
            return None
        data = response.get("data") if isinstance(response.get("data"), dict) else response
        if not isinstance(data, dict):
            return None
        for key in ("job_details_id", "id", "job_id"):
            raw = data.get(key)
            if raw in (None, ""):
                continue
            try:
                n = int(raw)
                if n > 0:
                    return n
            except (TypeError, ValueError):
                continue
        return None

    @staticmethod
    def resolve_automation_owner_from_details(user_details: dict | None) -> dict:
        """
        Normalize /api/user-details/ for automation ownership stamping + list filters.
        created_by_id = UserMaster PK; branch/company from is_default branch.
        """
        details = user_details if isinstance(user_details, dict) else {}
        created_by_id = details.get("usermaster_id")
        if created_by_id is None:
            created_by_id = details.get("user_id")
        try:
            created_by_id = int(created_by_id) if created_by_id is not None else None
        except (TypeError, ValueError):
            created_by_id = None
        branch_code = str(details.get("branch_code") or "").strip() or None
        company_code = str(details.get("company_code") or "").strip() or None
        country_branch_codes = [
            str(c).strip()
            for c in (details.get("country_branch_codes") or [])
            if str(c).strip()
        ]
        is_admin = bool(details.get("is_staff")) or str(
            details.get("role") or ""
        ).strip().lower() == "admin"
        display_name = (
            str(details.get("full_name") or "").strip()
            or str(details.get("email") or "").strip()
            or "unknown"
        )
        return {
            "created_by_id": created_by_id,
            "branch_code": branch_code,
            "company_code": company_code,
            "country_branch_codes": country_branch_codes,
            "is_admin": is_admin,
            "display_name": display_name,
            "active_branch_id": details.get("active_branch_id"),
        }

    @staticmethod
    async def create_job(Request,payload: dict) -> Dict[str, Any]:
        """
        Hits the Job Creation API via POST request.

        Args:
            payload: Dictionary containing job creation data.

        Returns:
            Response data from the API.
        """
        url = f"{os.getenv('BASE_URL')}/api/job-create/"
        print(f"API URL: {url}")
          # 🔁 replace with your actual endpoint
        token = Request.headers.get("Authorization", "").replace("Bearer ", "")  # Extract token from incoming request  
        headers = {
        "Content-Type": "application/json",
        "Authorization": f'Bearer {token}'
    }
        # Mark job as automation-created (job_details.is_automated)
        if isinstance(payload, dict):
            payload = {**payload, "is_automated": True}
        print("PAYLOAD TYPE:", type(payload))  # must be <class 'dict'>
        print("PAYLOAD KEYS:", payload.keys())
        print("SERIALIZED:", json.dumps(payload, indent=2)) 
        try:
            async with httpx.AsyncClient(timeout=300) as client:
                response = await client.post(url, json=payload,headers=headers)

            response.raise_for_status()
            # Print full response even on error BEFORE raise_for_status
            print(f"Status: {response.status_code}")
            print(f"Response body: {response.text}")
            return response.json()

        except httpx.HTTPStatusError as e:
            print(f"HTTP error during job creation: {e.response.status_code} - {e.response.text}")
            raise HTTPException(status_code=e.response.status_code, detail=e.response.text)

        except httpx.TimeoutException:
            print("Job creation API timed out")
            raise HTTPException(status_code=504, detail="Job creation API timed out")

        except Exception as e:
            print(f"Unexpected error during job creation: {e}")
            raise HTTPException(status_code=500, detail=str(e))
        
    @staticmethod
    async def search_from_db(table: str, column: str, where_col: str, where_val: str, or_conditions: list = None):
     """
    Search a single column from any table with a case-insensitive WHERE condition.
    Supports additional dynamic OR conditions.

    :param table:           Table name to query
    :param column:          Column to select
    :param where_col:       Column name for primary WHERE condition
    :param where_val:       Value to match (case-insensitive)
    :param or_conditions:   Optional list of (col, val) tuples for OR conditions
                            e.g. [("email", "test@example.com"), ("phone", "12345")]
    :return:                First matching value or None
    """
     try:
        async with get_async_db() as session:
            bind_params = {"val0": where_val}
            query = f"SELECT {column} FROM {table} WHERE LOWER({where_col}) = LOWER(:val0)"

            if or_conditions:
                for i, (col, val) in enumerate(or_conditions, start=1):
                    key = f"val{i}"
                    query += f" OR LOWER({col}) = LOWER(:{key})"
                    bind_params[key] = val

            query += " LIMIT 1"
            result = await session.execute(text(query), bind_params)
            row = result.fetchone()
            return row[0] if row else None

     except Exception as e:
        raise Exception(f"Database query failed: {str(e)}")

    @staticmethod
    async def find_import_booking(
        Request,
        *,
        houseno: str = None,
        shipment_code: str = None,
        exact_houseno: bool = True,
    ) -> Dict[str, Any] | None:
        """
        Look up an IMPORT booking (BOOKED/RECEIVED) by house BL or shipment_code.
        Returns {id, shipment_code, houseno} or None when zero/ambiguous.
        """
        filters: Dict[str, Any] = {
            "service_type": "IMPORT",
            "status": ["BOOKED", "RECEIVED"],
        }
        if houseno and str(houseno).strip():
            filters["houseno"] = str(houseno).strip()
        elif shipment_code and str(shipment_code).strip():
            filters["shipment_code"] = str(shipment_code).strip()
        else:
            return None

        url = f"{os.getenv('BASE_URL')}/api/customer-service-shipment/filter/"
        token = Request.headers.get("Authorization", "").replace("Bearer ", "")
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {token}",
        }
        try:
            async with httpx.AsyncClient(timeout=60, follow_redirects=True) as client:
                response = await client.post(
                    url,
                    json={"filters": filters},
                    headers=headers,
                    params={"index": 0, "limit": 20},
                )
            response.raise_for_status()
            body = response.json() if response.content else {}
            rows = body.get("data") if isinstance(body, dict) else None
            if not isinstance(rows, list) or not rows:
                return None

            def _norm_bl(val) -> str:
                s = str(val or "").strip().lower()
                for ch in (" ", "-", "_", "/", "\\", "."):
                    s = s.replace(ch, "")
                return s

            target = _norm_bl(houseno) if houseno else ""
            if exact_houseno and target:
                exact = [
                    r
                    for r in rows
                    if isinstance(r, dict) and _norm_bl(r.get("houseno")) == target
                ]
                if exact:
                    rows = exact
                else:
                    # Soft contain match when exact normalized fails
                    soft = [
                        r
                        for r in rows
                        if isinstance(r, dict)
                        and (
                            target in _norm_bl(r.get("houseno"))
                            or _norm_bl(r.get("houseno")) in target
                        )
                    ]
                    rows = soft if soft else rows

            if not rows:
                return None

            # Prefer single exact / BOOKED; if several, take BOOKED then first (like HouseCreate)
            if len(rows) > 1 and shipment_code:
                code = str(shipment_code).strip().lower()
                exact_code = [
                    r
                    for r in rows
                    if isinstance(r, dict)
                    and str(r.get("shipment_code") or "").strip().lower() == code
                ]
                if exact_code:
                    rows = exact_code

            booked = [
                r
                for r in rows
                if isinstance(r, dict)
                and str(r.get("status") or "").strip().upper() == "BOOKED"
            ]
            row = (booked[0] if booked else rows[0]) if rows else None
            if not isinstance(row, dict) or row.get("id") is None:
                return None
            try:
                bid = int(row["id"])
            except (TypeError, ValueError):
                return None
            return {
                "id": bid,
                "shipment_code": str(row.get("shipment_code") or ""),
                "houseno": str(row.get("houseno") or ""),
            }
        except Exception as e:
            print(f"[BookingLookup] Failed | houseno={houseno} | shipment_code={shipment_code} | reason={e}")
            return None

    @staticmethod
    async def get_import_booking_by_id(Request, booking_id: int) -> Dict[str, Any] | None:
        """Fetch full IMPORT booking row (incl. rate_details) by PK."""
        try:
            bid = int(booking_id)
        except (TypeError, ValueError):
            return None
        url = f"{os.getenv('BASE_URL')}/api/customer-service-shipment/filter/"
        token = Request.headers.get("Authorization", "").replace("Bearer ", "")
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {token}",
        }
        try:
            async with httpx.AsyncClient(timeout=60, follow_redirects=True) as client:
                response = await client.post(
                    url,
                    json={
                        "filters": {
                            "service_type": "IMPORT",
                            "status": ["BOOKED", "RECEIVED"],
                            "id": bid,
                        }
                    },
                    headers=headers,
                    params={"index": 0, "limit": 5},
                )
            response.raise_for_status()
            body = response.json() if response.content else {}
            rows = body.get("data") if isinstance(body, dict) else None
            if not isinstance(rows, list):
                return None
            for row in rows:
                if isinstance(row, dict) and int(row.get("id") or 0) == bid:
                    return row
            return rows[0] if rows and isinstance(rows[0], dict) else None
        except Exception as e:
            print(f"[BookingLookup] get_by_id failed | id={booking_id} | reason={e}")
            return None

    @staticmethod
    def map_mbl_charges_from_rate_details(rate_details) -> list:
        """Map booking rate_details → job house mbl_charges (sell + cost)."""
        if not isinstance(rate_details, list):
            return []
        out = []
        for c in rate_details:
            if not isinstance(c, dict):
                continue
            no_of_unit = c.get("no_of_units") if c.get("no_of_units") not in (None, "") else c.get("no_of_unit") or ""
            try:
                qty = float(no_of_unit) if no_of_unit not in (None, "") else None
            except (TypeError, ValueError):
                qty = None
            sell_raw = c.get("sell_per_unit")
            try:
                sell = float(sell_raw) if sell_raw not in (None, "") else None
            except (TypeError, ValueError):
                sell = None
            amount = ""
            if qty is not None and sell is not None and qty > 0 and sell > 0:
                amount = round(qty * sell, 2)
            total_sell = c.get("total_sell")
            if total_sell in (None, ""):
                total_sell = c.get("sell_amount_total") or ""

            cost_raw = c.get("cost_per_unit")
            try:
                cost_pu = float(cost_raw) if cost_raw not in (None, "") else None
            except (TypeError, ValueError):
                cost_pu = None
            # Currency cost: units × cost_per_unit (booking.total_cost is local)
            total_cost = ""
            if qty is not None and cost_pu is not None and qty > 0 and cost_pu > 0:
                total_cost = round(qty * cost_pu, 2)
            booking_total_cost = c.get("total_cost")
            if booking_total_cost not in (None, ""):
                cost_local = booking_total_cost
            elif total_cost not in (None, "") and c.get("roe") not in (None, ""):
                try:
                    cost_local = round(float(total_cost) * float(c.get("roe")), 2)
                except (TypeError, ValueError):
                    cost_local = ""
            else:
                cost_local = ""

            # Prefer numeric FKs only — code strings fail MblChargesSerializer
            unit_id = c.get("unit_id") if c.get("unit_id") not in (None, "") else ""
            currency_id = c.get("currency_id") if c.get("currency_id") not in (None, "") else ""
            out.append({
                "charge_id": c.get("charge_id") or "",
                # Display-only for Form View / extracted payload; create uses charge_id FK
                "charge_name": c.get("charge_name") or c.get("charge_code") or "",
                "charge_code": c.get("charge_code") or "",
                "supplier_code": "",
                "pp_cc": c.get("pp_cc") or "",
                "unit_id": unit_id,
                "no_of_unit": no_of_unit,
                "amount": amount,
                "amount_per_unit": sell_raw if sell_raw is not None else "",
                "currency_id": currency_id,
                "roe": c.get("roe") if c.get("roe") is not None else "",
                "sell_local_amount": total_sell,
                "unit_cost": cost_raw if cost_raw is not None else "",
                "total_cost": total_cost,
                "cost_local_amount": cost_local,
            })
        return out

    @staticmethod
    def _is_empty_merge_value(v) -> bool:
        if v is None:
            return True
        if isinstance(v, str) and not v.strip():
            return True
        if isinstance(v, (list, dict)) and len(v) == 0:
            return True
        return False

    # Cargo measure keys: treat 0 / "0.00" as empty so extract can fill booking placeholders.
    _CARGO_PLACEHOLDER_KEYS = frozenset({
        "gross_weight",
        "gross_weight_kgs",
        "volume",
        "volume_cbm",
        "chargeable_weight",
        "no_of_packages",
        "packages",
        "no_of_containers",
    })

    @staticmethod
    def _is_empty_cargo_scalar(v) -> bool:
        """Like _is_empty_merge_value, plus numeric/string zero placeholders for cargo measures."""
        if HelperFunctionController._is_empty_merge_value(v):
            return True
        if isinstance(v, bool):
            return False
        if isinstance(v, (int, float)) and v == 0:
            return True
        if isinstance(v, str):
            s = v.strip()
            if not s:
                return True
            try:
                return float(s) == 0.0
            except (TypeError, ValueError):
                return False
        return False

    @staticmethod
    def _first_nonempty(*vals):
        for v in vals:
            if not HelperFunctionController._is_empty_merge_value(v):
                return v
        return None

    @staticmethod
    def _normalize_routed(value) -> str:
        raw = str(value or "").strip()
        if not raw:
            return ""
        lower = raw.lower()
        if lower in ("self", "agent"):
            return lower
        return raw

    @staticmethod
    def _map_booking_events_for_job(booking: dict) -> list:
        raw = booking.get("events") if isinstance(booking.get("events"), list) else []
        out = []
        for e in raw:
            if not isinstance(e, dict):
                continue
            etype = str(e.get("type") or e.get("event_type") or "").strip()
            date = str(e.get("date") or e.get("event_date") or "").strip()
            if not etype and not date:
                continue
            row = {"type": etype, "date": date}
            eid = e.get("id")
            if eid not in (None, ""):
                try:
                    row["id"] = int(eid)
                except (TypeError, ValueError):
                    pass
            out.append(row)
        return out

    @staticmethod
    def _cargo_scalar(*vals):
        for v in vals:
            if HelperFunctionController._is_empty_cargo_scalar(v):
                continue
            return v
        return None

    @staticmethod
    def _package_type_from_cargo(row: dict) -> str:
        if not isinstance(row, dict):
            return ""
        for key in (
            "package_type_code",
            "package_type",
            "package_code",
            "pkg_type",
        ):
            val = row.get(key)
            if isinstance(val, dict):
                code = val.get("package_type_code") or val.get("code") or ""
                if str(code).strip():
                    return str(code).strip()
            elif not HelperFunctionController._is_empty_merge_value(val):
                return str(val).strip()
        return ""

    @staticmethod
    def _normalize_house_cargo_row(row: dict) -> dict:
        """Job uses gross_weight/volume; extract often uses *_kgs / *_cbm aliases."""
        if not isinstance(row, dict):
            row = {}
        gw = HelperFunctionController._cargo_scalar(
            row.get("gross_weight"), row.get("gross_weight_kgs")
        )
        vol = HelperFunctionController._cargo_scalar(
            row.get("volume"), row.get("volume_cbm")
        )
        pkgs = HelperFunctionController._cargo_scalar(
            row.get("no_of_packages"),
            row.get("packages"),
            row.get("no_of_containers"),
        )
        container_no = HelperFunctionController._cargo_scalar(
            row.get("container_no"), row.get("container_number")
        )
        pkg_type = HelperFunctionController._package_type_from_cargo(row)
        out = dict(row)
        out["container_no"] = str(container_no) if container_no not in (None, "") else None
        out["no_of_packages"] = pkgs
        out["package_type"] = pkg_type
        out["package_type_code"] = pkg_type or None
        out["gross_weight"] = gw if gw is not None else ""
        out["volume"] = vol if vol is not None else ""
        out["gross_weight_kgs"] = HelperFunctionController._cargo_scalar(
            row.get("gross_weight_kgs"), gw
        ) or ""
        out["volume_cbm"] = HelperFunctionController._cargo_scalar(
            row.get("volume_cbm"), vol
        ) or ""
        out["chargeable_weight"] = HelperFunctionController._cargo_scalar(
            row.get("chargeable_weight")
        ) or ""
        out["haz"] = bool(row.get("haz"))
        un_no = str(row.get("un_no") or "").strip()
        class_name = str(row.get("class_name") or "").strip()
        pkg_group = str(row.get("pkg_group") or "").strip()
        if un_no:
            out["un_no"] = un_no
        else:
            out.pop("un_no", None)
        if class_name:
            out["class_name"] = class_name
        else:
            out.pop("class_name", None)
        if pkg_group:
            out["pkg_group"] = pkg_group
        else:
            out.pop("pkg_group", None)
        return out

    @staticmethod
    def _pick_complete_haz_details(rows: list) -> dict | None:
        """Return UN/Class/PKG when a haz=True row has all three (BE create-job contract)."""
        for row in rows:
            if not isinstance(row, dict) or not row.get("haz"):
                continue
            un_no = str(row.get("un_no") or "").strip()
            class_name = str(row.get("class_name") or "").strip()
            pkg_group = str(row.get("pkg_group") or "").strip()
            if un_no and class_name and pkg_group:
                return {
                    "un_no": un_no,
                    "class_name": class_name,
                    "pkg_group": pkg_group,
                }
        return None

    @staticmethod
    def _enrich_incomplete_haz_cargo_rows(rows: list, haz_source: list) -> list:
        """Fill missing UN/Class/PKG on haz rows from booking cargo with complete haz."""
        details = HelperFunctionController._pick_complete_haz_details(haz_source)
        if not details:
            return rows
        out = []
        for row in rows:
            if not isinstance(row, dict) or not row.get("haz"):
                out.append(row)
                continue
            un_no = str(row.get("un_no") or "").strip()
            class_name = str(row.get("class_name") or "").strip()
            pkg_group = str(row.get("pkg_group") or "").strip()
            if un_no and class_name and pkg_group:
                out.append(row)
                continue
            out.append(
                HelperFunctionController._normalize_house_cargo_row(
                    {
                        **row,
                        "un_no": un_no or details["un_no"],
                        "class_name": class_name or details["class_name"],
                        "pkg_group": pkg_group or details["pkg_group"],
                    }
                )
            )
        return out

    @staticmethod
    def _map_booking_cargo_for_job(booking: dict) -> list:
        """
        Map IMPORT booking cargo → house cargo_details (LCL + FCL).
        Mirrors FE mapCargoDetails: nested FCL containers, shipment-level fallbacks,
        job field names + Form View aliases.
        """
        if not isinstance(booking, dict):
            return []
        cargo = booking.get("cargo_details")
        if not isinstance(cargo, list):
            cargo = []
        is_fcl = str(booking.get("service") or "").strip().upper() == "FCL"

        un_no = str(booking.get("un_no") or "").strip() or None
        class_name = str(booking.get("class_name") or "").strip() or None
        pkg_group = str(booking.get("pkg_group") or "").strip() or None
        haz = bool(booking.get("is_hazardous")) and bool(un_no and class_name and pkg_group)
        haz_fields = (
            {"haz": True, "un_no": un_no, "class_name": class_name, "pkg_group": pkg_group}
            if haz
            else {"haz": False}
        )

        booking_gw = HelperFunctionController._cargo_scalar(
            booking.get("gross_weight"), booking.get("gross_weight_kgs")
        )
        booking_vol = HelperFunctionController._cargo_scalar(
            booking.get("volume"), booking.get("volume_cbm")
        )
        booking_cw = HelperFunctionController._cargo_scalar(
            booking.get("chargeable_weight")
        )
        booking_pkgs = booking.get("no_of_packages")

        def _row(**kwargs):
            return HelperFunctionController._normalize_house_cargo_row({**kwargs, **haz_fields})

        if is_fcl:
            out = []
            for c in cargo:
                if not isinstance(c, dict):
                    continue
                nested = c.get("containers") if isinstance(c.get("containers"), list) else []
                if nested:
                    for n in nested:
                        if not isinstance(n, dict):
                            continue
                        pkg = HelperFunctionController._package_type_from_cargo(n) or (
                            HelperFunctionController._package_type_from_cargo(c)
                        )
                        out.append(
                            _row(
                                container_no=HelperFunctionController._cargo_scalar(
                                    n.get("container_no"), c.get("container_no")
                                ),
                                no_of_packages=HelperFunctionController._cargo_scalar(
                                    n.get("no_of_packages"),
                                    c.get("no_of_packages"),
                                    booking_pkgs,
                                ),
                                package_type=pkg,
                                package_type_code=pkg or None,
                                gross_weight=HelperFunctionController._cargo_scalar(
                                    n.get("gross_weight"),
                                    n.get("gross_weight_kgs"),
                                    c.get("gross_weight"),
                                    c.get("gross_weight_kgs"),
                                    booking_gw,
                                ),
                                volume=HelperFunctionController._cargo_scalar(
                                    n.get("volume"),
                                    n.get("volume_cbm"),
                                    c.get("volume"),
                                    c.get("volume_cbm"),
                                    booking_vol,
                                ),
                                chargeable_weight=HelperFunctionController._cargo_scalar(
                                    n.get("chargeable_weight"),
                                    c.get("chargeable_weight"),
                                    booking_cw,
                                ),
                            )
                        )
                    continue
                pkg = HelperFunctionController._package_type_from_cargo(c)
                out.append(
                    _row(
                        container_no=HelperFunctionController._cargo_scalar(
                            c.get("container_no"), c.get("container_number")
                        ),
                        no_of_packages=HelperFunctionController._cargo_scalar(
                            c.get("no_of_packages"),
                            c.get("no_of_containers"),
                            booking_pkgs,
                        ),
                        package_type=pkg,
                        package_type_code=pkg or None,
                        gross_weight=HelperFunctionController._cargo_scalar(
                            c.get("gross_weight"),
                            c.get("gross_weight_kgs"),
                            booking_gw,
                        ),
                        volume=HelperFunctionController._cargo_scalar(
                            c.get("volume"), c.get("volume_cbm"), booking_vol
                        ),
                        chargeable_weight=HelperFunctionController._cargo_scalar(
                            c.get("chargeable_weight"), booking_cw
                        ),
                    )
                )
            if out:
                return out
            return [
                _row(
                    container_no=None,
                    no_of_packages=booking_pkgs,
                    package_type="",
                    package_type_code=None,
                    gross_weight=booking_gw,
                    volume=booking_vol,
                    chargeable_weight=booking_cw,
                )
            ]

        if not cargo:
            return [
                _row(
                    no_of_packages=booking_pkgs,
                    package_type="",
                    package_type_code=None,
                    gross_weight=booking_gw,
                    volume=booking_vol,
                    chargeable_weight=booking_cw,
                )
            ]

        out = []
        for c in cargo:
            if not isinstance(c, dict):
                continue
            pkg = HelperFunctionController._package_type_from_cargo(c)
            out.append(
                _row(
                    container_no=HelperFunctionController._cargo_scalar(
                        c.get("container_no"), c.get("container_number")
                    ),
                    no_of_packages=HelperFunctionController._cargo_scalar(
                        c.get("no_of_packages"),
                        c.get("packages"),
                        c.get("no_of_containers"),
                        booking_pkgs,
                    ),
                    package_type=pkg,
                    package_type_code=pkg or None,
                    gross_weight=HelperFunctionController._cargo_scalar(
                        c.get("gross_weight"),
                        c.get("gross_weight_kgs"),
                        booking_gw,
                    ),
                    volume=HelperFunctionController._cargo_scalar(
                        c.get("volume"), c.get("volume_cbm"), booking_vol
                    ),
                    chargeable_weight=HelperFunctionController._cargo_scalar(
                        c.get("chargeable_weight"), booking_cw
                    ),
                )
            )
        return out

    @staticmethod
    def _merge_automation_cargo_details(extract_cargo, booking_cargo) -> list:
        """Booking non-empty fields win; extract fills gaps (match by container_no / index)."""
        extract = [
            HelperFunctionController._normalize_house_cargo_row(
                r if isinstance(r, dict) else {}
            )
            for r in (extract_cargo if isinstance(extract_cargo, list) else [])
        ]
        booking = [
            HelperFunctionController._normalize_house_cargo_row(
                r if isinstance(r, dict) else {}
            )
            for r in (booking_cargo if isinstance(booking_cargo, list) else [])
        ]
        if not booking:
            return extract
        if not extract:
            return booking

        used = set()
        result = []
        for i, b_row in enumerate(booking):
            e_idx = -1
            b_cont = str(b_row.get("container_no") or "").strip().lower()
            if b_cont:
                for idx, e in enumerate(extract):
                    if idx in used:
                        continue
                    if str(e.get("container_no") or "").strip().lower() == b_cont:
                        e_idx = idx
                        break
            if e_idx < 0 and i < len(extract) and i not in used:
                e_idx = i
            if e_idx >= 0:
                used.add(e_idx)
            e_row = extract[e_idx] if e_idx >= 0 else {}
            merged = dict(e_row)
            for k, v in b_row.items():
                empty = (
                    HelperFunctionController._is_empty_cargo_scalar(v)
                    if k in HelperFunctionController._CARGO_PLACEHOLDER_KEYS
                    else HelperFunctionController._is_empty_merge_value(v)
                )
                if not empty:
                    merged[k] = v
            result.append(HelperFunctionController._normalize_house_cargo_row(merged))
        for idx, e in enumerate(extract):
            if idx not in used:
                result.append(e)
        # Leftover extract haz rows may lack UN/Class/PKG — fill from booking when complete
        return HelperFunctionController._enrich_incomplete_haz_cargo_rows(result, booking)

    @staticmethod
    def build_ocean_housing_fields_from_booking(booking: dict) -> dict:
        """
        Scalar / list house fields from IMPORT booking (mirrors FE buildOceanHousing
        merge surface for automation — no documents/pickup-delivery).
        """
        if not isinstance(booking, dict):
            return {}
        routed = HelperFunctionController._normalize_routed(booking.get("routed"))
        return {
            "origin_code": HelperFunctionController._first_nonempty(
                booking.get("origin_code"), booking.get("origin_code_read")
            ) or "",
            "destination_code": HelperFunctionController._first_nonempty(
                booking.get("destination_code"), booking.get("destination_code_read")
            ) or "",
            "trade": "Import",
            "routed": routed,
            "routed_by": str(booking.get("routed_by") or "").strip(),
            "customer_service": str(
                HelperFunctionController._first_nonempty(
                    booking.get("customer_service_name"),
                    booking.get("customer_service"),
                )
                or ""
            ).strip(),
            "agent_name": HelperFunctionController._first_nonempty(
                booking.get("destination_agent_name"),
                booking.get("forwarder_name"),
                booking.get("agent_name"),
            ) or "",
            "agent_address": HelperFunctionController._first_nonempty(
                booking.get("destination_agent_address"),
                booking.get("agent_address"),
            ) or "",
            "agent_email": HelperFunctionController._first_nonempty(
                booking.get("destination_agent_email"),
                booking.get("agent_email"),
            ) or "",
            "forwarder_id": booking.get("forwarder_id")
            if booking.get("forwarder_id") not in (None, "")
            else None,
            "forwarder_name": booking.get("forwarder_name") or "",
            "forwarder_address": booking.get("forwarder_address") or "",
            "forwarder_email": booking.get("forwarder_email") or "",
            "billing_customer_id": booking.get("billing_customer_id")
            if booking.get("billing_customer_id") not in (None, "")
            else None,
            "billing_customer_name": booking.get("billing_customer_name") or "",
            "billing_customer_address": HelperFunctionController._first_nonempty(
                booking.get("billing_customer_address"),
                booking.get("billing_customer_address_text"),
            ) or "",
            "billing_customer_email": booking.get("billing_customer_email") or "",
            "shipper_name": HelperFunctionController._first_nonempty(
                booking.get("shipper_name"), booking.get("customer_name")
            ) or "",
            "shipper_address": booking.get("shipper_address") or "",
            "shipper_email": booking.get("shipper_email") or "",
            "consignee_name": booking.get("consignee_name") or "",
            "consignee_address": booking.get("consignee_address") or "",
            "consignee_email": booking.get("consignee_email") or "",
            "notify1_customer_name": HelperFunctionController._first_nonempty(
                booking.get("notify1_customer_name"),
                booking.get("notify_customer_name"),
            ) or "",
            "notify1_customer_address": HelperFunctionController._first_nonempty(
                booking.get("notify1_customer_address"),
                booking.get("notify_customer_address"),
            ) or "",
            "notify1_customer_email": HelperFunctionController._first_nonempty(
                booking.get("notify1_customer_email"),
                booking.get("notify_customer_email"),
            ) or "",
            "cha_name": HelperFunctionController._first_nonempty(
                booking.get("cha"), booking.get("cha_name")
            ) or "",
            "cha_address": booking.get("cha_address") or "",
            "commodity_description": booking.get("commodity_description") or "",
            "marks_no": booking.get("marks_no") or "",
            "shipment_terms_code": HelperFunctionController._first_nonempty(
                booking.get("shipment_terms_code"),
                booking.get("shipment_terms_code_read"),
            ) or "",
            "cargo_details": HelperFunctionController._map_booking_cargo_for_job(booking),
            "events": HelperFunctionController._map_booking_events_for_job(booking),
        }

    @staticmethod
    def merge_import_housing_from_booking(extract_house: dict, booking: dict) -> dict:
        """
        Booking non-empty fields win; extract fills remaining empties.
        Always keep extracted hbl_number. Charges exclusive when booking linked.
        Same priority as FE mergeImportAutomationHousingFromBooking.
        """
        if not isinstance(extract_house, dict):
            extract_house = {}
        if not isinstance(booking, dict):
            return dict(extract_house)

        merged = dict(extract_house)
        extract_hbl = merged.get("hbl_number")
        from_booking = HelperFunctionController.build_ocean_housing_fields_from_booking(
            booking
        )
        for key, value in from_booking.items():
            if key in ("hbl_number", "mbl_charges", "cargo_details"):
                continue
            if HelperFunctionController._is_empty_merge_value(value):
                continue
            merged[key] = value

        merged["cargo_details"] = (
            HelperFunctionController._merge_automation_cargo_details(
                extract_house.get("cargo_details"),
                from_booking.get("cargo_details"),
            )
        )

        rates = booking.get("rate_details")
        if isinstance(rates, list) and len(rates) > 0:
            merged["mbl_charges"] = (
                HelperFunctionController.map_mbl_charges_from_rate_details(rates)
            )
        else:
            merged["mbl_charges"] = []

        try:
            bid = int(booking["id"]) if booking.get("id") not in (None, "") else None
        except (TypeError, ValueError):
            bid = None
        merged["booking_id"] = bid
        merged["booking_shipment_code"] = str(booking.get("shipment_code") or "")
        if extract_hbl not in (None, ""):
            merged["hbl_number"] = extract_hbl
        return merged

    @staticmethod
    def strip_housing_display_only_fields(ho: dict) -> None:
        """Remove UI-only keys before Backend job-create serializer."""
        if not isinstance(ho, dict):
            return
        ho.pop("booking_shipment_code", None)
        charges = ho.get("mbl_charges")
        if isinstance(charges, list):
            for ch in charges:
                if isinstance(ch, dict):
                    ch.pop("charge_name", None)
                    ch.pop("charge_code", None)

    @staticmethod
    async def enrich_extracted_housing_booking(Request, extracted_data: dict) -> dict:
        """
        After HBL extract: match IMPORT booking (BOOKED/RECEIVED), then merge
        booking fields over extract (booking wins; extract fills gaps), including
        booking_id, booking_shipment_code, and exclusive mbl_charges.
        """
        if not isinstance(extracted_data, dict):
            return extracted_data
        root = extracted_data
        housing = root.get("housing_details")
        if not isinstance(housing, list):
            nested = root.get("data") if isinstance(root.get("data"), dict) else None
            if nested is not None:
                housing = nested.get("housing_details")
                root = nested
        if not isinstance(housing, list):
            return extracted_data

        for i, ho in enumerate(housing):
            if not isinstance(ho, dict):
                continue
            bid = ho.get("booking_id")
            try:
                bid = int(bid) if bid not in (None, "") else None
            except (TypeError, ValueError):
                bid = None

            full = None
            if bid:
                full = await HelperFunctionController.get_import_booking_by_id(
                    Request, bid
                )
            else:
                hbl_no = (ho.get("hbl_number") or "").strip()
                if not hbl_no:
                    continue
                found = await HelperFunctionController.find_import_booking(
                    Request, houseno=hbl_no
                )
                if not found:
                    continue
                full = await HelperFunctionController.get_import_booking_by_id(
                    Request, found["id"]
                )
                if full is None:
                    # Filter summary only — still stamp ids from match
                    full = {
                        "id": found["id"],
                        "shipment_code": found.get("shipment_code") or "",
                        "rate_details": [],
                    }

            if not isinstance(full, dict) or full.get("id") in (None, ""):
                continue

            merged = HelperFunctionController.merge_import_housing_from_booking(ho, full)
            housing[i] = merged
            print(
                f"[BookingEnrich] HBL merged | hbl={merged.get('hbl_number')} | "
                f"booking_id={merged.get('booking_id')} | "
                f"code={merged.get('booking_shipment_code')} | "
                f"charges={len(merged.get('mbl_charges') or [])}"
            )

        return extracted_data
