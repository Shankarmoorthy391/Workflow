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

            target = (houseno or "").strip().lower()
            if exact_houseno and target:
                exact = [
                    r
                    for r in rows
                    if isinstance(r, dict)
                    and str(r.get("houseno") or "").strip().lower() == target
                ]
                rows = exact if exact else rows

            if len(rows) != 1:
                # Ambiguous or empty after filter — do not auto-map
                if len(rows) > 1 and shipment_code:
                    code = str(shipment_code).strip().lower()
                    exact_code = [
                        r
                        for r in rows
                        if isinstance(r, dict)
                        and str(r.get("shipment_code") or "").strip().lower() == code
                    ]
                    if len(exact_code) == 1:
                        rows = exact_code
                    else:
                        return None
                else:
                    return None

            row = rows[0] if isinstance(rows[0], dict) else None
            if not row or row.get("id") is None:
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
