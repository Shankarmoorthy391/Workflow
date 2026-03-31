import httpx
import os
from typing import Dict, Any
from fastapi import HTTPException
from dotenv import load_dotenv
load_dotenv()
import json
from sqlalchemy import text
from Config.database import get_async_db

class HelperFunctionController:

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
    