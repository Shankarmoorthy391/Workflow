import httpx
import os
from typing import Dict, Any
from fastapi import HTTPException
from dotenv import load_dotenv
load_dotenv()
import json

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
        url = 'http://127.0.0.1:8000/api/job-create/'
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