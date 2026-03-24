"""
extractor.py
------------
Async, class-based HBL PDF extraction using Claude AI.
All errors are printed to stdout with context.

Usage (standalone CLI):
    python extractor.py "path/to/file.pdf"

Usage (import):
    from extractor import HBLExtractor
    extractor = HBLExtractor()
    result    = await extractor.extract("path/to/file.pdf")
"""

import os
import sys
import json
import base64
import asyncio
import subprocess
import traceback
from pathlib import Path
import fitz
import anthropic
import pdfplumber

from dotenv import load_dotenv
load_dotenv()
# ---------------------------------------------------------------------------
# Custom Exceptions
# ---------------------------------------------------------------------------

class ExtractionError(Exception):
    """Raised when Claude extraction fails."""
    pass


class PDFReadError(Exception):
    """Raised when PDF cannot be read."""
    pass


class ParseError(Exception):
    """Raised when Claude response cannot be parsed into JSON."""
    pass


# ---------------------------------------------------------------------------
# HBLExtractor — async class
# ---------------------------------------------------------------------------

class HBLExtractor:

    MODEL             = "claude-sonnet-4-6"
    MAX_TOKENS        = 8192
    INPUT_COST_PER_M  = 3.00
    OUTPUT_COST_PER_M = 15.00
    SCANNED_THRESHOLD = 100
    DPI               = 200

    SYSTEM_PROMPT = """
SYSTEM PROMPT — JSON SCHEMA-AWARE EXTRACTION (HBL)
===================================================
You are a document data extraction specialist. Extract every piece of information
from a House Bill of Lading (HBL) document and return a single valid JSON object.

STRICT RULES
------------
1. REQUIRED fields must always be present. If not found, output null.
2. OPTIONAL fields absent from the document must be omitted entirely.
3. TYPE ENFORCEMENT — follow schema types exactly.
4. additionalProperties is true — add extra fields as snake_case keys.
5. Preserve original casing, punctuation, and numeric values exactly.
6. NIL or N/M in document → capture the literal string value.
7. phone/email with multiple values → JSON array of strings.
8. Dates → preserve exactly as printed.
9. Weights and volumes → always numbers, never strings.

VALIDATION CHECKS
-----------------
- All required schema fields are present.
- Sum of container gross_weight_kgs == cargo_summary.gross_weight_kgs.
- Sum of container volume_cbm == cargo_summary.volume_cbm.
- Count of containers array == cargo_summary.total_containers.
- If any check fails, note it in DIFF SUMMARY and output raw values as-is.

OUTPUT FORMAT
-------------
Return ONLY two blocks, no additional prose:
1. The populated JSON object (valid, parseable JSON). No markdown fences.
2. A plain-text DIFF SUMMARY:
   a. NULLED REQUIRED FIELDS
   b. NEW FIELDS ADDED
   c. VALIDATION WARNINGS
""".strip()


    SCHEMA = {
  "$schema": "http://json-schema.org/draft-07/schema#",
  "title": "Shipment Schema (MBL + HBL)",
  "description": "JSON Schema for FCL/LCL shipments covering Master Bill of Lading and House Bill of Lading details",
  "type": "object",
  "required": [
    "service",
    "service_type",
    "agent",
    "origin_code",
    "destination_code"
  ],
  "properties": {
    "id": {
      "type": "integer",
      "description": "Unique shipment identifier",
      "examples": [328]
    },
    "service": {
      "type": "string",
      "description": "Service type (FCL, LCL, AIR, etc.)",
      "examples": ["FCL", "LCL"]
    },
    "service_type": {
      "type": "string",
      "description": "Direction of the shipment",
      "examples": ["Import"]
    },
    "agent": {
      "type": "string",
      "description": "Agent customer code",
      "examples": ["CUST30756"]
    },
    "origin_code": {
    "type": "string",
    # "description": (
    #     "5-character UN/LOCODE for the PORT OF LOADING. "
    #     "Always uppercase. First 2 chars = ISO country code, next 3 = port code. "
    #     "e.g. 'THLCH' = Thailand Laem Chabang, 'INMAA' = India Chennai. "
    #     "If the value in the document looks garbled or mixed-case, reconstruct "
    #     "the correct UN/LOCODE from context (port name, country)."
    # ),
     "description": "origin sea port UN/LOCODE",
    "pattern": "^[A-Z]{2}[A-Z0-9]{3}$",
    "examples": ["THLCH", "INMAA", "USLAX"]
},
"destination_code": {
    "type": "string", 
    # "description": (
    #     "5-character UN/LOCODE for the PORT OF DISCHARGE. "
    #     "Always uppercase. Derive from 'Port of Discharge' or 'Place of Delivery' field."
    # ),
      "description": "Destination sea port UN/LOCODE",
    "pattern": "^[A-Z]{2}[A-Z0-9]{3}$",
    "examples": ["INMAA", "INVTZ", "INBOM"]
},
    "etd": {
      "type": "string",
      "format": "date-time",
      "description": "Estimated time of departure (ISO 8601)",
      "examples": ["2026-03-17T10:30:00Z"]
    },
    "eta": {
      "type": "string",
      "format": "date-time",
      "description": "Estimated time of arrival (ISO 8601)",
      "examples": ["2026-03-18T14:00:00Z"]
    },
    "atd": {
      "type": "string",
      "format": "date-time",
      "description": "Actual time of departure (ISO 8601)",
      "examples": ["2026-03-19T10:30:00Z"]
    },
    "ata": {
      "type": "string",
      "format": "date-time",
      "description": "Actual time of arrival (ISO 8601)",
      "examples": ["2026-03-20T10:30:00Z"]
    },
    "carrier_code": {
      "type": "string",
      "description": "Carrier/shipping line code",
      "examples": ["MMTU"]
    },
    "vessel_name": {
      "type": "string",
      "description": "Name of the vessel",
      "examples": ["MAERSK SHIPPING"]
    },
    "voyage_number": {
      "type": "string",
      "description": "Voyage number",
      "examples": ["V0017"]
    },
    "mbl_number": {
      "type": "string",
      "description": "Master Bill of Lading number",
      "examples": ["MBL123456"]
    },
    "mbl_date": {
      "type": "string",
      "format": "date",
      "description": "Date of Master Bill of Lading (YYYY-MM-DD)",
      "examples": ["2026-03-17"]
    },
    "carrier_booking_no": {
      "type": "string",
      "description": "Carrier booking reference number",
      "examples": ["CBRN2024019"]
    },
    "igm_no": {
      "type": "string",
      "description": "Import General Manifest number",
      "examples": ["Igm0018"]
    },
    "igm_date": {
      "type": "string",
      "format": "date",
      "description": "Date of IGM (YYYY-MM-DD)",
      "examples": ["2026-03-18"]
    },
    # "ocean_routings": {
    #   "type": "array",
    #   "description": "List of ocean/multimodal routing legs",
    #   "items": {
    #     "$ref": "#/definitions/OceanRouting"
    #   }
    # },
    "container_details": {
      "type": "array",
      "description": "List of containers in the shipment",
      "items": {
        "$ref": "#/definitions/ContainerDetail"
      }
    },
    "estimates": {
      "type": "array",
      "description": "MBL-level cost estimates",
      "items": {
        "$ref": "#/definitions/Estimate"
      }
    },
    "housing_details": {
      "type": "array",
      "description": "List of House Bill of Lading records",
      "items": {
        "$ref": "#/definitions/HousingDetail"
      }
    }
  },
  "definitions": {
    
    # "OceanRouting": {
    #   "type": "object",
    #   "description": "A single routing leg (vessel, rail, truck, etc.)",
    #   "required": ["transport_type", "from_port_code", "to_port_code"],
    #   "properties": {
    #     "id": {
    #       "type": "integer",
    #       "description": "Unique routing leg identifier",
    #       "examples": [671]
    #     },
    #     "transport_type": {
    #       "type": "string",
    #       "description": "Mode of transport for this leg",
    #       "enum": ["SEA", "RAIL", "TRUCK", "AIR"],
    #       "examples": ["Rail"]
    #     },
    #     "from_port_code": {
    #       "type": "string",
    #       "description": "Origin port/location code for this leg",
    #       "examples": ["USNYC"]
    #     },
    #     "to_port_code": {
    #       "type": "string",
    #       "description": "Destination port/location code for this leg",
    #       "examples": ["INMAA"]
    #     },
    #     "carrier_code": {
    #       "type": "string",
    #       "description": "Carrier or operator for this leg",
    #       "examples": ["Multimodel Carrier"]
    #     },
    #     "vessel": {
    #       "type": "string",
    #       "description": "Vessel name (if transport_type is Vessel or Feeder)",
    #       "examples": ["MAERSK SHIPPING"]
    #     },
    #     "voyage_number": {
    #       "type": "string",
    #       "description": "Voyage number (if applicable)",
    #       "examples": ["V0017"]
    #     },
    #     "rail_no": {
    #       "type": "string",
    #       "description": "Rail train/service number (if transport_type is Rail)",
    #       "examples": ["R017"]
    #     },
    #     "etd": {
    #       "type": "string",
    #       "format": "date-time",
    #       "description": "Estimated departure for this leg",
    #       "examples": ["2026-02-01T10:30:00Z"]
    #     },
    #     "eta": {
    #       "type": "string",
    #       "format": "date-time",
    #       "description": "Estimated arrival for this leg",
    #       "examples": ["2026-02-02T14:00:00Z"]
    #     },
    #     "atd": {
    #       "type": "string",
    #       "format": "date-time",
    #       "description": "Actual departure for this leg",
    #       "examples": ["2026-02-03T10:30:00Z"]
    #     },
    #     "ata": {
    #       "type": "string",
    #       "format": "date-time",
    #       "description": "Actual arrival for this leg",
    #       "examples": ["2026-02-04T10:30:00Z"]
    #     }
    #   },
    #   "additionalProperties": False
    # },
    
    "ContainerDetail": {
      "type": "object",
      "description": "Details of an individual container",
      "required": ["container_type_input", "container_no"],
      "properties": {
        "id": {
          "type": "integer",
          "description": "Unique container record identifier",
          "examples": [259]
        },
        "container_type_input": {
          "type": "string",
          "description": "Container ISO size/type code",
          "examples": ["42U1","22R1","42R1","45G0"
]
        },
        "container_no": {
          "type": "string",
          "description": "Container number",
          "examples": ["CONT123456"]
        },
        "actual_seal_no": {
          "type": "string",
          "description": "Actual seal number applied to the container",
          "examples": ["SEAL017"]
        },
        "customs_seal_no": {
          "type": "string",
          "description": "Customs seal number",
          "examples": ["CSEAL017"]
        },
        "loading_date": {
          "type": "string",
          "format": "date",
          "description": "Date the container was loaded (YYYY-MM-DD)",
          "examples": ["2026-01-17"]
        },
        "uploading_date": {
          "type": "string",
          "format": "date",
          "description": "Date the container was discharged/unloaded (YYYY-MM-DD)",
          "examples": ["2026-01-17"]
        },
        "cfs_id": {
          "type": "integer",
          "description": "Container Freight Station identifier",
          "examples": [1]
        }
      },
      "additionalProperties": False
    },
    "Estimate": {
      "type": "object",
      "description": "MBL-level cost/revenue estimate line item",
      "required": [
        "charge_id",
        "pp_cc",
        "unit_id",
        "no_of_unit",
        "currency_id",
        "roe",
        "cost_per_unit",
        "total_cost"
      ],
      "properties": {
        "id": {
          "type": "integer",
          "description": "Unique estimate record identifier",
          "examples": [75]
        },
        "supplier_code": {
          "type": "string",
          "description": "Supplier/vendor customer code",
          "examples": ["CUST15752"]
        },
        "charge_id": {
          "type": "integer",
          "description": "Charge type identifier",
          "examples": [89]
        },
        "pp_cc": {
          "type": "string",
          "description": "Prepaid or Collect indicator",
          "enum": ["Prepaid", "Collect", "PP", "CC"],
          "examples": ["Prepaid"]
        },
        "unit_id": {
          "type": "integer",
          "description": "Unit of measurement identifier",
          "examples": [126]
        },
        "no_of_unit": {
          "type": "integer",
          "description": "Number of units",
          "minimum": 0,
          "examples": [10]
        },
        "currency_id": {
          "type": "integer",
          "description": "Currency identifier",
          "examples": [68]
        },
        "roe": {
          "type": "number",
          "description": "Rate of exchange (2 decimal places)",
          "minimum": 0,
          "multipleOf": 0.01,
          "examples": [17.00]
        },
        "cost_per_unit": {
          "type": "number",
          "description": "Cost per unit (2 decimal places)",
          "minimum": 0,
          "multipleOf": 0.01,
          "examples": [100.50]
        },
        "total_cost": {
          "type": "number",
          "description": "Total cost — no_of_unit × cost_per_unit (2 decimal places)",
          "minimum": 0,
          "multipleOf": 0.01,
          "examples": [1005.00]
        }
      },
      "additionalProperties": False
    },
    "HousingDetail": {
      "type": "object",
      "description": "A single House Bill of Lading record",
      "required": [
        "hbl_number",
        "origin_code",
        "destination_code",
        "trade",
        "routed",
        # "routed_by",
        # "customer_service",
        "agent_name",
        "agent_address",
        "agent_email",
        "shipper_name",
        "shipper_address",
        "shipper_email",
        "consignee_name",
        "consignee_address",
        "consignee_email",
        "notify1_customer_name",
        "notify1_customer_address",
        "notify1_customer_email",
        "commodity_description",
        "marks_no",
        "shipment_terms_code",
        "cargo_details"
      ],
      "properties": {
        "id": {
          "type": "integer",
          "description": "Unique housing detail record identifier",
          "examples": [438]
        },
        "hbl_number": {
          "type": "string",
          "description": "House Bill of Lading number",
          "examples": ["HBL123456"]
        },
        "origin_code": {
          "type": "string",
          "description": "Origin sea port  UN/LOCODE",
          "examples": ["INMAA"]
        },
        "destination_code": {
          "type": "string",
          "description": "Destination sea port UN/LOCODE",
          "examples": ["INMAA"]
        },
        "trade": {
          "type": "string",
          "description": "Trade direction",
          "enum": ["Import"],
          "examples": ["Import"]
        },
        "routed": {
          "type": "string",
          "description": "Routing type",
          "examples": ["SELF"]
        },
        # "routed_by": {
        #   "type": "string",
        #   "description": "Person or entity responsible for routing",
        #   "examples": ["John Doe"]
        # },
        # "customer_service": {
        #   "type": "string",
        #   "description": "Customer service identifier or label",
        #   "examples": ["Customer Service"]
        # },
        "agent_name": {
          "type": "string",
          "description": "Name of the freight agent",
          "examples": ["Powermax Fitness India Pvt Ltd."]
        },
        "agent_address": {
          "type": "string",
          "description": "Full address of the freight agent",
          "examples": ["123 Agent Street, New York, USA"]
        },
        "agent_email": {
          "type": "string",
          "format": "email",
          "description": "Email of the freight agent",
          "examples": ["agent@example.com"]
        },
        "shipper_name": {
          "type": "string",
          "description": "Name of the shipper",
          "examples": ["Rahi Imports"]
        },
        "shipper_address": {
          "type": "string",
          "description": "Full address of the shipper",
          "examples": ["456 Shipper Street, New York, USA"]
        },
        "shipper_email": {
          "type": "string",
          "format": "email",
          "description": "Email of the shipper",
          "examples": ["shipper@example.com"]
        },
        "consignee_name": {
          "type": "string",
          "description": "Name of the consignee",
          "examples": ["Renew Photovoltaics Private Limited"]
        },
        "consignee_address": {
          "type": "string",
          "description": "Full address of the consignee",
          "examples": ["789 Consignee Street, Chennai, India"]
        },
        "consignee_email": {
          "type": "string",
          "format": "email",
          "description": "Email of the consignee",
          "examples": ["consignee@example.com"]
        },
        "notify1_customer_name": {
          "type": "string",
          "description": "Name of the first notify party",
          "examples": ["Lingo Logistics(H.K) Limited"]
        },
        "notify1_customer_address": {
          "type": "string",
          "description": "Address of the first notify party",
          "examples": ["321 Notify Street, Chennai, India"]
        },
        "notify1_customer_email": {
          "type": "string",
          "format": "email",
          "description": "Email of the first notify party",
          "examples": ["notify1@example.com"]
        },
        "notify2_customer_name": {
          "type": "string",
          "description": "Name of the second notify party",
          "examples": ["Chainvalue (Vietnam) Company Limited"]
        },
        "notify2_customer_address": {
          "type": "string",
          "description": "Address of the second notify party",
          "examples": ["321 Notify Street, Chennai, India"]
        },
        "notify2_customer_email": {
          "type": "string",
          "format": "email",
          "description": "Email of the second notify party",
          "examples": ["notify2@example.com"]
        },
        "commodity_description": {
          "type": "string",
          "description": "Description of the shipped commodity",
          "examples": ["Electronics and Machinery"]
        },
        "marks_no": {
          "type": "string",
          "description": "Marks and numbers on the packages",
          "examples": ["MARK017"]
        },
        "shipment_terms_code": {
          "type": "string",
          "description": "Incoterms shipment terms code",
          "enum": [
            "EXW", "FCA", "CPT", "CIP", "DAP", "DPU", "DDP",
            "FAS", "FOB", "CFR", "CIF"
          ],
          "examples": ["EXW"]
        },
        "item_no": {
          "type": "string",
          "description": "Item number reference",
          "examples": ["ITEM017"]
        },
        "sub_item_no": {
          "type": "string",
          "description": "Sub-item number reference",
          "examples": ["SUB017"]
        },
        "cargo_details": {
          "type": "array",
          "description": "List of cargo package line items",
          "minItems": 1,
          "items": {
            "$ref": "#/definitions/CargoDetail"
          }
        },
        "mbl_charges": {
          "type": "array",
          "description": "Charge line items associated with this HBL",
          "items": {
            "$ref": "#/definitions/MblCharge"
          }
        },
        "events": {
          "type": "array",
          "description": "Shipment milestone events for this HBL",
          "items": {
            "$ref": "#/definitions/ShipmentEvent"
          }
        }
      },
      "additionalProperties": True
    },
    "CargoDetail": {
      "type": "object",
      "description": "Individual cargo package detail within an HBL",
      "required": [
        "no_of_packages",
        "gross_weight",
        "volume",
        "chargeable_weight",
        "haz"
      ],
      "properties": {
        "id": {
          "type": "integer",
          "description": "Unique cargo detail record identifier",
          "examples": [656]
        },
        "container_no": {
          "type": "string",
          "description": "Container number this cargo belongs to",
          "examples": ["CONT17"]
        },
        "no_of_packages": {
          "type": "integer",
          "description": "Number of packages",
          "minimum": 0,
          "examples": [17]
        },
        "gross_weight": {
          "type": "string",
          "description": "Gross weight as a numeric string with up to 2 decimal places (KG)",
          "pattern": "^[0-9]+(\\.[0-9]{1,2})?$",
          "examples": ["2.00"]
        },
        "volume": {
          "type": "string",
          "description": "Volume as a numeric string with up to 2 decimal places (CBM)",
          "pattern": "^[0-9]+(\\.[0-9]{1,2})?$",
          "examples": ["2.00"]
        },
        "chargeable_weight": {
          "type": "string",
          "description": "Chargeable weight as a numeric string with up to 2 decimal places (KG)",
          "pattern": "^[0-9]+(\\.[0-9]{1,2})?$",
          "examples": ["20.00"]
        },
        "haz": {
          "type": "boolean",
          "description": "Whether the cargo is hazardous",
          "examples": [False, True]
        }
      },
      "additionalProperties": False
    },
    "MblCharge": {
      "type": "object",
      "description": "Charge line item within an HBL",
      "required": [
        "charge_id",
        "pp_cc",
        "unit_id",
        "no_of_unit",
        "currency_id",
        "roe",
        "amount_per_unit",
        "amount"
      ],
      "properties": {
        "id": {
          "type": "integer",
          "description": "Unique charge record identifier",
          "examples": [839]
        },
        "charge_id": {
          "type": "integer",
          "description": "Charge type identifier",
          "examples": [89]
        },
        "pp_cc": {
          "type": "string",
          "description": "Prepaid or Collect indicator",
          "enum": ["Prepaid", "Collect", "PP", "CC"],
          "examples": ["Prepaid"]
        },
        "unit_id": {
          "type": "integer",
          "description": "Unit of measurement identifier",
          "examples": [126]
        },
        "no_of_unit": {
          "type": "integer",
          "description": "Number of units",
          "minimum": 0,
          "examples": [31]
        },
        "currency_id": {
          "type": "integer",
          "description": "Currency identifier",
          "examples": [68]
        },
        "roe": {
          "type": "number",
          "description": "Rate of exchange (2 decimal places)",
          "minimum": 0,
          "multipleOf": 0.01,
          "examples": [17.50]
        },
        "amount_per_unit": {
          "type": "number",
          "description": "Charge amount per unit (2 decimal places)",
          "minimum": 0,
          "multipleOf": 0.01,
          "examples": [2.50]
        },
        "amount": {
          "type": "number",
          "description": "Total charge amount (2 decimal places)",
          "minimum": 0,
          "multipleOf": 0.01,
          "examples": [753.00]
        },
        "sell_local_amount": {
          "type": "string",
          "description": "Selling amount in local currency (up to 2 decimal places)",
          "pattern": "^[0-9]+(\\.[0-9]{1,2})?$",
          "examples": ["16.00"]
        },
        "cost_local_amount": {
          "type": "string",
          "description": "Cost amount in local currency (up to 2 decimal places)",
          "pattern": "^[0-9]+(\\.[0-9]{1,2})?$",
          "examples": ["15.00"]
        },
        "unit_cost": {
          "type": "string",
          "description": "Cost per unit (up to 2 decimal places)",
          "pattern": "^[0-9]+(\\.[0-9]{1,2})?$",
          "examples": ["10.00"]
        },
        "total_cost": {
          "type": "string",
          "description": "Total cost (up to 2 decimal places)",
          "pattern": "^[0-9]+(\\.[0-9]{1,2})?$",
          "examples": ["1000.00"]
        }
      },
      "additionalProperties": False
    },
    "ShipmentEvent": {
      "type": "object",
      "description": "A milestone event in the shipment lifecycle",
      "required": ["type", "date"],
      "properties": {
        "id": {
          "type": "integer",
          "description": "Unique event record identifier",
          "examples": [64]
        },
        "type": {
          "type": "string",
          "description": "Event type/milestone name",
          "enum": [
            "On Board",
            "Departed",
            "Arrived",
            "Delivered",
            "Customs Cleared",
            "Gate In",
            "Gate Out",
            "Empty Return"
          ],
          "examples": ["On Board"]
        },
        "date": {
          "type": "string",
          "format": "date",
          "description": "Date of the event (YYYY-MM-DD)",
          "examples": ["2026-03-18"]
        }
      },
      "additionalProperties": False
    }
  }
}

    # -------------------------------------------------------------------------
    # Constructor
    # -------------------------------------------------------------------------

    def __init__(
        self,
        api_key: str = None,
        model: str = None,
        max_tokens: int = None,
        scanned_threshold: int = None,
        dpi: int = None,
    ):
        self.api_key           = api_key           or os.getenv("ANTHROPIC_API_KEY", "your-anthropic-api-key")
        self.model             = model             or self.MODEL
        self.max_tokens        = max_tokens        or self.MAX_TOKENS
        self.scanned_threshold = scanned_threshold or self.SCANNED_THRESHOLD
        self.dpi               = dpi               or self.DPI
        self._client           = anthropic.AsyncAnthropic(api_key=self.api_key)
        print(f"[HBLExtractor] Initialized | model={self.model} | max_tokens={self.max_tokens}")

    # -------------------------------------------------------------------------
    # Public API
    # -------------------------------------------------------------------------

    async def extract(self, pdf_path: str) -> dict:
        """
        Full async extraction pipeline for one PDF.

        Returns:
        {
            "success":        bool,
            "pdf_type":       "digital" | "scanned",
            "extracted_data": { ...HBL JSON... } | None,
            "diff_summary":   str | None,
            "input_tokens":   int,
            "output_tokens":  int,
            "cost_usd":       float,
            "error":          str | None
        }
        """
        print(f"[HBLExtractor] Starting extraction | file={pdf_path}")
        result = self._empty_result()

        try:
            # Step 1 — validate file
            self._validate_file(pdf_path)

            # Step 2 — detect PDF type and build prompt
            pdf_type, system_prompt, messages = await self._prepare_prompt(pdf_path)
            result["pdf_type"] = pdf_type
            print(f"[HBLExtractor] PDF type detected | type={pdf_type} | file={pdf_path}")

            # Step 3 — call Claude
            raw, input_tokens, output_tokens = await self._call_claude(system_prompt, messages)
            result["input_tokens"]  = input_tokens
            result["output_tokens"] = output_tokens
            result["cost_usd"]      = self._compute_cost(input_tokens, output_tokens)
            print(f"[HBLExtractor] Claude response received | input_tokens={input_tokens} | output_tokens={output_tokens} | cost_usd={result['cost_usd']}")

            # Step 4 — parse response
            extracted_data, diff_summary = self._parse_response(raw)
            result["extracted_data"] = extracted_data
            result["diff_summary"]   = diff_summary
            result["success"]        = True
            print(f"[HBLExtractor] Extraction successful | file={pdf_path}")

        except PDFReadError as e:
            result["error"] = f"PDFReadError: {str(e)}"
            print(f"[HBLExtractor][ERROR] PDFReadError | file={pdf_path} | reason={str(e)}")
            print(traceback.format_exc())

        except ExtractionError as e:
            result["error"] = f"ExtractionError: {str(e)}"
            print(f"[HBLExtractor][ERROR] ExtractionError | file={pdf_path} | reason={str(e)}")
            print(traceback.format_exc())

        except ParseError as e:
            result["error"] = f"ParseError: {str(e)}"
            print(f"[HBLExtractor][ERROR] ParseError | file={pdf_path} | reason={str(e)}")
            print(traceback.format_exc())

        except anthropic.APIConnectionError as e:
            result["error"] = f"APIConnectionError: Could not reach Anthropic API — {str(e)}"
            print(f"[HBLExtractor][ERROR] APIConnectionError | file={pdf_path} | reason={str(e)}")
            print(traceback.format_exc())

        except anthropic.AuthenticationError as e:
            result["error"] = f"AuthenticationError: Invalid API key — {str(e)}"
            print(f"[HBLExtractor][ERROR] AuthenticationError | file={pdf_path} | reason={str(e)}")
            print(traceback.format_exc())

        except anthropic.RateLimitError as e:
            result["error"] = f"RateLimitError: Too many requests — {str(e)}"
            print(f"[HBLExtractor][ERROR] RateLimitError | file={pdf_path} | reason={str(e)}")
            print(traceback.format_exc())

        except anthropic.APIStatusError as e:
            result["error"] = f"APIStatusError [{e.status_code}]: {str(e)}"
            print(f"[HBLExtractor][ERROR] APIStatusError | status_code={e.status_code} | file={pdf_path} | reason={str(e)}")
            print(traceback.format_exc())

        except json.JSONDecodeError as e:
            result["error"] = f"JSONDecodeError: {str(e)}"
            print(f"[HBLExtractor][ERROR] JSONDecodeError | file={pdf_path} | reason={str(e)}")
            print(traceback.format_exc())

        except Exception as e:
            result["error"] = f"UnexpectedError: {type(e).__name__}: {str(e)}\n\n{traceback.format_exc()}"
            print(f"[HBLExtractor][ERROR] UnexpectedError | type={type(e).__name__} | file={pdf_path} | reason={str(e)}")
            print(traceback.format_exc())

        return result

    async def get_page_count(self, pdf_path: str) -> int:
        """Return number of pages in a PDF. Returns 0 on any error."""
        try:
            loop = asyncio.get_event_loop()
            count = await loop.run_in_executor(None, self._read_page_count, pdf_path)
            print(f"[HBLExtractor] Page count | file={pdf_path} | pages={count}")
            return count
        except Exception as e:
            print(f"[HBLExtractor][ERROR] get_page_count failed | file={pdf_path} | reason={str(e)}")
            return 0

    # -------------------------------------------------------------------------
    # Step 1 — Validate file
    # -------------------------------------------------------------------------

    def _validate_file(self, pdf_path: str):
        """Validate that the file exists and is a PDF."""
        path = Path(pdf_path)
        if not path.exists():
            raise PDFReadError(f"File not found: {pdf_path}")
        if not path.is_file():
            raise PDFReadError(f"Path is not a file: {pdf_path}")
        if path.suffix.lower() != ".pdf":
            raise PDFReadError(f"File is not a PDF: {pdf_path}")
        print(f"[HBLExtractor] File validated | file={pdf_path}")

    # -------------------------------------------------------------------------
    # Step 2 — Detect PDF type + build prompt
    # -------------------------------------------------------------------------

    async def _prepare_prompt(self, pdf_path: str) -> tuple:
        """
        Detect whether PDF is digital or scanned, then build the appropriate prompt.
        Returns (pdf_type, system_prompt, messages).
        """
        try:
            loop     = asyncio.get_event_loop()
            pdf_text = await loop.run_in_executor(None, self._extract_text, pdf_path)
            print(f"[HBLExtractor] Text extracted | chars={len(pdf_text.strip())} | file={pdf_path}")
        except Exception as e:
            print(f"[HBLExtractor][ERROR] Text extraction failed | file={pdf_path} | reason={str(e)}")
            print(traceback.format_exc())
            raise PDFReadError(f"Failed to read PDF text: {str(e)}") from e

        if len(pdf_text.strip()) >= self.scanned_threshold:
            print(f"[HBLExtractor] Building text prompt | file={pdf_path}")
            return "digital", self.SYSTEM_PROMPT, self._build_text_messages(pdf_text)
        else:
            print(f"[HBLExtractor] Scanned PDF detected — converting pages to images | file={pdf_path}")
            try:
                loop       = asyncio.get_event_loop()
                images_b64 = await loop.run_in_executor(None, self._pdf_to_images_b64, pdf_path)
                print(f"[HBLExtractor] Pages converted to images | count={len(images_b64)} | file={pdf_path}")
            except Exception as e:
                print(f"[HBLExtractor][ERROR] Image conversion failed | file={pdf_path} | reason={str(e)}")
                print(traceback.format_exc())
                raise PDFReadError(f"Failed to convert scanned PDF to images: {str(e)}") from e

            if not images_b64:
                raise PDFReadError("No pages could be rendered from the scanned PDF.")

            print(f"[HBLExtractor] Building vision prompt | file={pdf_path}")
            return "scanned", self.SYSTEM_PROMPT, self._build_vision_messages(images_b64)

    # -------------------------------------------------------------------------
    # Step 3 — Call Claude API (async)
    # -------------------------------------------------------------------------

    async def _call_claude(self, system_prompt: str, messages: list) -> tuple:
        """
        Send the prompt to Claude and return (raw_text, input_tokens, output_tokens).
        Raises ExtractionError on empty response.
        """
        print(f"[HBLExtractor] Calling Claude API | model={self.model}")
        try:
            response = await self._client.messages.create(
                model=self.model,
                max_tokens=self.max_tokens,
                system=system_prompt,
                messages=messages
            )
        except (
            anthropic.APIConnectionError,
            anthropic.AuthenticationError,
            anthropic.RateLimitError,
            anthropic.APIStatusError
        ):
            # Re-raise Anthropic-specific errors to be handled in extract()
            raise

        except Exception as e:
            print(f"[HBLExtractor][ERROR] Unexpected error calling Claude | reason={str(e)}")
            print(traceback.format_exc())
            raise ExtractionError(f"Unexpected error calling Claude: {str(e)}") from e

        if not response.content:
            print(f"[HBLExtractor][ERROR] Claude returned empty response")
            raise ExtractionError("Claude returned an empty response.")

        raw           = response.content[0].text
        input_tokens  = response.usage.input_tokens
        output_tokens = response.usage.output_tokens
        print(f"[HBLExtractor] Claude API call success | response_chars={len(raw)}")
        return raw, input_tokens, output_tokens

    # -------------------------------------------------------------------------
    # Step 4 — Parse Claude response
    # -------------------------------------------------------------------------

    def _parse_response(self, raw: str) -> tuple:
        """
        Split Claude's raw output into extracted dict + diff summary.
        Raises ParseError if JSON cannot be found or parsed.
        """
        print(f"[HBLExtractor] Parsing Claude response | raw_chars={len(raw)}")
        try:
            first_brace = raw.find("{")
            last_brace  = raw.rfind("}")

            if first_brace == -1 or last_brace == -1:
                print(f"[HBLExtractor][ERROR] No JSON braces found in Claude response")
                raise ParseError("No JSON object found in Claude response.")

            json_str     = raw[first_brace: last_brace + 1]
            diff_summary = raw[last_brace + 1:].strip().lstrip("`").strip()
            extracted    = json.loads(json_str)
            print(f"[HBLExtractor] Response parsed successfully | keys={list(extracted.keys())}")
            return extracted, diff_summary

        except json.JSONDecodeError as e:
            print(f"[HBLExtractor][ERROR] JSON decode failed | reason={str(e)}")
            print(traceback.format_exc())
            raise ParseError(f"JSON decode failed: {str(e)}") from e

        except ParseError:
            raise

        except Exception as e:
            print(f"[HBLExtractor][ERROR] Unexpected parse error | reason={str(e)}")
            print(traceback.format_exc())
            raise ParseError(f"Unexpected parse error: {str(e)}") from e

    # -------------------------------------------------------------------------
    # Prompt builders
    # -------------------------------------------------------------------------

    def _build_text_messages(self, pdf_text: str) -> list:
        schema_str = json.dumps(self.SCHEMA, indent=2)
        return [{
            "role": "user",
            "content": (
                "Extract all information from this HBL and return a schema-conformant "
                "JSON object plus DIFF SUMMARY.\n\n"
                f"--- BEGIN JSON SCHEMA ---\n{schema_str}\n--- END JSON SCHEMA ---\n\n"
                f"--- BEGIN DOCUMENT TEXT ---\n{pdf_text}\n--- END DOCUMENT TEXT ---"
            )
        }]

    def _build_vision_messages(self, images_b64: list) -> list:
        schema_str = json.dumps(self.SCHEMA, indent=2)
        content    = []
        for img_b64 in images_b64:
            content.append({
                "type": "image",
                "source": {"type": "base64", "media_type": "image/png", "data": img_b64}
            })
        content.append({
            "type": "text",
            "text": (
                "Read every page carefully — all text, stamps, tables, handwritten annotations.\n"
                "Extract all information and return a schema-conformant JSON object plus DIFF SUMMARY.\n\n"
                f"--- BEGIN JSON SCHEMA ---\n{schema_str}\n--- END JSON SCHEMA ---"
            )
        })
        return [{"role": "user", "content": content}]

    # -------------------------------------------------------------------------
    # Sync helpers (run in executor to keep async loop unblocked)
    # -------------------------------------------------------------------------

    def _extract_text(self, pdf_path: str) -> str:
        """Extract raw text from a PDF using pdfplumber (sync)."""
        try:
            texts = []
            with pdfplumber.open(pdf_path) as pdf:
                for i, page in enumerate(pdf.pages):
                    try:
                        text = page.extract_text()
                        if text:
                            texts.append(text)
                    except Exception as page_err:
                        print(f"[HBLExtractor][WARN] Could not extract text from page {i} | reason={str(page_err)}")
            return "\n\n".join(texts)
        except Exception as e:
            print(f"[HBLExtractor][ERROR] pdfplumber failed | file={pdf_path} | reason={str(e)}")
            print(traceback.format_exc())
            raise PDFReadError(f"pdfplumber failed to open PDF: {str(e)}") from e

    def _pdf_to_images_b64(self, pdf_path: str) -> list:
     """Convert each PDF page to base64 PNG using pymupdf (sync)."""
     try:
        import fitz
     except ImportError as e:
        print(f"[HBLExtractor][ERROR] pymupdf not installed | reason={str(e)}")
        raise PDFReadError("pymupdf is not installed. Run: uv add pymupdf") from e

     try:
        zoom       = self.dpi / 72
        mat        = fitz.Matrix(zoom, zoom)
        images_b64 = []
        doc        = fitz.open(pdf_path)

        for page_num, page in enumerate(doc):
            try:
                pix = page.get_pixmap(matrix=mat)
                images_b64.append(
                    base64.standard_b64encode(pix.tobytes("png")).decode("utf-8")
                )
                print(f"[HBLExtractor] Page {page_num} rendered | file={pdf_path}")
            except Exception as page_err:
                print(f"[HBLExtractor][WARN] Could not render page {page_num} | reason={str(page_err)}")

        doc.close()
        return images_b64

     except Exception as e:
        print(f"[HBLExtractor][ERROR] pymupdf failed to render PDF | file={pdf_path} | reason={str(e)}")
        print(traceback.format_exc())
        raise PDFReadError(f"pymupdf failed to render PDF: {str(e)}") from e

    def _read_page_count(self, pdf_path: str) -> int:
        """Return page count (sync)."""
        try:
            with pdfplumber.open(pdf_path) as pdf:
                return len(pdf.pages)
        except Exception as e:
            print(f"[HBLExtractor][ERROR] Page count failed | file={pdf_path} | reason={str(e)}")
            return 0

    # -------------------------------------------------------------------------
    # Utilities
    # -------------------------------------------------------------------------

    def _compute_cost(self, input_tokens: int, output_tokens: int) -> float:
        return round(
            (input_tokens  / 1_000_000) * self.INPUT_COST_PER_M +
            (output_tokens / 1_000_000) * self.OUTPUT_COST_PER_M,
            6
        )

    def _empty_result(self) -> dict:
        return {
            "success":        False,
            "pdf_type":       "unknown",
            "extracted_data": None,
            "diff_summary":   None,
            "input_tokens":   0,
            "output_tokens":  0,
            "cost_usd":       0.0,
            "error":          None,
        }


# ---------------------------------------------------------------------------
# CLI usage: python extractor.py "path/to/file.pdf"
# ---------------------------------------------------------------------------

async def _cli_main():
    sys.stdout.reconfigure(encoding="utf-8")

    if len(sys.argv) < 2:
        print("Usage: python extractor.py \"path/to/file.pdf\"")
        sys.exit(1)

    pdf_path  = sys.argv[1]
    extractor = HBLExtractor()

    result = await extractor.extract(pdf_path)

    if not result["success"]:
        print(f"\n[FAILED] Extraction error:\n{result['error']}")
        sys.exit(1)

    out_path = str(Path(pdf_path).with_suffix(".json"))
    try:
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(result["extracted_data"], f, indent=2, ensure_ascii=False)
        print(f"[CLI] JSON saved | path={out_path}")
    except OSError as e:
        print(f"[CLI][ERROR] Could not save JSON file | reason={str(e)}")
        sys.exit(1)

    print(f"\n--- DIFF SUMMARY ---\n{result['diff_summary'] or '(none)'}")
    print(f"\n--- COST ---")
    print(f"  Input  : {result['input_tokens']:,} tokens")
    print(f"  Output : {result['output_tokens']:,} tokens")
    print(f"  Cost   : ${result['cost_usd']:.4f}  (~₹{result['cost_usd'] * 83:.2f})")


if __name__ == "__main__":
    asyncio.run(_cli_main())