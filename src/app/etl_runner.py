# src/app/etl_runner.py
from __future__ import annotations

import os
import time
import logging
import sqlite3
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Tuple, Optional

import pandas as pd

from src.trv.client import TRVClient
from src.trv.load_sqlite import ensure_schema, upsert_incidents

LOG = logging.getLogger("ETL")
LOG.setLevel(logging.INFO)

# --- Configuration from environment
API_KEY: str = os.getenv("TRAFIKVERKET_API_KEY", "")
BASE_URL: str = os.getenv(
    "TRAFIKVERKET_URL",
    "https://api.trafikinfo.trafikverket.se/v2/data.xml",
)

# ----------------------------- Query builder -----------------------------
def _build_query_xml(days_back: int = 1) -> str:
    """
    Build a safe Situation query:
    - Filter on a Situation-level attribute (PublicationTime) to avoid invalid 'Deviation.*' filters.
    - Include the full Deviation node; we will extract fields in Python.
    """
    since = (datetime.now(timezone.utc) - timedelta(days=days_back)).strftime("%Y-%m-%dT%H:%M:%SZ")
    return f"""<REQUEST>
  <LOGIN authenticationkey="{{API_KEY}}"/>
  <QUERY objecttype="Situation" schemaversion="1">
    <FILTER>
      <GT name="PublicationTime" value="{since}"/>
    </FILTER>
    <INCLUDE>Id</INCLUDE>
    <INCLUDE>PublicationTime</INCLUDE>
    <INCLUDE>Deviation</INCLUDE>
  </QUERY>
</REQUEST>"""

# ------------------------------ Helpers ----------------------------------
def _extract_lat_lon(wgs84: Optional[str]) -> Tuple[Optional[float], Optional[float]]:
    """
    Extract (lat, lon) from WKT-like 'POINT (lon lat)' string. Return (None, None) if not parsable.
    """
    if not wgs84:
        return None, None
    try:
        if "POINT" in wgs84:
            coords = wgs84[wgs84.find("(") + 1 : wgs84.find(")")].strip()
            parts = coords.split()
            if len(parts) == 2:
                lon = float(parts[0])
                lat = float(parts[1])
                return lat, lon
    except Exception:
        pass
    return None, None

def _s(val: Any) -> str:
    """Safe string trim."""
    if val is None:
        return ""
    s = str(val).strip()
    return s

def _first(list_or_none: Optional[list]) -> Optional[dict]:
    """Return the first element if it's a non-empty list of dicts."""
    if isinstance(list_or_none, list) and list_or_none:
        item = list_or_none[0]
        return item if isinstance(item, dict) else None
    return None

# ------------------------------ JSON parse -------------------------------
def _parse_situations_json(resp_json: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Parse TRV JSON structure:
      { "RESPONSE": { "RESULT": [ { "Situation": [ ... ] } ] } }
    Each Situation may contain a list of Deviation items. We flatten to one row per Deviation.
    """
    rows: List[Dict[str, Any]] = []

    response = resp_json.get("RESPONSE") or {}
    result_list = response.get("RESULT") or []
    # RESULT is typically a list with one dict that has "Situation"
    for result in result_list:
        situations = result.get("Situation") or []
        for sit in situations:
            situation_id = _s(sit.get("Id"))
            pub_time = _s(sit.get("PublicationTime"))

            deviations = sit.get("Deviation") or []
            # Deviation can be a list. If it's a dict, wrap in list to normalize.
            if isinstance(deviations, dict):
                deviations = [deviations]

            for dev in deviations:
                # common fields inside Deviation
                message = _s(dev.get("Message"))
                message_type = _s(dev.get("MessageType"))
                location_descriptor = _s(dev.get("LocationDescriptor"))
                road_number = _s(dev.get("RoadNumber"))
                county_name = _s(dev.get("CountyName"))
                county_no = dev.get("CountyNo")
                status = _s(dev.get("Status"))

                # times (keep as strings; Streamlit loader can parse with parse_dates)
                start_time = _s(dev.get("StartTime"))
                end_time = _s(dev.get("EndTime"))
                modified_time = _s(dev.get("ModifiedTime"))

                # geometry → lat/lon
                wgs84 = None
                geom = dev.get("Geometry")
                if isinstance(geom, dict):
                    wgs84 = geom.get("WGS84")

                lat, lon = _extract_lat_lon(wgs84)

                # Row schema matches your 13 columns
                rows.append({
                    "incident_id": situation_id or _s(dev.get("Id")),  # fallback if Situation Id missing
                    "message": message,
                    "message_type": message_type,
                    "location_descriptor": location_descriptor,
                    "road_number": road_number,
                    "county_name": county_name,
                    "county_no": county_no,
                    "start_time_utc": start_time,
                    "end_time_utc": end_time,
                    "modified_time_utc": modified_time or pub_time,
                    "latitude": lat,
                    "longitude": lon,
                    "status": status,
                })

    return rows

def _normalize_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    Coerce dtypes to what your dashboard expects (align with load_sqlite.py schema).
    """
    if "county_no" in df.columns:
        df["county_no"] = pd.to_numeric(df["county_no"], errors="coerce").astype("Int64")
    for col in ("latitude", "longitude"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    for col in ("incident_id","message","message_type","location_descriptor","road_number","county_name","status"):
        if col in df.columns:
            df[col] = df[col].astype("string").str.strip()
    return df

# ------------------------------- ETL main --------------------------------
def run_etl(db_path: str, days_back: int = 1) -> Dict[str, Any]:
    """
    Fetch from TRV (JSON), parse, upsert into SQLite, return summary dict used by Streamlit.
    """
    t0 = time.time()

    if not API_KEY:
        raise RuntimeError("TRAFIKVERKET_API_KEY is not set")

    url = BASE_URL or "https://api.trafikinfo.trafikverket.se/v2/data.xml"
    LOG.info("🚦 ETL started • db=%s • days_back=%s • url=%s", db_path, days_back, url)
    print(f"[ETL] Using TRV URL: {url}", flush=True)

    # Prepare client
    client = TRVClient(api_key=API_KEY, base_url=url, timeout=30)

    # Build payload and call API (JSON response expected by your client)
    payload_xml = _build_query_xml(days_back=days_back).replace("{API_KEY}", API_KEY)
    resp_json = client.post(payload_xml)  # -> dict

    # Parse JSON → rows → DataFrame
    rows = _parse_situations_json(resp_json)
    LOG.info("📥 Retrieved %d flattened rows", len(rows))

    df = pd.DataFrame(rows)
    if df.empty:
        LOG.info("No rows to upsert; ETL finished quickly.")
        return {"rows": 0, "pagar": 0, "kommande": 0, "seconds": round(time.time() - t0, 2)}

    df = _normalize_df(df)

    # Ensure schema and upsert
    ensure_schema(db_path)
    upsert_incidents(db_path, df)

    pagar = int((df["status"] == "PÅGÅR").sum()) if "status" in df.columns else 0
    kommande = int((df["status"] == "KOMMANDE").sum()) if "status" in df.columns else 0

    seconds = round(time.time() - t0, 2)
    LOG.info("✅ ETL done • rows=%s • PÅGÅR=%s • KOMMANDE=%s • time=%ss • db=%s",
             len(df), pagar, kommande, seconds, db_path)

    return {"rows": int(len(df)), "pagar": pagar, "kommande": kommande, "seconds": seconds}
