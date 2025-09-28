# src/app/etl_runner.py
# WORKING VERSION - Based on proven GitHub example
import os
import time
import sqlite3
from typing import List, Dict, Any, Tuple
from datetime import datetime, timedelta, timezone
from xml.etree import ElementTree as ET

import pandas as pd

from src.trv.client import TRVClient

# --- Configuration from environment
API_KEY = os.getenv("TRAFIKVERKET_API_KEY", "")
BASE_URL = os.getenv("TRAFIKVERKET_URL", "https://api.trafikinfo.trafikverket.se/v2/data.xml")


def _build_query_xml(days_back: int = 1) -> str:
    """
    Build TRV XML query - WORKING VERSION based on GitHub example.
    NO Deviation.Id anywhere - this was the problem!
    """
    since = (datetime.now(timezone.utc) - timedelta(days=days_back)).strftime("%Y-%m-%dT%H:%M:%SZ")
    
    # This query works - tested and proven
    return f"""<REQUEST>
<LOGIN authenticationkey="{{API_KEY}}" />
<QUERY objecttype="Situation" schemaversion="1.2">
<FILTER>
<GT name="PublicationTime" value="{since}" />
</FILTER>
<INCLUDE>Id</INCLUDE>
<INCLUDE>PublicationTime</INCLUDE>
<INCLUDE>Deviation.Message</INCLUDE>
<INCLUDE>Deviation.RoadNumber</INCLUDE>
<INCLUDE>Deviation.CountyNo</INCLUDE>
<INCLUDE>Deviation.StartTime</INCLUDE>
<INCLUDE>Deviation.Geometry.WGS84</INCLUDE>
</QUERY>
</REQUEST>"""


def _extract_lat_lon(wgs84: str) -> Tuple[float | None, float | None]:
    """Extract (lat, lon) from WGS84 string like 'POINT (lon lat)'."""
    try:
        if wgs84 and "POINT" in wgs84:
            coords = wgs84[wgs84.find("(") + 1 : wgs84.find(")")].strip()
            lon_str, lat_str = coords.split()
            return float(lat_str), float(lon_str)
    except Exception:
        pass
    return None, None


def _parse_xml(xml_text: str) -> List[Dict[str, Any]]:
    """Parse TRV XML - WORKING VERSION."""
    root = ET.fromstring(xml_text)
    rows: List[Dict[str, Any]] = []

    for sit in root.findall(".//Situation"):
        # Get Situation ID (NOT Deviation.Id!)
        situation_id = sit.find("Id")
        situation_id_text = situation_id.text.strip() if (situation_id is not None and situation_id.text) else ""
        
        for dev_idx, dev in enumerate(sit.findall("Deviation")):
            def text(tag: str) -> str:
                el = dev.find(tag)
                return el.text.strip() if (el is not None and el.text) else ""

            wgs84 = text("Geometry/WGS84")
            lat, lon = _extract_lat_lon(wgs84)

            # Create unique ID: situation_id + deviation_index
            incident_id = f"{situation_id_text}_{dev_idx}" if situation_id_text else f"unknown_{dev_idx}"

            rows.append({
                "incident_id": incident_id,
                "message": text("Message"),
                "message_type": "",  # Keep simple for now
                "location_descriptor": "",
                "road_number": text("RoadNumber"),
                "county_name": "",
                "county_no": text("CountyNo"),
                "start_time_utc": text("StartTime"),
                "end_time_utc": "",
                "modified_time_utc": "",
                "latitude": lat,
                "longitude": lon,
                "status": "",
            })

    return rows


def _normalize_df(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize DataFrame dtypes."""
    if "county_no" in df.columns:
        df["county_no"] = pd.to_numeric(df["county_no"], errors="coerce").astype("Int64")

    for col in ["latitude", "longitude"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    for col in [
        "incident_id", "message", "message_type", "location_descriptor",
        "road_number", "county_name", "status",
        "start_time_utc", "end_time_utc", "modified_time_utc",
    ]:
        if col in df.columns:
            df[col] = df[col].astype("string").str.strip()

    return df


def run_etl(db_path: str, days_back: int = 1) -> Dict[str, Any]:
    """
    ETL function - WORKING VERSION.
    This will NOT get Deviation.Id error!
    """
    t0 = time.time()

    if not API_KEY:
        raise RuntimeError("TRAFIKVERKET_API_KEY is not set")

    url = BASE_URL or "https://api.trafikinfo.trafikverket.se/v2/data.xml"
    print(f"[ETL] Using TRV URL: {url}", flush=True)

    client = TRVClient(api_key=API_KEY, base_url=url, timeout=30)

    # Build and send request
    payload_xml = _build_query_xml(days_back=days_back).replace("{API_KEY}", API_KEY)
    print("[ETL] Sending WORKING query (masked):", payload_xml.replace(API_KEY, "***"), flush=True)

    xml_text = client.post(payload_xml)
    print(f"[ETL] Got response length: {len(xml_text)}", flush=True)

    # Parse and store
    rows = _parse_xml(xml_text)
    df = pd.DataFrame(rows)
    
    print(f"[ETL] Parsed {len(rows)} incidents", flush=True)
    
    if df.empty:
        print("[ETL] No data found", flush=True)
        return {"rows": 0, "pagar": 0, "kommande": 0, "seconds": round(time.time() - t0, 2)}

    df = _normalize_df(df)

    # Store in SQLite
    con = sqlite3.connect(db_path)
    cur = con.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS incidents (
            incident_id TEXT PRIMARY KEY,
            message TEXT,
            message_type TEXT,
            location_descriptor TEXT,
            road_number TEXT,
            county_name TEXT,
            county_no INTEGER,
            start_time_utc TEXT,
            end_time_utc TEXT,
            modified_time_utc TEXT,
            latitude REAL,
            longitude REAL,
            status TEXT
        )
    """)

    cols = [
        "incident_id","message","message_type","location_descriptor","road_number",
        "county_name","county_no","start_time_utc","end_time_utc","modified_time_utc",
        "latitude","longitude","status"
    ]

    sql = """
        INSERT INTO incidents (
            incident_id, message, message_type, location_descriptor, road_number,
            county_name, county_no, start_time_utc, end_time_utc, modified_time_utc,
            latitude, longitude, status
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
        ON CONFLICT(incident_id) DO UPDATE SET
            message=excluded.message,
            message_type=excluded.message_type,
            location_descriptor=excluded.location_descriptor,
            road_number=excluded.road_number,
            county_name=excluded.county_name,
            county_no=excluded.county_no,
            start_time_utc=excluded.start_time_utc,
            end_time_utc=excluded.end_time_utc,
            modified_time_utc=excluded.modified_time_utc,
            latitude=excluded.latitude,
            longitude=excluded.longitude,
            status=excluded.status
    """

    cur.executemany(sql, [tuple(r.get(c) for c in cols) for r in df.to_dict(orient="records")])
    con.commit()
    con.close()

    print(f"[ETL] Successfully stored {len(df)} incidents", flush=True)

    return {
        "rows": int(len(df)),
        "pagar": 0,  # Will be 0 until we figure out status values
        "kommande": 0,
        "seconds": round(time.time() - t0, 2),
    }
