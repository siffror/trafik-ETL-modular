# src/app/etl_runner.py
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
    """Build minimal TRV XML query for Situation; filter on Situation-level field."""
    since = (datetime.now(timezone.utc) - timedelta(days=days_back)).strftime("%Y-%m-%dT%H:%M:%SZ")
    return f"""<REQUEST>
  <LOGIN authenticationkey="{{API_KEY}}"/>
  <QUERY objecttype="Situation" schemaversion="1">
    <FILTER>
      <GT name="PublicationTime" value="{since}"/>
    </FILTER>

    <!-- Situation id for primary key -->
    <INCLUDE>Id</INCLUDE>

    <!-- Deviation fields (valid under Situation) -->
    <INCLUDE>Deviation.StartTime</INCLUDE>
    <INCLUDE>Deviation.EndTime</INCLUDE>
    <INCLUDE>Deviation.ModifiedTime</INCLUDE>
    <INCLUDE>Deviation.Message</INCLUDE>
    <INCLUDE>Deviation.MessageType</INCLUDE>
    <INCLUDE>Deviation.RoadNumber</INCLUDE>
    <INCLUDE>Deviation.LocationDescriptor</INCLUDE>
    <INCLUDE>Deviation.CountyNo</INCLUDE>
    <INCLUDE>Deviation.Geometry.WGS84</INCLUDE>
  </QUERY>
</REQUEST>"""



def _extract_lat_lon(wgs84: str) -> Tuple[float | None, float | None]:
    """
    Extract (lat, lon) from a WGS84 string like 'POINT (lon lat)'.
    Return (None, None) if not parsable.
    """
    try:
        if wgs84 and "POINT" in wgs84:
            coords = wgs84[wgs84.find("(") + 1 : wgs84.find(")")].strip()
            lon_str, lat_str = coords.split()
            return float(lat_str), float(lon_str)
    except Exception:
        pass
    return None, None


def _parse_xml(xml_text: str) -> List[Dict[str, Any]]:
    """
    Parse TRV XML into a list of incident dicts by iterating Situation/Deviation.
    Missing fields are returned as empty strings (or None for coordinates).
    """
    root = ET.fromstring(xml_text)
    rows: List[Dict[str, Any]] = []

    for sit in root.findall(".//Situation"):
        # Get the Situation ID (this is the primary identifier)
        situation_id = sit.find("Id")
        situation_id_text = situation_id.text.strip() if (situation_id is not None and situation_id.text) else ""
        
        for dev_idx, dev in enumerate(sit.findall("Deviation")):
            def text(tag: str) -> str:
                el = dev.find(tag)
                return el.text.strip() if (el is not None and el.text) else ""

            wgs84 = text("Geometry/WGS84")
            lat, lon = _extract_lat_lon(wgs84)

            # Create a unique incident ID by combining situation ID and deviation index
            incident_id = f"{situation_id_text}_{dev_idx}" if situation_id_text else f"unknown_{dev_idx}"

            rows.append({
                "incident_id": incident_id,
                "message": text("Message"),
                "message_type": text("MessageType"),
                "location_descriptor": text("LocationDescriptor"),
                "road_number": text("RoadNumber"),
                "county_name": text("CountyName"),
                "county_no": text("CountyNo"),
                "start_time_utc": text("StartTime"),
                "end_time_utc": text("EndTime"),
                "modified_time_utc": text("ModifiedTime"),
                "latitude": lat,
                "longitude": lon,
                "status": text("Status"),
            })

    return rows


def _normalize_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    Coerce dtypes to what the Streamlit app expects.
    Do not convert the time strings to datetimes here; the app handles that.
    """
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
    Fetch data from TRV (XML), parse, upsert into SQLite, return summary.
    """
    t0 = time.time()

    # Fail fast if the API key is missing
    if not API_KEY:
        raise RuntimeError("TRAFIKVERKET_API_KEY is not set")

    url = BASE_URL or "https://api.trafikinfo.trafikverket.se/v2/data.xml"
    print(f"[ETL] Using TRV URL: {url}", flush=True)

    client = TRVClient(api_key=API_KEY, base_url=url, timeout=30)

    # Build payload and call API (mask key in logs)
    payload_xml = _build_query_xml(days_back=days_back).replace("{API_KEY}", API_KEY)
    print("[ETL] Payload (masked):", payload_xml.replace(API_KEY, "***"), flush=True)

    xml_text = client.post(payload_xml)

    # Parse XML → DataFrame
    rows = _parse_xml(xml_text)
    df = pd.DataFrame(rows)
    if df.empty:
        return {"rows": 0, "pagar": 0, "kommande": 0, "seconds": round(time.time() - t0, 2)}

    df = _normalize_df(df)

    # Upsert into SQLite (conflict on primary key incident_id)
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

    pagar = int((df["status"] == "PÅGÅR").sum()) if "status" in df.columns else 0
    kommande = int((df["status"] == "KOMMANDE").sum()) if "status" in df.columns else 0

    return {
        "rows": int(len(df)),
        "pagar": pagar,
        "kommande": kommande,
        "seconds": round(time.time() - t0, 2),
    }
