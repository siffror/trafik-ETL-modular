# src/app/etl_runner.py
import os
import time
import sqlite3
import pandas as pd
from datetime import datetime, timedelta, timezone
from xml.etree import ElementTree as ET
from typing import List, Dict, Any, Tuple

from src.trv.client import TRVClient

# --- Read configuration from environment
API_KEY = os.getenv("TRAFIKVERKET_API_KEY", "")
BASE_URL = os.getenv("TRAFIKVERKET_URL", "https://api.trafikinfo.trafikverket.se/v2/data.xml")


def _require_api_key() -> str:
    """Return API key or raise a clear error if missing."""
    key = (os.getenv("TRAFIKVERKET_API_KEY") or API_KEY or "").strip()
    if not key:
        raise RuntimeError("TRAFIKVERKET_API_KEY is not set")
    return key


def _build_query_xml(days_back: int = 1) -> str:
    """Build minimal TRV XML query (adjust to your schema)."""
    since = (datetime.now(timezone.utc) - timedelta(days=days_back)).strftime("%Y-%m-%dT%H:%M:%SZ")
    return f"""<REQUEST>
  <LOGIN authenticationkey="{{API_KEY}}"/>
  <QUERY objecttype="Situation" schemaversion="1">
    <FILTER>
      <GT name="StartTime" value="{since}"/>
    </FILTER>
    <INCLUDE>Id</INCLUDE>
    <INCLUDE>StartTime</INCLUDE>
    <INCLUDE>EndTime</INCLUDE>
    <INCLUDE>ModifiedTime</INCLUDE>
    <INCLUDE>CountyNo</INCLUDE>
    <INCLUDE>CountyName</INCLUDE>
    <INCLUDE>Message</INCLUDE>
    <INCLUDE>MessageType</INCLUDE>
    <INCLUDE>RoadNumber</INCLUDE>
    <INCLUDE>LocationDescriptor</INCLUDE>
    <INCLUDE>Status</INCLUDE>
    <INCLUDE>Geometry.WGS84</INCLUDE>
  </QUERY>
</REQUEST>"""


def _parse_xml(xml_text: str) -> List[Dict[str, Any]]:
    """Parse TRV XML into list of dicts (adapt tags to your schema)."""
    root = ET.fromstring(xml_text)
    rows: List[Dict[str, Any]] = []
    for node in root.findall(".//Situation"):
        def text(path: str) -> str:
            el = node.find(path)
            return el.text.strip() if (el is not None and el.text) else ""
        wgs84 = text("Geometry/WGS84")
        lat, lon = _extract_lat_lon(wgs84)
        rows.append({
            "incident_id": text("Id"),
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


def _extract_lat_lon(wgs84: str) -> Tuple[float, float]:
    """Extract (lat, lon) from 'POINT (lon lat)'; return (None, None) if not parsable."""
    try:
        if "POINT" in wgs84:
            coords = wgs84[wgs84.find("(")+1:wgs84.find(")")].strip()
            parts = coords.split()
            if len(parts) == 2:
                lon = float(parts[0]); lat = float(parts[1])
                return lat, lon
    except Exception:
        pass
    return (None, None)


def _normalize_df(df: pd.DataFrame) -> pd.DataFrame:
    """Coerce dtypes to what the Streamlit app expects."""
    if "county_no" in df.columns:
        df["county_no"] = pd.to_numeric(df["county_no"], errors="coerce").astype("Int64")
    for col in ["latitude", "longitude"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    for col in ["incident_id","message","message_type","location_descriptor","road_number","county_name","status"]:
        if col in df.columns:
            df[col] = df[col].astype("string").str.strip()
    return df


def run_etl(db_path: str, days_back: int = 1) -> Dict[str, Any]:
    """Fetch from TRV (XML), parse, upsert into SQLite, return summary."""
    t0 = time.time()

    # Fail fast (no tricky inline indentation)
    key = _require_api_key()

    url = BASE_URL or "https://api.trafikinfo.trafikverket.se/v2/data.xml"
    print(f"[ETL] Using TRV URL: {url}", flush=True)

    client = TRVClient(api_key=key, base_url=url, timeout=30)

    # Build payload and call API
    payload_xml = _build_query_xml(days_back=days_back).replace("{API_KEY}", key)
    xml_text = client.post(payload_xml)  # TRVClient.post returns XML text

    # Parse XML → rows
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
            incident_id,message,message_type,location_descriptor,road_number,
            county_name,county_no,start_time_utc,end_time_utc,modified_time_utc,
            latitude,longitude,status
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
    return {"rows": int(len(df)), "pagar": pagar, "kommande": kommande, "seconds": round(time.time() - t0, 2)}


# Backwards-compat helper
def _get_api_key() -> str:
    return os.getenv("TRAFIKVERKET_API_KEY", "")
