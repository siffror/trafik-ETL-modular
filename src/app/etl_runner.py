# src/app/etl_runner.py
import sqlite3
import pandas as pd
from datetime import datetime, timedelta, timezone
from xml.etree import ElementTree as ET
from typing import List, Dict, Any, Tuple

from src.trv.client import TRVClient

def _build_query_xml(days_back: int = 1) -> str:
    """
    Build a minimal TRV XML query. Adjust the OBJECT and fields as per TRV API.
    This is just an example skeleton.
    """
    # Time window
    since = (datetime.now(timezone.utc) - timedelta(days=days_back)).strftime("%Y-%m-%dT%H:%M:%SZ")

    # NOTE: Replace OBJECT/Filter/Include with your actual TRV object & fields.
    # Trafikverket's API typically expects <QUERY><LOGIN><APIKEY>... etc,
    # but many setups proxy that. Keep your working envelope if you already have it.
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
    <INCLUDE>MessageType</INCLUDE>
    <INCLUDE>RoadNumber</INCLUDE>
    <INCLUDE>LocationDescriptor</INCLUDE>
    <INCLUDE>Status</INCLUDE>
    <INCLUDE>Geometry.WGS84</INCLUDE>
  </QUERY>
</REQUEST>"""

def _parse_xml(xml_text: str) -> List[Dict[str, Any]]:
    """
    Parse TRV XML response into a list of dicts.
    Adjust tag names and paths to match your actual response schema.
    """
    root = ET.fromstring(xml_text)

    rows: List[Dict[str, Any]] = []

    # Typical TRV structure: <RESPONSE><RESULT><Situation>...</Situation>...</RESULT></RESPONSE>
    # Adapt the tag names if your object differs.
    # We'll search for all elements that look like incident nodes.
    # For safety, do a broad search and map fields defensively.
    for node in root.findall(".//Situation"):
        def text(path: str) -> str:
            el = node.find(path)
            return el.text.strip() if (el is not None and el.text) else ""

        # Geometry often appears as WKT-like "POINT (lon lat)" or similar.
        wgs84 = text("Geometry/WGS84")
        lat, lon = _extract_lat_lon(wgs84)

        rows.append({
            "incident_id": text("Id"),
            "message": text("Message"),  # if present
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
    """
    Extract lat/lon from a common WGS84 string.
    Many TRV responses use 'POINT (lon lat)'.
    Return (lat, lon) as floats, or (None, None) if not parsable.
    """
    try:
        # Example: 'POINT (18.063 59.334)'
        if "POINT" in wgs84:
            coords = wgs84[wgs84.find("(")+1:wgs84.find(")")].strip()
            parts = coords.split()
            if len(parts) == 2:
                lon = float(parts[0])
                lat = float(parts[1])
                return lat, lon
    except Exception:
        pass
    return (None, None)

def _normalize_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    Coerce dtypes as your Streamlit app expects.
    """
    if "county_no" in df.columns:
        df["county_no"] = pd.to_numeric(df["county_no"], errors="coerce").astype("Int64")
    for col in ["latitude", "longitude"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    for col in ["incident_id","message","message_type","location_descriptor","road_number","county_name","status"]:
        if col in df.columns:
            df[col] = df[col].astype("string").str.strip()

    # Ensure datetime-like strings are kept as text here;
    # your Streamlit loader converts them to datetime with parse_dates.
    return df

def run_etl(db_path: str, days_back: int = 1) -> Dict[str, Any]:
    """
    Pull from TRV (XML), parse, upsert into SQLite 'incidents' table,
    and return summary counters used by the Streamlit UI.
    """
    # Instantiate TRV client (provide your real API key/base URL)
    # If your LOGIN is inside the XML, api_key might be unused here.
    client = TRVClient(api_key="UNUSED_OR_ENV", base_url="https://api.trafikverket.se/v2/data.xml")

    # Build payload and call API
    payload_xml = _build_query_xml(days_back=days_back).replace("{API_KEY}", _get_api_key())
    xml_text = client.post(payload_xml)

    # Parse XML → rows
    rows = _parse_xml(xml_text)

    # DataFrame
    df = pd.DataFrame(rows)
    if df.empty:
        # Still return a sane summary
        return {"rows": 0, "pagar": 0, "kommande": 0, "seconds": 0}

    df = _normalize_df(df)

    # Upsert into SQLite
    con = sqlite3.connect(db_path)
    cur = con.cursor()

    # Ensure table exists
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

    # Upsert logic (replace existing by primary key)
    df.to_sql("incidents", con, if_exists="append", index=False)
    # If you need true upsert, do it row by row with INSERT OR REPLACE:
    # for r in df.to_dict(orient="records"):
    #     cur.execute("""
    #         INSERT INTO incidents (...)
    #         VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
    #         ON CONFLICT(incident_id) DO UPDATE SET
    #             message=excluded.message,
    #             ...
    #     """, (...))

    con.commit()

    # Summary for Streamlit KPIs
    pagar = int((df["status"] == "PÅGÅR").sum()) if "status" in df.columns else 0
    kommande = int((df["status"] == "KOMMANDE").sum()) if "status" in df.columns else 0

    con.close()

    # NOTE: If you want elapsed seconds, measure at start/end.
    return {"rows": int(len(df)), "pagar": pagar, "kommande": kommande, "seconds": 0}


# Helpers
import os
def _get_api_key() -> str:
    return os.getenv("TRAFIKVERKET_API_KEY", "")
