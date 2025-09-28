# src/app/etl_runner.py
import os
import time
import sqlite3
import pandas as pd
from datetime import datetime, timedelta, timezone
from xml.etree import ElementTree as ET
from typing import List, Dict, Any, Tuple, Optional

from src.trv.client import TRVClient

API_KEY  = os.getenv("TRAFIKVERKET_API_KEY", "")
BASE_URL = os.getenv("TRAFIKVERKET_URL", "https://api.trafikinfo.trafikverket.se/v2/data.xml")

# ---------- XML BUILDERS ----------

def _iso_z(dt_utc: datetime) -> str:
    """UTC -> 'YYYY-MM-DDTHH:MM:SSZ'."""
    if dt_utc.tzinfo is None:
        dt_utc = dt_utc.replace(tzinfo=timezone.utc)
    return dt_utc.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

def _build_query_xml(days_back: int = 1) -> str:
    """
    Valid Situation query:
    - Filter on Situation-level ModifiedTime (NOT Deviation.*)
    - Include whole Deviation subtree, then parse in Python.
    """
    since = _iso_z(datetime.now(timezone.utc) - timedelta(days=days_back))
    return f"""<REQUEST>
  <LOGIN authenticationkey="{{API_KEY}}"/>
  <QUERY objecttype="Situation" schemaversion="1">
    <FILTER>
      <GT name="ModifiedTime" value="{since}"/>
    </FILTER>

    <!-- Situation fields -->
    <INCLUDE>Id</INCLUDE>
    <INCLUDE>ModifiedTime</INCLUDE>

    <!-- Include the entire Deviation node (no dot-paths here) -->
    <INCLUDE>Deviation</INCLUDE>
  </QUERY>
</REQUEST>"""

# ---------- PARSING ----------

def _extract_lat_lon_from_wgs84(wgs84: str) -> Tuple[Optional[float], Optional[float]]:
    """Parse 'POINT (lon lat)' -> (lat, lon)."""
    try:
        if not wgs84:
            return (None, None)
        if "POINT" in wgs84:
            coords = wgs84[wgs84.find("(")+1:wgs84.find(")")].strip()
            parts = coords.split()
            if len(parts) == 2:
                lon = float(parts[0]); lat = float(parts[1])
                return (lat, lon)
    except Exception:
        pass
    return (None, None)

def _safe_text(node: ET.Element, path: str) -> str:
    el = node.find(path)
    return (el.text or "").strip() if (el is not None and el.text) else ""

def _derive_status(start_iso: str, end_iso: str) -> str:
    """Make 'PÅGÅR'/'KOMMANDE' from times if no Status provided."""
    try:
        now = datetime.now(timezone.utc)
        start = datetime.fromisoformat(start_iso.replace("Z", "+00:00")) if start_iso else None
        end   = datetime.fromisoformat(end_iso.replace("Z", "+00:00")) if end_iso else None
        if start and now < start:
            return "KOMMANDE"
        if start and (not end or start <= now <= end):
            return "PÅGÅR"
    except Exception:
        pass
    return ""

def _parse_xml(xml_text: str) -> List[Dict[str, Any]]:
    """
    Parse Situation payload where each Situation may contain multiple <Deviation>.
    We flatten each Deviation into one incident row.
    """
    rows: List[Dict[str, Any]] = []
    root = ET.fromstring(xml_text)

    for sit in root.findall(".//Situation"):
        sit_id  = _safe_text(sit, "Id")
        modtime = _safe_text(sit, "ModifiedTime")

        # Deviation can be many; if none, skip
        devs = sit.findall("Deviation")
        if not devs:
            continue

        for idx, dev in enumerate(devs):
            # Try to read nested fields; many payloads contain these
            dev_id   = _safe_text(dev, "Id")  # may or may not exist
            msg      = _safe_text(dev, "Message")
            mtype    = _safe_text(dev, "MessageType")
            locdesc  = _safe_text(dev, "LocationDescriptor")
            roadno   = _safe_text(dev, "RoadNumber")
            countyno = _safe_text(dev, "CountyNo")
            # CountyName is not always present under Deviation
            county_name = _safe_text(dev, "CountyName")

            start_ts = _safe_text(dev, "StartTime")
            end_ts   = _safe_text(dev, "EndTime")
            status   = _safe_text(dev, "Status") or _derive_status(start_ts, end_ts)

            wgs84 = _safe_text(dev, "Geometry/WGS84")
            lat, lon = _extract_lat_lon_from_wgs84(wgs84)

            incident_id = dev_id if dev_id else f"{sit_id}:{idx}"

            rows.append({
                "incident_id": incident_id,
                "message": msg,
                "message_type": mtype,
                "location_descriptor": locdesc,
                "road_number": roadno,
                "county_name": county_name,   # may be empty
                "county_no": countyno,
                "start_time_utc": start_ts,
                "end_time_utc": end_ts,
                "modified_time_utc": modtime,
                "latitude": lat,
                "longitude": lon,
                "status": status,
            })

    return rows

# ---------- NORMALIZATION & DB ----------

def _normalize_df(df: pd.DataFrame) -> pd.DataFrame:
    if "county_no" in df.columns:
        df["county_no"] = pd.to_numeric(df["county_no"], errors="coerce").astype("Int64")
    for col in ["latitude", "longitude"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    for col in ["incident_id","message","message_type","location_descriptor","road_number","county_name","status"]:
        if col in df.columns:
            df[col] = df[col].astype("string").str.strip()
    return df

# ---------- MAIN ETL ----------

def run_etl(db_path: str, days_back: int = 1) -> Dict[str, Any]:
    t0 = time.time()

    if not API_KEY:
        raise RuntimeError("TRAFIKVERKET_API_KEY is not set")

    url = BASE_URL or "https://api.trafikinfo.trafikverket.se/v2/data.xml"
    print(f"[ETL] Using TRV URL: {url}", flush=True)

    client = TRVClient(api_key=API_KEY, base_url=url, timeout=30)

    # Build and call
    payload_xml = _build_query_xml(days_back=days_back).replace("{API_KEY}", API_KEY)
    xml_text = client.post(payload_xml)  # returns XML string

    # Parse → rows → DataFrame
    rows = _parse_xml(xml_text)
    df = pd.DataFrame(rows)
    if df.empty:
        return {"rows": 0, "pagar": 0, "kommande": 0, "seconds": round(time.time() - t0, 2)}

    df = _normalize_df(df)

    # Upsert into SQLite
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
    con.commit(); con.close()

    pagar = int((df["status"] == "PÅGÅR").sum()) if "status" in df.columns else 0
    kommande = int((df["status"] == "KOMMANDE").sum()) if "status" in df.columns else 0
    return {
        "rows": int(len(df)),
        "pagar": pagar,
        "kommande": kommande,
        "seconds": round(time.time() - t0, 2),
    }
