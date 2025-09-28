# src/app/etl_runner.py
import os
import time
import sqlite3
import pandas as pd
from datetime import datetime, timedelta, timezone
from xml.etree import ElementTree as ET
from typing import List, Dict, Any, Tuple, Optional

from src.trv.client import TRVClient

# ========== Miljökonfig ==========
API_KEY = os.getenv("TRAFIKVERKET_API_KEY", "")
BASE_URL = os.getenv("TRAFIKVERKET_URL", "https://api.trafikinfo.trafikverket.se/v2/data.xml")
ETL_DEBUG = os.getenv("ETL_DEBUG", "")

# ========== Hjälpare ==========
def _iso_now_utc() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

def _mask_key(xml_text: str) -> str:
    # maska authenticationkey="...."
    return (
        xml_text
        .replace(API_KEY, "****") if API_KEY else xml_text
    ).replace(
        'authenticationkey="', 'authenticationkey="****'
    )

def _first_text(node: ET.Element, paths: List[str]) -> str:
    for p in paths:
        el = node.find(p)
        if el is not None and el.text:
            return el.text.strip()
    return ""

def _parse_dt(s: str) -> Optional[datetime]:
    if not s:
        return None
    for fmt in ("%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%d %H:%M:%S"):
        try:
            dt = datetime.strptime(s, fmt)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc)
        except Exception:
            continue
    try:
        # sista chans: pandas
        return pd.to_datetime(s, utc=True).to_pydatetime()
    except Exception:
        return None

def _extract_lat_lon(wgs84: str) -> Tuple[Optional[float], Optional[float]]:
    """Förväntar 'POINT (lon lat)' → (lat, lon)."""
    try:
        if not wgs84:
            return (None, None)
        if "POINT" in wgs84.upper():
            start = wgs84.find("(")
            end = wgs84.find(")")
            if start >= 0 and end > start:
                coords = wgs84[start + 1 : end].strip().split()
                if len(coords) == 2:
                    lon = float(coords[0]); lat = float(coords[1])
                    return (lat, lon)
    except Exception:
        pass
    return (None, None)

# ========== Bygg XML-fråga ==========
def _build_query_xml(days_back: int = 1) -> str:
    """
    Bygg en giltig TRV-fråga för Situation:
    - Filtrera på Situation.PublicationTime
    - Inkludera hela Deviation-noden (inte Deviation.* fält)
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
    <INCLUDE>VersionTime</INCLUDE>
    <INCLUDE>Deviation</INCLUDE>
  </QUERY>
</REQUEST>"""


# ========== Parser ==========
def _parse_xml(xml_text: str) -> List[Dict[str, Any]]:
    """
    Parsa Situation + inbäddade Deviation-noder.
    Vi flatten: 1 rad per Deviation (fallback till Situation om något saknas).
    """
    def _txt(node, path: str) -> str:
        el = node.find(path)
        return el.text.strip() if (el is not None and el.text) else ""

    root = ET.fromstring(xml_text)
    rows: List[Dict[str, Any]] = []

    for sit in root.findall(".//Situation"):
        sit_id  = _txt(sit, "Id")
        pub     = _txt(sit, "PublicationTime")
        ver     = _txt(sit, "VersionTime")

        devs = sit.findall(".//Deviation")
        if not devs:
            # fallback: rad på Situation-nivå (om inga Deviation finns)
            rows.append({
                "incident_id": sit_id,
                "message": _txt(sit, "Message"),
                "message_type": _txt(sit, "MessageType"),
                "location_descriptor": _txt(sit, "LocationDescriptor"),
                "road_number": _txt(sit, "RoadNumber"),
                "county_name": _txt(sit, "CountyName"),
                "county_no": _txt(sit, "CountyNo"),
                "start_time_utc": pub,  # inget bättre i detta fall
                "end_time_utc": "",
                "modified_time_utc": ver,
                "latitude": None,
                "longitude": None,
                "status": _txt(sit, "Status"),
            })
            continue

        for d in devs:
            # För geometri kan det variera var den ligger
            wgs84 = (
                _txt(d, "Geometry/WGS84")
                or _txt(d, "Location/Geometry/WGS84")
            )
            lat, lon = _extract_lat_lon(wgs84)

            rows.append({
                "incident_id": _txt(d, "Id") or sit_id,
                "message": _txt(d, "Message") or _txt(sit, "Message"),
                "message_type": _txt(d, "MessageType") or _txt(sit, "MessageType"),
                "location_descriptor": _txt(d, "LocationDescriptor"),
                "road_number": _txt(d, "RoadNumber"),
                "county_name": _txt(d, "CountyName") or _txt(d, "Location/CountyName"),
                "county_no": _txt(d, "CountyNo") or _txt(d, "Location/CountyNo"),
                "start_time_utc": _txt(d, "StartTime") or pub,
                "end_time_utc": _txt(d, "EndTime"),
                "modified_time_utc": _txt(d, "ModifiedTime") or ver,
                "latitude": lat,
                "longitude": lon,
                "status": _txt(d, "Status") or _txt(sit, "Status"),
            })

    return rows


# ========== Normalisering ==========
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

# ========== ETL-huvud ==========
def run_etl(db_path: str, days_back: int = 1) -> Dict[str, Any]:
    """
    Hämta från TRV (XML) → parse → upsert i SQLite → returnera summering.
    """
    t0 = time.time()

    if not API_KEY:
        raise RuntimeError("TRAFIKVERKET_API_KEY is not set")

    url = BASE_URL or "https://api.trafikinfo.trafikverket.se/v2/data.xml"
    print(f"[ETL] Using TRV URL: {url}", flush=True)

    client = TRVClient(api_key=API_KEY, base_url=url, timeout=30)

    # Bygg & logga payload (maskad)
    payload_xml = _build_query_xml(days_back=days_back).replace("{API_KEY}", API_KEY)
    if ETL_DEBUG:
        print("[ETL] Outgoing XML (masked):")
        print(_mask_key(payload_xml))

    # Anropa TRV
    xml_text = client.post(payload_xml)
    if ETL_DEBUG:
        print("[ETL] Incoming XML (first 1200 chars):")
        print((_mask_key(xml_text) if API_KEY else xml_text)[:1200])

    # Parse → DataFrame
    rows = _parse_xml(xml_text)
    df = pd.DataFrame(rows)
    if df.empty:
        return {"rows": 0, "pagar": 0, "kommande": 0, "seconds": round(time.time() - t0, 2)}

    df = _normalize_df(df)

    # Upsert i SQLite
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
