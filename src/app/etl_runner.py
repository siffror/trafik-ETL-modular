# src/app/etl_runner.py
import os
import time
import sqlite3
import pandas as pd
from datetime import datetime, timedelta, timezone
from xml.etree import ElementTree as ET
from typing import List, Dict, Any, Tuple, Optional

from src.trv.client import TRVClient

# ---- Miljövariabler ----
API_KEY = os.getenv("TRAFIKVERKET_API_KEY", "")
BASE_URL = os.getenv("TRAFIKVERKET_URL", "https://api.trafikinfo.trafikverket.se/v2/data.xml")

# ---------------- XML byggare ----------------
def _build_query_xml(days_back: int = 1) -> str:
    """
    Fråga Situation (schemaversion 1) och inkludera hela Deviation.
    Filtrera på ett fält som finns på Situation, t.ex. PublicationTime.
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

# ---------------- Hjälpare ----------------
def _extract_lat_lon(wkt_or_wgs84: str) -> Tuple[Optional[float], Optional[float]]:
    """Försök tolka 'POINT (lon lat)' eller extrahera två första siffror."""
    if not wkt_or_wgs84:
        return (None, None)
    s = str(wkt_or_wgs84)
    try:
        if "POINT" in s:
            coords = s[s.find("(")+1:s.find(")")].strip()
            parts = coords.split()
            if len(parts) >= 2:
                lon = float(parts[0]); lat = float(parts[1])
                return (lat, lon)
    except Exception:
        pass
    # fallback: plocka ut första två tal
    import re
    nums = re.findall(r"[-+]?\d+(?:\.\d+)?", s)
    if len(nums) >= 2:
        lon = float(nums[0]); lat = float(nums[1])
        return (lat, lon)
    return (None, None)

def _txt(node: ET.Element, path: str) -> str:
    el = node.find(path)
    return (el.text.strip() if (el is not None and el.text) else "")

def _first_txt(node: ET.Element, paths: List[str]) -> str:
    for p in paths:
        v = _txt(node, p)
        if v:
            return v
    return ""

# ---------------- XML → rader ----------------
def _parse_xml(xml_text: str) -> List[Dict[str, Any]]:
    """
    Parserar Situation-svar. Varje Situation kan ha flera Deviation.
    Vi skapar en rad per Deviation.
    """
    root = ET.fromstring(xml_text)
    rows: List[Dict[str, Any]] = []

    for sit in root.findall(".//Situation"):
        sit_id = _txt(sit, "Id")
        sit_pub = _txt(sit, "PublicationTime")

        deviations = sit.findall("./Deviation") or []
        if not deviations:
            # Skapa ev. en tom rad om du vill, men normalt hoppar vi över.
            continue

        for i, dev in enumerate(deviations):
            # Fält som oftast ligger på Deviation
            incident_id = _first_txt(dev, ["Id"]) or (sit_id + f":{i}")
            message = _first_txt(dev, ["Message"])
            message_type = _first_txt(dev, ["MessageType"])
            location_descriptor = _first_txt(dev, ["LocationDescriptor"])
            road_number = _first_txt(dev, ["RoadNumber"])
            county_no = _first_txt(dev, ["CountyNo"])
            county_name = _first_txt(dev, ["CountyName"])  # finns inte alltid
            status = _first_txt(dev, ["Status"])           # finns inte alltid

            start_time = _first_txt(dev, ["StartTime"])
            end_time = _first_txt(dev, ["EndTime"])
            modified_time = sit_pub  # använd Situation.PublicationTime som "modified" proxy

            # Geometri brukar ligga under Deviation/Geometry/WGS84
            wgs84 = _first_txt(dev, ["Geometry/WGS84", ".//Geometry/WGS84"])
            lat, lon = _extract_lat_lon(wgs84)

            rows.append({
                "incident_id": incident_id,
                "message": message,
                "message_type": message_type,
                "location_descriptor": location_descriptor,
                "road_number": road_number,
                "county_name": county_name,
                "county_no": county_no,
                "start_time_utc": start_time,
                "end_time_utc": end_time,
                "modified_time_utc": modified_time,
                "latitude": lat,
                "longitude": lon,
                "status": status,
            })
    return rows

def _normalize_df(df: pd.DataFrame) -> pd.DataFrame:
    """Normalisera dtypes så Streamlit-filtret funkar stabilt."""
    if "county_no" in df.columns:
        df["county_no"] = pd.to_numeric(df["county_no"], errors="coerce").astype("Int64")
    for col in ["latitude", "longitude"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    for col in ["incident_id","message","message_type","location_descriptor","road_number","county_name","status"]:
        if col in df.columns:
            df[col] = df[col].astype("string").str.strip()
    return df

# ---------------- Huvud-ETL ----------------
def run_etl(db_path: str, days_back: int = 1) -> Dict[str, Any]:
    """Hämtar XML från TRV, parsar, uppsertar i SQLite, och returnerar KPI-summering."""
    t0 = time.time()

    if not API_KEY:
        raise RuntimeError("TRAFIKVERKET_API_KEY is not set")

    url = BASE_URL or "https://api.trafikinfo.trafikverket.se/v2/data.xml"
    print(f"[ETL] Using TRV URL: {url}", flush=True)

    client = TRVClient(api_key=API_KEY, base_url=url, timeout=30)

    # Bygg payload och hämta
    payload_xml = _build_query_xml(days_back).replace("{API_KEY}", API_KEY)
    xml_text = client.post(payload_xml)

    # Parse → DataFrame
    rows = _parse_xml(xml_text)
    df = pd.DataFrame(rows)
    if df.empty:
        return {"rows": 0, "pagar": 0, "kommande": 0, "seconds": round(time.time() - t0, 2)}

    df = _normalize_df(df)

    # SQLite upsert
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

    pagar = int((df.get("status") == "PÅGÅR").sum()) if "status" in df.columns else 0
    kommande = int((df.get("status") == "KOMMANDE").sum()) if "status" in df.columns else 0
    return {
        "rows": int(len(df)),
        "pagar": pagar,
        "kommande": kommande,
        "seconds": round(time.time() - t0, 2),
    }
