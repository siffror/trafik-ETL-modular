# src/app/etl_runner.py

import os
import time
import sqlite3
from typing import Dict, List, Tuple, Optional, Any
import pandas as pd
from datetime import datetime, timedelta, timezone
from xml.etree import ElementTree as ET

from src.trv.client import TRVClient

# -----------------------------------------------------------
# Konfiguration
# -----------------------------------------------------------
BASE_URL = os.getenv(
    "TRAFIKVERKET_URL",
    "https://api.trafikinfo.trafikverket.se/v2/data.xml",
)

# -----------------------------------------------------------
# Hjälpfunktioner
# -----------------------------------------------------------
def _require_api_key() -> str:
    key = (os.getenv("TRAFIKVERKET_API_KEY") or "").strip()
    if not key:
        raise RuntimeError("TRAFIKVERKET_API_KEY is not set")
    return key


def _extract_lat_lon(wgs84: str) -> Tuple[Optional[float], Optional[float]]:
    """Extraherar (lat, lon) ur 'POINT (lon lat)'. Misslyckas tyst till (None, None)."""
    try:
        if wgs84 and "POINT" in wgs84:
            coords = wgs84[wgs84.find("(") + 1 : wgs84.find(")")].strip()
            parts = coords.split()
            if len(parts) == 2:
                lon = float(parts[0])
                lat = float(parts[1])
                return lat, lon
    except Exception:
        pass
    return None, None


def _normalize_df(df: pd.DataFrame) -> pd.DataFrame:
    """Sätter förväntade dtypes och städar text."""
    if "county_no" in df.columns:
        df["county_no"] = pd.to_numeric(df["county_no"], errors="coerce").astype("Int64")

    for col in ("latitude", "longitude"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    text_cols = (
        "incident_id",
        "message",
        "message_type",
        "location_descriptor",
        "road_number",
        "county_name",
        "status",
    )
    for col in text_cols:
        if col in df.columns:
            df[col] = df[col].astype("string").str.strip()

    return df


# -----------------------------------------------------------
# TRV: payload & XML-parsning
# -----------------------------------------------------------
def build_trv_payload(api_key: str, days_back: int = 1) -> str:
    """
    Giltig TRV-XML för Situation/Deviation:
    - Filter: Deviation.StartTime samt Situation.CreationTime/LastUpdateTime
    - Include: fält på både Situation- och Deviation-nivå som vi använder
    """
    since = (datetime.now(timezone.utc) - timedelta(days=days_back)).strftime("%Y-%m-%dT%H:%M:%S")

    return f"""<REQUEST>
  <LOGIN authenticationkey="{api_key}"/>
  <QUERY objecttype="Situation" schemaversion="1">
    <FILTER>
      <OR>
        <GT name="Deviation.StartTime" value="{since}"/>
        <GT name="CreationTime" value="{since}"/>
        <GT name="LastUpdateTime" value="{since}"/>
      </OR>
    </FILTER>

    <!-- Situation-level -->
    <INCLUDE>Id</INCLUDE>
    <INCLUDE>CreationTime</INCLUDE>
    <INCLUDE>LastUpdateTime</INCLUDE>

    <!-- Deviation-level -->
    <INCLUDE>Deviation.Id</INCLUDE>
    <INCLUDE>Deviation.Message</INCLUDE>
    <INCLUDE>Deviation.MessageType</INCLUDE>
    <INCLUDE>Deviation.LocationDescriptor</INCLUDE>
    <INCLUDE>Deviation.RoadNumber</INCLUDE>
    <INCLUDE>Deviation.CountyNo</INCLUDE>
    <INCLUDE>Deviation.StartTime</INCLUDE>
    <INCLUDE>Deviation.EndTime</INCLUDE>
    <INCLUDE>Deviation.Status</INCLUDE>
    <INCLUDE>Deviation.Geometry.WGS84</INCLUDE>
  </QUERY>
</REQUEST>"""


def _parse_xml(xml_text: str) -> List[Dict[str, Any]]:
    """
    Parsar till en platt lista: en rad per Deviation (barn till Situation).
    """
    root = ET.fromstring(xml_text)
    rows: List[Dict[str, Any]] = []

    for situation in root.findall(".//Situation"):
        situation_id = (situation.findtext("Id") or "").strip()
        creation = (situation.findtext("CreationTime") or "").strip()
        last_update = (situation.findtext("LastUpdateTime") or "").strip()

        for dev in situation.findall("Deviation"):
            def dtext(path: str) -> str:
                val = dev.findtext(path)
                return val.strip() if val else ""

            wgs84 = dtext("Geometry/WGS84")
            lat, lon = _extract_lat_lon(wgs84)

            dev_id = dtext("Id")
            incident_id = dev_id or f"{situation_id}:{dtext('StartTime')}"

            rows.append(
                {
                    "incident_id": incident_id,
                    "message": dtext("Message"),
                    "message_type": dtext("MessageType"),
                    "location_descriptor": dtext("LocationDescriptor"),
                    "road_number": dtext("RoadNumber"),
                    "county_name": "",  # CountyNo -> namn kräver egen mappingtabell
                    "county_no": dtext("CountyNo"),
                    "start_time_utc": dtext("StartTime") or creation,
                    "end_time_utc": dtext("EndTime"),
                    "modified_time_utc": last_update,
                    "latitude": lat,
                    "longitude": lon,
                    "status": dtext("Status"),
                }
            )

    return rows


# -----------------------------------------------------------
# Publik ETL
# -----------------------------------------------------------
def run_etl(db_path: str, days_back: int = 1) -> Dict[str, int]:
    """
    Hämtar XML från TRV, parsar, upsertar till SQLite och returnerar en summering.
    """
    t0 = time.time()

    api_key = _require_api_key()
    url = BASE_URL or "https://api.trafikinfo.trafikverket.se/v2/data.xml"
    print(f"[ETL] Using TRV URL: {url}", flush=True)

    client = TRVClient(api_key=api_key, base_url=url, timeout=30)

    # 1) Hämta XML
    payload_xml = build_trv_payload(api_key=api_key, days_back=days_back)
    xml_text = client.post(payload_xml)

    # 2) XML -> DataFrame
    rows = _parse_xml(xml_text)
    df = pd.DataFrame(rows)
    if df.empty:
        return {
            "rows": 0,
            "pagar": 0,
            "kommande": 0,
            "seconds": round(time.time() - t0, 2),
        }

    df = _normalize_df(df)

    # 3) Upsert till SQLite
    con = sqlite3.connect(db_path)
    cur = con.cursor()
    cur.execute(
        """
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
        """
    )

    cols = [
        "incident_id",
        "message",
        "message_type",
        "location_descriptor",
        "road_number",
        "county_name",
        "county_no",
        "start_time_utc",
        "end_time_utc",
        "modified_time_utc",
        "latitude",
        "longitude",
        "status",
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
    cur.executemany(
        sql, [tuple(r.get(c) for c in cols) for r in df.to_dict(orient="records")]
    )
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
