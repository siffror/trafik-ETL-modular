# src/app/etl_runner.py
from __future__ import annotations

import os
import time
import sqlite3
import logging
from typing import Dict, Any, List

import pandas as pd
import datetime as dt

from src.trv.client import TRVClient
from src.trv.endpoints import iterate_incidents

# ---------------- Logging ----------------
log = logging.getLogger("ETL")
if not log.handlers:
    logging.basicConfig(
        format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
        level=logging.INFO,
    )

# ---------------- Config ----------------
API_KEY = os.getenv("TRAFIKVERKET_API_KEY", "")
BASE_URL = os.getenv(
    "TRAFIKVERKET_URL",
    "https://api.trafikinfo.trafikverket.se/v2/data.xml",
)

# ---------------- Helpers ----------------
def _normalize_df(df: pd.DataFrame) -> pd.DataFrame:
    """Coerce dtypes till vad Streamlit-appen förväntar sig."""
    if "county_no" in df.columns:
        df["county_no"] = pd.to_numeric(df["county_no"], errors="coerce").astype("Int64")
    for col in ["latitude", "longitude"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    text_cols = [
        "incident_id",
        "message",
        "message_type",
        "location_descriptor",
        "road_number",
        "county_name",
        "status",
    ]
    for col in text_cols:
        if col in df.columns:
            df[col] = df[col].astype("string").str.strip()
    # Låt datumfält vara str — Streamlit loader gör parse_dates senare.
    return df

def _ensure_schema(con: sqlite3.Connection) -> None:
    con.execute(
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

# ---------------- Main ETL ----------------
def run_etl(db_path: str, days_back: int = 1) -> Dict[str, Any]:
    """
    Hämtar Situation->Deviation via iterate_incidents(), flattenar rader,
    upsert:ar till SQLite och returnerar summary för UI.
    """
    t0 = time.time()

    if not API_KEY:
        raise RuntimeError("TRAFIKVERKET_API_KEY is not set")

    # Klargör i loggar vad som används
    log.info("🚦 ETL startad • db=`%s` • days_back=`%s` • %s UTC",
             db_path, days_back, dt.datetime.now(dt.UTC).isoformat(timespec="seconds"))
    log.info("[ETL] Using TRV URL: %s", BASE_URL)

    # TRV-klient som returnerar XML-text
    client = TRVClient(api_key=API_KEY, base_url=BASE_URL, timeout=30)

    # Hämta sidor sedan 'since_utc'
    since_utc = dt.datetime.now(dt.UTC) - dt.timedelta(days=days_back)

    rows: List[Dict[str, Any]] = []
    for item in iterate_incidents(
        client=client,
        since_utc=since_utc,
        page_size=200,          # kan fintrimmas; endpoints har intern pagination
        future_days_limit=14,   # hämta upp till 14 dagar framåt
        max_pages=25,           # skydd mot oändlig paging
    ):
        rows.append(item)

    log.info("📥 Hämtat och flattenat %d deviation-rader", len(rows))

    if not rows:
        elapsed = round(time.time() - t0, 2)
        log.info("✅ ETL klar • rader=`0` • tid=`%ss` • db=`%s`", elapsed, db_path)
        return {"rows": 0, "pagar": 0, "kommande": 0, "seconds": elapsed}

    # Bygg DataFrame i den kolumnordning Streamlit använder
    df = pd.DataFrame(rows)
    # Säkerställ att alla förväntade kolumner finns
    for col in [
        "incident_id", "message", "message_type", "location_descriptor",
        "road_number", "county_name", "county_no",
        "start_time_utc", "end_time_utc", "modified_time_utc",
        "latitude", "longitude", "status",
    ]:
        if col not in df.columns:
            df[col] = pd.NA

    df = df[
        [
            "incident_id", "message", "message_type", "location_descriptor",
            "road_number", "county_name", "county_no",
            "start_time_utc", "end_time_utc", "modified_time_utc",
            "latitude", "longitude", "status",
        ]
    ]

    df = _normalize_df(df)
    log.info("🧮 Normaliserat → %d rader", len(df))

    # Upsert till SQLite
    con = sqlite3.connect(db_path)
    try:
        _ensure_schema(con)
        log.info("Schema kontrollerat/skapats")

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
        con.executemany(sql, [tuple(r.get(c) for c in cols) for r in df.to_dict(orient="records")])
        con.commit()
        log.info("Data upsertad i SQLite")
    finally:
        con.close()

    pagar = int((df["status"] == "PÅGÅR").sum()) if "status" in df.columns else 0
    kommande = int((df["status"] == "KOMMANDE").sum()) if "status" in df.columns else 0
    elapsed = round(time.time() - t0, 2)

    log.info("✅ ETL klar • rader=`%d` • PÅGÅR=`%d` • KOMMANDE=`%d` • tid=`%ss` • db=`%s`",
             len(df), pagar, kommande, elapsed, db_path)

    return {"rows": int(len(df)), "pagar": pagar, "kommande": kommande, "seconds": elapsed}
