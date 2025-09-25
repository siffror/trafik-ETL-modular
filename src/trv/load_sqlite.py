# src/trv/load_sqlite.py
from __future__ import annotations
import sqlite3
import pandas as pd

DDL_13 = """
CREATE TABLE IF NOT EXISTS incidents (
  incident_id TEXT PRIMARY KEY,
  message TEXT,
  message_type TEXT,
  location_descriptor TEXT,
  road_number TEXT,
  county_name TEXT,
  county_no INTEGER,
  start_time_utc TIMESTAMP,
  end_time_utc TIMESTAMP,
  modified_time_utc TIMESTAMP,
  latitude REAL,
  longitude REAL,
  status TEXT
);
CREATE INDEX IF NOT EXISTS ix_incidents_start    ON incidents(start_time_utc);
CREATE INDEX IF NOT EXISTS ix_incidents_county   ON incidents(county_name);
CREATE INDEX IF NOT EXISTS ix_incidents_modified ON incidents(modified_time_utc);
"""

COLS_13 = [
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

UPSERT_SQL_13 = """
INSERT INTO incidents(
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
  status=excluded.status;
"""

def ensure_schema(db_path: str) -> None:
    """Skapa tabell + index om de saknas."""
    con = sqlite3.connect(db_path)
    try:
        con.executescript(DDL_13)
        con.commit()
    finally:
        con.close()

def upsert_incidents(db_path: str, df: pd.DataFrame, batch_size: int = 500) -> None:
    """Skriv bara de 13 kolumnerna som tabellen förväntar sig."""
    if df.empty:
        return

    # säkerställ kolumnerna finns
    df = df.copy()
    for c in COLS_13:
        if c not in df.columns:
            df[c] = None

    # typer
    df["county_no"] = pd.to_numeric(df["county_no"], errors="coerce").astype("Int64")
    df["latitude"] = pd.to_numeric(df["latitude"], errors="coerce")
    df["longitude"] = pd.to_numeric(df["longitude"], errors="coerce")
    
    con = sqlite3.connect(db_path)
    try:
        cur = con.cursor()
        cur.executescript(DDL_13)

        rows = []
        for record in df[COLS_13].itertuples(index=False, name=None):
            clean = tuple(None if (v is pd.NA or pd.isna(v)) else v for v in record)
            rows.append(clean)

        for i in range(0, len(rows), batch_size):
            cur.executemany(UPSERT_SQL_13, rows[i:i+batch_size])
            con.commit()
    finally:
        con.close()
