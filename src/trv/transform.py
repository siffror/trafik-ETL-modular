# src/trv/transform.py
from __future__ import annotations
import re
import datetime as dt
from datetime import timezone
from typing import Any, Dict, List, Tuple
import pandas as pd
from shapely import wkt as shapely_wkt
from shapely.geometry import Point, LineString, Polygon, MultiPoint, MultiLineString, MultiPolygon

COUNTY_MAP = {
    1:"Stockholms län",3:"Uppsala län",4:"Södermanlands län",5:"Östergötlands län",6:"Jönköpings län",
    7:"Kronobergs län",8:"Kalmar län",9:"Gotlands län",10:"Blekinge län",12:"Skåne län",13:"Hallands län",
    14:"Västra Götalands län",17:"Värmlands län",18:"Örebro län",19:"Västmanlands län",20:"Dalarnas län",
    21:"Gävleborgs län",22:"Västernorrlands län",23:"Jämtlands län",24:"Västerbottens län",25:"Norrbottens län"
}

def _to_utc_iso(s: str | None) -> str | None:
    if not s: return None
    try:
        return dt.datetime.fromisoformat(s.replace("Z", "+00:00")).astimezone(timezone.utc).isoformat(timespec="seconds")
    except Exception:
        return None

def _latlon_from_wkt(wkt_text: str | None) -> Tuple[float | None, float | None]:
    if not wkt_text or not isinstance(wkt_text, str):
        return None, None
    try:
        geom = shapely_wkt.loads(wkt_text)
        if isinstance(geom, Point):
            lon, lat = geom.x, geom.y
            return float(lat), float(lon)
        elif isinstance(geom, (LineString, MultiLineString, Polygon, MultiPolygon, MultiPoint)):
            c = geom.centroid
            lon, lat = c.x, c.y
            return float(lat), float(lon)
        coords = list(getattr(geom, "coords", []))
        if coords:
            lon, lat = coords[0][0], coords[0][1]
            return float(lat), float(lon)
    except Exception:
        pass
    nums = re.findall(r"[-+]?\d+(?:\.\d+)?", wkt_text)
    if len(nums) >= 2:
        lon, lat = float(nums[0]), float(nums[1])
        return lat, lon
    return None, None

def normalize_incidents(situations: List[Dict[str, Any]]) -> pd.DataFrame:
    rows: List[Dict[str, Any]] = []
    now = dt.datetime.now(timezone.utc)

    for sit in situations:
        situation_id = sit.get("Id")
        modified_utc = _to_utc_iso(sit.get("ModifiedTime"))
        deviations = sit.get("Deviation") or []
        for d in deviations:
            msg = (d.get("Message") or "").strip()
            if not msg:
                continue
            deviation_id = d.get("Id")
            incident_id = deviation_id or f"{situation_id}:{d.get('StartTime')}"
            start_utc = _to_utc_iso(d.get("StartTime"))
            end_utc   = _to_utc_iso(d.get("EndTime"))
            start_dt = dt.datetime.fromisoformat(start_utc) if start_utc else None
            end_dt   = dt.datetime.fromisoformat(end_utc) if end_utc else None

            if start_dt and start_dt > now:
                status = "KOMMANDE"
            elif (not start_dt or start_dt <= now) and (not end_dt or end_dt > now):
                status = "PÅGÅR"
            else:
                continue

            wkt = (d.get("Geometry") or {}).get("WGS84")
            lat, lon = _latlon_from_wkt(wkt)

            county_no = d.get("CountyNo")
            if isinstance(county_no, list) and county_no:
                county_no = county_no[0]
            county_name = COUNTY_MAP.get(int(county_no)) if county_no is not None else None

            rows.append({
                "incident_id": incident_id,
                "situation_id": situation_id,
                "deviation_id": deviation_id,
                "message": msg,
                "message_type": d.get("MessageType"),
                "location_descriptor": d.get("LocationDescriptor"),
                "road_number": d.get("RoadNumber"),
                "county_no": county_no,
                "county_name": county_name,
                "start_time_utc": start_utc,
                "end_time_utc": end_utc,
                "latitude": lat,
                "longitude": lon,
                "geometry_wgs84": wkt,
                "severity_code": None,
                "icon_id": None,
                "created_time_utc": start_utc,
                "modified_time_utc": modified_utc,
                "status": status,
            })

    df = pd.DataFrame(rows)

    if df.empty:
        return df

    # dedupe & sort – samma som i monoliten
    df = df.drop_duplicates(
        subset=["message","location_descriptor","start_time_utc","end_time_utc"],
        keep="first"
    )

    df["_mod_dt"] = pd.to_datetime(df["modified_time_utc"], errors="coerce")
    df = df.sort_values(by=["incident_id","_mod_dt"], ascending=[True, False]) \
           .drop_duplicates(subset=["incident_id"], keep="first")

    # sortera: PÅGÅR först, sen KOMMANDE
    df["_start_dt"] = pd.to_datetime(df["start_time_utc"], errors="coerce")
    status_rank = {"PÅGÅR": 0, "KOMMANDE": 1}
    df["status_rank"] = df["status"].map(status_rank).fillna(9)
    df = df.sort_values(
        by=["status_rank","_mod_dt","_start_dt"],
        ascending=[True, False, False]
    ).drop(columns=["status_rank","_mod_dt","_start_dt"])

    # typer
    if "county_no" in df: df["county_no"] = pd.to_numeric(df["county_no"], errors="coerce").astype("Int64")
    if "latitude"  in df: df["latitude"]  = pd.to_numeric(df["latitude"], errors="coerce")
    if "longitude" in df: df["longitude"] = pd.to_numeric(df["longitude"], errors="coerce")

    for c in ["message","message_type","location_descriptor","county_name","road_number","status"]:
        if c in df: df[c] = df[c].astype("string").str.strip()

    return df
