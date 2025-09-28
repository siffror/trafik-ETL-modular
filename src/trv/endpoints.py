# src/trv/endpoints.py
from __future__ import annotations
import datetime as dt
from typing import Dict, Any, Iterator, Optional, List, Tuple
from xml.etree import ElementTree as ET

from .config import DEFAULT_PAGE_SIZE  # behåll din egna config

# --------- Hjälpare ---------
def _iso_z(ts: dt.datetime) -> str:
    """ISO-8601 UTC med 'Z'."""
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=dt.UTC)
    return ts.astimezone(dt.UTC).isoformat(timespec="seconds").replace("+00:00", "Z")

def _wgs84_to_latlon(wgs84: str) -> Tuple[Optional[float], Optional[float]]:
    """
    Vanligt TRV-format: 'POINT (lon lat)'. Returnerar (lat, lon) som float.
    """
    if not wgs84:
        return None, None
    try:
        if "POINT" in wgs84:
            coords = wgs84[wgs84.find("(")+1:wgs84.find(")")].strip()
            parts = coords.split()
            if len(parts) == 2:
                lon = float(parts[0]); lat = float(parts[1])
                return lat, lon
    except Exception:
        pass
    return None, None

def _compute_status(start_iso: str, end_iso: str) -> str:
    """
    Grov status-beräkning om API:t inte lämnar ett statusfält.
    PÅGÅR om start <= nu < end (eller end saknas), annars KOMMANDE om start > nu.
    """
    now = dt.datetime.now(dt.UTC)
    def _parse(s: str) -> Optional[dt.datetime]:
        if not s:
            return None
        try:
            # Hantera både ...Z och offset
            dtp = dt.datetime.fromisoformat(s.replace("Z", "+00:00"))
            return dtp if dtp.tzinfo else dtp.replace(tzinfo=dt.UTC)
        except Exception:
            return None

    st = _parse(start_iso)
    en = _parse(end_iso)

    if st and st > now:
        return "KOMMANDE"
    if st and st <= now and (not en or en > now):
        return "PÅGÅR"
    # fallback
    return "PÅGÅR" if not st else "KOMMANDE"

# --------- XML-byggare (utan Deviation.* i FILTER/ORDERBY/INCLUDE) ---------
def _build_query_xml(
    api_key: str,
    since_utc: dt.datetime,
    limit: int,
    future_days_limit: Optional[int],
    lt_modified: Optional[str] = None,
    lt_publication: Optional[str] = None,
) -> str:
    """
    Giltig Situation-fråga:
    - Filtrerar på Situation.PublicationTime (inte Deviation.*)
    - Returnerar hela Deviation-noden (utan punktnotation)
    - Sorterar på Situation-fält
    """
    since_iso = _iso_z(since_utc)
    future_cap = None
    if future_days_limit is not None:
        future_cap = _iso_z(dt.datetime.now(dt.UTC) + dt.timedelta(days=future_days_limit))

    filters = [f'<GT name="PublicationTime" value="{since_iso}" />']
    if future_cap:
        filters.append(f'<LT name="PublicationTime" value="{future_cap}" />')
    if lt_modified:
        filters.append(f'<LT name="ModifiedTime" value="{lt_modified}" />')
    if lt_publication:
        filters.append(f'<LT name="PublicationTime" value="{lt_publication}" />')

    filters_xml = "\n      ".join(filters)

    return f"""
<REQUEST>
  <LOGIN authenticationkey="{api_key}" />
  <QUERY objecttype="Situation" schemaversion="1" limit="{limit}"
         orderby="ModifiedTime desc, PublicationTime desc">
    <FILTER>
      {filters_xml}
    </FILTER>

    <INCLUDE>Id</INCLUDE>
    <INCLUDE>ModifiedTime</INCLUDE>
    <INCLUDE>PublicationTime</INCLUDE>
    <INCLUDE>Deviation</INCLUDE>
  </QUERY>
</REQUEST>
""".strip()

# --------- Parsning & flatten ---------
def _flatten_situations(xml_text: str) -> Tuple[List[Dict[str, Any]], Optional[str], Optional[str]]:
    """
    Parsar XML-svar, flattenar varje Deviation under Situation till en rad.
    Returnerar (rows, last_modified, last_publication) för pagination.
    """
    root = ET.fromstring(xml_text)
    situations = root.findall(".//Situation")
    rows: List[Dict[str, Any]] = []

    for s in situations:
        situation_id = (s.findtext("Id") or "").strip()
        mod_time     = (s.findtext("ModifiedTime") or "").strip()
        pub_time     = (s.findtext("PublicationTime") or "").strip()

        deviations = s.findall("Deviation") or []
        for d in deviations:
            # Tolerera saknade fält
            dev_id   = (d.findtext("Id") or "").strip()
            msg      = (d.findtext("Message") or "").strip()
            mtype    = (d.findtext("MessageType") or "").strip()
            loc_desc = (d.findtext("LocationDescriptor") or "").strip()
            road_no  = (d.findtext("RoadNumber") or "").strip()
            county_no= (d.findtext("CountyNo") or "").strip()
            start    = (d.findtext("StartTime") or "").strip()
            end      = (d.findtext("EndTime") or "").strip()
            wgs84    = ""
            geom_node = d.find("Geometry")
            if geom_node is not None:
                wgs84 = (geom_node.findtext("WGS84") or "").strip()
            lat, lon = _wgs84_to_latlon(wgs84)

            status = _compute_status(start, end)

            rows.append({
                # nycklar som din Streamlit-app förväntar sig
                "incident_id": dev_id or situation_id,
                "message": msg,
                "message_type": mtype,
                "location_descriptor": loc_desc,
                "road_number": road_no,
                "county_name": "",          # CountyName saknas ofta i Deviation; kan härledas via CountyNo om du har tabell
                "county_no": county_no,
                "start_time_utc": start,
                "end_time_utc": end,
                "modified_time_utc": mod_time,
                "latitude": lat,
                "longitude": lon,
                "status": status,
                # ev. debug/metadata
                "situation_id": situation_id,
                "publication_time_utc": pub_time,
            })

    # Cursor (från sista Situation i sidan – API:n levererar i orderby-ordning)
    last_modified = situations[-1].findtext("ModifiedTime").strip() if situations else None
    last_pub      = situations[-1].findtext("PublicationTime").strip() if situations else None
    return rows, last_modified or None, last_pub or None

# --------- Publikt API: paginerad generator ---------
def iterate_incidents(
    client,
    since_utc: dt.datetime,
    page_size: int = DEFAULT_PAGE_SIZE,
    future_days_limit: Optional[int] = 14,
    max_pages: int = 20,
) -> Iterator[Dict[str, Any]]:
    """
    Hämtar Situation-sidor och yield:ar flattenade Deviation-rader.
    Cursor-baserad pagination med LT på (ModifiedTime, PublicationTime).
    """
    seen_ids: set[str] = set()
    lt_modified: Optional[str] = None
    lt_publication: Optional[str] = None

    for _ in range(max_pages):
        xml = _build_query_xml(
            api_key=client.api_key,
            since_utc=since_utc,
            limit=page_size,
            future_days_limit=future_days_limit,
            lt_modified=lt_modified,
            lt_publication=lt_publication,
        )
        xml_text = client.post(xml)  # <-- TRVClient måste returnera XML-string
        page_rows, lt_modified, lt_publication = _flatten_situations(xml_text)

        if not page_rows:
            break

        new_any = False
        for r in page_rows:
            rid = r.get("incident_id")
            if rid and rid not in seen_ids:
                seen_ids.add(rid)
                new_any = True
                yield r

        # slutvillkor
        if not new_any or len(page_rows) < page_size or not lt_modified:
            break
