# src/trv/endpoints.py
from __future__ import annotations
import datetime as dt
from typing import Dict, Any, Iterator, Optional, List, Tuple
from .config import DEFAULT_PAGE_SIZE

def _iso_z(ts: dt.datetime) -> str:
    """Return an ISO-8601 UTC string with trailing 'Z'."""
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=dt.UTC)
    return ts.astimezone(dt.UTC).isoformat(timespec="seconds").replace("+00:00", "Z")

def _build_query_xml(
    api_key: str,
    since_utc: dt.datetime,
    limit: int,
    future_days_limit: Optional[int],
    lt_modified: Optional[str] = None,
    lt_start: Optional[str] = None,
) -> str:
    """
    Build Trafikverket XML query.
    When lt_modified/lt_start are provided, we page "down" by requesting rows
    with ModifiedTime/Deviation.StartTime strictly less than the last page.
    """
    now_utc = dt.datetime.now(dt.UTC)
    since_iso = _iso_z(since_utc)
    now_iso   = _iso_z(now_utc)
    future_cap = _iso_z(now_utc + dt.timedelta(days=future_days_limit)) if future_days_limit is not None else None

    # Base filters (recent and upcoming)
    filters = f"""
      <OR>
        <GT name="Deviation.StartTime" value="{since_iso}" />
        <GT name="Deviation.StartTime" value="{now_iso}" />
      </OR>
      {f'<LT name="Deviation.StartTime" value="{future_cap}" />' if future_cap else ''}
    """

    # Cursor filters to keep pages strictly descending (avoid duplicates)
    if lt_modified:
        filters += f'\n      <LT name="ModifiedTime" value="{lt_modified}" />'
    if lt_start:
        filters += f'\n      <LT name="Deviation.StartTime" value="{lt_start}" />'

    return f"""
<REQUEST>
  <LOGIN authenticationkey="{api_key}" />
  <QUERY objecttype="Situation" schemaversion="1" limit="{limit}"
         orderby="ModifiedTime desc, Deviation.StartTime desc">
    <FILTER>
{filters}
    </FILTER>

    <INCLUDE>Id</INCLUDE>
    <INCLUDE>ModifiedTime</INCLUDE>

    <INCLUDE>Deviation.Id</INCLUDE>
    <INCLUDE>Deviation.MessageType</INCLUDE>
    <INCLUDE>Deviation.Message</INCLUDE>
    <INCLUDE>Deviation.LocationDescriptor</INCLUDE>
    <INCLUDE>Deviation.StartTime</INCLUDE>
    <INCLUDE>Deviation.EndTime</INCLUDE>
    <INCLUDE>Deviation.RoadNumber</INCLUDE>
    <INCLUDE>Deviation.CountyNo</INCLUDE>
    <INCLUDE>Deviation.Geometry.WGS84</INCLUDE>
  </QUERY>
</REQUEST>
""".strip()

def _extract_rows(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for res in payload.get("RESPONSE", {}).get("RESULT", []):
        rows.extend(res.get("Situation", []) or [])
    return rows

def _get_cursor_from_last(rows: List[Dict[str, Any]]) -> Tuple[Optional[str], Optional[str]]:
    """
    Read the last row's ModifiedTime and Deviation.StartTime (as strings).
    Assumes API honors orderby="ModifiedTime desc, Deviation.StartTime desc".
    """
    if not rows:
        return None, None
    last = rows[-1]
    lt_modified = last.get("ModifiedTime")
    # Deviation may be list or object depending on payload; handle both
    dev = last.get("Deviation")
    if isinstance(dev, list) and dev:
        dev = dev[0]
    lt_start = dev.get("StartTime") if isinstance(dev, dict) else None
    return lt_modified, lt_start

def iterate_incidents(
    client,
    since_utc: dt.datetime,
    page_size: int = DEFAULT_PAGE_SIZE,
    future_days_limit: Optional[int] = 14,
    max_pages: int = 20,
) -> Iterator[Dict[str, Any]]:
    """
    Yield Situation rows with simple cursor-based pagination.
    - Keeps requesting older pages using LT filters on (ModifiedTime, Deviation.StartTime).
    - Stops when fewer than page_size rows are returned or max_pages is reached.
    """
    seen_ids: set = set()
    lt_modified: Optional[str] = None
    lt_start: Optional[str] = None

    for _ in range(max_pages):
        xml = _build_query_xml(
            client.api_key,
            since_utc=since_utc,
            limit=page_size,
            future_days_limit=future_days_limit,
            lt_modified=lt_modified,
            lt_start=lt_start,
        )
        data = client.post(xml)
        rows = _extract_rows(data)
        if not rows:
            break

        # Yield unique rows only
        new_count = 0
        for row in rows:
            rid = row.get("Id")
            if rid and rid not in seen_ids:
                seen_ids.add(rid)
                new_count += 1
                yield row

        # If page smaller than requested or no new rows -> done
        if len(rows) < page_size or new_count == 0:
            break

        # Advance cursor to strictly older rows next round
        lt_modified, lt_start = _get_cursor_from_last(rows)
        if not lt_modified:
            break
