# src/trv/endpoints.py
from __future__ import annotations
import datetime as dt
from typing import Dict, Any, Iterator, Optional
from .config import DEFAULT_PAGE_SIZE

def _iso_z(ts: dt.datetime) -> str:
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=dt.UTC)
    return ts.astimezone(dt.UTC).isoformat(timespec="seconds").replace("+00:00", "Z")

def _build_query_xml(api_key: str, since_utc: dt.datetime, limit: int,
                     future_days_limit: Optional[int]) -> str:
    now_utc = dt.datetime.now(dt.UTC)
    since_iso = _iso_z(since_utc)
    now_iso   = _iso_z(now_utc)
    future_cap = _iso_z(now_utc + dt.timedelta(days=future_days_limit)) if future_days_limit is not None else None

    return f"""
<REQUEST>
  <LOGIN authenticationkey="{api_key}" />
  <QUERY objecttype="Situation" schemaversion="1" limit="{limit}"
         orderby="ModifiedTime desc, Deviation.StartTime desc">
    <FILTER>
      <OR>
        <GT name="Deviation.StartTime" value="{since_iso}" />
        <GT name="Deviation.StartTime" value="{now_iso}" />
      </OR>
      {f'<LT name="Deviation.StartTime" value="{future_cap}" />' if future_cap else ''}
    </FILTER>

    <!-- Situation -->
    <INCLUDE>Id</INCLUDE>
    <INCLUDE>ModifiedTime</INCLUDE>

    <!-- Deviation (alla fält ska vara under Deviation.* i schema 1) -->
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

def iterate_incidents(client, since_utc: dt.datetime,
                      page_size: int = DEFAULT_PAGE_SIZE,
                      future_days_limit: Optional[int] = 14) -> Iterator[Dict[str, Any]]:
    xml = _build_query_xml(client.api_key, since_utc, page_size, future_days_limit)
    # debug vid behov: print(xml.replace(client.api_key, "***"))
    data = client.post(xml)
    for res in data.get("RESPONSE", {}).get("RESULT", []):
        for row in res.get("Situation", []) or []:
            yield row
