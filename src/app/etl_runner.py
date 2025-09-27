# src/app/etl_runner.py
from __future__ import annotations

import time
import datetime as dt
from typing import Dict

from src.trv.config import TRV_API_KEY, TRV_BASE_URL, DEFAULT_DAYS_BACK
from src.trv.client import TRVClient
from src.trv.endpoints import iterate_incidents
from src.trv.transform import normalize_incidents
from src.trv.load_sqlite import ensure_schema, upsert_incidents
from src.utils.notifier import notify


def run_etl(db_path: str = "trafik.db", days_back: int = DEFAULT_DAYS_BACK) -> Dict[str, object]:
    """
    Run the TRV ETL and upsert results into SQLite.
    Sends Slack notifications on start, success, warnings, and errors.
    Returns a summary dict for the Streamlit UI.
    """
    t0 = time.time()
    started_at = dt.datetime.now(dt.UTC).strftime("%Y-%m-%d %H:%M:%S UTC")
    since = dt.datetime.now(dt.UTC) - dt.timedelta(days=days_back)

    # --- configuration sanity check ---
    if not TRV_API_KEY or not TRV_BASE_URL:
        raise RuntimeError("Missing TRV_API_KEY or TRV_BASE_URL in secrets/env")

    # --- start notification ---
    notify(f"ETL started • db={db_path} • days_back={days_back} • {started_at}", level="info")

    try:
        # --- init client + ensure schema ---
        client = TRVClient(api_key=TRV_API_KEY, base_url=TRV_BASE_URL)
        ensure_schema(db_path)

        # --- extract ---
        situations = list(iterate_incidents(client, since_utc=since, page_size=500))
        notify(f"Fetched {len(situations)} Situation objects", level="info")

        # --- transform ---
        df = normalize_incidents(situations)
        notify(f"Normalized → {len(df)} rows", level="info")

        # --- load (upsert) ---
        upsert_incidents(db_path, df)

        # --- simple KPIs ---
        pagar = kommande = 0
        if not df.empty and "status" in df.columns:
            counts = df["status"].value_counts().to_dict()
            pagar, kommande = counts.get("PÅGÅR", 0), counts.get("KOMMANDE", 0)

        secs = round(time.time() - t0, 1)

        # --- success notification ---
        notify(
            f"ETL finished • rows={len(df)} • PÅGÅR={pagar} • KOMMANDE={kommande} • time={secs}s • db={db_path}",
            level="success",
        )

        # --- warning notifications (optional heuristics) ---
        if len(df) == 0:
            notify("Warning: ETL produced 0 rows.", level="warning")

        return {
            "rows": len(df),
            "pagar": pagar,
            "kommande": kommande,
            "seconds": secs,
            "db_path": db_path,
        }

    except Exception as e:
        # --- error notification then re-raise ---
        notify(f"ETL FAILED: {e}", level="error")
        raise
