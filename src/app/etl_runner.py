# src/app/etl_runner.py
import time
import datetime as dt
from src.trv.config import TRV_API_KEY, TRV_BASE_URL, DEFAULT_DAYS_BACK
from src.trv.client import TRVClient
from src.trv.endpoints import iterate_incidents
from src.trv.transform import normalize_incidents
from src.trv.load_sqlite import ensure_schema, upsert_incidents

def run_etl(db_path: str = "trafik.db", days_back: int = DEFAULT_DAYS_BACK) -> dict:
    """Run ETL pipeline against TRV API and update local SQLite DB."""
    t0 = time.time()
    since = dt.datetime.now(dt.UTC) - dt.timedelta(days=days_back)

    if not TRV_API_KEY or not TRV_BASE_URL:
        raise RuntimeError("Missing TRV_API_KEY or TRV_BASE_URL in secrets/env")

    client = TRVClient(api_key=TRV_API_KEY, base_url=TRV_BASE_URL)
    ensure_schema(db_path)

    # Extract
    situations = list(iterate_incidents(client, since_utc=since, page_size=500))

    # Transform
    df = normalize_incidents(situations)

    # Load
    upsert_incidents(db_path, df)

    pagar = kommande = 0
    if not df.empty and "status" in df.columns:
        c = df["status"].value_counts().to_dict()
        pagar, kommande = c.get("PÅGÅR", 0), c.get("KOMMANDE", 0)

    return {
        "rows": len(df),
        "pagar": pagar,
        "kommande": kommande,
        "seconds": round(time.time() - t0, 1),
        "db_path": db_path,
    }
