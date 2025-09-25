# src/cli.py
import click
import datetime as dt
import os
import time

from src.logger import setup_logger                    # Logger-funktion för att skriva loggar
from src.trv.config import TRV_API_KEY, TRV_BASE_URL, DEFAULT_DAYS_BACK  # Konfig och API-nycklar
from src.trv.client import TRVClient                   # Klientklass för att prata med TRV:s API
from src.trv.endpoints import iterate_incidents        # Funktion för att hämta incidenter (med pagination)
from src.trv.transform import normalize_incidents      # Funktion som normaliserar API-data → DataFrame
from src.trv.load_sqlite import ensure_schema, upsert_incidents  # Skapar schema + laddar data till SQLite
from src.utils.notifier import notify                  # Funktion för Slack-notiser

# ------------------------------------------------------------
# Konfig: gränsvärden för "rimligt" antal rader
# Hämtas från .env (om de finns satta).
# Om rader < EXPECT_MIN_ROWS eller > EXPECT_MAX_ROWS skickas varning.
# ------------------------------------------------------------
EXPECT_MIN_ROWS = int(os.getenv("EXPECT_MIN_ROWS", "0") or 0)
EXPECT_MAX_ROWS = int(os.getenv("EXPECT_MAX_ROWS", "0") or 0)


# ------------------------------------------------------------
# CLI-kommando: extract_trv
# Anropas från terminalen:
#   python -m src.cli --db-path trafik.db --days-back 1
# ------------------------------------------------------------
@click.command()
@click.option("--db-path", default="trafik.db", show_default=True)  # SQLite-filens namn
@click.option("--days-back", default=DEFAULT_DAYS_BACK, show_default=True)  # Hur många dagar bakåt vi hämtar data
def extract_trv(db_path: str, days_back: int):
    """
    Kör en ETL som hämtar Situation/Deviation från Trafikverket (TRV),
    normaliserar datan och laddar in den i SQLite.
    Skickar notiser till Slack under körningen.
    """
    logger = setup_logger("ETL")  # Starta loggern
    t0 = time.time()              # Starttid för körningen
    started_at = dt.datetime.now(dt.UTC).strftime("%Y-%m-%d %H:%M:%S UTC")  # Tidpunkt som text
    since = dt.datetime.now(dt.UTC) - dt.timedelta(days=days_back)  # Tidsgräns för hämtning

    # 1) Start-notis
    start_msg = f"🚦 ETL startad • db=`{db_path}` • days_back=`{days_back}` • {started_at}"
    logger.info(start_msg)
    notify(start_msg, level="info")

    try:
        # 2) Validera API-konfig
        if not TRV_API_KEY:
            raise RuntimeError("Saknar TRV_API_KEY i .env")  # Stanna om nyckel saknas
        if not TRV_BASE_URL:
            raise RuntimeError("Saknar TRV_BASE_URL i .env")

        # 3) Initiera TRV-klient + kontrollera schema i SQLite
        client = TRVClient(api_key=TRV_API_KEY, base_url=TRV_BASE_URL)
        ensure_schema(db_path)  # Skapa tabeller om de inte finns
        logger.info("Schema kontrollerat/skapats")

        # 4) Extrahera data (hämtar alla incidenter sedan 'since')
        situations = list(iterate_incidents(client, since_utc=since, page_size=500))
        got_msg = f"📥 Hämtat {len(situations)} Situation-objekt"
        logger.info(got_msg)
        notify(got_msg, level="info")

        # 5) Transformera data (normalisera till tabellformat)
        df = normalize_incidents(situations)
        norm_msg = f"🧮 Normaliserat → {len(df)} rader"
        logger.info(norm_msg)
        notify(norm_msg, level="info")

        # 6) Ladda data till SQLite (UPSERT = uppdatera befintliga + lägg till nya)
        upsert_incidents(db_path, df)
        logger.info("Data upsertad i SQLite")

        # 7) Summering + KPI (räknar antal PÅGÅR och KOMMANDE incidenter)
        pagar = kommande = 0
        if not df.empty and "status" in df.columns:
            c = df["status"].value_counts().to_dict()
            pagar, kommande = c.get("PÅGÅR", 0), c.get("KOMMANDE", 0)

        # Beräkna körtid i sekunder
        secs = round(time.time() - t0, 1)

        # Skicka klart-notis
        done_msg = (
            f"✅ ETL klar • rader=`{len(df)}` • PÅGÅR=`{pagar}` • KOMMANDE=`{kommande}` • "
            f"tid=`{secs}s` • db=`{db_path}`"
        )
        logger.info(done_msg)
        notify(done_msg, level="success")

        # 8) Varningar om antal rader är misstänkt (0, för få eller för många)
        if len(df) == 0:
            notify("⚠️ Varning: ETL returnerade 0 rader.", level="warning")
        if EXPECT_MIN_ROWS and len(df) < EXPECT_MIN_ROWS:
            notify(f"⚠️ Varning: lågt antal rader ({len(df)} < {EXPECT_MIN_ROWS}).", level="warning")
        if EXPECT_MAX_ROWS and len(df) > EXPECT_MAX_ROWS:
            notify(f"⚠️ Varning: högt antal rader ({len(df)} > {EXPECT_MAX_ROWS}).", level="warning")

    except Exception as e:
        # 9) Felhantering (skicka felnotis till Slack + logga full stacktrace)
        logger.exception("Kritiskt fel i ETL-pipelinen")
        notify(f"🚨 ETL FEL: {e}", level="error")
        raise


# ------------------------------------------------------------
# Main entrypoint – körs om filen exekveras direkt
# ------------------------------------------------------------
if __name__ == "__main__":
    extract_trv()
