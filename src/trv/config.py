import os
from dotenv import load_dotenv

# Ladda .env automatiskt
load_dotenv()

TRV_BASE_URL = "https://api.trafikinfo.trafikverket.se/v2/data.json"
TRV_API_KEY = os.getenv("TRV_API_KEY", "").strip()

DEFAULT_DAYS_BACK = 1
DEFAULT_PAGE_SIZE = 500

if not TRV_API_KEY:
    raise RuntimeError("Saknar TRV_API_KEY i .env eller miljön.")
