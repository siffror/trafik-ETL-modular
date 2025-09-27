# src/trv/config.py
import os

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

try:
    import streamlit as st
    TRV_API_KEY = st.secrets.get("TRV_API_KEY", os.getenv("TRV_API_KEY", ""))
    SLACK_WEBHOOK_URL = st.secrets.get("SLACK_WEBHOOK_URL", os.getenv("SLACK_WEBHOOK_URL", ""))
    TRV_BASE_URL = st.secrets.get("TRV_BASE_URL", os.getenv("TRV_BASE_URL", "https://api.trafikinfo.trafikverket.se/v2/data.xml"))
except ModuleNotFoundError:
    TRV_API_KEY = os.getenv("TRV_API_KEY", "")
    SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL", "")
    TRV_BASE_URL = os.getenv("TRV_BASE_URL", "https://api.trafikinfo.trafikverket.se/v2/data.xml")

# Default config
DEFAULT_DAYS_BACK = int(os.getenv("DEFAULT_DAYS_BACK", "1"))

# 🔹 Add this missing one
DEFAULT_PAGE_SIZE = int(os.getenv("DEFAULT_PAGE_SIZE", "500"))
