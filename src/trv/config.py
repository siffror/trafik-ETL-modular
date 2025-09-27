# src/trv/config.py
import os

# Try to load .env if running locally
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# Streamlit secrets (used on Streamlit Cloud)
try:
    import streamlit as st
    TRV_API_KEY = st.secrets.get("TRV_API_KEY", os.getenv("TRV_API_KEY", ""))
    SLACK_WEBHOOK_URL = st.secrets.get("SLACK_WEBHOOK_URL", os.getenv("SLACK_WEBHOOK_URL", ""))
    TRV_BASE_URL = st.secrets.get("TRV_BASE_URL", os.getenv("TRV_BASE_URL", "https://api.trafikverket.se"))
except ModuleNotFoundError:
    # Local fallback if not running under Streamlit
    TRV_API_KEY = os.getenv("TRV_API_KEY", "")
    SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL", "")
    TRV_BASE_URL = os.getenv("TRV_BASE_URL", "https://api.trafikverket.se")

# Default config
DEFAULT_DAYS_BACK = int(os.getenv("DEFAULT_DAYS_BACK", "1"))
