# src/trv/config.py
import os

# Try to load .env if running locally
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# Streamlit secrets (Cloud)
try:
    import streamlit as st
    TRV_API_KEY = st.secrets.get("TRV_API_KEY", os.getenv("TRV_API_KEY", ""))
    TRV_BASE_URL = st.secrets.get("TRV_BASE_URL", os.getenv("TRV_BASE_URL", ""))
except ModuleNotFoundError:
    # Local dev fallback
    TRV_API_KEY = os.getenv("TRV_API_KEY", "")
    TRV_BASE_URL = os.getenv("TRV_BASE_URL", "")

# Default config
DEFAULT_DAYS_BACK = int(os.getenv("DEFAULT_DAYS_BACK", "1"))
