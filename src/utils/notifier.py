# src/utils/notifier.py
import os
import json
import requests
import logging
from dotenv import load_dotenv
import streamlit as st

# Load .env locally
load_dotenv()

# Prefer Streamlit secrets if running on Cloud
SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL") or st.secrets.get("SLACK_WEBHOOK_URL")

logger = logging.getLogger("notifier")

def _safe_post(payload: dict) -> None:
    """Send JSON payload to Slack webhook (silent on failure)."""
    if not SLACK_WEBHOOK_URL:
        logger.debug("No SLACK_WEBHOOK_URL defined – skipping Slack.")
        return
    try:
        resp = requests.post(
            SLACK_WEBHOOK_URL,
            data=json.dumps(payload),
            headers={"Content-Type": "application/json"},
            timeout=10,
        )
        if resp.status_code != 200:
            logger.warning(f"Slack API responded {resp.status_code}: {resp.text}")
    except Exception as e:
        logger.error(f"Could not send to Slack: {e}")

def notify(text: str, level: str = "info") -> None:
    """Send notification to Slack + log locally."""
    emojis = {"info": "ℹ️", "warning": "⚠️", "error": "🚨", "success": "✅"}
    emoji = emojis.get(level, "ℹ️")
    message = f"{emoji} {text}"

    # Always log locally
    getattr(logger, "error" if level=="error" else "warning" if level=="warning" else "info")(message)

    # Send to Slack if configured
    _safe_post({"text": message})

# Backward compatibility alias
def send_slack(text: str, level: str = "info") -> None:
    notify(text, level)
