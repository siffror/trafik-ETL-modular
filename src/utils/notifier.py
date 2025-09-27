# src/utils/notifier.py
import os, json, requests, logging
from dotenv import load_dotenv

# Optional import: on Streamlit Cloud we prefer st.secrets
try:
    import streamlit as st  # type: ignore
except Exception:
    st = None  # running outside Streamlit

# Load .env for local runs; harmless on Cloud
load_dotenv()

# Prefer Streamlit secrets if available
SLACK_WEBHOOK_URL = (
    (st.secrets.get("SLACK_WEBHOOK_URL") if st else None)
    or os.getenv("SLACK_WEBHOOK_URL")
)

logger = logging.getLogger("notifier")

def _safe_post(payload: dict) -> dict:
    """
    Send JSON payload to Slack webhook.
    Returns a status dict for diagnostics:
      {"sent": bool, "configured": bool, "status": int|None, "error": str|None}
    """
    if not SLACK_WEBHOOK_URL:
        logger.debug("No SLACK_WEBHOOK_URL configured.")
        return {"sent": False, "configured": False, "status": None, "error": None}

    try:
        resp = requests.post(
            SLACK_WEBHOOK_URL,
            data=json.dumps(payload),
            headers={"Content-Type": "application/json"},
            timeout=10,
        )
        if resp.status_code != 200:
            msg = f"Slack responded {resp.status_code}: {resp.text}"
            logger.warning(msg)
            return {"sent": False, "configured": True, "status": resp.status_code, "error": msg}
        return {"sent": True, "configured": True, "status": resp.status_code, "error": None}
    except Exception as e:
        msg = f"Slack request failed: {e}"
        logger.error(msg)
        return {"sent": False, "configured": True, "status": None, "error": str(e)}

def notify(text: str, level: str = "info") -> dict:
    """
    Send notification to Slack + log locally.
    Returns the status dict from _safe_post (see above).
    """
    emojis = {"info": "ℹ️", "warning": "⚠️", "error": "🚨", "success": "✅"}
    emoji = emojis.get(level, "ℹ️")
    message = f"{emoji} {text}"

    # Local log
    if level == "error":
        logger.error(message)
    elif level == "warning":
        logger.warning(message)
    else:
        logger.info(message)

    # Slack
    return _safe_post({"text": message})

# Backward-compat alias
def send_slack(text: str, level: str = "info") -> dict:
    return notify(text, level)
