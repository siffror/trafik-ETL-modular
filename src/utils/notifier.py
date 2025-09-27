# src/utils/notifier.py
import os, json, requests, logging
from dotenv import load_dotenv
try:
    import streamlit as st
except Exception:
    st = None

load_dotenv()

SLACK_WEBHOOK_URL = (
    (st.secrets.get("SLACK_WEBHOOK_URL") if st else None)
    or os.getenv("SLACK_WEBHOOK_URL")
)
SLACK_NOTIFY_USER = (
    (st.secrets.get("SLACK_NOTIFY_USER") if st else None)
    or os.getenv("SLACK_NOTIFY_USER")
)

logger = logging.getLogger("notifier")

def _safe_post(payload: dict) -> dict:
    """Send JSON payload to Slack webhook; always return a status dict."""
    if not SLACK_WEBHOOK_URL:
        return {"sent": False, "configured": False, "status": None, "error": "no_webhook"}
    try:
        resp = requests.post(
            SLACK_WEBHOOK_URL,
            data=json.dumps(payload),
            headers={"Content-Type": "application/json"},
            timeout=10,
        )
        ok = (resp.status_code == 200)
        return {
            "sent": ok,
            "configured": True,
            "status": resp.status_code,
            "error": None if ok else resp.text,
        }
    except Exception as e:
        return {"sent": False, "configured": True, "status": None, "error": str(e)}

def notify(text: str, level: str = "info", ping: bool = False, ping_user: bool = False) -> dict:
    """
    Send notification to Slack + local log.
    - ping=True adds <!here> to trigger channel notifications.
    - ping_user=True adds <@USERID> if SLACK_NOTIFY_USER is set.
    Always returns a dict from _safe_post.
    """
    emojis = {"info": "ℹ️", "warning": "⚠️", "error": "🚨", "success": "✅"}
    emoji = emojis.get(level, "ℹ️")

    prefix = ""
    if ping:
        prefix += "<!here> "
    if ping_user and SLACK_NOTIFY_USER:
        prefix += f"<@{SLACK_NOTIFY_USER}> "

    message = f"{emoji} {prefix}{text}"

    # local log
    (logger.error if level == "error" else logger.warning if level == "warning" else logger.info)(message)

    return _safe_post({"text": message, "mrkdwn": True})
