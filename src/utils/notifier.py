# src/utils/notifier.py
import os, json, requests, logging
from dotenv import load_dotenv
try:
    import streamlit as st  # type: ignore
except Exception:
    st = None

load_dotenv()

SLACK_WEBHOOK_URL = (
    (st.secrets.get("SLACK_WEBHOOK_URL") if st else None)
    or os.getenv("SLACK_WEBHOOK_URL")
)
# Optional: ping a specific user on errors (Slack member ID like U012ABCDEF)
SLACK_NOTIFY_USER = (
    (st.secrets.get("SLACK_NOTIFY_USER") if st else None)
    or os.getenv("SLACK_NOTIFY_USER")
)

logger = logging.getLogger("notifier")

def _safe_post(payload: dict) -> dict:
    """Send JSON payload to Slack webhook; always return a status dict."""
    if not SLACK_WEBHOOK_URL:
        logger.debug("No SLACK_WEBHOOK_URL configured.")
        return {"sent": False, "configured": False, "status": None, "error": "no_webhook"}

    try:
        resp = requests.post(
            SLACK_WEBHOOK_URL,
            data=json.dumps(payload),
            headers={"Content-Type": "application/json"},
            timeout=10,
        )
        ok = (resp.status_code == 200)
        if not ok:
            msg = f"Slack responded {resp.status_code}: {resp.text}"
            logger.warning(msg)
        return {"sent": ok, "configured": True, "status": resp.status_code,
                "error": None if ok else resp.text}
    except Exception as e:
        msg = f"Slack request failed: {e}"
        logger.error(msg)
        return {"sent": False, "configured": True, "status": None, "error": str(e)}

def notify(text: str, level: str = "info", ping: bool = False, ping_user: bool | None = None) -> dict:
    """
    Post a message to Slack and log locally.
    - ping=True adds '<!here>' to trigger channel notifications.
    - ping_user=True (or None to use env default) mentions SLACK_NOTIFY_USER if set.
    Always returns a status dict from _safe_post.
    """
    emojis = {"info": "ℹ️", "warning": "⚠️", "error": "🚨", "success": "✅"}
    emoji = emojis.get(level, "ℹ️")

    # Build mention prefix to trigger notifications
    prefix = ""
    if ping:
        prefix += "<!here> "
    uid = SLACK_NOTIFY_USER if ping_user is None else (SLACK_NOTIFY_USER if ping_user else None)
    if uid:
        prefix += f"<@{uid}> "

    message = f"{emoji} {prefix}{text}"

    # Local log
    (logger.error if level == "error" else logger.warning if level == "warning" else logger.info)(message)

    # Slack (mrkdwn=True helps parse mentions consistently)
    return _safe_post({"text": message, "mrkdwn": True})

# Backward-compat alias
def send_slack(text: str, level: str = "info") -> dict:
    return notify(text, level)
