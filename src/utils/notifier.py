import os
import json
import requests
import logging
from dotenv import load_dotenv

# Ladda .env (för SLACK_WEBHOOK_URL)
load_dotenv()
SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL")

# Lokal logger (fallback)
logger = logging.getLogger("notifier")

def _safe_post(payload: dict) -> None:
    """Skicka JSON till Slack webhook (tyst vid fel)."""
    if not SLACK_WEBHOOK_URL:
        logger.debug("Ingen SLACK_WEBHOOK_URL definierad – hoppar över Slack.")
        return
    try:
        resp = requests.post(
            SLACK_WEBHOOK_URL,
            data=json.dumps(payload),
            headers={"Content-Type": "application/json"},
            timeout=10
        )
        if resp.status_code != 200:
            logger.warning(f"Slack API svarade med {resp.status_code}: {resp.text}")
    except Exception as e:
        logger.error(f"Kunde inte skicka till Slack: {e}")

def notify(text: str, level: str = "info") -> None:
    """
    Skicka notis till Slack och logga lokalt.
    level: 'info' | 'warning' | 'error' | 'success'
    """
    emojis = {"info": "ℹ️", "warning": "⚠️", "error": "🚨", "success": "✅"}
    emoji = emojis.get(level, "ℹ️")
    message = f"{emoji} {text}"

    # Logga alltid lokalt
    if level == "error":
        logger.error(message)
    elif level == "warning":
        logger.warning(message)
    else:
        logger.info(message)

    # Skicka till Slack (om satt)
    _safe_post({"text": message})

# Bakåtkompatibelt alias (om koden någonstans kallar send_slack)
def send_slack(text: str, level: str = "info") -> None:
    notify(text, level)
