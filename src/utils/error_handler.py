import logging

def handle_error(logger: logging.Logger, msg: str, exc: Exception = None):
    """Central felhantering (logg + plats för notifieringar)."""
    if exc:
        logger.exception(f"{msg} – {exc}")
    else:
        logger.error(msg)

    # Här kan du lägga till notifieringar t.ex. Slack/Teams/webhook
    # send_slack_alert(msg)
