# src/trv/client.py
from __future__ import annotations
import time
import logging
from typing import Any
import requests

log = logging.getLogger(__name__)

class TRVClient:
    """HTTP-klient mot Trafikverket som returnerar XML-råtext."""

    def __init__(self, api_key: str, base_url: str, timeout: int = 30):
        self.api_key = api_key
        self.base_url = base_url
        self.timeout = timeout
        self._session = requests.Session()
        self._session.headers.update({
            "Content-Type": "application/xml",
            "Accept": "application/xml",   # 👈 be om XML, inte JSON
        })

    def _sleep_backoff(self, attempt: int):
        time.sleep(min(2 ** attempt, 10))

    def post(self, payload_xml: str) -> str:
        """
        Skickar XML-query till Trafikverket och returnerar XML som sträng.
        Gör retries på tillfälliga fel.
        """
        url = self.base_url
        for attempt in range(5):
            try:
                resp = self._session.post(url, data=payload_xml.encode("utf-8"), timeout=self.timeout)

                if resp.status_code == 200:
                    return resp.text   # 👈 XML tillbaka som text

                log.warning("TRV %s: %s", resp.status_code, resp.text[:500])
                if resp.status_code in (429, 500, 502, 503, 504):
                    self._sleep_backoff(attempt)
                    continue

                resp.raise_for_status()

            except requests.RequestException as e:
                log.exception("Nätverksfel mot TRV: %s", e)
                self._sleep_backoff(attempt)

        raise RuntimeError("Kunde inte hämta från TRV efter flera försök.")
