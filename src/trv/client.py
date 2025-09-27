# src/trv/client.py
from __future__ import annotations
import time
import logging
from typing import Any
import requests
import random

log = logging.getLogger(__name__)

class TRVClient:
    """HTTP client for Trafikverket that posts XML and returns raw XML text."""

    def __init__(self, api_key: str, base_url: str, timeout: int = 30):
        # Note: api_key is not used here; TRV expects it inside the XML <LOGIN>.
        self.api_key = api_key
        self.base_url = base_url
        self.timeout = timeout
        self._session = requests.Session()
        self._session.headers.update({
            "Content-Type": "application/xml",
            "Accept": "application/xml",
            "User-Agent": "trafik-etl-modular/1.0 (+github actions)"
        })

    def _sleep_backoff(self, attempt: int):
        """Exponential backoff with jitter to avoid thundering herd."""
        base = min(2 ** attempt, 10)
        time.sleep(base + random.random())

    def post(self, payload_xml: str) -> str:
        """
        Sends the XML query to Trafikverket and returns XML as a string.
        Retries on transient errors.
        """
        url = self.base_url
        for attempt in range(5):
            try:
                resp = self._session.post(
                    url, data=payload_xml.encode("utf-8"), timeout=self.timeout
                )

                if resp.status_code == 200:
                    return resp.text  # raw XML

                log.warning("TRV %s: %s", resp.status_code, resp.text[:500])

                # Transient server/rate errors → retry
                if resp.status_code in (429, 500, 502, 503, 504):
                    self._sleep_backoff(attempt)
                    continue

                # Non-retryable HTTP errors
                resp.raise_for_status()

            except requests.RequestException as e:
                log.exception("Network error calling TRV: %s", e)
                self._sleep_backoff(attempt)

        raise RuntimeError("Failed to fetch from TRV after multiple attempts.")
