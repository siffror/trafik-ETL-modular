# src/trv/client.py
from __future__ import annotations
import time
import logging
import random
from typing import Any, Dict, Optional
import requests

log = logging.getLogger(__name__)

class TRVClient:
    """HTTP client with retries, backoff and safe JSON/XML parsing."""
    def __init__(self, api_key: str, base_url: str, timeout: int = 30, force_json: bool = True):
        self.api_key = api_key
        self.base_url = base_url.rstrip("?")
        self.timeout = timeout
        self.force_json = force_json

        self._session = requests.Session()
        # You POST XML, but you can still prefer JSON back
        self._session.headers.update({
            "Content-Type": "application/xml",
            "Accept": "application/json" if force_json else "*/*",
        })

    def _sleep_backoff(self, attempt: int):
        # Exponential backoff with a little jitter
        time.sleep(min(2 ** attempt, 10) + random.uniform(0, 0.25))

    def _with_format_param(self, url: str) -> str:
        if not self.force_json:
            return url
        # Append ?format=json (or &format=json) if not present
        sep = "&" if "?" in url else "?"
        if "format=" not in url:
            return f"{url}{sep}format=json"
        return url

    def _snippet(self, text: str, n: int = 300) -> str:
        if not text:
            return "<empty>"
        t = text.strip().replace("\n", " ")
        return (t[:n] + ("…" if len(t) > n else ""))

    def post(self, payload_xml: str) -> Dict[str, Any]:
        """
        Sends XML query to Trafikverket. Retries on transient errors.
        Tries JSON first; if not JSON, tries xmltodict; else returns raw text under {'raw': ...}.
        """
        url = self._with_format_param(self.base_url)

        for attempt in range(5):
            try:
                resp = self._session.post(url, data=payload_xml.encode("utf-8"), timeout=self.timeout)
            except requests.RequestException as e:
                log.exception("Network error towards TRV: %s", e)
                self._sleep_backoff(attempt)
                continue

            ct = (resp.headers.get("Content-Type") or "").lower()
            body_snip = self._snippet(resp.text)

            if resp.status_code == 200:
                # Prefer JSON if header says so
                if "application/json" in ct:
                    try:
                        return resp.json()  # type: ignore[return-value]
                    except ValueError:
                        log.error("Invalid JSON from TRV (Content-Type said json): %s", body_snip)
                        # fall through to xml / raw parsing below

                # Try JSON anyway (some gateways forget proper header)
                try:
                    return resp.json()  # type: ignore[return-value]
                except ValueError:
                    pass  # not JSON

                # Try XML if available
                try:
                    import xmltodict  # optional dependency
                    parsed = xmltodict.parse(resp.text)
                    # normalize to dict
                    return {"xml": parsed}
                except Exception:
                    # As a last resort, return raw body so caller can decide
                    log.warning("Non-JSON response from TRV; returning raw. CT=%s :: %s", ct, body_snip)
                    return {"raw": resp.text}

            # Non-200: log and decide whether to retry
            log.warning("TRV %s for %s :: %s", resp.status_code, url, body_snip)
            if resp.status_code in (429, 500, 502, 503, 504):
                self._sleep_backoff(attempt)
                continue

            # Other HTTP errors: raise with context
            try:
                resp.raise_for_status()
            except requests.HTTPError as e:
                raise RuntimeError(
                    f"TRV HTTP {resp.status_code} (Content-Type={ct}): {body_snip}"
                ) from e

        raise RuntimeError("Failed to fetch from TRV after multiple attempts.")
