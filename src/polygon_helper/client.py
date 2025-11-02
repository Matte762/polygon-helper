import os
from typing import Dict, Any, Optional
import requests
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

DEFAULT_BASE = "https://api.polygon.io"

class PolygonAuthError(Exception):
    pass

class PolygonClient:
    def __init__(self, api_key: Optional[str] = None, base_url: str = DEFAULT_BASE, session: Optional[requests.Session] = None):
        self.api_key = api_key or "Gc_5gx7SynFpJGDezYkWBPRJ56rTFyX0"
        if not self.api_key:
            raise PolygonAuthError("POLYGON_API_KEY not set. Export it or pass api_key=...")
        self.base_url = base_url.rstrip("/")
        self.session = session or requests.Session()
        self.session.headers.update({"Accept": "application/json"})

    @retry(
        reraise=True,
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=0.5, min=0.5, max=8),
        retry=retry_if_exception_type((requests.Timeout, requests.ConnectionError)),
    )
    def get(self, path: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        url = path if path.startswith("http") else f"{self.base_url}{path}"
        params = params.copy() if params else {}
        params["apiKey"] = self.api_key

        resp = self.session.get(url, params=params, timeout=30)
        # 429/backoff: let the caller decide; 5xx will be retried by tenacity if connection error/timeout
        if resp.status_code == 429:
            # surface a clear error; you can also inspect resp.headers for retry-after
            raise requests.HTTPError("Rate limit exceeded (HTTP 429)", response=resp)
        resp.raise_for_status()
        return resp.json()
