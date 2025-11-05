from typing import Dict, Any, Optional
import requests
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

DEFAULT_BASE = "https://api.polygon.io"

class PolygonAuthError(Exception):
    pass

#Notice that I pass api_key and url in self for simplicity of use later
class PolygonClient:
    def __init__(self, api_key: Optional[str] = None, base_url: str = DEFAULT_BASE):
        self.api_key = api_key
        if not self.api_key:
            raise PolygonAuthError("POLYGON_API_KEY not set. Export it or pass api_key=...")
        self.base_url = base_url

    #This basically is going to run the get functions a couple of times
    #Definetly not an essential part but useful 
    @retry(
        reraise=True,
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=0.5, min=0.5, max=8),
        retry=retry_if_exception_type((requests.Timeout, requests.ConnectionError)),
    )
    def get(self, path: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        url = f"{self.base_url}{path}"
        q = params.copy() if params else {}
        q["apiKey"] = self.api_key

        resp = requests.get(url, params=q, headers={"Accept": "application/json"}, timeout=30)

        if resp.status_code == 429:
            raise requests.HTTPError("Rate limit exceeded (HTTP 429)", response=resp)

        resp.raise_for_status()
        return resp.json()
