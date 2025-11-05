from typing import Dict, Any, Optional, List, Literal
from datetime import datetime
import pandas as pd
from dateutil import parser as dateparser

from ..client import PolygonClient

Timespan = Literal["minute", "hour", "day", "week", "month", "quarter", "year"]

def _normalize_results(results: List[Dict[str, Any]]) -> pd.DataFrame:
    if not results:
        return pd.DataFrame(columns=["open","high","low","close","volume","vwap","transactions"])
    df = pd.DataFrame(results).rename(columns={
        "t":"timestamp", "o":"open", "h":"high", "l":"low",
        "c":"close", "v":"volume", "vw":"vwap", "n":"transactions"
    })
    df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms", utc=True).dt.tz_convert(None)
    df = df.set_index("timestamp").sort_index()
    cols = ["open","high","low","close","volume","vwap","transactions"]
    for col in cols:
        if col not in df.columns:
            df[col] = pd.NA
    return df[cols]

def get_price_series(
    client: PolygonClient,
    ticker: str,
    start: str | datetime,
    end: str | datetime,
    *,
    timespan: Timespan = "day",
    multiplier: int = 1,
    adjusted: bool = True,
    sort: Literal["asc","desc"] = "asc",
    limit: int = 50000,
) -> pd.DataFrame:
    """Fetch OHLCV bars and return a pandas DataFrame."""
    def _to_iso(x: str | datetime) -> str:
        dt = dateparser.parse(str(x))
        return dt.date().isoformat()

    path = f"/v2/aggs/ticker/{ticker.upper()}/range/{multiplier}/{timespan}/{_to_iso(start)}/{_to_iso(end)}"
    params: Dict[str, Any] = {
        "adjusted": "true" if adjusted else "false",
        "sort": sort,
        "limit": limit,
    }

    data = client.get(path, params=params)
    results = data.get("results", [])
    return _normalize_results(results)
