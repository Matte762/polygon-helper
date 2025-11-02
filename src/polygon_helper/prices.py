from __future__ import annotations
from typing import Literal, Optional, Dict, Any, List
from datetime import datetime
import pandas as pd
from dateutil import parser as dateparser

from .client import PolygonClient

Timespan = Literal["minute", "hour", "day", "week", "month", "quarter", "year"]

def _normalize_results(results: List[Dict[str, Any]]) -> pd.DataFrame:
    if not results:
        return pd.DataFrame(columns=["open","high","low","close","volume","vwap","transactions"])
    df = pd.DataFrame(results)
    # Polygon aggregate result keys: t (timestamp ms), o,h,l,c, v, vw, n
    rename = {"t":"timestamp", "o":"open", "h":"high", "l":"low", "c":"close", "v":"volume", "vw":"vwap", "n":"transactions"}
    df = df.rename(columns=rename)
    # ms to ns to avoid timezone rounding issues
    df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms", utc=True).dt.tz_convert(None)
    df = df.set_index("timestamp").sort_index()
    # ensure column order
    cols = ["open","high","low","close","volume","vwap","transactions"]
    for col in cols:
        if col not in df.columns:
            df[col] = pd.NA
    return df[cols]

def get_price_series(
    ticker: str,
    start: str | datetime,
    end: str | datetime,
    *,
    timespan: Timespan = "day",
    multiplier: int = 1,
    adjusted: bool = True,
    sort: Literal["asc","desc"] = "asc",
    limit: int = 50000,
    client: Optional[PolygonClient] = None,
) -> pd.DataFrame:
    """
    Fetch OHLCV aggregate bars from Polygon and return a pandas DataFrame indexed by timestamp.

    Args:
        ticker: e.g. "AAPL"
        start: ISO string or datetime (inclusive)
        end: ISO string or datetime (inclusive)
        timespan: 'minute'|'hour'|'day'|...
        multiplier: bar size multiplier (e.g., 1 day, 5 minute)
        adjusted: include dividend/split adjustments
        sort: 'asc' or 'desc'
        limit: per-page limit (Polygon may cap this)
        client: optional injected PolygonClient for testing

    Returns:
        DataFrame with columns: open, high, low, close, volume, vwap, transactions
    """
    c = client or PolygonClient()

    # normalize dates to ISO (YYYY-MM-DD or RFC3339 is fine; Polygon accepts both)
    from dateutil import parser as dateparser

    def _to_date_only(x):
        dt = dateparser.parse(str(x))
        return dt.date().isoformat()  # YYYY-MM-DD


    start_iso = _to_date_only(start)
    end_iso   = _to_date_only(end)

    path = f"/v2/aggs/ticker/{ticker.upper()}/range/{multiplier}/{timespan}/{start_iso}/{end_iso}"
    params: Dict[str, Any] = {
        "adjusted": "true" if adjusted else "false",
        "sort": sort,
        "limit": limit,
    }

    all_results: List[Dict[str, Any]] = []
    data = c.get(path, params=params)
    # Polygon v2 aggregates return shape: { "results": [...], "next_url": "...", "queryCount":..., "resultsCount":..., "ticker":... }
    all_results.extend(data.get("results", []))

    next_url = data.get("next_url")  # may be absent when no pagination is needed
    # Follow next_url if provided. It is a fully-qualified URL that already contains the apiKey.
    while next_url:
        # Use client's get so we keep retries; pass no params because next_url has them baked in.
        data = c.get(next_url, params=None)
        all_results.extend(data.get("results", []))
        next_url = data.get("next_url")

    return _normalize_results(all_results)


if __name__ == "__main__":
    import os, argparse
    from .client import PolygonClient

    parser = argparse.ArgumentParser(description="Fetch a Polygon price time series.")
    parser.add_argument("--ticker", required=True)
    parser.add_argument("--start", required=True)
    parser.add_argument("--end", required=True)
    parser.add_argument("--timespan", default="day")
    parser.add_argument("--multiplier", type=int, default=1)
    parser.add_argument("--adjusted", type=lambda x: str(x).lower() != "false", default=True)
    parser.add_argument("--api-key")  # ← add this
    args = parser.parse_args()

    api_key = args.api_key or os.getenv("POLYGON_API_KEY")
    if not api_key:
        raise SystemExit("No API key provided. Use --api-key or set POLYGON_API_KEY")

    client = PolygonClient(api_key=api_key)

    df = get_price_series(
        ticker=args.ticker,
        start=args.start,
        end=args.end,
        timespan=args.timespan,
        multiplier=args.multiplier,
        adjusted=args.adjusted,
        client=client,
    )
    print(f"Rows: {len(df)}")
    print(df.head().to_string())
