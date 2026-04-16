"""Parallel yfinance fetch with cache write-through."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Iterable

import pandas as pd
import yfinance as yf
from rich.progress import (
    BarColumn,
    MofNCompleteColumn,
    Progress,
    TextColumn,
    TimeElapsedColumn,
    TimeRemainingColumn,
)

from .cache import Cache


# Map of our flat metric names -> yfinance .info keys.
# yfinance occasionally renames keys across versions; we try a few aliases.
METRIC_KEYS: dict[str, tuple[str, ...]] = {
    "name": ("shortName", "longName"),
    "sector": ("sector",),
    "industry": ("industry",),
    "price": ("currentPrice", "regularMarketPrice"),
    "market_cap": ("marketCap",),
    "trailing_pe": ("trailingPE",),
    "forward_pe": ("forwardPE",),
    "price_to_book": ("priceToBook",),
    "peg_ratio": ("pegRatio", "trailingPegRatio"),
    "return_on_equity": ("returnOnEquity",),
    "profit_margin": ("profitMargins",),
    "operating_margin": ("operatingMargins",),
    "revenue_growth": ("revenueGrowth",),
    "earnings_growth": ("earningsGrowth",),
    "dividend_yield": ("dividendYield",),
    "payout_ratio": ("payoutRatio",),
}

# Metrics that must be numeric when present. yfinance occasionally returns a
# string (e.g. "Infinity" or stale formatted values); we coerce to float or None
# so downstream comparisons in screen.py don't blow up.
NUMERIC_METRICS = {
    "price",
    "market_cap",
    "trailing_pe",
    "forward_pe",
    "price_to_book",
    "peg_ratio",
    "return_on_equity",
    "profit_margin",
    "operating_margin",
    "revenue_growth",
    "earnings_growth",
    "dividend_yield",
    "payout_ratio",
}


def _coerce_number(v):
    if v is None:
        return None
    if isinstance(v, bool):  # bool is a subclass of int in python
        return None
    if isinstance(v, (int, float)):
        import math
        if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
            return None
        return float(v)
    try:
        f = float(v)
        import math
        return None if (math.isnan(f) or math.isinf(f)) else f
    except (TypeError, ValueError):
        return None


def _normalize(info: dict) -> dict:
    """Flatten a yfinance .info dict into our canonical metric names."""
    out: dict = {}
    for metric, keys in METRIC_KEYS.items():
        for k in keys:
            if k in info and info[k] is not None:
                out[metric] = info[k]
                break
        else:
            out[metric] = None
    for k in NUMERIC_METRICS:
        out[k] = _coerce_number(out.get(k))
    return out


def _fetch_one(ticker: str) -> dict | None:
    """Fetch + normalize one ticker. Returns None on failure."""
    try:
        info = yf.Ticker(ticker).info or {}
    except Exception:
        return None
    # yfinance returns a stub dict for delisted/invalid tickers; skip empties.
    if not info.get("symbol") and not info.get("shortName") and not info.get("longName"):
        return None
    return _normalize(info)


def fetch(
    tickers: Iterable[str],
    cache: Cache,
    workers: int = 20,
    refresh: bool = False,
    cache_only: bool = False,
    show_progress: bool = True,
) -> pd.DataFrame:
    """Fetch normalized fundamentals for all tickers, using the cache.

    If cache_only is True, the network is never touched: only tickers already
    present in the cache are returned.

    Returns a DataFrame indexed by ticker, with one column per metric.
    """
    tickers = list(dict.fromkeys(t.upper().strip() for t in tickers if t))

    # Pull anything already cached, unless we're forcing a refresh.
    cached: dict[str, dict] = {} if (refresh and not cache_only) else cache.get_many(tickers)
    missing = [] if cache_only else [t for t in tickers if t not in cached]

    results: dict[str, dict] = dict(cached)

    if missing:
        progress_cls = Progress if show_progress else _NoopProgress
        with progress_cls(
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            MofNCompleteColumn(),
            TimeElapsedColumn(),
            TimeRemainingColumn(),
        ) as progress:
            task = progress.add_task(
                f"Fetching {len(missing)} tickers (of {len(tickers)} total)",
                total=len(missing),
            )
            with ThreadPoolExecutor(max_workers=workers) as pool:
                futures = {pool.submit(_fetch_one, t): t for t in missing}
                for fut in as_completed(futures):
                    t = futures[fut]
                    data = fut.result()
                    if data is not None:
                        cache.put(t, data)
                        results[t] = data
                    progress.advance(task)

    if not results:
        return pd.DataFrame()

    df = pd.DataFrame.from_dict(results, orient="index")
    df.index.name = "ticker"
    # Preserve input order where possible.
    df = df.reindex([t for t in tickers if t in results])
    return df


class _NoopProgress:
    """Drop-in for rich.Progress when progress is disabled (tests, piping)."""

    def __init__(self, *a, **kw):
        pass

    def __enter__(self):
        return self

    def __exit__(self, *a):
        pass

    def add_task(self, *a, **kw):
        return 0

    def advance(self, *a, **kw):
        pass
