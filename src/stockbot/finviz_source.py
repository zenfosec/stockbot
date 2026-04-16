"""Bulk stock fundamentals via finviz (free, no API key).

Uses finvizfinance to scrape finviz.com's stock screener. Much faster than
fetching per-ticker from yfinance because finviz returns tabular data for many
stocks at once, and we can apply approximate server-side filters to reduce the
page count.
"""

from __future__ import annotations

import math
import re

import pandas as pd
from rich.console import Console

from .cache import Cache
from .screen import Filter

console = Console()

# ---------------------------------------------------------------------------
# Column indices for the finviz "Custom" screener.  These map 1-to-1 with the
# column picker at https://finviz.com/screener.ashx?v=152.
# ---------------------------------------------------------------------------
CUSTOM_COLUMNS = [
    1,   # Ticker
    2,   # Company
    3,   # Sector
    4,   # Industry
    6,   # Market Cap
    7,   # P/E
    8,   # Forward P/E
    9,   # PEG
    11,  # P/B
    14,  # Dividend
    15,  # Payout Ratio
    17,  # EPS This Y  (→ earnings growth)
    23,  # Sales Q/Q   (→ revenue growth)
    33,  # ROE
    40,  # Oper M
    41,  # Profit M
    65,  # Price
]

# Map finviz column names (as returned by finvizfinance) → our metric names.
COLUMN_MAP = {
    "Ticker": "_ticker",
    "Company": "name",
    "Sector": "sector",
    "Industry": "industry",
    "Market Cap": "market_cap",
    "P/E": "trailing_pe",
    "Forward P/E": "forward_pe",
    "PEG": "peg_ratio",
    "P/B": "price_to_book",
    "Dividend": "dividend_yield",
    "Payout Ratio": "payout_ratio",
    "EPS This Y": "earnings_growth",
    "Sales Q/Q": "revenue_growth",
    "ROE": "return_on_equity",
    "Oper M": "operating_margin",
    "Profit M": "profit_margin",
    "Price": "price",
}

# ---------------------------------------------------------------------------
# Finviz server-side filter mapping.
# We map our exact thresholds to the closest *inclusive* finviz bucket so we
# never accidentally exclude a valid result.  Our local filter logic then
# applies the exact threshold afterward.
# ---------------------------------------------------------------------------

# (metric, op) -> (finviz_key, list of (threshold, finviz_value) pairs)
# For "max" ops the list is ascending; we pick the smallest bucket >= our value.
# For "min" ops the list is ascending; we pick the largest bucket <= our value.

_PE_MAX = [(5, "Under 5"), (10, "Under 10"), (15, "Under 15"), (20, "Under 20"),
           (25, "Under 25"), (30, "Under 30"), (35, "Under 35"), (40, "Under 40"),
           (50, "Under 50")]
_PE_MIN = [(5, "Over 5"), (10, "Over 10"), (15, "Over 15"), (20, "Over 20"),
           (25, "Over 25"), (30, "Over 30"), (40, "Over 40"), (50, "Over 50")]

_ROE_MIN = [(0, "Positive (>0%)"), (5, "Over +5%"), (10, "Over +10%"),
            (15, "Over +15%"), (20, "Over +20%"), (25, "Over +25%"),
            (30, "Over +30%"), (35, "Over +35%"), (40, "Over +40%"),
            (45, "Over +45%"), (50, "Over +50%")]

_MARGIN_MIN = [(0, "Positive (>0%)"), (5, "Over 5%"), (10, "Over 10%"),
               (15, "Over 15%"), (20, "Over 20%"), (25, "Over 25%"),
               (30, "Over 30%"), (35, "Over 35%"), (40, "Over 40%"),
               (45, "Over 45%"), (50, "Over 50%")]

_DIV_MIN = [(1, "Over 1%"), (2, "Over 2%"), (3, "Over 3%"), (4, "Over 4%"),
            (5, "Over 5%"), (6, "Over 6%"), (7, "Over 7%"), (8, "Over 8%"),
            (9, "Over 9%"), (10, "Over 10%")]

_PEG_MAX = [(1, "Low (<1)"), (2, "Under 2"), (3, "Under 3")]

_PB_MAX = [(1, "Low (<1)"), (2, "Under 2"), (3, "Under 3"), (4, "Under 4"),
           (5, "Under 5"), (6, "Under 6"), (7, "Under 7"), (8, "Under 8"),
           (9, "Under 9"), (10, "Under 10")]

_MCAP_MIN = [(50e6, "+Micro (over $50mln)"), (300e6, "+Small (over $300mln)"),
             (2e9, "+Mid (over $2bln)"), (10e9, "+Large (over $10bln)")]

_MCAP_MAX = [(300e6, "-Micro (under $300mln)"), (2e9, "-Small (under $2bln)"),
             (10e9, "-Mid (under $10bln)"), (200e9, "-Large (under $200bln)")]

FILTER_MAPPING: dict[tuple[str, str], tuple[str, list]] = {
    ("trailing_pe", "max"): ("P/E", _PE_MAX),
    ("trailing_pe", "min"): ("P/E", _PE_MIN),
    ("forward_pe", "max"): ("Forward P/E", _PE_MAX),
    ("forward_pe", "min"): ("Forward P/E", _PE_MIN),
    ("peg_ratio", "max"): ("PEG", _PEG_MAX),
    ("price_to_book", "max"): ("P/B", _PB_MAX),
    ("return_on_equity", "min"): ("Return on Equity", _ROE_MIN),
    ("profit_margin", "min"): ("Net Profit Margin", _MARGIN_MIN),
    ("operating_margin", "min"): ("Operating Margin", _MARGIN_MIN),
    ("dividend_yield", "min"): ("Dividend Yield", _DIV_MIN),
    ("market_cap", "min"): ("Market Cap.", _MCAP_MIN),
    ("market_cap", "max"): ("Market Cap.", _MCAP_MAX),
}


def _pick_bucket(value: float, buckets: list[tuple], op: str) -> str | None:
    """Pick the closest inclusive finviz bucket for an exact threshold.

    For "max" filters (e.g. max-pe 18), pick the smallest bucket >= the value
    so we don't accidentally exclude valid results (Under 20 ⊇ ≤18).

    For "min" filters (e.g. min-roe 0.12), pick the largest bucket ≤ the value
    (Over 10% ⊇ ≥12%).
    """
    # For ROE/margin/dividend: our values are decimals (0.15 = 15%), but
    # finviz buckets are in percent (15).
    if op == "max":
        for threshold, label in buckets:
            if threshold >= value:
                return label
        return None  # value exceeds all buckets — don't apply server-side
    else:  # min
        best = None
        for threshold, label in buckets:
            if threshold <= value:
                best = label
        return best


def _build_finviz_filters(filters: list[Filter]) -> dict[str, str]:
    """Convert our Filter objects to a finviz filters_dict.

    Returns only the filters that have a finviz mapping.  Filters without a
    mapping (e.g. sector, payout_ratio max) are applied locally afterward.
    """
    result: dict[str, str] = {}
    for f in filters:
        # Convert decimal ratios to percent for ROE/margin/dividend
        value = f.value
        if f.column in ("return_on_equity", "profit_margin", "operating_margin",
                         "dividend_yield") and isinstance(value, (int, float)):
            value = value * 100

        key = (f.column, f.op)
        if key not in FILTER_MAPPING:
            continue
        finviz_key, buckets = FILTER_MAPPING[key]
        if finviz_key in result:
            continue  # already have a filter on this key
        label = _pick_bucket(value, buckets, f.op)
        if label:
            result[finviz_key] = label
    return result


# ---------------------------------------------------------------------------
# Value parsing — finviz returns strings like "1.5B", "15.2%", "-".
# ---------------------------------------------------------------------------

_SUFFIX_MULT = {"T": 1e12, "B": 1e9, "M": 1e6, "K": 1e3}


def _parse_number(val) -> float | None:
    """Parse a finviz cell value into a float or None."""
    if val is None:
        return None
    if isinstance(val, (int, float)):
        if math.isnan(val) or math.isinf(val):
            return None
        return float(val)
    s = str(val).strip()
    if not s or s == "-" or s.lower() == "nan":
        return None
    # Handle percentage: "15.2%" -> 0.152
    if s.endswith("%"):
        try:
            return float(s[:-1]) / 100
        except ValueError:
            return None
    # Handle suffix: "1.5B" -> 1_500_000_000
    if s and s[-1] in _SUFFIX_MULT:
        try:
            return float(s[:-1]) * _SUFFIX_MULT[s[-1]]
        except ValueError:
            return None
    try:
        return float(s.replace(",", ""))
    except ValueError:
        return None


def _normalize_df(df: pd.DataFrame) -> pd.DataFrame:
    """Rename finviz columns to our metric names and parse values."""
    # Rename columns we recognize.
    rename = {}
    for col in df.columns:
        mapped = COLUMN_MAP.get(col)
        if mapped:
            rename[col] = mapped
    df = df.rename(columns=rename)

    # Set ticker as index.
    if "_ticker" in df.columns:
        df = df.set_index("_ticker")
        df.index.name = "ticker"
    elif "Ticker" in df.columns:
        df = df.set_index("Ticker")
        df.index.name = "ticker"
    # Remove any "No." column if present.
    if "No." in df.columns:
        df = df.drop(columns=["No."])

    # Parse numeric columns.
    numeric_cols = [
        "trailing_pe", "forward_pe", "peg_ratio", "price_to_book",
        "return_on_equity", "profit_margin", "operating_margin",
        "revenue_growth", "earnings_growth",
        "dividend_yield", "payout_ratio",
        "market_cap", "price",
    ]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = df[col].apply(_parse_number)

    return df


# ---------------------------------------------------------------------------
# Main fetch entry point.
# ---------------------------------------------------------------------------

def fetch(
    filters: list[Filter],
    cache: Cache,
    show_progress: bool = True,
) -> pd.DataFrame:
    """Fetch fundamentals from finviz, applying server-side filters where
    possible.  Results are cached for future --cache-only runs.

    Returns a DataFrame indexed by ticker, with our standard metric columns.
    """
    from finvizfinance.screener.custom import Custom

    finviz_filters = _build_finviz_filters(filters)

    if finviz_filters:
        console.print(f"[dim]Applying {len(finviz_filters)} server-side filter(s) via finviz.[/dim]")
    else:
        console.print("[dim]Fetching full universe from finviz (no server-side filters).[/dim]")

    fcustom = Custom()
    fcustom.set_filter(filters_dict=finviz_filters)

    try:
        df = fcustom.screener_view(columns=CUSTOM_COLUMNS, verbose=0, limit=100000)
    except Exception as e:
        console.print(f"[red]finviz fetch failed: {e}[/red]")
        return pd.DataFrame()

    if df is None or df.empty:
        return pd.DataFrame()

    df = _normalize_df(df)
    console.print(f"[dim]finviz returned {len(df)} tickers.[/dim]")

    # Write through to cache so --cache-only works later.
    for ticker in df.index:
        row = df.loc[ticker]
        data = {k: v for k, v in row.to_dict().items()
                if v is not None and not (isinstance(v, float) and math.isnan(v))}
        cache.put(ticker, data)

    return df
