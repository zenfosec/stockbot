"""Fetch and cache the broader US stock universe.

Primary source is a daily-updated GitHub mirror of the NASDAQ Trader symbol
directory: https://github.com/rreichel3/US-Stock-Symbols. It publishes per-
exchange JSON files (NASDAQ, NYSE, AMEX).

Fallback source is the original NASDAQ Trader HTTP endpoint
(ftp.nasdaqtrader.com/dynamic/SymDir/*.txt). The FTP host is blocked on some
networks, which is why the GitHub mirror is the default.
"""

from __future__ import annotations

import csv
import io
import json
import time
from datetime import timedelta
from pathlib import Path

import requests

from .cache import DEFAULT_DIR


# Primary: daily GitHub mirror (works over regular HTTPS / any network).
MIRROR_URLS = {
    "nasdaq": "https://raw.githubusercontent.com/rreichel3/US-Stock-Symbols/main/nasdaq/nasdaq_full_tickers.json",
    "nyse": "https://raw.githubusercontent.com/rreichel3/US-Stock-Symbols/main/nyse/nyse_full_tickers.json",
    "amex": "https://raw.githubusercontent.com/rreichel3/US-Stock-Symbols/main/amex/amex_full_tickers.json",
}

# Fallback: original NASDAQ Trader pipe-delimited files.
NASDAQ_TRADER_URLS = {
    "nasdaq": "https://ftp.nasdaqtrader.com/dynamic/SymDir/nasdaqlisted.txt",
    "other": "https://ftp.nasdaqtrader.com/dynamic/SymDir/otherlisted.txt",
}

UNIVERSE_FILE = DEFAULT_DIR / "universe.csv"
UNIVERSE_TTL = timedelta(hours=24)
TIMEOUT_SECS = 30


def _clean_symbol(sym: str) -> str | None:
    """Normalize a symbol. Returns None if it should be skipped."""
    sym = (sym or "").strip().upper()
    if not sym:
        return None
    # Skip warrants / units / preferred shares / when-issued etc.
    if any(c in sym for c in "$.^"):
        return None
    return sym


def _fetch_mirror() -> list[dict]:
    """Pull from the GitHub mirror (primary source)."""
    rows: list[dict] = []
    seen: set[str] = set()
    for exchange, url in MIRROR_URLS.items():
        resp = requests.get(url, timeout=TIMEOUT_SECS)
        resp.raise_for_status()
        for entry in resp.json():
            sym = _clean_symbol(entry.get("symbol", ""))
            if not sym or sym in seen:
                continue
            seen.add(sym)
            rows.append({"symbol": sym, "name": (entry.get("name") or "").strip()})
    return rows


def _parse_trader(
    text: str, symbol_col: str, name_col: str, etf_col: str, test_col: str
) -> list[dict]:
    reader = csv.DictReader(io.StringIO(text), delimiter="|")
    rows: list[dict] = []
    for r in reader:
        sym = _clean_symbol(r.get(symbol_col) or "")
        if not sym or sym.startswith("FILE CREATION TIME"):
            continue
        if (r.get(test_col) or "").strip().upper() == "Y":
            continue
        if (r.get(etf_col) or "").strip().upper() == "Y":
            continue
        rows.append({"symbol": sym, "name": (r.get(name_col) or "").strip()})
    return rows


def _fetch_nasdaq_trader() -> list[dict]:
    """Fallback: pull directly from ftp.nasdaqtrader.com."""
    nasdaq = requests.get(NASDAQ_TRADER_URLS["nasdaq"], timeout=TIMEOUT_SECS)
    nasdaq.raise_for_status()
    other = requests.get(NASDAQ_TRADER_URLS["other"], timeout=TIMEOUT_SECS)
    other.raise_for_status()

    rows = _parse_trader(
        nasdaq.text,
        symbol_col="Symbol",
        name_col="Security Name",
        etf_col="ETF",
        test_col="Test Issue",
    )
    rows += _parse_trader(
        other.text,
        symbol_col="ACT Symbol",
        name_col="Security Name",
        etf_col="ETF",
        test_col="Test Issue",
    )
    seen: set[str] = set()
    out: list[dict] = []
    for r in rows:
        if r["symbol"] in seen:
            continue
        seen.add(r["symbol"])
        out.append(r)
    return out


def _download_all() -> list[dict]:
    """Try mirror first, then NASDAQ Trader. Raise if both fail."""
    errors: list[str] = []
    for name, fn in (("github-mirror", _fetch_mirror), ("nasdaq-trader", _fetch_nasdaq_trader)):
        try:
            rows = fn()
            if rows:
                return rows
            errors.append(f"{name}: returned no rows")
        except Exception as e:
            errors.append(f"{name}: {e.__class__.__name__}: {e}")
    raise RuntimeError(
        "Could not fetch ticker universe from any source:\n  - "
        + "\n  - ".join(errors)
    )


def _write(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["symbol", "name"])
        w.writeheader()
        w.writerows(rows)


def _read(path: Path) -> list[dict]:
    with path.open() as f:
        return list(csv.DictReader(f))


def load(refresh: bool = False, path: Path = UNIVERSE_FILE) -> list[dict]:
    """Return the cached universe, refreshing from the network if stale.

    Each entry is a dict: {"symbol": ..., "name": ...}.
    """
    fresh = (
        path.exists()
        and (time.time() - path.stat().st_mtime) < UNIVERSE_TTL.total_seconds()
    )
    if refresh or not fresh:
        rows = _download_all()
        _write(path, rows)
        return rows
    return _read(path)


def refresh(path: Path = UNIVERSE_FILE) -> list[dict]:
    """Force a re-download of the universe."""
    return load(refresh=True, path=path)
