# stockbot

A simple CLI stock screener backed by [finviz](https://finviz.com) and
[yfinance](https://github.com/ranaroussi/yfinance). Filter the broader US
market by valuation, profitability, growth, and dividend metrics.

## Install

```bash
pip install -e .
```

Python 3.10+.

## Quick start

```bash
# Undervalued growth: profitable companies growing fast at a cheap price.
stockbot screen \
    --min-market-cap 2e9 \
    --max-pe 20 \
    --min-roe 0.20 \
    --min-profit-margin 0.10 \
    --sort-by trailing_pe \
    --top 25

# Dividend income: high yield, sustainable payout, quality earnings.
stockbot screen \
    --min-market-cap 5e9 \
    --min-dividend-yield 0.03 \
    --max-payout-ratio 0.70 \
    --min-roe 0.10 \
    --min-profit-margin 0.05 \
    --sort-by dividend_yield --desc \
    --top 25

# Quality compounders: high-margin, high-return businesses at any price.
stockbot screen \
    --min-market-cap 1e10 \
    --min-roe 0.25 \
    --min-profit-margin 0.20 \
    --min-operating-margin 0.20 \
    --sort-by return_on_equity --desc \
    --top 25

# Screen your own short list (uses yfinance for per-ticker detail).
stockbot screen --tickers AAPL,MSFT,GOOG,NVDA,AMZN --max-pe 40
```

## Data sources

stockbot supports two data sources, selectable with `--source`:

| Source | Default? | Speed | Coverage | Notes |
|--------|----------|-------|----------|-------|
| **finviz** | Yes | Fast (seconds) | Full US market | Bulk server-side filtering. Free, no API key. |
| **yfinance** | No | Slow (per-ticker) | Any ticker | Richer fundamentals. Throttled at 0.3s/request. |

- `stockbot screen ...` uses finviz by default. Filters are mapped to finviz's
  categorical buckets for server-side pre-filtering, then exact thresholds are
  applied locally.
- `stockbot screen --source yfinance ...` uses yfinance for the full universe.
  Slower but provides fields finviz doesn't have.
- `stockbot screen --tickers AAPL,MSFT ...` always uses yfinance (direct lookup).

### Useful flags

Valuation: `--max-pe`, `--min-pe`, `--max-forward-pe`, `--max-pb`, `--max-peg`
Profitability: `--min-roe`, `--min-profit-margin`, `--min-operating-margin`
Growth: `--min-revenue-growth`, `--min-earnings-growth`
Dividends: `--min-dividend-yield`, `--max-payout-ratio`
Size: `--min-market-cap`, `--max-market-cap`
Other: `--sector`, `--sort-by`, `--desc`, `--top`, `--export results.csv`
Fetch: `--source finviz|yfinance`, `--refresh`, `--cache-only`, `--workers N`

All growth/margin/yield flags take decimals (e.g. `0.15` = 15%).

### Offline / cache-only screens

All results are cached in SQLite. Once the cache is warm, screen without
touching the network:

```bash
stockbot screen --cache-only --max-pe 20 --min-roe 0.15 --top 20
```

### Cache management

Fundamentals are cached in `~/.stockbot/cache.db` for 24 hours. The ticker
universe (used only with `--source yfinance`) is cached in
`~/.stockbot/universe.csv` for 24 hours.

```bash
stockbot cache stats    # show entry count and age
stockbot cache clear    # wipe fundamentals cache
stockbot universe refresh  # force re-download of ticker list
```

Add `--refresh` to any `screen` invocation to bypass the cache.

## Tests

```bash
pip install -e '.[dev]'
pytest
```

## Notes

- **finviz** (default) returns bulk data in seconds. Filters are mapped to the
  closest inclusive finviz bucket, so results may include a few tickers just
  outside your exact threshold — the local filter pass removes them.
- **yfinance** is an unofficial Yahoo Finance scraper. Yahoo aggressively blocks
  high-volume scraping with HTTP 401 "Invalid Crumb" errors. Mitigations:
  lower `--workers` (try `5`), use `--tickers` for small sets, or stick with
  the finviz default.
- Filters treat missing data as non-matching: a ticker with no reported ROE
  will never pass `--min-roe`.
- This tool is for research only. Do your own due diligence; none of its output
  is investment advice.
