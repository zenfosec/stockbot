# stockbot

A simple CLI stock screener backed by [yfinance](https://github.com/ranaroussi/yfinance).
Filter the broader US market by valuation, profitability, growth, and dividend metrics.

## Install

```bash
pip install -e .
```

Python 3.10+.

## Quick start

```bash
# First-time setup: download the ticker universe (NASDAQ + NYSE + AMEX).
stockbot universe refresh

# Screen your own short list.
stockbot screen --tickers AAPL,MSFT,GOOG,NVDA,AMZN --max-pe 40

# A value-ish screen on the full US market (first run fills cache; later runs are fast).
stockbot screen \
    --min-market-cap 1e9 \
    --max-pe 20 \
    --min-roe 0.15 \
    --min-revenue-growth 0.05 \
    --sort-by trailing_pe \
    --top 25

# A dividend-income screen.
stockbot screen \
    --min-dividend-yield 0.03 \
    --max-payout-ratio 0.70 \
    --min-market-cap 5e9 \
    --sort-by dividend_yield --desc \
    --top 25
```

### Useful flags

Valuation: `--max-pe`, `--min-pe`, `--max-forward-pe`, `--max-pb`, `--max-peg`
Profitability: `--min-roe`, `--min-profit-margin`, `--min-operating-margin`
Growth: `--min-revenue-growth`, `--min-earnings-growth`
Dividends: `--min-dividend-yield`, `--max-payout-ratio`
Size: `--min-market-cap`, `--max-market-cap`
Other: `--sector`, `--sort-by`, `--desc`, `--top`, `--export results.csv`
Fetch:  `--refresh`, `--cache-only`, `--workers N`

All growth/margin/yield flags take decimals (e.g. `0.15` = 15%).

### Offline / cache-only screens

Once the cache is warm, run any screen without touching the network:

```bash
stockbot screen --cache-only --max-pe 20 --min-roe 0.15 --top 20
```

Useful for demos, experimentation, and working around Yahoo rate limits.

### Cache

Fundamentals are cached in `~/.stockbot/cache.db` for 24 hours to avoid
re-hitting yfinance on every run. The ticker universe is cached in
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

- yfinance is an unofficial Yahoo Finance scraper. Fields sometimes go missing
  or rename. Filters treat missing data as non-matching, so a ticker with no
  reported ROE will never pass `--min-roe`.
- The first full-universe run fetches ~6–8k tickers and can take ~5–10 minutes.
  Subsequent runs use the cache and complete in seconds.
- **Rate limits.** Yahoo aggressively blocks high-volume scraping with
  HTTP 401 "Invalid Crumb" errors. If you start seeing them, you're temporarily
  rate-limited (usually 15–60 min). Mitigations: lower `--workers` (try `5`),
  narrow the universe with `--limit` or `--tickers` while iterating, and use
  `--cache-only` for repeated screens over the same data.
- This tool is for research only. Do your own due diligence; none of its output
  is investment advice.
