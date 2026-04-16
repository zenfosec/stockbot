"""Click-based CLI for stockbot."""

from __future__ import annotations

import sys
from pathlib import Path

import click
import pandas as pd
from rich.console import Console
from rich.table import Table

from . import __version__, cache as cache_mod, fetch as fetch_mod, screen, universe


console = Console()


# Columns we always show (identity), plus the filterable metric set.
IDENTITY_COLS = ["name", "sector", "industry", "price", "market_cap"]
METRIC_COLS = [
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
]


def _format_cell(col: str, val) -> str:
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return "-"
    # Percent-style metrics from yfinance are already decimals (0.15 = 15%).
    percent_cols = {
        "return_on_equity",
        "profit_margin",
        "operating_margin",
        "revenue_growth",
        "earnings_growth",
        "dividend_yield",
        "payout_ratio",
    }
    if col in percent_cols and isinstance(val, (int, float)):
        return f"{val * 100:.1f}%"
    if col == "market_cap" and isinstance(val, (int, float)):
        for unit, div in (("T", 1e12), ("B", 1e9), ("M", 1e6)):
            if abs(val) >= div:
                return f"{val / div:.2f}{unit}"
        return f"{val:.0f}"
    if col == "price" and isinstance(val, (int, float)):
        return f"${val:,.2f}"
    if isinstance(val, float):
        return f"{val:.2f}"
    return str(val)


def _render_table(df: pd.DataFrame, columns: list[str]) -> Table:
    table = Table(show_lines=False, header_style="bold cyan")
    table.add_column("ticker", style="bold")
    for c in columns:
        table.add_column(c)
    for ticker, row in df.iterrows():
        table.add_row(str(ticker), *(_format_cell(c, row.get(c)) for c in columns))
    return table


@click.group()
@click.version_option(__version__, prog_name="stockbot")
def cli() -> None:
    """Simple CLI stock screener backed by yfinance."""


# ---------------------------------------------------------------------------
# screen
# ---------------------------------------------------------------------------


@cli.command()
# Universe selection.
@click.option(
    "--tickers",
    default=None,
    help="Comma-separated list of tickers to screen instead of the full US universe.",
)
@click.option(
    "--limit",
    type=int,
    default=None,
    help="Limit the universe to the first N tickers (useful for quick tests).",
)
# Valuation filters.
@click.option("--max-pe", type=float, default=None, help="Max trailing P/E.")
@click.option("--min-pe", type=float, default=None, help="Min trailing P/E.")
@click.option("--max-forward-pe", type=float, default=None, help="Max forward P/E.")
@click.option("--max-pb", type=float, default=None, help="Max price-to-book.")
@click.option("--max-peg", type=float, default=None, help="Max PEG ratio.")
# Profitability filters.
@click.option("--min-roe", type=float, default=None, help="Min return on equity (e.g. 0.15 for 15%).")
@click.option("--min-profit-margin", type=float, default=None, help="Min profit margin.")
@click.option("--min-operating-margin", type=float, default=None, help="Min operating margin.")
# Growth filters.
@click.option("--min-revenue-growth", type=float, default=None, help="Min revenue growth YoY.")
@click.option("--min-earnings-growth", type=float, default=None, help="Min earnings growth YoY.")
# Dividend filters.
@click.option("--min-dividend-yield", type=float, default=None, help="Min dividend yield.")
@click.option("--max-payout-ratio", type=float, default=None, help="Max dividend payout ratio.")
# Size filter.
@click.option("--min-market-cap", type=float, default=None, help="Min market cap in dollars.")
@click.option("--max-market-cap", type=float, default=None, help="Max market cap in dollars.")
# Sector filter.
@click.option("--sector", default=None, help="Restrict to a single sector (case-insensitive).")
# Output.
@click.option("--sort-by", default="trailing_pe", help="Column to sort by.")
@click.option("--desc", is_flag=True, help="Sort descending instead of ascending.")
@click.option("--top", type=int, default=50, help="Show at most N results.")
@click.option("--export", type=click.Path(dir_okay=False), default=None, help="Write results to a CSV file.")
# Data fetch behaviour.
@click.option("--refresh", is_flag=True, help="Ignore cache and re-fetch all tickers.")
@click.option(
    "--cache-only",
    is_flag=True,
    help="Never hit the network. Screen only over tickers already in the cache.",
)
@click.option("--workers", type=int, default=20, help="Parallel fetch workers.")
def screen_cmd(
    tickers,
    limit,
    max_pe,
    min_pe,
    max_forward_pe,
    max_pb,
    max_peg,
    min_roe,
    min_profit_margin,
    min_operating_margin,
    min_revenue_growth,
    min_earnings_growth,
    min_dividend_yield,
    max_payout_ratio,
    min_market_cap,
    max_market_cap,
    sector,
    sort_by,
    desc,
    top,
    export,
    refresh,
    cache_only,
    workers,
):
    """Run a screen across the broader US market (or a given ticker list)."""

    # 1. Pick universe.
    if tickers:
        symbols = [t.strip().upper() for t in tickers.split(",") if t.strip()]
        console.print(f"[dim]Screening {len(symbols)} user-supplied tickers.[/dim]")
    else:
        rows = universe.load()
        symbols = [r["symbol"] for r in rows]
        if limit:
            import random
            random.shuffle(symbols)
            symbols = symbols[:limit]
        console.print(f"[dim]Screening {len(symbols)} tickers from US universe.[/dim]")

    # 2. Fetch fundamentals (cache-aware).
    cache = cache_mod.Cache()
    try:
        df = fetch_mod.fetch(
            symbols,
            cache=cache,
            workers=workers,
            refresh=refresh,
            cache_only=cache_only,
        )
    finally:
        cache.close()

    if df.empty:
        msg = "No cached fundamentals found." if cache_only else "No fundamentals returned."
        console.print(f"[yellow]{msg}[/yellow]")
        sys.exit(1)

    if cache_only:
        console.print(
            f"[dim]Cache-only: screening {len(df)} tickers with cached data "
            f"(skipped {len(symbols) - len(df)} uncached).[/dim]"
        )

    # 3. Build filter list.
    filters: list[screen.Filter] = []
    def add(col, op, value):
        if value is not None:
            filters.append(screen.Filter(col, op, value))

    add("trailing_pe", "max", max_pe)
    add("trailing_pe", "min", min_pe)
    add("forward_pe", "max", max_forward_pe)
    add("price_to_book", "max", max_pb)
    add("peg_ratio", "max", max_peg)
    add("return_on_equity", "min", min_roe)
    add("profit_margin", "min", min_profit_margin)
    add("operating_margin", "min", min_operating_margin)
    add("revenue_growth", "min", min_revenue_growth)
    add("earnings_growth", "min", min_earnings_growth)
    add("dividend_yield", "min", min_dividend_yield)
    add("payout_ratio", "max", max_payout_ratio)
    add("market_cap", "min", min_market_cap)
    add("market_cap", "max", max_market_cap)
    if sector:
        filters.append(screen.Filter("sector", "eq", sector))

    # 4. Apply filters, sort, top-N.
    result = screen.apply_filters(df, filters)
    result = screen.sort_and_top(result, sort_by=sort_by, ascending=not desc, top=top)

    if result.empty:
        console.print("[yellow]No matches. Try loosening filters.[/yellow]")
        sys.exit(0)

    # 5. Output.
    # Pick columns: identity + any filtered columns + the sort column.
    filtered_cols = [f.column for f in filters]
    extra = [c for c in filtered_cols if c not in IDENTITY_COLS]
    if sort_by and sort_by not in IDENTITY_COLS and sort_by not in extra:
        extra.append(sort_by)
    display_cols = [c for c in IDENTITY_COLS + extra if c in result.columns]

    console.print(f"[bold green]{len(result)} matches[/bold green] (showing top {min(len(result), top)})")
    console.print(_render_table(result[display_cols], display_cols))

    if export:
        result.to_csv(Path(export))
        console.print(f"[dim]Wrote {export}[/dim]")


# ---------------------------------------------------------------------------
# universe
# ---------------------------------------------------------------------------


@cli.group()
def universe_cmd() -> None:
    """Manage the ticker universe."""


@universe_cmd.command("refresh")
def universe_refresh() -> None:
    """Force re-download of the NASDAQ Trader ticker lists."""
    rows = universe.refresh()
    console.print(f"[green]Loaded {len(rows)} tickers.[/green]")


@universe_cmd.command("count")
def universe_count() -> None:
    """Show the current universe size (loads or uses cache)."""
    rows = universe.load()
    console.print(f"{len(rows)} tickers in universe.")


# ---------------------------------------------------------------------------
# cache
# ---------------------------------------------------------------------------


@cli.group()
def cache_cmd() -> None:
    """Manage the fundamentals cache."""


@cache_cmd.command("stats")
def cache_stats() -> None:
    """Show cache location, entry count, and age."""
    c = cache_mod.Cache()
    try:
        for k, v in c.stats().items():
            console.print(f"  {k}: {v}")
    finally:
        c.close()


@cache_cmd.command("clear")
def cache_clear() -> None:
    """Delete all cached fundamentals."""
    c = cache_mod.Cache()
    try:
        n = c.clear()
    finally:
        c.close()
    console.print(f"[green]Cleared {n} entries.[/green]")


# Register renamed commands under their user-facing names.
cli.add_command(screen_cmd, name="screen")
cli.add_command(universe_cmd, name="universe")
cli.add_command(cache_cmd, name="cache")


if __name__ == "__main__":
    cli()
