"""Unit tests for stockbot.screen. No network, no yfinance."""

from __future__ import annotations

import pandas as pd
import pytest

from stockbot.screen import Filter, apply_filters, sort_and_top


@pytest.fixture
def df():
    return pd.DataFrame(
        {
            "trailing_pe": [10.0, 25.0, None, 15.0],
            "return_on_equity": [0.20, 0.05, 0.30, None],
            "dividend_yield": [0.03, 0.00, None, 0.01],
            "sector": ["Tech", "Tech", "Healthcare", "Financial"],
            "market_cap": [1e10, 5e9, 2e11, 8e9],
        },
        index=["AAA", "BBB", "CCC", "DDD"],
    )


def test_min_filter_excludes_nulls(df):
    out = apply_filters(df, [Filter("return_on_equity", "min", 0.10)])
    assert list(out.index) == ["AAA", "CCC"]


def test_max_filter_excludes_nulls(df):
    # Null trailing_pe row CCC must be excluded even though "null <= 20" is undefined.
    out = apply_filters(df, [Filter("trailing_pe", "max", 20.0)])
    assert list(out.index) == ["AAA", "DDD"]


def test_combined_filters_and_semantics(df):
    out = apply_filters(
        df,
        [
            Filter("trailing_pe", "max", 20.0),
            Filter("return_on_equity", "min", 0.10),
        ],
    )
    # AAA matches both; DDD has null ROE so excluded.
    assert list(out.index) == ["AAA"]


def test_eq_filter_case_insensitive(df):
    out = apply_filters(df, [Filter("sector", "eq", "tech")])
    assert set(out.index) == {"AAA", "BBB"}


def test_missing_column_yields_no_matches(df):
    out = apply_filters(df, [Filter("does_not_exist", "min", 0)])
    assert out.empty


def test_empty_filters_returns_all(df):
    out = apply_filters(df, [])
    assert list(out.index) == list(df.index)


def test_sort_ascending_nulls_last(df):
    out = sort_and_top(df, sort_by="trailing_pe", ascending=True)
    # Nulls should come last (CCC has null trailing_pe).
    assert list(out.index) == ["AAA", "DDD", "BBB", "CCC"]


def test_sort_descending_and_top(df):
    out = sort_and_top(df, sort_by="market_cap", ascending=False, top=2)
    assert list(out.index) == ["CCC", "AAA"]


def test_top_zero_or_none_keeps_all(df):
    out = sort_and_top(df, sort_by=None, top=None)
    assert len(out) == len(df)
