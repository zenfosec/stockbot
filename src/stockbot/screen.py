"""Pure filter / sort / top-N logic. No I/O, no network."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

import pandas as pd


Op = Literal["min", "max", "eq"]


@dataclass(frozen=True)
class Filter:
    """One threshold constraint against a metric column.

    op="min" keeps rows where column >= value.
    op="max" keeps rows where column <= value.
    op="eq"  keeps rows where column == value (case-insensitive for strings).

    Null values are always treated as non-matching (conservative default).
    """

    column: str
    op: Op
    value: object

    def apply(self, df: pd.DataFrame) -> pd.Series:
        if self.column not in df.columns:
            # Missing column -> no row matches.
            return pd.Series(False, index=df.index)
        col = df[self.column]
        if self.op == "min":
            num = pd.to_numeric(col, errors="coerce")
            return num.notna() & (num >= self.value)
        if self.op == "max":
            num = pd.to_numeric(col, errors="coerce")
            return num.notna() & (num <= self.value)
        if self.op == "eq":
            if isinstance(self.value, str):
                return col.notna() & (col.astype(str).str.casefold() == self.value.casefold())
            return col.notna() & (col == self.value)
        raise ValueError(f"Unknown op: {self.op}")


def apply_filters(df: pd.DataFrame, filters: list[Filter]) -> pd.DataFrame:
    """Return the subset of rows matching every filter."""
    if df.empty or not filters:
        return df
    mask = pd.Series(True, index=df.index)
    for f in filters:
        mask &= f.apply(df)
    return df[mask]


def sort_and_top(
    df: pd.DataFrame,
    sort_by: str | None,
    ascending: bool = True,
    top: int | None = None,
) -> pd.DataFrame:
    """Sort by column (nulls last) and optionally take top-N rows."""
    if sort_by and sort_by in df.columns:
        df = df.sort_values(sort_by, ascending=ascending, na_position="last")
    if top is not None and top > 0:
        df = df.head(top)
    return df
