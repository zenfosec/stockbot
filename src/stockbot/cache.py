"""SQLite-backed cache for fetched fundamentals."""

from __future__ import annotations

import json
import sqlite3
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Iterable


DEFAULT_DIR = Path.home() / ".stockbot"
DEFAULT_DB = DEFAULT_DIR / "cache.db"
DEFAULT_TTL = timedelta(hours=24)


class Cache:
    """Key/value cache of ticker -> fundamentals dict, with TTL."""

    def __init__(self, path: Path = DEFAULT_DB, ttl: timedelta = DEFAULT_TTL):
        self.path = Path(path)
        self.ttl = ttl
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(self.path)
        self._conn.execute(
            """
            CREATE TABLE IF NOT EXISTS fundamentals (
                ticker TEXT PRIMARY KEY,
                fetched_at REAL NOT NULL,
                data TEXT NOT NULL
            )
            """
        )
        self._conn.commit()

    def get(self, ticker: str) -> dict | None:
        """Return cached data if present and fresh; else None."""
        cutoff = time.time() - self.ttl.total_seconds()
        row = self._conn.execute(
            "SELECT data, fetched_at FROM fundamentals WHERE ticker = ? AND fetched_at >= ?",
            (ticker, cutoff),
        ).fetchone()
        if row is None:
            return None
        return json.loads(row[0])

    def get_many(self, tickers: Iterable[str]) -> dict[str, dict]:
        """Batch get. Returns only fresh entries."""
        tickers = list(tickers)
        if not tickers:
            return {}
        cutoff = time.time() - self.ttl.total_seconds()
        placeholders = ",".join("?" * len(tickers))
        rows = self._conn.execute(
            f"SELECT ticker, data FROM fundamentals "
            f"WHERE ticker IN ({placeholders}) AND fetched_at >= ?",
            (*tickers, cutoff),
        ).fetchall()
        return {t: json.loads(d) for t, d in rows}

    def put(self, ticker: str, data: dict) -> None:
        self._conn.execute(
            "INSERT OR REPLACE INTO fundamentals (ticker, fetched_at, data) VALUES (?, ?, ?)",
            (ticker, time.time(), json.dumps(data)),
        )
        self._conn.commit()

    def clear(self) -> int:
        cur = self._conn.execute("DELETE FROM fundamentals")
        self._conn.commit()
        return cur.rowcount

    def stats(self) -> dict:
        row = self._conn.execute(
            "SELECT COUNT(*), MIN(fetched_at), MAX(fetched_at) FROM fundamentals"
        ).fetchone()
        count, oldest, newest = row
        return {
            "path": str(self.path),
            "entries": count,
            "oldest": datetime.fromtimestamp(oldest).isoformat() if oldest else None,
            "newest": datetime.fromtimestamp(newest).isoformat() if newest else None,
            "ttl_hours": self.ttl.total_seconds() / 3600,
        }

    def close(self) -> None:
        self._conn.close()
