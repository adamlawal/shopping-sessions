#!/usr/bin/env python3
"""Shopping Sessions pipeline.

Reads the raw CSVs under DATA_DIR and writes the user_daily and campaign_daily
Parquet tables under OUTPUT_DIR, following rules.md exactly.

Usage:
    pipeline.py [DATA_DIR] [OUTPUT_DIR]

DATA_DIR / OUTPUT_DIR may be passed as arguments or as environment variables
(arguments win). This file is a scaffold: the I/O contract and output writing
are done, and each processing stage below is a stub to fill in. As written it
emits empty (but schema-correct) tables, so the harness runs end to end:

    python3 scripts/run_fixtures.py fixtures -- python3 pipeline.py
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

import polars as pl

# Output schemas, straight from rules.md. `day` must be a Parquet DATE.
USER_DAILY_SCHEMA: dict[str, pl.DataType] = {
    "user_id": pl.String,
    "day": pl.Date,
    "sessions": pl.Int64,
    "events": pl.Int64,
    "revenue_cents": pl.Int64,
    "top_category": pl.String,
}
CAMPAIGN_DAILY_SCHEMA: dict[str, pl.DataType] = {
    "campaign_id": pl.String,
    "channel": pl.String,
    "day": pl.Date,
    "sessions": pl.Int64,
    "purchasing_sessions": pl.Int64,
    "revenue_cents": pl.Int64,
    "assisted_sessions": pl.Int64,
}


# --- stages (to implement) --------------------------------------------------
# Each returns plain Python / Polars data; keep them small and testable, and
# check each against the fixtures as you go. See the cited rules.md sections.

def read_window(data_dir: Path) -> tuple[int, int]:
    """manifest.json -> (window_start_ms, window_end_ms), UTC epoch milliseconds."""
    # TODO: parse DATA_DIR/manifest.json (window_start, window_end are F1 "...Z").
    raise NotImplementedError


def resolve_people(data_dir: Path) -> dict[str, str]:
    """links/ -> map every user_id to its person's name (byte-wise smallest ID).

    rules.md "Linked IDs": drop malformed/empty-side lines, union transitively,
    name each person by the byte-wise smallest of their IDs. IDs compare exactly.
    """
    # TODO: union-find over links/*.csv.
    raise NotImplementedError


def load_events(data_dir: Path, window: tuple[int, int]) -> object:
    """events/ -> surviving events, deduped and validated.

    rules.md "Events": parse (drop malformed / empty event_id); dedup on event_id
    keeping the smallest ingest_ts (BEFORE validation); validate the kept copy;
    drop late events (ingest_ts - ts > 24h).
    """
    # TODO.
    raise NotImplementedError


def load_clicks(data_dir: Path, window: tuple[int, int]) -> object:
    """clicks/ -> valid, deduped clicks.

    rules.md "Clicks": parse (drop malformed / empty click_id); dedup on click_id;
    validate (non-empty user, ts valid & in click window, campaign in campaigns.csv).
    """
    # TODO.
    raise NotImplementedError


def sessionize_and_aggregate(people, events, clicks, products, campaigns):
    """Sessionize per person, credit/assist clicks, aggregate the two tables.

    rules.md "Events" (sessionize, one-day-per-session, categories) and "Clicks"
    (credit = latest click <= session_start within 7d, ties -> smallest click_id,
    else organic; assists = distinct campaigns with a qualifying click).

    Returns (user_daily_rows, campaign_daily_rows).
    """
    # TODO.
    raise NotImplementedError


def run(data_dir: Path, out_dir: Path) -> None:
    """Wire the stages together. For now, emit empty tables."""
    # window = read_window(data_dir)
    # people = resolve_people(data_dir)
    # events = load_events(data_dir, window)
    # clicks = load_clicks(data_dir, window)
    # products = load_products(data_dir); campaigns = load_campaigns(data_dir)
    # user_rows, campaign_rows = sessionize_and_aggregate(...)
    user_daily = pl.DataFrame(schema=USER_DAILY_SCHEMA)
    campaign_daily = pl.DataFrame(schema=CAMPAIGN_DAILY_SCHEMA)
    write_table(out_dir, "user_daily", user_daily)
    write_table(out_dir, "campaign_daily", campaign_daily)


# --- output -----------------------------------------------------------------

def write_table(out_dir: Path, name: str, df: pl.DataFrame) -> None:
    """Write one table as a single Parquet file in its own folder."""
    folder = out_dir / name
    folder.mkdir(parents=True, exist_ok=True)
    df.write_parquet(folder / "part-0.parquet")


def main() -> None:
    args = sys.argv[1:]
    data_dir = Path(args[0] if len(args) > 0 else os.environ["DATA_DIR"])
    out_dir = Path(args[1] if len(args) > 1 else os.environ["OUTPUT_DIR"])
    run(data_dir, out_dir)


if __name__ == "__main__":
    main()
