# Project context

## What this is

**Shopping Sessions** was a track in the Builders Buildathon (a Databricks-run
challenge). The task: take tens of millions of messy click-stream CSV events and
produce two exact daily Parquet tables — `user_daily` and `campaign_daily` —
following the rules in [`../rules.md`](../rules.md) precisely. Submissions ran as
a Docker container; the fastest **correct** run wins. Correctness is all-or-nothing:
the output must match the reference exactly, every edge case included.

- **Practice data:** ~20M events. **Final data:** ~60M events.
- **Scoring:** each run capped at 3 minutes; the score is the run time of the
  fastest correct submission.

The full specification is preserved in [`../rules.md`](../rules.md) — it is
self-contained, so the project does not depend on the challenge site staying up.

## How it went (the Rust run)

Built a native Rust pipeline over a weekend and iterated it hard (v1 → v14, then
an ingest rewrite and a final tuning pass). Highlights of the approach:

- Memory-mapped every input file; hand-written SIMD CSV tokenizer following the
  rules' exact dialect (`\r` is data, quotes wrap whole fields, ASCII digits only).
- Union-find over `links/` to resolve each person, named by their byte-wise
  smallest ID.
- Hash-partitioned dedup on `event_id` (keep smallest `ingest_ts`, before
  validation), then repartition by person to sessionize, credit clicks and count
  assists in one pass.
- `mimalloc` with large OS pages, ordered chunk scheduling, per-partition Parquet
  writes, lock-free person table.

**Results:**

- First submission scored ~3.98 s (rank ~244).
- Final tuned version reached ~1.1–1.2 s on the scorer (board calibration ≈
  codespace time ÷ 1.6).
- **Final placement: 146 / 400.** The prize cutoff (~$5,000) needed sub-0.4 s —
  reachable, but it takes deep systems work beyond where we landed.

The complete Rust source and its full commit history (v1 through the final shot)
are on the [`rust-pipeline`](https://github.com/adamlawal/shopping-sessions/tree/rust-pipeline)
branch. The grading harness, fixtures and spec on this branch are shared with it.

## The Python plan (this branch)

Rebuild the pipeline from scratch in Python — the goal here is **understanding**,
not raw speed. Suggested path:

1. **Pure Python on a small dataset first.** Implement every rule by hand so each
   edge case (quotes, the three timestamp forms, dedup-before-validation, linked
   IDs, session gaps, credit vs. assists, `top_category` ties) is understood.
   Get all 36 fixtures to `ok`.
2. **Then vectorise with Polars (or DuckDB).** Most stages become dataframe
   operations; the hard parts are matching the exact CSV dialect and sessionizing.
3. **Then profile and optimise**, re-running the correctness gate after every
   change — exactly the discipline that worked in Rust.

### The correctness gate

Both carry over unchanged from the Rust project:

- `scripts/run_fixtures.py fixtures -- python3 pipeline.py` — runs all 36
  edge-case fixtures and diffs each against the hand-written expected CSVs.
- `scripts/compare.py` — exact, order-insensitive diff of two output folders,
  used against the published dev answer on a generated dataset.

A generated dataset (via the organizers' `generate` binary) plus the dev answer
gives a full-scale correctness check beyond the fixtures.

## Easy-to-forget rules (bitten-by list)

- **Dedup comes before validation.** Keep the copy with the smallest `ingest_ts`;
  if that copy is invalid the event is dropped — a later valid copy does **not**
  rescue it.
- **`\r` is ordinary data**, part of the field it sits in (so `1000\r` is a bad
  price, `...Z\r` is a bad timestamp).
- **Three timestamp forms** (F1 `Z`, F2 `±HH:MM` offset, F3 epoch-ms digits);
  milliseconds imply F3; validate impossible calendar values and offset ranges.
- **Two windows:** events in `[window_start, window_end)`; clicks in
  `[window_start − 7d, window_end)`.
- **Strings never normalized** — exact, case-sensitive, untrimmed; a person is
  named by the byte-wise smallest of their linked IDs.
- **Sessions** split on a gap > 30 min; equal timestamps share a session; the day
  is the UTC date of the session's start (even if events cross midnight).
- **Credit** = latest click `≤ session_start` within 7 days (ties → smallest
  `click_id`), else `organic`. **Assists** = distinct campaigns with any
  qualifying click in the 7-day window; `organic` never assists.
- **`top_category`** ties → byte-wise smallest; `unknown` competes like any
  category; null when there are no purchases.
