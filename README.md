# Shopping Sessions

A data pipeline that turns the raw click-stream CSVs in `/data` into two daily
Parquet tables — `user_daily` and `campaign_daily` — exactly as specified in
[`rules.md`](rules.md).

This branch (`main`) is a clean, from-scratch **Python** build of that pipeline,
written to learn the problem properly: correctness first, then speed.

> The highly optimised **Rust** version from the Builders Buildathon lives on the
> [`rust-pipeline`](https://github.com/adamlawal/shopping-sessions/tree/rust-pipeline)
> branch. See [`docs/CONTEXT.md`](docs/CONTEXT.md) for the full backstory.

## The task, in one paragraph

Read messy event, click, link, product and campaign CSVs (duplicates, bad
timestamps, malformed rows, IDs that belong to the same person). Clean and
validate them per the rules, stitch each person's IDs together, split their
events into sessions, credit each session to a marketing campaign (or
`organic`), and aggregate into the two output tables. The rules in
[`rules.md`](rules.md) are exact — every edge case matters.

## Layout

| Path                     | What it is                                                        |
| ------------------------ | ----------------------------------------------------------------- |
| `rules.md`               | The specification. The single source of truth.                    |
| `pipeline.py`            | The pipeline. Reads `DATA_DIR`, writes the two tables to `OUTPUT_DIR`. |
| `fixtures/`              | 36 hand-written edge-case datasets, each with expected output.    |
| `scripts/run_fixtures.py`| Runs the pipeline on every fixture and diffs against expected.    |
| `scripts/compare.py`     | Exact, order-insensitive diff of two output folders.              |
| `scripts/docker_run.sh`  | Runs a container the way the official scorer does (for later).    |
| `docs/CONTEXT.md`        | Project history, results, and the plan for this Python build.     |

## Getting started

```sh
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
```

The pipeline is invoked as `pipeline.py DATA_DIR OUTPUT_DIR` (also read from the
`DATA_DIR` / `OUTPUT_DIR` env vars), and writes `user_daily/` and
`campaign_daily/` Parquet folders under `OUTPUT_DIR`.

### Run the correctness gate

```sh
python3 scripts/run_fixtures.py fixtures -- python3 pipeline.py
```

Every fixture should eventually print `ok`; the last line is `N/36 fixtures passed`.

### Run on a generated dataset

```sh
# make a 1M-event dev set with the organizers' generator (needs `generate` on PATH)
generate --seed 1 --events 1000000 --clicks 250000 --output dev

python3 pipeline.py dev out
python3 scripts/compare.py out dev-answer   # against the published dev answer
```

## License

MIT
