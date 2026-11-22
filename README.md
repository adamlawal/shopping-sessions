# Shopping Sessions (Builders Buildathon, Databricks track)

A native Rust pipeline that turns the click-stream CSVs in `/data` into the
`user_daily` and `campaign_daily` Parquet tables defined in `rules.md`.

- Memory-maps every input file and parses it in parallel with a hand-written
  CSV parser that follows the rules' dialect exactly.
- Union-find over `links/` to name each person by their byte-wise smallest ID.
- Hash-partitioned dedup on `event_id`, then repartition by person to
  sessionize, credit and count assists in one pass per person.

## Build and test

    docker buildx build --platform linux/amd64 --load -t shopping-sessions:latest .
    python3 scripts/run_fixtures.py fixtures -- scripts/docker_run.sh

## Open-source libraries used

rayon, memmap2, memchr, ahash, parquet (Apache Arrow Rust), bytes, libc, mimalloc. Test scripts and
fixtures come from the organizers' starter.

## License

MIT
