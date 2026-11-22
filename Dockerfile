# Shopping Sessions: native Rust pipeline.
FROM rust:1-slim-bookworm AS build
ARG TARGETARCH
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
RUN mkdir src && echo "fn main(){}" > src/main.rs && cargo build --release && rm -rf src
COPY src ./src
# x86-64-v3 = AVX2; the scorers are Intel Xeon Cascade Lake or newer.
RUN touch src/main.rs && \
    if [ "$TARGETARCH" = "amd64" ]; then export RUSTFLAGS="-C target-cpu=x86-64-v3"; fi && \
    cargo build --release && strip target/release/shopping-sessions && \
    ls -la target/release/shopping-sessions

FROM debian:bookworm-slim
# mimalloc backs the heap with 2 MB pages: far fewer page faults, cheap teardown at exit
ENV MIMALLOC_ALLOW_LARGE_OS_PAGES=1
COPY --from=build /src/target/release/shopping-sessions /app/pipeline
USER 1000:1000
CMD ["/app/pipeline"]
