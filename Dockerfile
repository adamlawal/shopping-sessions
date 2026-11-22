# Shopping Sessions: native Rust pipeline.
FROM rust:1-slim-bookworm AS build
WORKDIR /src
# x86-64-v3 = AVX2; the scorers are Cascade Lake or newer.
ENV RUSTFLAGS="-C target-cpu=x86-64-v3"
COPY Cargo.toml Cargo.lock ./
RUN mkdir src && echo "fn main(){}" > src/main.rs && cargo build --release && rm -rf src target/release/deps/shopping_sessions* target/release/shopping-sessions
COPY src ./src
RUN touch src/main.rs && cargo build --release && strip target/release/shopping-sessions

FROM debian:bookworm-slim
COPY --from=build /src/target/release/shopping-sessions /app/pipeline
USER 1000:1000
CMD ["/app/pipeline"]
