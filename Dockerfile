FROM rust:1.83-slim AS builder

RUN apt-get update && apt-get install -y protobuf-compiler && rm -rf /var/lib/apt/lists/*

WORKDIR /build
COPY Cargo.toml Cargo.lock ./
COPY crates/ crates/
COPY benches/ benches/

RUN cargo build --release -p concord-server

FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*

COPY --from=builder /build/target/release/concord-server /usr/local/bin/concord-server

EXPOSE 50051

ENTRYPOINT ["concord-server"]
