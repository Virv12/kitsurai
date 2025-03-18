FROM rust AS builder
WORKDIR /app
COPY Cargo.toml Cargo.lock .
COPY src src
RUN cargo build --release

FROM ubuntu
WORKDIR /app
RUN apt update && \
    apt install -y libsqlite3-dev && \
    rm -rf /var/lib/apt/lists/*
COPY --from=builder /app/target/release/kitsurai /app/kitsurai
CMD ["/app/kitsurai"]
