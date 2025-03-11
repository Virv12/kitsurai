FROM rust AS builder
WORKDIR /app
COPY Cargo.toml Cargo.lock .
COPY src src
RUN cargo build --release

FROM ubuntu
WORKDIR /app
COPY --from=builder /app/target/release/kitsurai /app/kitsurai
CMD ["/app/kitsurai"]
