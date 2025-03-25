FROM rust AS builder
WORKDIR /app
COPY Cargo.toml Cargo.lock .
RUN mkdir src && \
    echo 'fn main() {println!("if you see this, the build broke")}' > src/lib.rs && \
    CARGO_TERM_COLOR=always cargo build --release --lib
COPY src src
RUN touch src/lib.rs
RUN CARGO_TERM_COLOR=always cargo build --release --bins

FROM ubuntu
WORKDIR /app
RUN apt update && \
    apt install -y libsqlite3-dev curl bsdmainutils hyperfine && \
    rm -rf /var/lib/apt/lists/*
COPY --from=builder /app/target/release/ktd /app/target/release/ktc /app/
ENV RUST_BACKTRACE=full
CMD ["/app/ktd"]
