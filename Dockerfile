FROM rust AS builder
WORKDIR /app
COPY Cargo.toml Cargo.lock .
RUN mkdir src && \
    echo 'fn main() {println!("if you see this, the build broke")}' > src/lib.rs && \
    RUSTFLAGS="--cfg tokio_unstable" CARGO_TERM_COLOR=always cargo build --release --lib
COPY src src
RUN touch src/lib.rs
RUN RUSTFLAGS="--cfg tokio_unstable" CARGO_TERM_COLOR=always cargo build --release --bins

FROM ubuntu
WORKDIR /app
RUN apt update && \
    apt install -y libsqlite3-dev curl bsdmainutils hyperfine apache2-utils && \
    rm -rf /var/lib/apt/lists/*
COPY --from=builder /app/target/release/ktd /app/target/release/ktc /app/
ENV RUST_BACKTRACE=full
ENV RUST_LOG="ktd=debug,warn"
CMD ["/bin/bash", "-c", "ulimit -n 100000 && /app/ktd"]
