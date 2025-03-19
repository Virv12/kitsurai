FROM rust AS builder
WORKDIR /app
COPY Cargo.toml Cargo.lock .
RUN mkdir src && \
    echo 'fn main() {println!("if you see this, the build broke")}' > src/main.rs && \
    CARGO_TERM_COLOR=always cargo build --release
COPY src src
RUN CARGO_TERM_COLOR=always cargo build --release

FROM ubuntu
WORKDIR /app
RUN apt update && \
    apt install -y libsqlite3-dev curl bsdmainutils && \
    rm -rf /var/lib/apt/lists/*
COPY --from=builder /app/target/release/kitsurai /app/kitsurai
CMD ["/app/kitsurai"]
