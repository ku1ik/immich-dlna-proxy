fmt:
    cargo fmt --all

test:
    cargo test

lint:
    cargo clippy --all-targets --all-features -- -D warnings

build:
    cargo build --release

check:
    cargo fmt --all -- --check
    cargo test
    cargo clippy --all-targets --all-features -- -D warnings
    cargo build --release
