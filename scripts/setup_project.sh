#!/bin/bash

# AetherFlow workspace project structure setup script

# Create main directory structure
mkdir -p core derive remote cluster persistence streams examples tests benches docs scripts .github/workflows

# Create initial files
touch Cargo.toml CONTRIBUTING.md LICENSE README.md pull_request_template.md rustfmt.toml
touch .github/workflows/rust.yml

# Create initial content for.github/workflows/rust.yml
cat << 'EOF' > .github/workflows/rust.yml
name: AetherFlow CI

on:
  push:
    branches: ["main"]
  pull_request:
    branches: ["main"]
  schedule:
    - cron: '0 0 * * 0'  # Run every Sunday at 00:00

env:
  CARGO_TERM_COLOR: always

jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - name: Install latest nightly
        uses: dtolnay/rust-toolchain@nightly
        with:
          components: rustfmt, clippy

      - name: Cache dependencies
        uses: actions/cache@v3
        with:
          path: |
            ~/.cargo
            target/
          key: ${{ runner.os }}-cargo-${{ hashFiles('**/Cargo.lock') }}
          restore-keys: |
            ${{ runner.os }}-cargo-

      - name: Build
        run: cargo build --verbose

      - name: Run tests
        run: cargo test --verbose

      - name: Check formatting
        run: cargo fmt -- --check

      - name: Clippy
        run: cargo clippy -- -D warnings

  dependency-check:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - name: Install latest nightly
        uses: dtolnay/rust-toolchain@nightly

      - name: Install cargo-outdated
        run: cargo install cargo-outdated

      - name: Check for outdated dependencies
        run: cargo outdated --exit-code 1

  # Uncomment the following job if you want to run tests on multiple OS
  # test-on-os:
  #   runs-on: ${{ matrix.os }}
  #   strategy:
  #     matrix:
  #       os: [ubuntu-latest, windows-latest, macOS-latest]
  #   steps:
  #   - uses: actions/checkout@v4
  #   - name: Install latest nightly
  #     uses: dtolnay/rust-toolchain@nightly
  #   - name: Build
  #     run: cargo build --verbose
  #   - name: Run tests
  #     run: cargo test --verbose
EOF

# Create Cargo.toml files for each subproject
for dir in core derive remote cluster persistence streams; do
    mkdir -p $dir/src
    touch $dir/Cargo.toml
    echo "pub fn hello_from_$dir() {
    println!(\"Hello from $dir!\");
}" > $dir/src/lib.rs
done

# Create initial content for root Cargo.toml
cat << EOF > Cargo.toml
[workspace]
members = [
    "core",
    "derive",
    "remote",
    "cluster",
    "persistence",
    "streams",
]

[workspace.package]
version = "0.1.0"
authors = ["mash180sx <mash180sx@gmail.com>"]
edition = "2021"
rust-version = "1.83.0"
description = "A high-performance actor system framework in Rust"
documentation = "https://docs.rs/aetherflow"
readme = "README.md"
homepage = "https://github.com/org-408/aetherflow"
repository = "https://github.com/org-408/aetherflow"
license = "MIT"
keywords = ["actor", "concurrent", "distributed", "framework"]
categories = ["concurrency", "asynchronous"]

[workspace.dependencies]
tokio = { version = "1.0", features = ["full"] }
futures = "0.3"
tracing = "0.1"
serde = { version = "1.0", features = ["derive"] }
thiserror = "1.0"
async-trait = "0.1"

[profile.dev]
opt-level = 0
debug = true

[profile.release]
opt-level = 3
debug = false
lto = "thin"
codegen-units = 1

[profile.test]
opt-level = 0
debug = true

[profile.bench]
opt-level = 3
debug = false
lto = "thin"
codegen-units = 1

[workspace.metadata.docs.rs]
all-features = true
rustdoc-args = ["--cfg", "docsrs"]
EOF

# Create initial content for each subproject's Cargo.toml
for dir in core derive remote cluster persistence streams; do
cat << EOF > $dir/Cargo.toml
[package]
name = "aetherflow-$dir"
version.workspace = true
edition.workspace = true
authors.workspace = true
license.workspace = true

[dependencies]
# Add dependencies specific to $dir here

EOF
done

# Create examples, tests, and benchmarks
touch examples/hello_world.rs

# Create initial content for examples/hello_world.rs
cat << EOF > examples/hello_world.rs
use aetherflow::{Actor, Context, Message, System};

struct HelloWorld;

impl Actor for HelloWorld {
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        println!("Hello, world!");
        System::stop();
    }
}

#[derive(Message)]
struct Greet;

impl Handler<Greet> for HelloWorld {
    type Result = ();
    fn handle(&mut self, _msg: Greet, ctx: &mut Self::Context) {
        println!("Greetings!");
        ctx.stop();
    }
}

#[tokio::main]
async fn main() {
    let sys = System::new("Hello, World!");
    sys.add_actor(HelloWorld);
    sys.run().await;
}
EOF

touch tests/integration_tests.rs

# Create initial content for tests/integration_tests.rs
cat << EOF > tests/integration_tests.rs
use aetherflow::{Actor, Context, Message, System};

struct IntegrationTests;

impl Actor for IntegrationTests {
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        println!("Integration tests started!");
        System::stop();
    }
}

#[derive(Message)]
struct TestMessage;

impl Handler<TestMessage> for IntegrationTests {
    type Result = ();
    fn handle(&mut self, _msg: TestMessage, ctx: &mut Self::Context) {
        println!("Integration test completed!");
        ctx.stop();
    }
}

#[tokio::main]
async fn main() {
    let sys = System::new("Integration Tests");
    sys.add_actor(IntegrationTests);
    sys.run().await;
}
EOF

touch benches/benchmarks.rs

# Create initial content for benches/benchmarks.rs
cat << EOF > benches/benchmarks.rs
use criterion::{criterion_group, criterion_main, Criterion};

criterion_group!(benches, benchmark_hello_world);
criterion_main!(benches);

fn benchmark_hello_world(c: &mut Criterion) {
    c.bench_function("hello_world", |b| b.iter(|| {}));
}
EOF

# Create docs
echo "# AetherFlow Design Document" > docs/design.md

echo "AetherFlow workspace project structure has been set up successfully!"