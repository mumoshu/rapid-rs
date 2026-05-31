# rapid-rs test orchestration.
#
# Zero-setup invocation. With a stable Rust toolchain (https://rustup.rs)
# and a JDK 11+ in $PATH, the following commands fetch every external
# dependency, patch, build, and test:
#
#   make test       # every LOCAL category (unit, integration, doctest,
#                   # loom, quick fuzz, quick dhat). Auto-installs
#                   # nightly toolchain + cargo-fuzz as needed.
#
#   make test/all   # test + Java/Docker interop. Auto-clones
#                   # references/rapid-java/ at the pinned SHA, applies
#                   # tools/patches/rapid-java/*.patch, runs `mvn
#                   # package`, then drives every interop script.
#
# Run `make help` for the full target list, `make bootstrap` to pre-fetch
# without running tests.
#
# Tunables (override on the command line):
#   FUZZ_SECONDS   = 30   (CI gate: 1800 = 30 min)
#   SOAK_SECONDS   = 10   (CI gate: 1800 = 30 min)
#   SOAK_CHANGES   = 30   (CI gate: 100)
#
# Build artifacts produced lazily as prerequisites:
#   references/rapid-java/                                       (git)
#   references/rapid-java/examples/target/standalone-agent.jar   (mvn)
#   target/release/rapid-example                                 (cargo)
#   target/release/rapid-soak                                    (cargo)
#
# Notes
# - Targets are NOT parallel-safe (interop scripts share ports and
#   the docker network). Run with default `make` (no `-j`).
# - `make test/loom` uses a dedicated CARGO_TARGET_DIR because
#   RUSTFLAGS="--cfg loom" can otherwise leave the main workspace's
#   incremental build cache in a stale state.
# - `bootstrap/rapid-java` writes a sentinel whose filename encodes the
#   sha1 of every `*.patch` it applied. Editing a patch invalidates
#   the sentinel and a subsequent `make` re-bootstraps from scratch.

SHELL := /bin/bash

REPO_ROOT       := $(abspath $(dir $(lastword $(MAKEFILE_LIST))))
RUST_EXAMPLE_BIN := $(REPO_ROOT)/target/release/rapid-example
RUST_SOAK_BIN    := $(REPO_ROOT)/target/release/rapid-soak
JAVA_JAR         := $(REPO_ROOT)/references/rapid-java/examples/target/standalone-agent.jar
LOOM_CRATE       := $(REPO_ROOT)/crates/rapid-loom-tests
LOOM_TARGET_DIR  := $(REPO_ROOT)/target-loom
FUZZ_CRATE       := $(REPO_ROOT)/crates/rapid/fuzz
INTEROP_DIR      := $(REPO_ROOT)/crates/rapid-compat-tests/interop

# Upstream Java repo — `references/` is gitignored, so we clone lazily
# from this URL at the pinned SHA and apply our patches before mvn.
RAPID_JAVA_REPO        := https://github.com/lalithsuresh/rapid.git
RAPID_JAVA_PINNED_SHA  := c7edd6c26d2e7bc6aa311b16ba567da3868fb44c
RAPID_JAVA_DIR         := $(REPO_ROOT)/references/rapid-java
RAPID_JAVA_PATCHES     := $(sort $(wildcard $(REPO_ROOT)/tools/patches/rapid-java/*.patch))
# Sentinel file: presence means the clone is at the pinned SHA AND
# every patch has been applied. Encodes the patch-set fingerprint
# into the filename so `make` re-bootstraps automatically whenever a
# patch is added, edited, or removed.
RAPID_JAVA_SENTINEL    := $(RAPID_JAVA_DIR)/.rapid-rs-bootstrap-$(shell cat $(RAPID_JAVA_PATCHES) 2>/dev/null | sha1sum | cut -c1-12)

FUZZ_SECONDS ?= 30
SOAK_SECONDS ?= 10
SOAK_CHANGES ?= 30

.PHONY: help \
        test test/all \
        test/unit test/integration test/doctest test/loom test/fuzz test/dhat \
        test/interop test/interop/probe test/interop/mixed \
        test/interop/ctrlc test/interop/docker test/interop/ndjson \
        bootstrap bootstrap/rapid-java bootstrap/fuzz \
        check/cargo check/rustup check/nightly check/mvn check/java check/docker \
        build/java-jar build/rust-example build/rust-soak \
        clean distclean

# Default target: print help if invoked with no arguments.
.DEFAULT_GOAL := help

help: ## Show this help
	@echo "rapid-rs targets:"
	@echo
	@grep -E '^[a-zA-Z][a-zA-Z/_-]*:.*## ' $(MAKEFILE_LIST) \
	  | sort \
	  | awk -F':.*## ' '{printf "  %-22s %s\n", $$1, $$2}'
	@echo
	@echo "Composite: \`make test\` runs every local category;"
	@echo "           \`make test/all\` adds the interop suite."

# ====================================================================
# Composite targets
# ====================================================================

test: test/unit test/integration test/doctest test/loom test/fuzz test/dhat ## Run every LOCAL test category
	@echo
	@echo "==> test: all local categories passed"

test/all: test test/interop ## test + Java/Docker interop suite
	@echo
	@echo "==> test/all: every category passed"

# ====================================================================
# Local categories
# ====================================================================

test/unit: check/cargo ## cargo test --workspace --lib
	@echo "==> test/unit"
	cd $(REPO_ROOT) && cargo test --workspace --lib

test/integration: check/cargo ## cargo test --workspace --tests
	@echo "==> test/integration"
	cd $(REPO_ROOT) && cargo test --workspace --tests

test/doctest: check/cargo ## cargo test --workspace --doc
	@echo "==> test/doctest"
	cd $(REPO_ROOT) && cargo test --workspace --doc

test/loom: check/cargo ## loom model under RUSTFLAGS=--cfg loom (dedicated target dir)
	@echo "==> test/loom (CARGO_TARGET_DIR=$(LOOM_TARGET_DIR))"
	CARGO_TARGET_DIR=$(LOOM_TARGET_DIR) RUSTFLAGS="--cfg loom" \
	  cargo test --manifest-path $(LOOM_CRATE)/Cargo.toml --release

test/fuzz: bootstrap/fuzz ## cargo-fuzz on RapidRequest::decode (override FUZZ_SECONDS)
	@echo "==> test/fuzz (-max_total_time=$(FUZZ_SECONDS))"
	cd $(FUZZ_CRATE) && cargo +nightly fuzz run rapid_request_decode -- -max_total_time=$(FUZZ_SECONDS)

test/dhat: build/rust-soak ## rapid-soak heap leak gate (override SOAK_SECONDS / SOAK_CHANGES)
	@echo "==> test/dhat (--view-changes $(SOAK_CHANGES) --duration-secs $(SOAK_SECONDS))"
	$(RUST_SOAK_BIN) --view-changes $(SOAK_CHANGES) --duration-secs $(SOAK_SECONDS)

# ====================================================================
# Interop categories (Java + sometimes Docker)
# ====================================================================

test/interop: test/interop/probe test/interop/mixed test/interop/ctrlc test/interop/docker test/interop/ndjson ## Run every interop test
	@echo
	@echo "==> test/interop: every interop test passed"

test/interop/probe: check/java build/java-jar ## Rust → Java gRPC probe round-trip
	@echo "==> test/interop/probe"
	cd $(REPO_ROOT) && \
	  RAPID_JAVA_JAR=$(JAVA_JAR) \
	    cargo test -p rapid-compat-tests --features interop --test probe_interop

test/interop/mixed: check/java build/java-jar build/rust-example ## 3 Java + 3 Rust agents converge, then survive 1+1 failure
	@echo "==> test/interop/mixed"
	RAPID_JAVA_JAR=$(JAVA_JAR) RAPID_RUST_BIN=$(RUST_EXAMPLE_BIN) BASE_PORT=18550 \
	  bash $(INTEROP_DIR)/mixed_cluster.sh

test/interop/ctrlc: build/rust-example ## 4 Rust agents bootstrap, one SIGINT, survivors emit +1 ViewChange each
	@echo "==> test/interop/ctrlc"
	RAPID_RUST_BIN=$(RUST_EXAMPLE_BIN) BASE_PORT=19550 \
	  bash $(INTEROP_DIR)/four_node_ctrl_c.sh

test/interop/docker: check/docker build/java-jar build/rust-example ## docker-compose 3+3 cluster with verifier sidecar
	@echo "==> test/interop/docker"
	bash $(INTEROP_DIR)/docker/run.sh

test/interop/ndjson: check/java build/java-jar ## Java captures NDJSON wire trace; Rust parses + decodes every record
	@echo "==> test/interop/ndjson"
	@TRACE=$$(mktemp /tmp/rapid-trace-XXXXXX.ndjson); \
	  ( RAPID_JAVA_JAR=$(JAVA_JAR) OUTPUT_TRACE=$$TRACE BASE_PORT=22950 \
	      bash $(INTEROP_DIR)/capture_trace.sh ) && \
	  ( cd $(REPO_ROOT) && \
	      RAPID_NDJSON_TRACE_REPLAY=$$TRACE \
	        cargo test -p rapid-compat-tests --test ndjson_replay -- --nocapture ); \
	  status=$$?; rm -f $$TRACE; exit $$status

# ====================================================================
# Toolchain checks — fail fast with an actionable message
# ====================================================================

check/cargo: ## Require cargo in $PATH (install via https://rustup.rs)
	@command -v cargo >/dev/null 2>&1 || { \
	  echo "✗ cargo not in PATH — install via https://rustup.rs" >&2; exit 1; }

check/rustup: ## Require rustup in $PATH
	@command -v rustup >/dev/null 2>&1 || { \
	  echo "✗ rustup not in PATH — install via https://rustup.rs" >&2; exit 1; }

check/nightly: check/rustup ## Require the nightly toolchain
	@rustup toolchain list 2>/dev/null | grep -q '^nightly' || { \
	  echo "✗ rust nightly not installed — run: rustup toolchain install nightly" >&2; \
	  exit 1; }

check/mvn: ## Require mvn in $PATH (interop targets)
	@command -v mvn >/dev/null 2>&1 || { \
	  echo "✗ mvn not in PATH — install Apache Maven (apt: maven, brew: maven, sdkman: maven)" >&2; \
	  exit 1; }

check/java: check/mvn ## Require java + mvn (interop targets)
	@command -v java >/dev/null 2>&1 || { \
	  echo "✗ java not in PATH — install JDK 11+ (e.g., openjdk-21)" >&2; exit 1; }

check/docker: ## Require docker daemon reachable (docker-compose harness)
	@command -v docker >/dev/null 2>&1 || { \
	  echo "✗ docker not in PATH — install Docker Engine" >&2; exit 1; }
	@docker info >/dev/null 2>&1 || { \
	  echo "✗ docker daemon not reachable — start Docker" >&2; exit 1; }

# ====================================================================
# Bootstrap — clone & patch external sources, install missing tools
# ====================================================================

bootstrap: bootstrap/rapid-java bootstrap/fuzz ## One-shot setup of everything needed for `make test/all`
	@echo "==> bootstrap: all prerequisites present"

# Clone references/rapid-java at the pinned SHA and apply every
# *.patch in tools/patches/rapid-java/. The sentinel file's name
# encodes the patch-set fingerprint so editing or adding a patch
# invalidates the cache automatically.
bootstrap/rapid-java: $(RAPID_JAVA_SENTINEL) ## Clone + checkout + patch upstream Java tree

$(RAPID_JAVA_SENTINEL): $(RAPID_JAVA_PATCHES) | check/java
	@echo "==> bootstrap/rapid-java (SHA $(RAPID_JAVA_PINNED_SHA))"
	@if [ ! -d $(RAPID_JAVA_DIR)/.git ]; then \
	  mkdir -p $(dir $(RAPID_JAVA_DIR)); \
	  git clone $(RAPID_JAVA_REPO) $(RAPID_JAVA_DIR); \
	fi
	@rm -f $(RAPID_JAVA_DIR)/.rapid-rs-bootstrap-*
	cd $(RAPID_JAVA_DIR) && git fetch --quiet origin $(RAPID_JAVA_PINNED_SHA) || true
	cd $(RAPID_JAVA_DIR) && git reset --hard $(RAPID_JAVA_PINNED_SHA)
	cd $(RAPID_JAVA_DIR) && git clean -fd
	@for p in $(RAPID_JAVA_PATCHES); do \
	  echo "  apply $$(basename $$p)"; \
	  (cd $(RAPID_JAVA_DIR) && git apply "$$p") || exit 1; \
	done
	@touch $@

bootstrap/fuzz: check/rustup ## Install rust nightly + cargo-fuzz (idempotent)
	@echo "==> bootstrap/fuzz"
	@rustup toolchain list 2>/dev/null | grep -q '^nightly' || \
	  rustup toolchain install nightly --component rust-src --profile minimal
	@# Nightly must be at least as new as the workspace MSRV. If it
	@# isn't, refresh it — cargo-fuzz invokes `cargo +nightly build`
	@# against this crate and would otherwise hit "requires rustc X".
	@msrv=$$(awk -F'"' '/^rust-version/ { print $$2 }' Cargo.toml); \
	  nightly=$$(rustup run nightly rustc --version | awk '{print $$2}'); \
	  printf '%s\n%s\n' "$$msrv" "$$nightly" | sort -V -C 2>/dev/null \
	    || { echo "  nightly $$nightly < MSRV $$msrv → rustup update nightly"; rustup update nightly; }
	@command -v cargo-fuzz >/dev/null 2>&1 || cargo install --locked cargo-fuzz

# ====================================================================
# Build prerequisites
# ====================================================================

# Phony build targets always invoke the underlying build tool — cargo
# and mvn dedup unchanged inputs themselves, so this stays cheap.

build/java-jar: bootstrap/rapid-java ## mvn package references/rapid-java/examples/target/standalone-agent.jar
	@echo "==> build/java-jar"
	cd $(RAPID_JAVA_DIR) && mvn -q package -DskipTests

build/rust-example: check/cargo ## cargo build --release -p rapid-example
	@echo "==> build/rust-example"
	cd $(REPO_ROOT) && cargo build --release -p rapid-example

build/rust-soak: check/cargo ## cargo build --release -p rapid-soak --features dhat-heap
	@echo "==> build/rust-soak"
	cd $(REPO_ROOT) && cargo build --release -p rapid-soak --features dhat-heap

# ====================================================================
# Clean
# ====================================================================

clean: ## Remove cargo + loom + fuzz build artifacts (keeps references/rapid-java)
	@echo "==> clean"
	cd $(REPO_ROOT) && cargo clean
	rm -rf $(LOOM_TARGET_DIR)
	cd $(FUZZ_CRATE) && cargo clean
	rm -f $(INTEROP_DIR)/docker/standalone-agent.jar $(INTEROP_DIR)/docker/rapid-example

distclean: clean ## clean + remove cloned upstream sources (forces full re-bootstrap)
	@echo "==> distclean"
	rm -rf $(RAPID_JAVA_DIR)
