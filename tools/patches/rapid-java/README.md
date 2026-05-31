# `references/rapid-java/` patches

`references/` is gitignored — the upstream Java repo is cloned lazily by
`make bootstrap/rapid-java`. After clone, the makefile checks out
`RAPID_JAVA_PINNED_SHA` (see top of `Makefile`) and applies every
`*.patch` file in this directory in lexicographic order via `git apply`.

## 01-rapid-rs-interop.patch

Three categories of change against upstream HEAD:

1. **Build the project on modern JDKs** — root `pom.xml` drops the
   `findbugs-maven-plugin`, `maven-checkstyle-plugin`, and
   error-prone-bound `maven-compiler-plugin` (all of which fail under
   Java 11+ because their Groovy / Plexus versions predate
   `Locale.getDefault()` and `File.exists()` becoming methods).
   `rapid/pom.xml` adds an explicit `javax.annotation-api` dep so the
   protobuf-emitted `javax.annotation.Generated` import resolves on
   JDK ≥ 11.

2. **`StandaloneAgent.printClusterMembership` emits a machine-readable
   `view: <hex> [host:port,...]` line** every 500 ms. The harness
   scripts in `crates/rapid-compat-tests/interop/` grep this format
   from both Java and Rust agents.

3. **`GrpcServer.sendRequest` and `NdjsonTraceWriter`** — capture every
   inbound `RapidRequest` as a single NDJSON record when
   `RAPID_NDJSON_TRACE=<path>` is set in the env. The schema matches
   `PLAN.md` § *Replay trace format* and the Rust replay driver in
   `crates/rapid-compat-tests/src/ndjson.rs`.

## Updating the patch

When the patch needs to evolve:

```bash
# Inside references/rapid-java, after making changes:
git add -N rapid/src/main/java/com/vrg/rapid/messaging/impl/NdjsonTraceWriter.java
git diff > $REPO/tools/patches/rapid-java/01-rapid-rs-interop.patch
```

If you bump `RAPID_JAVA_PINNED_SHA`, regenerate the patch against the
new base.
