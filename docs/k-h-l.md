# K, H, L — the cut-detector parameters

Rapid's failure-detection machinery has three integer parameters, all
relating to a single per-node "multi-process cut detector." If you've
read the Suresh et al. paper or skimmed the code, you've seen the
triple `{K, H, L}`. This page explains what each one does in plain
English, how they relate to each other, and which defaults we ship.

## Mental model

Every node has **K observers** watching it (and watches K others
itself). When a node looks unreachable, up to K of its observers
raise an alert — "I can't reach this subject." A node's cut detector
tracks, for every other node `S`, the count of distinct observers
that have alerted on `S` so far.

That count gets classified into one of three zones:

```
   tally(S):
   0 ──────── L ──────── H ──────── K
      quiet  │ in-flux  │ stable
             │          │
             │          └── ≥ H confirmations
             │              "safe to remove S"
             │
             └── L ≤ tally < H
                 "S is being questioned —
                  freeze proposals globally"
```

- **K**  = how many *independent eyes* watch each node (and how many
            it watches).
- **H**  = how many eyes *must* report a problem before we're willing
            to act on it.
- **L**  = how few eyes raising a flag is enough to make us *hold
            off* on acting elsewhere.

## The rule the cut detector enforces

A proposal **only** fires when both conditions hold:

1. Some subject `S` has `tally(S) ≥ H`, **and**
2. *No* other subject is currently in the in-flux zone
   (`L ≤ tally < H`).

The L watermark is a global hold-off: while *any* subject is in-flux,
even an already-stable subject doesn't get proposed yet. When the
last in-flux subject finally crosses H, *all* stable subjects are
proposed together as a single multi-node cut.

You can see this directly in
[`crates/rapid/src/cut_detector.rs`](../crates/rapid/src/cut_detector.rs):

```rust
if num == self.l {
    self.updates_in_progress += 1;    // S entered in-flux
    self.pre_proposal.insert(dst_key);
}
if num == self.h {
    self.proposal.insert(dst_key);    // S crossed stable
    self.updates_in_progress -= 1;
    if self.updates_in_progress == 0 {
        // No other subject is in-flux → emit proposal.
        return ret;
    }
}
```

`updates_in_progress` is the count of subjects currently in the
in-flux band. The proposal only emits when that count drops back to
zero.

## Why both H and L exist — almost-everywhere agreement

The paper's central theorem (§8): with appropriate K, H, L, **all
healthy processes propose the same view change with high
probability**. L is what makes "high probability" work.

A concrete failure-mode without L: nodes A and B both run cut
detectors. Two subjects `S₁` and `S₂` are failing simultaneously.

- A sees `S₁`'s H-th report first → proposes `{S₁}`.
- B sees `S₂`'s H-th report first → proposes `{S₂}`.

Different proposals → Fast Paxos can't reach quorum on either →
fallback to classic Paxos → extra latency, extra work. **With** L,
both A and B notice "two subjects are simultaneously in-flux,"
freeze proposals until both reach H, then propose `{S₁, S₂}` as one
cut. One round, one proposal, every node decides identically.

## Constraint between K, H, L

The paper requires
[(§4.2, p. 391)](../references/rapid-paper.md):

```
1 ≤ L ≤ H ≤ K
```

- `H ≤ K` because you can't have more independent reports than
  observers.
- `L ≤ H` because the in-flux band has to fit *below* the stable
  threshold.
- `L ≥ 1` because a tally of 0 is "no signal at all" — there's
  nothing to defer on.

`H − L` is the **stability window**: how many additional
confirmations beyond "any signal at all" you require before
committing. Bigger window → more global hold-off (safer, slower);
smaller window → faster decisions (more risk of divergent
proposals).

## Defaults — paper vs Java vs us

| Source                                        | K   | H   | L   |
|-----------------------------------------------|-----|-----|-----|
| Rapid paper (all evaluation runs)             | 10  | 9   | **3** |
| Java upstream ([`Cluster.java:72-74`](../references/rapid-java/rapid/src/main/java/com/vrg/rapid/Cluster.java)) | 10  | 9   | **4** |
| rapid-rs ([`settings.rs`](../crates/rapid/src/settings.rs)) | 10  | 9   | **4** |

We follow Java's defaults exactly (L=4), not the paper's (L=3). The
bump from L=3 → L=4 is an operational tweak the Java maintainers
made after publication — slightly less hold-off overhead at the cost
of a slightly weaker almost-everywhere-agreement bound. The paper's
sensitivity study ([§7, p. 395](../references/rapid-paper.md))
sweeps `L ∈ {1, 2, 3, 4}` and shows the property holds across all
four values, so L=4 is squarely inside the validated range.

## Practical implications

- **You don't normally tune these.** The defaults are paper-validated
  + production-tuned. Touching them is a research exercise; we
  don't expose them on the public builder API for that reason.
- **They must be uniform cluster-wide.** Like K (see [section on K
  consistency](#about-k-uniformity) below), H and L are configured
  per node but the protocol assumes every node uses the same triple.
  There's no wire-level check.
- **The constraint `1 ≤ L ≤ H ≤ K` is enforced at construction.**
  [`MultiNodeCutDetector::new`](../crates/rapid/src/cut_detector.rs)
  returns an error if you pass anything outside that range.

## About K uniformity

K is *also* a global constant in the paper's model — every process
recomputes the K-ring topology locally from the same K, and the
"topology is deterministic over the membership set" property the
paper relies on requires that every node use the same K. Our
Settings field is configurable for testing, but in practice every
node in a cluster must agree on `{K, H, L}`.

See also:
- [`PLAN.md`](../PLAN.md) — implementation plan, including the
  parity stance on K/H/L.
- [`references/SUMMARY-paper.md`](../references/SUMMARY-paper.md) —
  distilled algorithm spec.
- [`crates/rapid/src/cut_detector.rs`](../crates/rapid/src/cut_detector.rs)
  — the actual detector code, ~150 lines and worth reading.
