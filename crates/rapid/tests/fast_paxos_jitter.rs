//! Expovariate jitter on classic-Paxos fallback — F5 gate.
//!
//! Java parity reference:
//! `FastPaxos::getRandomDelayMs`
//!   = -1000 * ln(1 - U) * N + `BASE_DELAY`
//! where U ~ Uniform[0, 1) and N is the membership size.
//!
//! Property: distinct `propose` calls yield non-identical delays.
//! With expovariate sampling, the probability of two consecutive
//! draws being equal at millisecond resolution is effectively zero
//! for N >= 2.

use std::collections::HashSet;
use std::time::Duration;

use rapid::consensus::fast_paxos::FastOutgoing;
use rapid::consensus::FastPaxos;
use rapid::pb;

fn ep(host: &str, port: i32) -> pb::Endpoint {
    pb::Endpoint {
        hostname: host.as_bytes().to_vec(),
        port,
    }
}

fn scheduled_delay(out: &[FastOutgoing]) -> Duration {
    for o in out {
        if let FastOutgoing::ScheduleClassicFallback(d) = o {
            return *d;
        }
    }
    panic!("propose() must emit a ScheduleClassicFallback");
}

#[test]
fn jitter_varies_across_proposals() {
    let me = ep("127.0.0.1", 1);
    let proposal = vec![ep("127.0.0.2", 2)];
    let base = Duration::from_millis(500);
    let mut seen: HashSet<Duration> = HashSet::new();
    for _ in 0..50 {
        let mut fp = FastPaxos::new(me.clone(), 1, 10, base);
        let outs = fp.propose(proposal.clone());
        let d = scheduled_delay(&outs);
        assert!(
            d >= base,
            "delay {d:?} below base {base:?} — jitter must be non-negative",
        );
        seen.insert(d);
    }
    assert!(
        seen.len() > 10,
        "expected substantial variation across 50 draws, only saw {} unique",
        seen.len()
    );
}

#[test]
fn jitter_scales_with_membership_size() {
    let me = ep("127.0.0.1", 1);
    let proposal = vec![ep("127.0.0.2", 2)];
    let base = Duration::from_millis(100);
    // Average of N=2 vs N=20: jitter rate is 1/N, so mean jitter
    // = N * 1000 ms. The N=20 series should be visibly larger.
    let mean = |n: usize| -> u128 {
        let mut total: u128 = 0;
        for _ in 0..200 {
            let mut fp = FastPaxos::new(me.clone(), 1, n, base);
            let outs = fp.propose(proposal.clone());
            total += scheduled_delay(&outs).as_millis();
        }
        total / 200
    };
    let mean_small = mean(2);
    let mean_large = mean(20);
    assert!(
        mean_large > mean_small,
        "mean(N=20) {mean_large} ms should exceed mean(N=2) {mean_small} ms"
    );
}
