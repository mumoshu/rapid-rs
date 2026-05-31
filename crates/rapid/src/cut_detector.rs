//! `MultiNodeCutDetector` — H/L watermark cut detector.
//!
//! Bit-exact port of
//! `references/rapid-java/.../MultiNodeCutDetector.java`. The single-owner
//! actor invariant lets us drop the `synchronized(lock)` and
//! `@GuardedBy("lock")` Java decorations.
//!
//! The semantic surface:
//! - `aggregate(alert)` consumes one [`pb::AlertMessage`] (one entry from a
//!   `BatchedAlertMessage`) and returns any newly-decided proposals.
//! - `invalidate_failing_edges(view)` runs the implicit-detection step,
//!   short-circuiting when no `DOWN` alerts have been observed.
//! - `clear()` resets state on view-change.

use std::collections::{HashMap, HashSet};

use crate::error::{Error, Result};
use crate::pb;
use crate::view::MembershipView;

/// Minimum permissible `K` (matches Java `MultiNodeCutDetector.K_MIN`).
pub const K_MIN: u8 = 3;

/// One-shot cut detector held by the membership service actor.
pub struct MultiNodeCutDetector {
    k: u8,
    h: u8,
    l: u8,
    proposal_count: u32,
    updates_in_progress: i32,
    reports_per_host: HashMap<EndpointKey, HashMap<u8, pb::Endpoint>>,
    proposal: HashSet<EndpointKey>,
    pre_proposal: HashSet<EndpointKey>,
    seen_link_down_events: bool,
    endpoint_objs: HashMap<EndpointKey, pb::Endpoint>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct EndpointKey {
    hostname: Vec<u8>,
    port: i32,
}

impl From<&pb::Endpoint> for EndpointKey {
    fn from(value: &pb::Endpoint) -> Self {
        Self {
            hostname: value.hostname.clone(),
            port: value.port,
        }
    }
}

impl MultiNodeCutDetector {
    /// Construct with watermarks.
    ///
    /// # Errors
    /// Returns [`Error::Internal`] if `K > H >= L > 0 && K >= K_MIN` is not
    /// satisfied (matches Java's `IllegalArgumentException`).
    pub fn new(k: u8, h: u8, l: u8) -> Result<Self> {
        if h > k || l > h || k < K_MIN || l == 0 || h == 0 {
            return Err(Error::Internal(format!(
                "MultiNodeCutDetector requires K >= K_MIN={K_MIN}, H<=K, 0<L<=H (got K={k}, H={h}, L={l})"
            )));
        }
        Ok(Self {
            k,
            h,
            l,
            proposal_count: 0,
            updates_in_progress: 0,
            reports_per_host: HashMap::new(),
            proposal: HashSet::new(),
            pre_proposal: HashSet::new(),
            seen_link_down_events: false,
            endpoint_objs: HashMap::new(),
        })
    }

    /// Java `getNumProposals()`.
    #[must_use]
    pub fn num_proposals(&self) -> u32 {
        self.proposal_count
    }

    /// Apply an [`pb::AlertMessage`]; one entry from a
    /// `BatchedAlertMessage`. Returns any newly-decided proposals.
    pub fn aggregate(&mut self, msg: &pb::AlertMessage) -> Vec<pb::Endpoint> {
        let Some(src) = msg.edge_src.as_ref() else {
            return Vec::new();
        };
        let Some(dst) = msg.edge_dst.as_ref() else {
            return Vec::new();
        };
        let status = pb::EdgeStatus::try_from(msg.edge_status).unwrap_or(pb::EdgeStatus::Up);
        let mut out = Vec::new();
        for ring_number in &msg.ring_number {
            #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
            let rn = *ring_number as u8;
            out.append(&mut self.aggregate_one(src, dst, status, rn));
        }
        out
    }

    fn aggregate_one(
        &mut self,
        link_src: &pb::Endpoint,
        link_dst: &pb::Endpoint,
        edge_status: pb::EdgeStatus,
        ring_number: u8,
    ) -> Vec<pb::Endpoint> {
        debug_assert!(ring_number < self.k);

        if edge_status == pb::EdgeStatus::Down {
            self.seen_link_down_events = true;
        }

        let dst_key = EndpointKey::from(link_dst);
        self.endpoint_objs
            .entry(dst_key.clone())
            .or_insert_with(|| link_dst.clone());
        let reports = self.reports_per_host.entry(dst_key.clone()).or_default();
        if reports.contains_key(&ring_number) {
            return Vec::new();
        }
        reports.insert(ring_number, link_src.clone());
        #[allow(clippy::cast_possible_truncation)]
        let num = reports.len() as u8;

        if num == self.l {
            self.updates_in_progress += 1;
            self.pre_proposal.insert(dst_key.clone());
        }

        if num == self.h {
            self.pre_proposal.remove(&dst_key);
            self.proposal.insert(dst_key);
            self.updates_in_progress -= 1;

            if self.updates_in_progress == 0 {
                self.proposal_count += 1;
                let ret: Vec<pb::Endpoint> = self
                    .proposal
                    .iter()
                    .map(|k| self.endpoint_objs[k].clone())
                    .collect();
                self.proposal.clear();
                return ret;
            }
        }
        Vec::new()
    }

    /// Java `invalidateFailingEdges(view)` — for every node currently in
    /// `pre_proposal`, route its observers through the cut detector with
    /// an implicit `DOWN` (or `UP` for joiners) alert.
    pub fn invalidate_failing_edges(&mut self, view: &mut MembershipView) -> Vec<pb::Endpoint> {
        if !self.seen_link_down_events {
            return Vec::new();
        }

        let pre_proposal_copy: Vec<pb::Endpoint> = self
            .pre_proposal
            .iter()
            .map(|k| self.endpoint_objs[k].clone())
            .collect();

        let mut proposals = Vec::new();
        for node_in_flux in &pre_proposal_copy {
            let observers = if view.is_host_present(node_in_flux) {
                view.get_observers_of(node_in_flux).unwrap_or_default()
            } else {
                view.get_expected_observers_of(node_in_flux)
            };
            #[allow(clippy::cast_possible_truncation)]
            for (ring_number, observer) in observers.iter().enumerate() {
                let observer_key = EndpointKey::from(observer);
                if self.proposal.contains(&observer_key)
                    || self.pre_proposal.contains(&observer_key)
                {
                    let edge_status = if view.is_host_present(node_in_flux) {
                        pb::EdgeStatus::Down
                    } else {
                        pb::EdgeStatus::Up
                    };
                    proposals.extend(self.aggregate_one(
                        observer,
                        node_in_flux,
                        edge_status,
                        ring_number as u8,
                    ));
                }
            }
        }
        proposals
    }

    /// Java `clear()` — reset state after a view change is applied.
    pub fn clear(&mut self) {
        self.reports_per_host.clear();
        self.proposal.clear();
        self.pre_proposal.clear();
        self.endpoint_objs.clear();
        self.updates_in_progress = 0;
        self.proposal_count = 0;
        self.seen_link_down_events = false;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ep(host: &str, port: i32) -> pb::Endpoint {
        pb::Endpoint {
            hostname: host.as_bytes().to_vec(),
            port,
        }
    }

    fn alert(
        src: pb::Endpoint,
        dst: pb::Endpoint,
        status: pb::EdgeStatus,
        ring_number: i32,
    ) -> pb::AlertMessage {
        pb::AlertMessage {
            edge_src: Some(src),
            edge_dst: Some(dst),
            edge_status: status as i32,
            configuration_id: -1,
            ring_number: vec![ring_number],
            node_id: None,
            metadata: None,
        }
    }

    /// Port of `CutDetectionTest.cutDetectionTest`: K=10, H=8, L=2 (Java
    /// `CutDetectionTest` constants).
    #[test]
    fn java_port_cut_detection() {
        let mut cd = MultiNodeCutDetector::new(10, 8, 2).unwrap();
        let dst = ep("127.0.0.2", 2);
        for i in 0..7 {
            let ret = cd.aggregate(&alert(
                ep("127.0.0.1", i + 1),
                dst.clone(),
                pb::EdgeStatus::Up,
                i,
            ));
            assert_eq!(ret.len(), 0);
            assert_eq!(cd.num_proposals(), 0);
        }
        let ret = cd.aggregate(&alert(ep("127.0.0.1", 8), dst, pb::EdgeStatus::Up, 7));
        assert_eq!(ret.len(), 1);
        assert_eq!(cd.num_proposals(), 1);
    }

    #[test]
    fn java_port_blocking_one_blocker() {
        let mut cd = MultiNodeCutDetector::new(10, 8, 2).unwrap();
        let dst1 = ep("127.0.0.2", 2);
        let dst2 = ep("127.0.0.3", 2);
        for i in 0..7 {
            assert_eq!(
                cd.aggregate(&alert(
                    ep("127.0.0.1", i + 1),
                    dst1.clone(),
                    pb::EdgeStatus::Up,
                    i
                ))
                .len(),
                0
            );
        }
        for i in 0..7 {
            assert_eq!(
                cd.aggregate(&alert(
                    ep("127.0.0.1", i + 1),
                    dst2.clone(),
                    pb::EdgeStatus::Up,
                    i
                ))
                .len(),
                0
            );
        }
        // dst1 reaches H but dst2 is between L and H → blocked.
        assert_eq!(
            cd.aggregate(&alert(
                ep("127.0.0.1", 8),
                dst1.clone(),
                pb::EdgeStatus::Up,
                7
            ))
            .len(),
            0
        );
        // dst2 also reaches H now → joint proposal of size 2.
        let ret = cd.aggregate(&alert(
            ep("127.0.0.1", 8),
            dst2.clone(),
            pb::EdgeStatus::Up,
            7,
        ));
        assert_eq!(ret.len(), 2);
        assert_eq!(cd.num_proposals(), 1);
    }

    #[test]
    fn java_port_blocking_three_blockers() {
        let mut cd = MultiNodeCutDetector::new(10, 8, 2).unwrap();
        let dst1 = ep("127.0.0.2", 2);
        let dst2 = ep("127.0.0.3", 2);
        let dst3 = ep("127.0.0.4", 2);
        for dst in [&dst1, &dst2, &dst3] {
            for i in 0..7 {
                assert_eq!(
                    cd.aggregate(&alert(
                        ep("127.0.0.1", i + 1),
                        dst.clone(),
                        pb::EdgeStatus::Up,
                        i,
                    ))
                    .len(),
                    0
                );
            }
        }
        assert_eq!(
            cd.aggregate(&alert(
                ep("127.0.0.1", 8),
                dst1.clone(),
                pb::EdgeStatus::Up,
                7
            ))
            .len(),
            0
        );
        assert_eq!(
            cd.aggregate(&alert(
                ep("127.0.0.1", 8),
                dst3.clone(),
                pb::EdgeStatus::Up,
                7
            ))
            .len(),
            0
        );
        let ret = cd.aggregate(&alert(
            ep("127.0.0.1", 8),
            dst2.clone(),
            pb::EdgeStatus::Up,
            7,
        ));
        assert_eq!(ret.len(), 3);
        assert_eq!(cd.num_proposals(), 1);
    }

    #[test]
    fn java_port_blocking_multiple_blockers_past_h() {
        let mut cd = MultiNodeCutDetector::new(10, 8, 2).unwrap();
        let dst1 = ep("127.0.0.2", 2);
        let dst2 = ep("127.0.0.3", 2);
        let dst3 = ep("127.0.0.4", 2);
        for dst in [&dst1, &dst2, &dst3] {
            for i in 0..7 {
                cd.aggregate(&alert(
                    ep("127.0.0.1", i + 1),
                    dst.clone(),
                    pb::EdgeStatus::Up,
                    i,
                ));
            }
        }
        // Add more past-H reports for dst1/dst3. Java sends ringNumber 7 each
        // time; once the ring-number slot is taken further alerts noop —
        // dst1/dst3 stay at numReports = H.
        cd.aggregate(&alert(
            ep("127.0.0.1", 8),
            dst1.clone(),
            pb::EdgeStatus::Up,
            7,
        ));
        let ret = cd.aggregate(&alert(
            ep("127.0.0.1", 9),
            dst1.clone(),
            pb::EdgeStatus::Up,
            7,
        ));
        assert_eq!(ret.len(), 0);
        cd.aggregate(&alert(
            ep("127.0.0.1", 8),
            dst3.clone(),
            pb::EdgeStatus::Up,
            7,
        ));
        let ret = cd.aggregate(&alert(
            ep("127.0.0.1", 9),
            dst3.clone(),
            pb::EdgeStatus::Up,
            7,
        ));
        assert_eq!(ret.len(), 0);
        let ret = cd.aggregate(&alert(
            ep("127.0.0.1", 8),
            dst2.clone(),
            pb::EdgeStatus::Up,
            7,
        ));
        assert_eq!(ret.len(), 3);
        assert_eq!(cd.num_proposals(), 1);
    }

    #[test]
    fn java_port_below_l_does_not_block() {
        let mut cd = MultiNodeCutDetector::new(10, 8, 2).unwrap();
        let dst1 = ep("127.0.0.2", 2);
        let dst2 = ep("127.0.0.3", 2);
        let dst3 = ep("127.0.0.4", 2);
        for i in 0..7 {
            cd.aggregate(&alert(
                ep("127.0.0.1", i + 1),
                dst1.clone(),
                pb::EdgeStatus::Up,
                i,
            ));
        }
        // dst2 only crosses L-1 (=1), so it should not enter preProposal.
        for i in 0..1 {
            cd.aggregate(&alert(
                ep("127.0.0.1", i + 1),
                dst2.clone(),
                pb::EdgeStatus::Up,
                i,
            ));
        }
        for i in 0..7 {
            cd.aggregate(&alert(
                ep("127.0.0.1", i + 1),
                dst3.clone(),
                pb::EdgeStatus::Up,
                i,
            ));
        }
        assert_eq!(
            cd.aggregate(&alert(
                ep("127.0.0.1", 8),
                dst1.clone(),
                pb::EdgeStatus::Up,
                7
            ))
            .len(),
            0
        );
        let ret = cd.aggregate(&alert(
            ep("127.0.0.1", 8),
            dst3.clone(),
            pb::EdgeStatus::Up,
            7,
        ));
        assert_eq!(ret.len(), 2);
        assert_eq!(cd.num_proposals(), 1);
    }

    #[test]
    fn java_port_batch() {
        let mut cd = MultiNodeCutDetector::new(10, 8, 2).unwrap();
        let n = 3;
        let mut dsts = Vec::new();
        for i in 0..n {
            dsts.push(ep("127.0.0.2", 2 + i));
        }
        let mut proposal = Vec::new();
        for d in &dsts {
            for ring in 0..10 {
                proposal.extend(cd.aggregate(&alert(
                    ep("127.0.0.1", 1),
                    d.clone(),
                    pb::EdgeStatus::Up,
                    ring,
                )));
            }
        }
        assert_eq!(proposal.len(), usize::try_from(n).unwrap());
    }

    #[test]
    fn invalidate_failing_edges_short_circuits_without_down() {
        let mut cd = MultiNodeCutDetector::new(10, 8, 2).unwrap();
        let mut view = MembershipView::new(10).unwrap();
        // No DOWN events yet — must return empty without touching the view.
        assert_eq!(cd.invalidate_failing_edges(&mut view).len(), 0);
    }

    #[test]
    fn rejects_bad_thresholds() {
        assert!(MultiNodeCutDetector::new(2, 1, 1).is_err()); // K < K_MIN
        assert!(MultiNodeCutDetector::new(10, 11, 1).is_err()); // H > K
        assert!(MultiNodeCutDetector::new(10, 5, 6).is_err()); // L > H
        assert!(MultiNodeCutDetector::new(10, 0, 0).is_err()); // H == 0
        assert!(MultiNodeCutDetector::new(10, 5, 0).is_err()); // L == 0
    }
}
