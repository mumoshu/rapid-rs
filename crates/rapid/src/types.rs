//! Newtype primitives for protocol-level domain values.
//!
//! RULES §Types: bare `i64` / `u32` in public signatures is a code smell —
//! argument-swap bugs are real and the type system can catch them at
//! compile time.

use std::fmt;

/// Membership-view configuration identifier.
///
/// Computed deterministically from `(identifiers, endpoints)` via
/// `view::configuration_id_for`. Matches Java's `long` ID byte-for-byte —
/// we keep the same signed-arithmetic semantics. Wraps on overflow per
/// Java's two's-complement convention.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ConfigurationId(pub i64);

impl ConfigurationId {
    /// Java `MembershipView.currentConfigurationId` initial value.
    pub const SENTINEL: Self = Self(-1);

    /// Inner `i64`.
    #[must_use]
    pub const fn as_i64(self) -> i64 {
        self.0
    }
}

impl fmt::Display for ConfigurationId {
    #[allow(clippy::cast_sign_loss)]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:016x}", self.0 as u64)
    }
}

/// Index of one of the `K` monitoring rings (`0..K`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct RingNumber(pub u8);

impl RingNumber {
    /// Inner `u8`.
    #[must_use]
    pub const fn as_u8(self) -> u8 {
        self.0
    }

    /// Convert to a `usize` for vector indexing.
    #[must_use]
    pub const fn as_usize(self) -> usize {
        self.0 as usize
    }
}

impl From<RingNumber> for i32 {
    fn from(value: RingNumber) -> Self {
        Self::from(value.0)
    }
}

/// Position of an endpoint inside a sorted ring (used by classic Paxos).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct NodeIndex(pub usize);

impl NodeIndex {
    /// Inner `usize`.
    #[must_use]
    pub const fn as_usize(self) -> usize {
        self.0
    }
}

/// Classic Paxos round number.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Round(pub i32);

impl Round {
    /// Inner `i32`.
    #[must_use]
    pub const fn as_i32(self) -> i32 {
        self.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn configuration_id_sentinel() {
        assert_eq!(ConfigurationId::SENTINEL.as_i64(), -1);
    }

    #[test]
    fn ring_number_conversions() {
        let r = RingNumber(7);
        assert_eq!(r.as_u8(), 7);
        assert_eq!(r.as_usize(), 7);
        assert_eq!(i32::from(r), 7);
    }
}
