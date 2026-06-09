//! Multi-resolution `l2Book` support: the closed ladder of aggregation
//! settings HL actually accepts, plus exact integer math reproducing the
//! bucket labels HL assigns at each resolution.
//!
//! All behavior here was verified live against `api.hyperliquid.xyz` on
//! 2026-06-09 (see `docs/hyperliquid-l2-book-cache-design.md` §3):
//!
//! - The only accepted parameter combinations are `nSigFigs` ∈ {2,3,4,5},
//!   with `mantissa` allowed only alongside `nSigFigs: 5` and only the
//!   values {2,5}. `nSigFigs: null` is byte-identical to `nSigFigs: 5`.
//!   Every other combination returns HTTP 500 with literal body `null` —
//!   indistinguishable from a real outage at the status-code level — so the
//!   resolution is modeled as a closed enum and invalid params are
//!   unrepresentable client-side.
//! - Bucket labels round *away from the spread*: ask prices ceil up to
//!   their bucket label, bid prices floor down (e.g. bid 61,759 →
//!   `nSigFigs:2` label 61,000, not the nearest 62,000). A coarse label is
//!   therefore the taker-worst-case price of its bucket's contents.
//! - The 20-levels-per-side response cap applies at every resolution.

use serde_json::Value;

/// The complete `l2Book` resolution ladder, finest to coarsest. At a ~$61k
/// price the bucket steps are $1 / $2 / $5 / $10 / $100 / $1,000.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum L2BookResolution {
    SigFigs5,
    SigFigs5Mantissa2,
    SigFigs5Mantissa5,
    SigFigs4,
    SigFigs3,
    SigFigs2,
}

impl L2BookResolution {
    /// Every resolution, ordered finest to coarsest — the iteration order
    /// for waterfall stitching and batch snapshot fetches.
    pub const LADDER_FINE_TO_COARSE: [L2BookResolution; 6] = [
        L2BookResolution::SigFigs5,
        L2BookResolution::SigFigs5Mantissa2,
        L2BookResolution::SigFigs5Mantissa5,
        L2BookResolution::SigFigs4,
        L2BookResolution::SigFigs3,
        L2BookResolution::SigFigs2,
    ];

    /// The `nSigFigs` wire value for this resolution.
    #[must_use]
    pub fn n_sig_figs(self) -> u8 {
        match self {
            L2BookResolution::SigFigs5
            | L2BookResolution::SigFigs5Mantissa2
            | L2BookResolution::SigFigs5Mantissa5 => 5,
            L2BookResolution::SigFigs4 => 4,
            L2BookResolution::SigFigs3 => 3,
            L2BookResolution::SigFigs2 => 2,
        }
    }

    /// The `mantissa` wire value, if this resolution carries one. HL only
    /// accepts a mantissa (2 or 5) alongside `nSigFigs: 5`.
    #[must_use]
    pub fn mantissa(self) -> Option<u8> {
        match self {
            L2BookResolution::SigFigs5Mantissa2 => Some(2),
            L2BookResolution::SigFigs5Mantissa5 => Some(5),
            L2BookResolution::SigFigs5
            | L2BookResolution::SigFigs4
            | L2BookResolution::SigFigs3
            | L2BookResolution::SigFigs2 => None,
        }
    }

    /// `/info` request body for an `l2Book` fetch at this resolution.
    /// `nSigFigs: 5` is sent explicitly (verified byte-identical to `null`);
    /// the `mantissa` key is present only for the two mantissa variants —
    /// HL 500s on a mantissa with any other `nSigFigs`.
    #[must_use]
    pub fn request_body(self, coin: &str) -> Value {
        let mut req = serde_json::json!({
            "type": "l2Book",
            "coin": coin,
            "nSigFigs": self.n_sig_figs(),
        });
        if let (Some(mantissa), Some(map)) = (self.mantissa(), req.as_object_mut()) {
            map.insert("mantissa".to_string(), serde_json::json!(mantissa));
        }
        req
    }
}

/// Bucket step at `raw` for `res`. With `D` = decimal digit count of `raw`:
/// `exponent = D − nSigFigs`; if `exponent < 0` the price already has fewer
/// significant digits than the resolution keeps, so no aggregation happens
/// (step 1 raw unit); otherwise `step = mantissa × 10^exponent` (mantissa 1
/// when the resolution carries none).
///
/// The math is scale-invariant: significant figures of the raw integer
/// equal significant figures of the decimal price (leading zeros don't
/// count), so callers can pass prices at any fixed-point scale. HL prices
/// are ≤5 sig figs and parsed at 8 decimals, leaving enormous u128 headroom.
fn bucket_step(raw: u128, res: L2BookResolution) -> u128 {
    let digits = raw.checked_ilog10().map_or(0, |log| log + 1);
    let exponent = digits as i32 - i32::from(res.n_sig_figs());
    if exponent < 0 {
        return 1;
    }
    u128::from(res.mantissa().unwrap_or(1)) * 10u128.pow(exponent as u32)
}

/// Smallest bucket label `>= raw` at `res` — how HL labels *ask* prices
/// (asks ceil away from the spread). `0` maps to `0`. Rounding a value just
/// below a power of ten may land on a label with one more digit (9,990 at
/// [`L2BookResolution::SigFigs2`] → 10,000); that matches HL.
#[must_use]
pub fn round_up_to_resolution(raw: u128, res: L2BookResolution) -> u128 {
    let step = bucket_step(raw, res);
    // Saturating: rounding up within one step of u128::MAX would otherwise
    // wrap in release builds. Unreachable for HL prices (≤5 sig figs at 8
    // decimals, ~10^13), but a wrapped label would silently corrupt the
    // stitch frontier, so saturate rather than rely on the caller's range.
    raw.div_ceil(step).saturating_mul(step)
}

/// Largest bucket label `<= raw` at `res` — how HL labels *bid* prices
/// (bids floor away from the spread).
#[must_use]
pub fn round_down_to_resolution(raw: u128, res: L2BookResolution) -> u128 {
    let step = bucket_step(raw, res);
    raw - raw % step
}

/// Largest label strictly below `label` at `res`; `prev_label(0) == 0`.
/// Computed as `round_down(label − 1)` so the step is derived from the
/// magnitude *below* a power-of-ten boundary, where it is smaller
/// (`prev_label(10_000, SigFigs2) == 9_900`, not 9,000).
#[must_use]
pub fn prev_label(label: u128, res: L2BookResolution) -> u128 {
    round_down_to_resolution(label.saturating_sub(1), res)
}

/// Smallest label strictly above `label` at `res`; saturates at `u128::MAX`.
#[must_use]
pub fn next_label(label: u128, res: L2BookResolution) -> u128 {
    round_up_to_resolution(label.saturating_add(1), res)
}

#[cfg(test)]
mod tests {
    use super::*;
    use L2BookResolution::{
        SigFigs2, SigFigs3, SigFigs4, SigFigs5, SigFigs5Mantissa2, SigFigs5Mantissa5,
    };

    /// HL prices parse at 8 decimals; golden vectors use `price × 10^8`.
    const E8: u128 = 100_000_000;

    #[test]
    fn golden_vectors_match_live_label_observations() {
        // Asks ceil: full-precision best ask 61,671 as labeled by coarser
        // concurrent snapshots (2026-06-09, UBTC/USDC).
        assert_eq!(
            round_up_to_resolution(61_671 * E8, SigFigs5Mantissa5),
            61_675 * E8
        );
        assert_eq!(round_up_to_resolution(61_671 * E8, SigFigs3), 61_700 * E8);
        assert_eq!(round_up_to_resolution(61_671 * E8, SigFigs2), 62_000 * E8);
        // Ask 61,751: nearest-rounding would give 61,750 in both cases.
        assert_eq!(
            round_up_to_resolution(61_751 * E8, SigFigs5Mantissa5),
            61_755 * E8
        );
        assert_eq!(round_up_to_resolution(61_751 * E8, SigFigs4), 61_760 * E8);
        // Bids floor: bid 61,759's coarse labels (nSigFigs:2 nearest would
        // be 62,000 — HL floored to 61,000).
        assert_eq!(
            round_down_to_resolution(61_759 * E8, SigFigs5Mantissa5),
            61_755 * E8
        );
        assert_eq!(round_down_to_resolution(61_759 * E8, SigFigs4), 61_750 * E8);
        assert_eq!(round_down_to_resolution(61_759 * E8, SigFigs3), 61_700 * E8);
        assert_eq!(round_down_to_resolution(61_759 * E8, SigFigs2), 61_000 * E8);
        // Best bid 61,667 floors past the nearest 61,700 bucket.
        assert_eq!(round_down_to_resolution(61_667 * E8, SigFigs3), 61_600 * E8);
        // Rounding an exact label is the identity.
        assert_eq!(round_up_to_resolution(61_760 * E8, SigFigs4), 61_760 * E8);
    }

    #[test]
    fn cross_magnitude_boundary_uses_step_below_the_boundary() {
        // Rounding up just below a power of ten gains a digit...
        assert_eq!(round_up_to_resolution(9_990, SigFigs2), 10_000);
        // ...and stepping back down must use the smaller step that applies
        // below the boundary, not 10,000's own $1k step.
        assert_eq!(prev_label(10_000, SigFigs2), 9_900);
        assert_eq!(next_label(9_900, SigFigs2), 10_000);
    }

    #[test]
    fn small_values_below_resolution_are_unaggregated() {
        // Any raw with <= nSigFigs digits is already exactly representable:
        // step 1, both roundings are the identity.
        for res in L2BookResolution::LADDER_FINE_TO_COARSE {
            let max_digits = match res.mantissa() {
                // Mantissa variants aggregate at exactly 5 digits, so only
                // 4-and-fewer-digit values are guaranteed fixed points.
                Some(_) => 4,
                None => u32::from(res.n_sig_figs()),
            };
            for raw in [1u128, 7, 99, 101, 9_999] {
                if raw.ilog10() + 1 > max_digits {
                    continue;
                }
                assert_eq!(round_up_to_resolution(raw, res), raw, "{res:?} {raw}");
                assert_eq!(round_down_to_resolution(raw, res), raw, "{res:?} {raw}");
            }
        }
        // At exactly 5 digits the mantissa becomes the step.
        assert_eq!(round_down_to_resolution(61_671, SigFigs5Mantissa5), 61_670);
        assert_eq!(round_up_to_resolution(61_671, SigFigs5Mantissa5), 61_675);
        assert_eq!(round_down_to_resolution(61_671, SigFigs5Mantissa2), 61_670);
        assert_eq!(round_up_to_resolution(61_671, SigFigs5Mantissa2), 61_672);
    }

    #[test]
    fn rounding_invariants_hold_across_magnitudes() {
        // Varied values spanning magnitudes 1..10^14, biased toward digit
        // boundaries and non-round interiors.
        const SWEEP: [u128; 50] = [
            1,
            2,
            5,
            7,
            9,
            10,
            11,
            19,
            42,
            55,
            99,
            100,
            101,
            123,
            500,
            999,
            1_000,
            1_001,
            2_500,
            6_167,
            9_990,
            9_999,
            10_000,
            10_001,
            12_345,
            55_555,
            61_671,
            61_759,
            99_999,
            100_000,
            123_456,
            999_999,
            1_000_000,
            1_234_567,
            5_000_005,
            9_999_999,
            12_345_678,
            99_999_999,
            100_000_001,
            616_710_000,
            999_999_999,
            6_167_100_000_000,
            6_175_900_000_000,
            6_200_000_000_000,
            9_999_000_000_000,
            10_000_000_000_001,
            12_345_678_901_234,
            55_555_555_555_555,
            99_999_999_999_999,
            100_000_000_000_000,
        ];
        for res in L2BookResolution::LADDER_FINE_TO_COARSE {
            for raw in SWEEP {
                let up = round_up_to_resolution(raw, res);
                let down = round_down_to_resolution(raw, res);
                assert!(up >= raw, "{res:?} {raw}");
                assert!(down <= raw, "{res:?} {raw}");
                assert_eq!(round_up_to_resolution(up, res), up, "{res:?} {raw}");
                assert_eq!(round_down_to_resolution(down, res), down, "{res:?} {raw}");
                assert!(down <= up, "{res:?} {raw}");
                if down != raw {
                    // raw is not a label: stepping back from the label above
                    // it must land strictly below raw.
                    assert!(prev_label(up, res) < raw, "{res:?} {raw}");
                }
            }
        }
    }

    #[test]
    fn request_bodies_match_verified_wire_shapes() {
        assert_eq!(
            SigFigs5.request_body("@142"),
            serde_json::json!({"type": "l2Book", "coin": "@142", "nSigFigs": 5})
        );
        assert_eq!(
            SigFigs5Mantissa2.request_body("@142"),
            serde_json::json!({"type": "l2Book", "coin": "@142", "nSigFigs": 5, "mantissa": 2})
        );
        assert_eq!(
            SigFigs5Mantissa5.request_body("@142"),
            serde_json::json!({"type": "l2Book", "coin": "@142", "nSigFigs": 5, "mantissa": 5})
        );
        assert_eq!(
            SigFigs4.request_body("UBTC/USDC"),
            serde_json::json!({"type": "l2Book", "coin": "UBTC/USDC", "nSigFigs": 4})
        );
        assert_eq!(
            SigFigs3.request_body("UBTC/USDC"),
            serde_json::json!({"type": "l2Book", "coin": "UBTC/USDC", "nSigFigs": 3})
        );
        assert_eq!(
            SigFigs2.request_body("UBTC/USDC"),
            serde_json::json!({"type": "l2Book", "coin": "UBTC/USDC", "nSigFigs": 2})
        );
        // No mantissa key may leak into the non-mantissa variants.
        for res in [SigFigs5, SigFigs4, SigFigs3, SigFigs2] {
            let body = res.request_body("@142");
            assert!(body.as_object().expect("object").get("mantissa").is_none());
        }
    }
}
