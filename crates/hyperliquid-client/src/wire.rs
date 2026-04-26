//! Wire-format helpers Hyperliquid signing expects.
//!
//! The SDK references these exact formatting rules in `helpers.rs`; see the
//! test module for fixtures copied verbatim from upstream so any drift would
//! show up as a signature mismatch against the golden vectors.

pub const WIRE_DECIMALS: u8 = 8;

/// Canonical float rendering used inside HL signing: 8 decimals, trailing
/// zeros stripped, trailing `.` stripped, `-0` collapsed to `0`.
#[must_use]
pub fn float_to_wire(x: f64) -> String {
    let mut s = format!("{:.*}", WIRE_DECIMALS as usize, x);
    while s.ends_with('0') {
        s.pop();
    }
    if s.ends_with('.') {
        s.pop();
    }
    if s == "-0" {
        "0".to_string()
    } else {
        s
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn float_to_wire_matches_sdk_vectors() {
        assert_eq!(float_to_wire(0.0), "0");
        assert_eq!(float_to_wire(-0.0), "0");
        assert_eq!(float_to_wire(-0.0000), "0");
        assert_eq!(float_to_wire(0.00076000), "0.00076");
        assert_eq!(float_to_wire(0.00000001), "0.00000001");
        assert_eq!(float_to_wire(0.12345678), "0.12345678");
        assert_eq!(float_to_wire(87_654_321.123_456_78), "87654321.12345678");
        assert_eq!(float_to_wire(987_654_321.000_000_00), "987654321");
        assert_eq!(float_to_wire(87_654_321.1234), "87654321.1234");
        assert_eq!(float_to_wire(0.000_760), "0.00076");
        assert_eq!(float_to_wire(0.00076), "0.00076");
        assert_eq!(float_to_wire(987_654_321.0), "987654321");
    }
}
