pub fn heights_to_scan(last_observed_height: Option<u64>, new_head_height: u64) -> Vec<u64> {
    match last_observed_height {
        Some(last) if new_head_height <= last => Vec::new(),
        Some(last) => ((last + 1)..=new_head_height).collect(),
        None => vec![new_head_height],
    }
}

#[cfg(test)]
mod tests {
    use super::heights_to_scan;

    #[test]
    fn scans_current_head_on_first_observation() {
        assert_eq!(heights_to_scan(None, 42), vec![42]);
    }

    #[test]
    fn gap_fill_includes_intermediate_heads() {
        assert_eq!(heights_to_scan(Some(10), 14), vec![11, 12, 13, 14]);
    }

    #[test]
    fn duplicate_or_reorged_height_does_not_rescan() {
        assert!(heights_to_scan(Some(10), 10).is_empty());
        assert!(heights_to_scan(Some(10), 9).is_empty());
    }
}
