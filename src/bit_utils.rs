/// Generate a bit-mask of the specified width
///
/// For example, 3 will become 0b111.
pub const fn mask(num_bits: u32) -> u32 {
    u32::MAX.unbounded_shr(u32::BITS.saturating_sub(num_bits))
}

/// Returns the value represented by the `num_bits` most significant
/// bits of `data`.
///
/// For example, extracting 8 bits from 0x12345678 will return 0x12
pub fn extract_msb(data: u32, num_bits: u32) -> Option<u32> {
    data.checked_shr(u32::BITS.checked_sub(num_bits)?)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mask() {
        assert_eq!(mask(0), 0);
        assert_eq!(mask(1), 0b1);
        assert_eq!(mask(2), 0b11);
        assert_eq!(mask(3), 0b111);
        assert_eq!(mask(30), 0b00111111_11111111_11111111_11111111);
        assert_eq!(mask(31), 0b01111111_11111111_11111111_11111111);
        assert_eq!(mask(32), 0b11111111_11111111_11111111_11111111);
        assert_eq!(mask(33), 0b11111111_11111111_11111111_11111111);
    }

    #[test]
    fn test_extract_msb() {
        assert_eq!(extract_msb(u32::MAX, 32), Some(u32::MAX));
        assert_eq!(extract_msb(0, 32), Some(0));
        assert_eq!(
            extract_msb(0b11010011_11111111_00000000_10101010, 1),
            Some(0b1)
        );
        assert_eq!(
            extract_msb(0b11010011_11111111_00000000_10101010, 1),
            Some(0b1)
        );
        assert_eq!(
            extract_msb(0b11010011_11111111_00000000_10101010, 2),
            Some(0b11)
        );
        assert_eq!(
            extract_msb(0b11010011_11111111_00000000_10101010, 8),
            Some(0b11010011)
        );
        assert_eq!(
            extract_msb(0b11010011_11111111_00000000_10101010, 5),
            Some(0b11010)
        );
        assert_eq!(
            extract_msb(0b11010011_11111111_00000000_10101010, 16),
            Some(0b11010011_11111111)
        );
        assert_eq!(
            extract_msb(0b11010011_11111111_00000000_10101010, 32),
            Some(0b11010011_11111111_00000000_10101010)
        );
        assert_eq!(extract_msb(0b11010011_11111111_00000000_10101010, 0), None);
        assert_eq!(extract_msb(0b11010011_11111111_00000000_10101010, 33), None);
    }
}
