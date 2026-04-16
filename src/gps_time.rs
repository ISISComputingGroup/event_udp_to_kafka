use chrono::{DateTime, NaiveDate, NaiveDateTime, Utc};

pub struct GpsTime {
    content: u64
}

// 8-byte GPS timestamp in the streamed packet format
impl GpsTime {

    pub fn from_packed_repr(packed_repr: u64) -> GpsTime {
        GpsTime { content: packed_repr }
    }

    pub fn years(&self) -> u64 {
        self.content >> (24 + 32)
    }

    pub fn days(&self) -> u64 {
        (self.content >> (15 + 32)) & 0b111111111  // 9 bits
    }

    pub fn hours(&self) -> u64 {
        (self.content >> (10 + 32)) & 0b11111  // 5 bits
    }

    pub fn minutes(&self) -> u64 {
        (self.content >> (4 + 32)) & 0b111111  // 6 bits
    }

    pub fn seconds(&self) -> u64 {
        (self.content >> 30) & 0b111111  // 6 bits
    }

    pub fn milliseconds(&self) -> u64 {
        (self.content >> 20) & 0b1111111111  // 10 bits
    }

    pub fn microseconds(&self) -> u64 {
        (self.content >> 10) & 0b1111111111  // 10 bits
    }

    pub fn nanoseconds(&self) -> u64 {
        self.content & 0b1111111111  // 10 bits
    }

    pub fn nanoseconds_since_epoch(&self) -> Option<u64> {
        const NANOS_PER_SEC: u64 = 1_000_000_000;

        let years: i32 = self.years().try_into().ok()?;
        let days = self.days().try_into().ok()?;
        println!("{years} {days}");
        let dt: DateTime<Utc> = DateTime::from_naive_utc_and_offset(
            NaiveDateTime::from(NaiveDate::from_yo_opt(years + 2000, days)?),
            Utc
        );

        let timestamp_s: u64 = dt.timestamp().try_into().ok()?;
        Some(timestamp_s * NANOS_PER_SEC
            + self.hours() * 3600 * NANOS_PER_SEC
            + self.minutes() * 60 * NANOS_PER_SEC
            + self.seconds() * NANOS_PER_SEC
            + self.milliseconds() * 1_000_000
            + self.microseconds() * 1000
            + self.nanoseconds()
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_gps_time_zero() {
        assert_eq!(GpsTime { content: 0 }.years(), 0);
        assert_eq!(GpsTime { content: 0 }.days(), 0);
        assert_eq!(GpsTime { content: 0 }.hours(), 0);
        assert_eq!(GpsTime { content: 0 }.minutes(), 0);
        assert_eq!(GpsTime { content: 0 }.seconds(), 0);
        assert_eq!(GpsTime { content: 0 }.milliseconds(), 0);
        assert_eq!(GpsTime { content: 0 }.microseconds(), 0);
        assert_eq!(GpsTime { content: 0 }.nanoseconds(), 0);
    }

    // Test decoding streamed GpsTime packet format.
    #[test]
    fn test_gps_time() {
        assert_eq!(GpsTime { content: 0b11010011_11111111_11111111_11111111_11111111_11111111_11111111_11111111 }.years(), 0b11010011);
        assert_eq!(GpsTime { content: !0b11010011_11111111_11111111_11111111_11111111_11111111_11111111_11111111 }.years(), 0b00101100);

        assert_eq!(GpsTime { content: 0b11111111_10101101_01111111_11111111_11111111_11111111_11111111_11111111 }.days(), 0b101011010);
        assert_eq!(GpsTime { content: !0b11111111_10101101_01111111_11111111_11111111_11111111_11111111_11111111 }.days(), 0b010100101);

        assert_eq!(GpsTime { content: 0b11111111_11111111_10101011_11111111_11111111_11111111_11111111_11111111 }.hours(), 0b01010);
        assert_eq!(GpsTime { content: !0b11111111_11111111_10101011_11111111_11111111_11111111_11111111_11111111 }.hours(), 0b10101);

        assert_eq!(GpsTime { content: 0b11111111_11111111_11111101_00101111_11111111_11111111_11111111_11111111 }.minutes(), 0b010010);
        assert_eq!(GpsTime { content: !0b11111111_11111111_11111101_00101111_11111111_11111111_11111111_11111111 }.minutes(), 0b101101);

        assert_eq!(GpsTime { content: 0b11111111_11111111_11111111_11110100_10111111_11111111_11111111_11111111 }.seconds(), 0b010010);
        assert_eq!(GpsTime { content: !0b11111111_11111111_11111111_11110100_10111111_11111111_11111111_11111111 }.seconds(), 0b101101);

        assert_eq!(GpsTime { content: 0b11111111_11111111_11111111_11111111_11010100_10101111_11111111_11111111 }.milliseconds(), 0b0101001010);
        assert_eq!(GpsTime { content: !0b11111111_11111111_11111111_11111111_11010100_10101111_11111111_11111111 }.milliseconds(), 0b1010110101);

        assert_eq!(GpsTime { content: 0b11111111_11111111_11111111_11111111_11111111_11110101_00101011_11111111 }.microseconds(), 0b0101001010);
        assert_eq!(GpsTime { content: !0b11111111_11111111_11111111_11111111_11111111_11110101_00101011_11111111 }.microseconds(), 0b1010110101);

        assert_eq!(GpsTime { content: 0b11111111_11111111_11111111_11111111_11111111_11111111_11111101_01001010 }.nanoseconds(), 0b0101001010);
        assert_eq!(GpsTime { content: !0b11111111_11111111_11111111_11111111_11111111_11111111_11111101_01001010 }.nanoseconds(), 0b1010110101);
    }

    #[test]
    fn test_nanoseconds_since_epoch() {
        // April 16th, 2026 at 17:09:35.123456789
        let t = GpsTime::from_packed_repr(
            (26 << (32 + 24))
            + (106 << (32 + 15))
            + (17 << (32 + 10))
            + (9 << (32 + 4))
            + (35 << 30)
            + (123 << 20)
            + (456 << 10)
            + (789)
        );

        assert_eq!(t.nanoseconds_since_epoch().unwrap(), 1776359375123456789);
    }

    #[test]
    fn test_nanoseconds_since_epoch_day_zero() {
        let t = GpsTime::from_packed_repr(
            (26 << (32 + 24))
                + (0 << (32 + 15))  // Invalid - day zero
                + (17 << (32 + 10))
                + (9 << (32 + 4))
                + (35 << 30)
                + (123 << 20)
                + (456 << 10)
                + (789)
        );

        assert_eq!(t.nanoseconds_since_epoch(), None);
    }
}