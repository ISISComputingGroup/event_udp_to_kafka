//! Utilities for interpreting headers from a UDP message.
//!
//! See https://isiscomputinggroup.github.io/ibex_developers_manual/specific_iocs/datastreaming/Datastreaming_udp_packet_formats.html
//! for details of packet format.

use crate::config::EventUdpToKafkaConfig;
use crate::gps_time::GpsTime;

/// Marker word for "start of header".
pub const HEADER_MARKER: &[u8; 4] = &[0xFF, 0xFF, 0xFF, 0xFF];

/// Minimum length of header in words.
/// This is the length of the fixed part of the header (13 words), plus a 1-word DDR checksum
pub const MINIMUM_HEADER_LEN_WORDS: usize = 14;

/// Minimum Length of header in bytes (14 4-byte words).
/// This is the length of the fixed part of the header, plus a 1-word DDR checksum
pub const MINIMUM_HEADER_LEN_BYTES: usize = MINIMUM_HEADER_LEN_WORDS * 4;

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum InvalidMessageReason {
    /// The content buffer was not long enough to contain a header
    ContentTooShort,
    /// The content buffer did not start with a header marker
    MissingHeaderMarker,
    /// Declared length is shorter than the minimum length of a header
    DeclaredLengthTooShort(usize),
    /// Declared length is longer than content buffer
    DeclaredLengthTooLong(usize),
}

/// View onto a UDP message byte-slice.
///
/// This struct provides helper methods for interpreting the bytes from the header of a UDP message.
#[derive(Debug)]
pub struct UdpMessageView<'a> {
    content: &'a [u8],
}

impl<'a> UdpMessageView<'a> {
    /// Create a new view onto a UDP message.
    ///
    /// The passed-in byte-slice may be longer than the actual message.
    ///
    /// This method will return None if:
    /// - The content buffer is not long enough to contain a header
    /// - The content buffer does not start with a header marker
    /// - The declared length is less than the length of the header itself
    /// - The content buffer is not long enough to contain the data-length declared by the header
    pub fn new(content: &[u8]) -> Result<UdpMessageView<'_>, InvalidMessageReason> {
        if content.len() < MINIMUM_HEADER_LEN_BYTES {
            return Err(InvalidMessageReason::ContentTooShort);
        }
        if !content.starts_with(HEADER_MARKER) {
            return Err(InvalidMessageReason::MissingHeaderMarker);
        }
        // Word 1, bits 24..=31 (the most-significant, big-endian byte) is always the
        // header marker byte `0xFF`.
        if content.get(4) != Some(&0xFF) {
            return Err(InvalidMessageReason::MissingHeaderMarker);
        }

        let view = UdpMessageView { content };
        let declared_length = view.total_length_bytes();

        if declared_length < MINIMUM_HEADER_LEN_BYTES {
            return Err(InvalidMessageReason::DeclaredLengthTooShort(
                declared_length,
            ));
        }
        if declared_length > content.len() {
            return Err(InvalidMessageReason::DeclaredLengthTooLong(declared_length));
        }
        Ok(view)
    }

    /// Extract a single word from the header
    fn header_word(&self, n: usize) -> [u8; 4] {
        assert!(n <= 15, "Invalid word requested from header");
        self.content[4 * n..4 * n + 4]
            .try_into()
            .expect("slice of length 4")
    }

    /// The total length, in 32-bit words, of the header and data for this message.
    pub fn total_length_words(&self) -> usize {
        ((u32::from_be_bytes(self.header_word(8)) >> 16) & 0xFFF) as usize
    }

    /// The total length, in bytes, of the header and data for this message.
    pub fn total_length_bytes(&self) -> usize {
        self.total_length_words() * 4
    }

    /// Length of the header, in 32-bit words
    pub fn header_length_words(&self) -> usize {
        self.header_word(1)[3] as usize
    }

    /// Length of the header, in bytes
    pub fn header_length_bytes(&self) -> usize {
        self.header_length_words() * 4
    }

    /// Frame number.
    pub fn frame_number(&self) -> u32 {
        u32::from_be_bytes(self.header_word(5))
    }

    /// Total events in this ISIS frame.
    ///
    /// Note: this is not the same as the total events in this UDP message; an ISIS
    /// frame may be split over multiple messages.
    pub fn events_in_frame(&self) -> u32 {
        u32::from_be_bytes(self.header_word(7))
    }

    /// Raw protons-per-pulse per frame; u8 exactly as transmitted over UDP.
    pub fn raw_ppp_per_frame(&self) -> u8 {
        self.header_word(8)[3]
    }

    /// uAh delivered during this ISIS frame.
    pub fn ppp_per_frame(&self, config: &EventUdpToKafkaConfig) -> f64 {
        self.raw_ppp_per_frame() as f64 * config.raw_to_uah_scaling()
    }

    /// Veto bits, as transmitted over UDP.
    pub fn vetoes(&self) -> u32 {
        u32::from_be_bytes(self.header_word(9))
    }

    /// Header flags (word 2, bits 0..=7), as transmitted over UDP.
    ///
    /// These flags are active-low: a bit that is low indicates the corresponding
    /// condition. When all bits are high, this is a neutron data packet.
    ///
    /// - Bit 0: End of run header marker
    /// - Bit 1: Veto frame packet header marker
    /// - Bit 2: Pause frame packet header marker
    /// - Bit 3: No frame sync (not implemented)
    fn header_flags(&self) -> u8 {
        self.header_word(2)[3]
    }

    /// The board type, as transmitted over UDP.
    /// For example, for a PC3544MS board, this will be 3544.
    pub fn board_type(&self) -> u16 {
        u16::from_be_bytes(
            self.header_word(2)[0..2]
                .try_into()
                .expect("slice of length 2"),
        )
    }

    /// Period number.
    pub fn period_number(&self) -> u16 {
        u16::from_be_bytes(
            self.header_word(6)[0..2]
                .try_into()
                .expect("slice of length 2"),
        )
    }

    /// GPS timestamp of this message.
    pub fn gps_time(&self) -> GpsTime {
        GpsTime::from_packed_repr(u64::from_be_bytes(
            self.content[3 * 4..5 * 4]
                .try_into()
                .expect("slice of length 8"),
        ))
    }

    /// Packet type.
    ///
    /// Determined from the header flags in word 2 (bits 0..=7), which are active-low.
    /// When all flag bits are high, this is a neutron data packet.
    pub fn packet_type(&self) -> UdpPacketType {
        match self.header_flags() {
            0b11111111 => UdpPacketType::NeutronData,
            0b11111110 => UdpPacketType::EndOfRun,
            0b11111101 => UdpPacketType::VetoFrame,
            _ => UdpPacketType::Invalid,
        }
    }

    pub fn board_specific_parameters(&self) -> &[u8] {
        // TODO: comment explaining this logic
        &self.content[13 * 4..(self.header_length_bytes() - 4)]
    }

    /// Get the non-header bytes from this message.
    ///
    /// For neutron frames, these bytes contain the neutron event data.
    pub fn data_bytes(&self) -> &[u8] {
        &self.content[self.header_length_bytes()..self.total_length_bytes()]
    }
}

/// Types of packets we may receive over UDP.
#[derive(Debug, Eq, PartialEq, strum::EnumIter, strum::IntoStaticStr)]
pub enum UdpPacketType {
    VetoFrame,
    SampleEnvironment,
    NeutronData,
    EndOfRun,
    Invalid,
}

impl UdpPacketType {
    pub fn as_prometheus_label(&self) -> &'static str {
        self.into()
    }
}

#[cfg(test)]
mod tests {
    use crate::boards::pc3544ms::Pc3544ms;
    use super::*;
    use crate::testing::make_udp_header;

    #[test]
    fn test_header() {
        let msg = make_udp_header::<Pc3544ms>(10, 23)
            .into_iter()
            .chain([0_u8; 9999])
            .collect::<Vec<_>>();
        let msg_view = UdpMessageView::new(&msg).unwrap();

        assert_eq!(msg_view.events_in_frame(), 10);
        assert_eq!(
            msg_view.total_length_bytes(),
            64 + 8 * 10  // 64 byte header + 10x 8-byte events
        );
        assert_eq!(
            msg_view.total_length_words(),
            16 + 2 * 10  // 16 word header + 10x 2-word events
        );
        assert_eq!(msg_view.board_type(), 3544);

        assert_eq!(msg_view.data_bytes().len(), 8 * 10);
    }

    #[test]
    fn test_header_no_events() {
        let msg = make_udp_header::<Pc3544ms>(0, 23);
        let header = UdpMessageView::new(&msg).unwrap();

        assert_eq!(header.events_in_frame(), 0);
        assert_eq!(header.total_length_bytes(), 64);
        assert_eq!(header.total_length_words(), 16);

        assert_eq!(header.data_bytes().len(), 0);
    }

    #[test]
    fn test_header_ppp() {
        let msg = make_udp_header::<Pc3544ms>(0, 23);
        let header = UdpMessageView::new(&msg).unwrap();

        assert_eq!(header.raw_ppp_per_frame(), 23);

        let mut config = EventUdpToKafkaConfig::make_default_config();
        config.raw_to_uah_scaling = Some(123.456);

        assert!((header.ppp_per_frame(&config) - 23. * 123.456).abs() < 0.01);
    }

    #[test]
    fn test_message_type() {
        let msg = make_udp_header::<Pc3544ms>(0, 23);
        let header = UdpMessageView::new(&msg).unwrap();

        assert_eq!(header.packet_type(), UdpPacketType::NeutronData);
    }

    #[test]
    fn test_invalid_header_short_content() {
        let view = UdpMessageView::new(&[0]);
        assert_eq!(view.unwrap_err(), InvalidMessageReason::ContentTooShort);
    }

    #[test]
    fn test_invalid_header_no_header_marker() {
        let view = UdpMessageView::new(&[0; 5000]);
        assert_eq!(view.unwrap_err(), InvalidMessageReason::MissingHeaderMarker);
    }

    #[test]
    fn test_invalid_header_length_longer_than_content() {
        let view = UdpMessageView::new(&[0xFF; 64]);
        assert_eq!(
            view.unwrap_err(),
            InvalidMessageReason::DeclaredLengthTooLong(0xFFF * 4)
        );
    }

    #[test]
    fn test_invalid_header_length_shorter_than_header() {
        // Valid word 0 marker and valid word 1 marker byte (0xFF), but a declared
        // length of zero (word 8 all-zero), which is shorter than a header.
        let bytes = [0xFF, 0xFF, 0xFF, 0xFF, 0xFF]
            .iter()
            .chain(&[0; 5000])
            .copied()
            .collect::<Vec<_>>();
        let view = UdpMessageView::new(&bytes);
        assert_eq!(
            view.unwrap_err(),
            InvalidMessageReason::DeclaredLengthTooShort(0)
        );
    }
}
