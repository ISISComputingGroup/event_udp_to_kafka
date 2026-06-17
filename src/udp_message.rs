//! Utilities for interpreting headers from a UDP message.

use crate::config::EventUdpToKafkaConfig;
use crate::gps_time::GpsTime;

/// Marker word for "start of header".
pub const HEADER_MARKER: &[u8; 4] = &[0xFF, 0xFF, 0xFF, 0xFF];

/// Marker for a "veto frame" data packet.
pub const VETO_FRAME_HEADER: &[u8; 4] = &[0b10111111, 0xFF, 0xFF, 0xFF];

/// Marker for a "sample environment" data packet.
pub const SE_FRAME_HEADER: &[u8; 4] = &[0b11011111, 0xFF, 0xFF, 0xFF];

pub const END_OF_RUN_HEADER: &[u8; 4] = &[0b01111111, 0xFF, 0xFF, 0xFF];

/// Marker for a neutron event data packet.
pub const NEUTRON_HEADER: &[u8; 4] = &[0xFF, 0xFF, 0xFF, 0xFF];

/// Length of header in words
pub const HEADER_LEN_WORDS: usize = 16;

/// Length of header in bytes (16 4-byte words).
pub const HEADER_LEN_BYTES: usize = HEADER_LEN_WORDS * 4;

/// View onto a UDP message byte-slice.
///
/// This struct provides helper methods for interpreting the bytes from the header of a UDP message.
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
    pub fn new(content: &[u8]) -> Option<UdpMessageView<'_>> {
        let view = (content.len() >= HEADER_LEN_BYTES && content.starts_with(HEADER_MARKER))
            .then_some(UdpMessageView { content })?;

        let declared_length = view.total_length_bytes();

        (declared_length >= HEADER_LEN_BYTES && content.len() >= declared_length).then_some(view)
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
        (u32::from_be_bytes(self.header_word(13)) & 0xFFF) as usize
    }

    /// The total length, in bytes, of the header and data for this message.
    pub fn total_length_bytes(&self) -> usize {
        self.total_length_words() * 4
    }

    /// Frame number.
    pub fn frame_number(&self) -> u32 {
        u32::from_be_bytes(self.header_word(3))
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
        self.header_word(8)[0]
    }

    /// uAh delivered during this ISIS frame.
    pub fn ppp_per_frame(&self, config: &EventUdpToKafkaConfig) -> f64 {
        self.raw_ppp_per_frame() as f64 * config.raw_to_uah_scaling()
    }

    /// Veto bits, as transmitted over UDP.
    pub fn vetoes(&self) -> u16 {
        u16::from_be_bytes(self.header_word(9)[0..2].try_into().unwrap())
    }

    /// Period number.
    pub fn period_number(&self) -> u16 {
        u16::from_be_bytes(self.header_word(6)[0..2].try_into().unwrap())
    }

    /// GPS timestamp of this message.
    pub fn gps_time(&self) -> GpsTime {
        GpsTime::from_packed_repr(u64::from_be_bytes(
            self.content[4 * 4..6 * 4]
                .try_into()
                .expect("slice of length 8"),
        ))
    }

    /// Packet type.
    pub fn packet_type(&self) -> UdpPacketType {
        match &self.header_word(1) {
            NEUTRON_HEADER => UdpPacketType::NeutronData,
            VETO_FRAME_HEADER => UdpPacketType::VetoFrame,
            SE_FRAME_HEADER => UdpPacketType::SampleEnvironment,
            END_OF_RUN_HEADER => UdpPacketType::EndOfRun,
            _ => UdpPacketType::Invalid,
        }
    }

    /// Get the non-header bytes from this message.
    ///
    /// For neutron frames, these bytes contain the neutron event data.
    pub fn data_bytes(&self) -> &[u8] {
        &self.content[HEADER_LEN_BYTES..self.total_length_bytes()]
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
    use super::*;
    use crate::testing::make_raw_neutron_udp_header;

    #[test]
    fn test_header() {
        let msg = make_raw_neutron_udp_header(10, 23)
            .into_iter()
            .chain([0_u8; 9999])
            .collect::<Vec<_>>();
        let msg_view = UdpMessageView::new(&msg).unwrap();

        assert_eq!(msg_view.events_in_frame(), 10);
        assert_eq!(msg_view.total_length_bytes(), HEADER_LEN_BYTES + 8 * 10);
        assert_eq!(msg_view.total_length_words(), HEADER_LEN_WORDS + 2 * 10);

        assert_eq!(msg_view.data_bytes().len(), 8 * 10);
    }

    #[test]
    fn test_header_no_events() {
        let msg = make_raw_neutron_udp_header(0, 23);
        let header = UdpMessageView::new(&msg).unwrap();

        assert_eq!(header.events_in_frame(), 0);
        assert_eq!(header.total_length_bytes(), HEADER_LEN_BYTES);
        assert_eq!(header.total_length_words(), HEADER_LEN_WORDS);

        assert_eq!(header.data_bytes().len(), 0);
    }

    #[test]
    fn test_header_ppp() {
        let msg = make_raw_neutron_udp_header(0, 23);
        let header = UdpMessageView::new(&msg).unwrap();

        assert_eq!(header.raw_ppp_per_frame(), 23);

        let config = EventUdpToKafkaConfig {
            raw_to_uah_scaling: Some(123.456),
            ..Default::default()
        };

        assert!((header.ppp_per_frame(&config) - 23. * 123.456).abs() < 0.01);
    }

    #[test]
    fn test_message_type() {
        let msg = make_raw_neutron_udp_header(0, 23);
        let header = UdpMessageView::new(&msg).unwrap();

        assert_eq!(header.packet_type(), UdpPacketType::NeutronData);
    }
}
