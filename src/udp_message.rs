use crate::gps_time::GpsTime;

/// Marker word for "start of header"
pub const HEADER_MARKER: &[u8; 4] = &[0xFF, 0xFF, 0xFF, 0xFF];
const VETO_FRAME_HEADER: &[u8; 4] = &[0xFC, 0xFF, 0xFF, 0xFF];
const SE_FRAME_HEADER: &[u8; 4] = &[0xFD, 0xFF, 0xFF, 0xFF];
const NEUTRON_HEADER: &[u8; 4] = &[0xFF, 0xFF, 0xFF, 0xFF];

/// Length of header in bytes (16 4-byte words)
pub const HEADER_LEN_BYTES: usize = 16 * 4;

/// View onto a UDP message byte-slice.
pub struct UdpMessageView<'a> {
    content: &'a [u8],
}

impl<'a> UdpMessageView<'a> {
    /// Create a new view onto a UDP message, if the slice is long enough to contain a header,
    /// starts with a header marker, and is a multiple of 4-byte words.
    pub fn new(content: &[u8]) -> Option<UdpMessageView<'_>> {
        (content.len() >= HEADER_LEN_BYTES && content.starts_with(HEADER_MARKER) && content.len().is_multiple_of(4))
            .then_some(UdpMessageView { content })
    }

    /// Extract a single word from the header
    fn header_word(&self, n: usize) -> [u8; 4] {
        assert!(n <= 15, "Invalid word requested from header");
        self.content[4 * n..4 * n + 4]
            .try_into()
            .expect("slice of length 4")
    }

    pub fn frame_number(&self) -> u32 {
        u32::from_be_bytes(self.header_word(3))
    }

    pub fn events_in_frame(&self) -> u32 {
        u32::from_be_bytes(self.header_word(7))
    }

    pub fn ppp_in_frame(&self) -> u16 {
        u16::from_be_bytes(self.header_word(8)[0..2].try_into().unwrap())
    }

    pub fn vetoes(&self) -> u16 {
        u16::from_be_bytes(self.header_word(9)[0..2].try_into().unwrap())
    }

    pub fn period_number(&self) -> u16 {
        u16::from_be_bytes(self.header_word(6)[0..2].try_into().unwrap())
    }

    pub fn gps_time(&self) -> GpsTime {
        GpsTime::from_packed_repr(u64::from_be_bytes(
            self.content[4 * 4..6 * 4]
                .try_into()
                .expect("slice of length 8"),
        ))
    }

    /// Extract the packet type from the header.
    pub fn packet_type(&self) -> Option<UdpPacketType> {
        match &self.header_word(1) {
            NEUTRON_HEADER => Some(UdpPacketType::NeutronData),
            VETO_FRAME_HEADER => Some(UdpPacketType::VetoFrame),
            SE_FRAME_HEADER => Some(UdpPacketType::SampleEnvironment),
            _ => None,
        }
    }

    /// Get the non-header bytes from this message.
    ///
    /// For neutron frames, these bytes contain the neutron event data.
    pub fn data_bytes(&self) -> &[u8] {
        &self.content[HEADER_LEN_BYTES..]
    }
}

/// Types of packets we may receive over UDP.
pub enum UdpPacketType {
    VetoFrame,
    SampleEnvironment,
    NeutronData,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_raw_udp_message(num_events: usize, ppp: u16, gps_time: u64) -> Vec<u8> {
        // Note: 4-byte words
        // Total header length: 64 bytes (16 words)
        [255_u8; 4] // Header word 0: 'running' header marker
            .iter()
            .chain(NEUTRON_HEADER) // Header word 1: neutron data header marker
            .chain(&[0_u8; 4]) // Header word 2: information
            .chain(&[0_u8; 4]) // Header word 3: frame number
            .chain(&gps_time.to_be_bytes()) // Header words 4 & 5: GPS timestamp
            .chain(&[0_u8; 2]) // Header word 6: period number
            .chain(&[0_u8; 2]) // Header word 6: unused
            .chain(&(num_events as u32).to_be_bytes()) // Header word 7: events in frame
            .chain(&ppp.to_be_bytes()) // Header word 8: ppp_in_frame
            .chain(&[0_u8; 2]) // Header word 8: unused
            .chain(&[0_u8; 4]) // Header word 9: vetoes
            .chain(&[0_u8; 4]) // Header word 10: address of next frame
            .chain(&[0_u8; 4]) // Header word 11: address of next frame (word address)
            .chain(&[0_u8; 4]) // Header word 12: streamed frame number
            .chain(&[0_u8; 4]) // Header word 13: not used
            .chain(&[0_u8; 4]) // Header word 14: not used
            .chain(&[0_u8; 4]) // Header word 15: not used
            .copied()
            .collect()
    }

    #[test]
    fn test_header() {
        let msg = make_raw_udp_message(10, 23, 0);
        let header = UdpMessageView::new(&msg).unwrap();

        assert_eq!(header.events_in_frame(), 10);
        assert_eq!(header.ppp_in_frame(), 23);
    }
}
