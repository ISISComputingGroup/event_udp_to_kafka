use crate::gps_time::GpsTime;

pub const HEADER_LEN_BYTES: usize = 16 * 4; // 16 4-byte words

pub struct UdpHeaderView<'a> {
    content: &'a [u8],
}

impl<'a> UdpHeaderView<'a> {
    pub fn new(content: &[u8]) -> Option<UdpHeaderView<'_>> {
        (content.len() >= HEADER_LEN_BYTES).then_some(UdpHeaderView { content })
    }

    pub fn word(&self, n: usize) -> [u8; 4] {
        self.content[4 * n..4 * n + 4]
            .try_into()
            .expect("content not a multiple of 4 bytes")
    }

    pub fn is_neutron_data_header(&self) -> bool {
        self.word(0) == [0xFF, 0xFF, 0xFF, 0xFF] && self.word(1) == [0xFF, 0xFF, 0xFF, 0xFF]
    }

    pub fn frame_number(&self) -> u32 {
        u32::from_be_bytes(self.word(3))
    }

    pub fn events_in_frame(&self) -> u32 {
        u32::from_be_bytes(self.word(7))
    }

    pub fn ppp_in_frame(&self) -> u16 {
        u16::from_be_bytes(self.word(8)[0..2].try_into().unwrap())
    }

    pub fn vetoes(&self) -> u16 {
        u16::from_be_bytes(self.word(9)[0..2].try_into().unwrap())
    }

    pub fn period_number(&self) -> u16 {
        u16::from_be_bytes(self.word(6)[0..2].try_into().unwrap())
    }

    pub fn gps_time(&self) -> GpsTime {
        GpsTime::from_packed_repr(u64::from_be_bytes(
            self.content[4 * 4..6 * 4]
                .try_into()
                .expect("content length was already checked"),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_raw_udp_message(num_events: usize, ppp: u16, gps_time: u64) -> Vec<u8> {
        // Note: 4-byte words
        // Total header length: 60 bytes (15 words)
        [255_u8; 4] // Header word 0: 'running' header marker
            .iter()
            .chain(&[255_u8; 4]) // Header word 1: neutron data header marker
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
        let header = UdpHeaderView::new(&msg).unwrap();

        assert!(header.is_neutron_data_header());
        assert_eq!(header.events_in_frame(), 10);
        assert_eq!(header.ppp_in_frame(), 23);
    }
}
