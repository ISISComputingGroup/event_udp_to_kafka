use crate::WiringConfigRecord;
use crate::boards::{Board, EventData, TestableBoard};
use anyhow::{Context, bail};
use std::net::{IpAddr, Ipv4Addr};

pub struct Pc3634m1s;

impl Board for Pc3634m1s {
    const BOARD_ID: u16 = 3634;

    fn parse_raw_data(
        data: &[u8],
        wiring_config: &[&WiringConfigRecord],
    ) -> anyhow::Result<EventData> {
        if !data.len().is_multiple_of(8) {
            bail!("Event data is not a multiple of pairs of 4-byte words");
        }

        if wiring_config.len() > 1 {
            bail!("Got multiple wiring config rows for a PC3634 packet; this is invalid")
        }

        let packet_config = wiring_config.first().context("No wiring config")?;

        let (time_of_flight, pixel_id) = match packet_config.packet_type.as_str() {
            "DIM_OUT" => data
                .as_chunks::<8>()
                .0
                .iter()
                .map(|event| {
                    let tof = u32::from_be_bytes(event[0..4].try_into().unwrap()) & 0xFFFFFF;
                    let mut val = u32::from_be_bytes(event[4..8].try_into().unwrap());
                    val += packet_config.mantid_detector_id_start;
                    (tof as i32, val as i32)
                })
                .unzip(),
            _ => {
                bail!("Unable to process PC3634M1S events: unknown stream type in config");
            }
        };

        EventData::new(time_of_flight, pixel_id)
    }
}

impl TestableBoard for Pc3634m1s {
    fn make_fake_event() -> Vec<u8> {
        // tof = 456000 ns
        // detector ID = 123456789
        vec![
            0xFF, 0x06, 0xF5, 0x40, // 456000ns
            0x07, 0x5B, 0xCD, 0x15, // Detector ID = 123456789
        ]
    }

    fn make_fake_wiring_config(src_ip: Option<IpAddr>) -> Vec<WiringConfigRecord> {
        vec![WiringConfigRecord {
            brd_num: 0,
            brd_ref: "WLSF0".to_owned(),
            packet_type: "DIM_OUT".to_owned(),
            sw_pos: 0,
            streaming_ip: src_ip.unwrap_or(IpAddr::V4(Ipv4Addr::new(192, 168, 1, 1))),
            ch: 0,
            mantid_detector_id_start: 0,
            mantid_detector_id_length: 1,
            comment: "".to_owned(),
        }]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_pc3634_event() {
        let data = Pc3634m1s::make_fake_event();
        let wiring = Pc3634m1s::make_fake_wiring_config(None);

        let events = Pc3634m1s::parse_raw_data(&data, &wiring.iter().collect::<Vec<_>>())
            .expect("parsing should work");

        assert_eq!(events.time_of_flight(), [456_000]);
        assert_eq!(events.pixel_id(), [123456789]);
    }

    #[test]
    fn test_parse_empty_pc3634m1s_events() {
        let data = [];
        let wiring = Pc3634m1s::make_fake_wiring_config(None);

        let events = Pc3634m1s::parse_raw_data(&data, &wiring.iter().collect::<Vec<_>>())
            .expect("parsing should work");

        assert!(events.is_empty())
    }

    #[test]
    fn test_parse_pc3634_event_invalid_length() {
        let data = vec![0; 9]; // invalid length: 9 bytes
        let wiring = Pc3634m1s::make_fake_wiring_config(None);

        let events = Pc3634m1s::parse_raw_data(&data, &wiring.iter().collect::<Vec<_>>());

        assert!(events.is_err_and(|e| {
            e.to_string()
                .contains("not a multiple of pairs of 4-byte words")
        }));
    }
}
