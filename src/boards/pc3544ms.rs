use crate::WiringConfigRecord;
use crate::boards::{Board, EventData, TestableBoard};
use anyhow::{Context, bail};
use std::net::{IpAddr, Ipv4Addr};

pub struct Pc3544ms;

impl Pc3544ms {
    fn decode_position_packet(
        data: &[u8],
        wiring_config: &[&WiringConfigRecord],
    ) -> anyhow::Result<EventData> {
        let (time_of_flight, pixel_id) = data
            .as_chunks::<8>()
            .0
            .iter()
            .filter_map(|event| {
                let channel = (event[4] >> 2) & 0b111; // Bits 26..=28
                let event_position = u32::from_be_bytes(event[4..8].try_into().unwrap()) & 0xFFF;

                if let Some(channel_config) = wiring_config.iter().find(|c| c.ch == channel) {
                    let detector_id = (event_position
                        / (4096 / channel_config.mantid_detector_id_length))
                        + channel_config.mantid_detector_id_start;

                    let tof = u32::from_be_bytes(event[0..4].try_into().unwrap()) & 0xFFFFFF;

                    Some((tof as i32, detector_id as i32))
                } else {
                    None
                }
            })
            .unzip();

        EventData::new(time_of_flight, pixel_id)
    }

    fn decode_pulse_height_packet(
        data: &[u8],
        wiring_config: &[&WiringConfigRecord],
    ) -> anyhow::Result<EventData> {
        let (time_of_flight, pixel_id) = data
            .as_chunks::<8>()
            .0
            .iter()
            .filter_map(|event| {
                let channel = (event[4] >> 2) & 0b111; // Bits 26..=28
                let pulse_height = (u32::from_be_bytes(event[4..8].try_into().ok()?) >> 12) & 0xFFF;

                if let Some(channel_config) = wiring_config.iter().find(|c| c.ch == channel) {
                    let detector_id = (pulse_height
                        / (4096 / channel_config.mantid_detector_id_length))
                        + channel_config.mantid_detector_id_start;
                    let event_tof = u32::from_be_bytes(event[0..4].try_into().ok()?) & 0xFFFFFF;

                    Some((event_tof as i32, detector_id as i32))
                } else {
                    None
                }
            })
            .unzip();

        EventData::new(time_of_flight, pixel_id)
    }
}

impl Board for Pc3544ms {
    const BOARD_ID: u16 = 3544;

    fn parse_raw_data(
        data: &[u8],
        wiring_config: &[&WiringConfigRecord],
    ) -> anyhow::Result<EventData> {
        if !data.len().is_multiple_of(8) {
            bail!("Event data is not a multiple of pairs of 4-byte words");
        }

        let packet_config = wiring_config.first().context("No wiring config")?;

        match packet_config.packet_type.as_str() {
            "Position" => Pc3544ms::decode_position_packet(data, wiring_config),
            "PulseHeight" => Pc3544ms::decode_pulse_height_packet(data, wiring_config),
            _ => {
                bail!("Unable to process events: unknown stream type in config");
            }
        }
    }
}

impl TestableBoard for Pc3544ms {
    fn make_fake_event() -> Vec<u8> {
        // Position packet
        // tof = 456000 ns
        // channel 2, position 1234
        vec![
            0xFF, 0x06, 0xF5, 0x40, // 456000ns
            0b11101011, 0xFF, 0xF4, 0xD2, // Channel 2 (b010), 0x4D2 = position 1234
        ]
    }

    fn make_fake_wiring_config(src_ip: Option<IpAddr>) -> Vec<WiringConfigRecord> {
        vec![
            WiringConfigRecord {
                brd_num: 0,
                brd_ref: "WLSF0".to_owned(),
                packet_type: "Position".to_owned(),
                sw_pos: 0,
                streaming_ip: src_ip.unwrap_or(IpAddr::V4(Ipv4Addr::new(192, 168, 1, 1))),
                ch: 0,
                mantid_detector_id_start: 11100001,
                mantid_detector_id_length: 256,
                comment: "".to_owned(),
            },
            WiringConfigRecord {
                brd_num: 0,
                brd_ref: "WLSF0".to_owned(),
                packet_type: "Position".to_owned(),
                sw_pos: 0,
                streaming_ip: src_ip.unwrap_or(IpAddr::V4(Ipv4Addr::new(192, 168, 1, 1))),
                ch: 1,
                mantid_detector_id_start: 11101001,
                mantid_detector_id_length: 256,
                comment: "".to_owned(),
            },
            WiringConfigRecord {
                brd_num: 0,
                brd_ref: "WLSF0".to_owned(),
                packet_type: "Position".to_owned(),
                sw_pos: 0,
                streaming_ip: src_ip.unwrap_or(IpAddr::V4(Ipv4Addr::new(192, 168, 1, 1))),
                ch: 2,
                mantid_detector_id_start: 11102001,
                mantid_detector_id_length: 256,
                comment: "".to_owned(),
            },
            WiringConfigRecord {
                brd_num: 0,
                brd_ref: "WLSF0".to_owned(),
                packet_type: "Position".to_owned(),
                sw_pos: 0,
                streaming_ip: src_ip.unwrap_or(IpAddr::V4(Ipv4Addr::new(192, 168, 1, 1))),
                ch: 3,
                mantid_detector_id_start: 11103001,
                mantid_detector_id_length: 256,
                comment: "".to_owned(),
            },
        ]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_pc3544_event() {
        let data = Pc3544ms::make_fake_event();
        let wiring = Pc3544ms::make_fake_wiring_config(None);

        let events = Pc3544ms::parse_raw_data(&data, &wiring.iter().collect::<Vec<_>>())
            .expect("parsing should work");

        assert_eq!(events.time_of_flight(), [456_000]);
        assert_eq!(events.pixel_id(), [11102078]);
    }

    #[test]
    fn test_parse_empty_pc3544_events() {
        let data = [];
        let wiring = Pc3544ms::make_fake_wiring_config(None);

        let events = Pc3544ms::parse_raw_data(&data, &wiring.iter().collect::<Vec<_>>())
            .expect("parsing should work");

        assert!(events.is_empty())
    }

    #[test]
    fn test_parse_pc3544_event_invalid_length() {
        let data = vec![0; 9]; // invalid length: 9 bytes
        let wiring = Pc3544ms::make_fake_wiring_config(None);

        let events = Pc3544ms::parse_raw_data(&data, &wiring.iter().collect::<Vec<_>>());

        assert!(events.is_err_and(|e| {
            e.to_string()
                .contains("not a multiple of pairs of 4-byte words")
        }));
    }
}
