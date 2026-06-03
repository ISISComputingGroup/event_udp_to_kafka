//! # `event_udp_to_kafka`
//!
//! This module listens to an input kafka topic containing JSON payloads
//! of the following form:
//!
//! ```json
//! {
//!     "src": "192.168.1.1",
//!     "packet_data": "abc123",
//! }
//! ```
//!
//! Where `src` is the IP address from which a message was received, and `packet_data`
//! is a hexed representation of the received data.
//!
//! `event_udp_to_kafka` then converts these received messages to flatbuffers-encoded messages,
//! and then sends them to an output topic (usually `_rawEvents`).

pub mod config;
pub mod data_processing;
pub mod gps_time;
pub mod metrics;
pub mod udp_message;

use crate::data_processing::process_udp_bytes_to_kafka;

use clap::Parser;
use rdkafka::config::ClientConfig;
use rdkafka::producer::{DefaultProducerContext, ThreadedProducer};

use crate::config::EventUdpToKafkaConfig;
use crate::metrics::{
    INCOMING_UDP_PACKET_ERRORS, OUTGOING_KAFKA_MESSAGE_SIZE, OUTGOING_KAFKA_MESSAGES,
    OUTGOING_KAFKA_PRODUCE_ERRORS, PROCESSING_TIME,
};
use ::metrics::{counter, histogram};
use flatbuffers::FlatBufferBuilder;
use log::{debug, error, info};
use std::fs::File;
use std::net::UdpSocket;
use std::path::Path;
use std::time::Instant;

/// Command-line arguments for the `event_udp_to_kafka`.
#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
pub struct Args {
    /// Path to config file
    #[arg(short, long)]
    pub config: String,

    #[command(flatten)]
    pub verbosity: clap_verbosity_flag::Verbosity,
}

/// Wiring table information.
#[derive(Debug, serde::Deserialize)]
#[allow(unused)]
pub struct WiringConfigRecord {
    #[serde(rename = "BRD_NUM")]
    pub brd_num: u8,
    #[serde(rename = "BRD_Ref")]
    pub brd_ref: String,
    #[serde(rename = "BRD_Type")]
    pub brd_type: String,
    #[serde(rename = "Packet_Type")]
    pub packet_type: String,
    #[serde(rename = "SW_Pos")]
    pub sw_pos: u8,
    #[serde(rename = "StreamingIP")]
    pub streaming_ip: String,
    #[serde(rename = "CH")]
    pub ch: u8,
    #[serde(rename = "Mantid_DetectorID_Start")]
    pub mantid_detector_id_start: u32,
    #[serde(rename = "Mantid_Detector_ID_Lenght")]
    pub mantid_detector_id_length: u32,
    #[serde(rename = "Comment")]
    pub comment: String,
}

pub fn read_csv<P: AsRef<Path>>(filename: P) -> Vec<WiringConfigRecord> {
    let file = File::open(filename).unwrap();
    let mut rdr = csv::Reader::from_reader(file);

    rdr.deserialize()
        .collect::<Result<Vec<_>, _>>()
        .unwrap_or_else(|err| panic!("Cannot deserialize wiring table line: {err}"))
}

fn make_producer(config: &EventUdpToKafkaConfig) -> ThreadedProducer<DefaultProducerContext> {
    let mut kafka_producer_config = ClientConfig::new();

    config.kafka_producer.iter().for_each(|(k, v)| {
        kafka_producer_config.set(k, v);
    });

    kafka_producer_config
        .create()
        .expect("Producer creation error")
}

/// Listen to the input Kafka topic and produce messages onto the output Kafka topic forever.
pub fn udp_process(config: &EventUdpToKafkaConfig, wiring_config: Vec<WiringConfigRecord>) -> ! {
    info!("Configuration: {:#?}", config);

    let producer = make_producer(config);

    let mut fbb = FlatBufferBuilder::new();

    let mut udp_buf = vec![0; config.udp_buffer_size()];

    let socket = UdpSocket::bind(&config.udp_bind_addr).expect("Unable to bind UDP socket");

    loop {
        let read_result = socket.recv_from(&mut udp_buf);

        if let Ok((number_of_bytes, src_sock_addr)) = read_result {
            let now = Instant::now();
            let src_ip = src_sock_addr.ip().to_string();

            process_udp_bytes_to_kafka(
                &mut fbb,
                &udp_buf[..number_of_bytes],
                &src_ip,
                &wiring_config,
                |payload| {
                    let result = producer.send(
                        rdkafka::producer::BaseRecord::to(&config.dest_kafka_topic)
                            .key("")
                            .payload(payload),
                    );

                    if let Err(e) = result {
                        error!("Kafka error: {:?}", e);
                        counter!(OUTGOING_KAFKA_PRODUCE_ERRORS).increment(1);
                    } else {
                        counter!(OUTGOING_KAFKA_MESSAGES).increment(1);
                        counter!(OUTGOING_KAFKA_MESSAGE_SIZE).increment(payload.len() as u64);
                    }
                },
            );

            let elapsed = now.elapsed();
            debug!(
                "Packet IP: {} - Processing time: {:.3}us",
                src_ip,
                elapsed.as_micros()
            );
            histogram!(PROCESSING_TIME).record(elapsed.as_secs_f64());
        } else {
            error!("Error reading from UDP socket: {:?}", read_result);
            counter!(INCOMING_UDP_PACKET_ERRORS).increment(1);
        }
    }
}
