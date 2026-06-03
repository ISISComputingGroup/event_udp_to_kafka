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

use crate::data_processing::process_udp_to_kafka;

use clap::Parser;
use rdkafka::config::{ClientConfig, RDKafkaLogLevel};
use rdkafka::consumer::stream_consumer::StreamConsumer;
use rdkafka::consumer::{CommitMode, Consumer, DefaultConsumerContext};
use rdkafka::message::Message;
use rdkafka::producer::{DefaultProducerContext, ThreadedProducer};
use serde::Deserialize;

use crate::config::EventUdpToKafkaConfig;
use crate::metrics::{
    OUTGOING_KAFKA_MESSAGE_SIZE, OUTGOING_KAFKA_MESSAGES, OUTGOING_KAFKA_PRODUCE_ERRORS,
    PROCESSING_TIME,
};
use ::metrics::{counter, histogram};
use flatbuffers::FlatBufferBuilder;
use log::{debug, error, info};
use std::fs::File;
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

/// Schema for JSON data on the input Kafka topic.
#[derive(Deserialize)]
struct RawUdpJson {
    src: String,
    packet_data: String,
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

fn make_consumer(config: &EventUdpToKafkaConfig) -> StreamConsumer<DefaultConsumerContext> {
    let mut kafka_consumer_config = ClientConfig::new();

    for (k, v) in config.kafka_consumer.iter() {
        kafka_consumer_config.set(k, v);
    }

    kafka_consumer_config.set_log_level(RDKafkaLogLevel::Debug);

    kafka_consumer_config
        .create()
        .expect("Consumer creation failed")
}

fn make_producer(config: &EventUdpToKafkaConfig) -> ThreadedProducer<DefaultProducerContext> {
    let mut kafka_producer_config = ClientConfig::new();

    for (k, v) in config.kafka_producer.iter() {
        kafka_producer_config.set(k, v);
    }

    kafka_producer_config
        .create()
        .expect("Producer creation error")
}

/// Listen to the input Kafka topic and produce messages onto the output Kafka topic forever.
pub async fn kafka_udp_process(
    config: &EventUdpToKafkaConfig,
    wiring_config: Vec<WiringConfigRecord>,
) -> ! {
    info!("Configuration: {:#?}", config);

    let consumer = make_consumer(config);
    let producer = make_producer(config);

    consumer
        .subscribe(&[&config.src_kafka_topic])
        .expect("Can't subscribe to specified topics");

    info!("Mode 0 - Kafka -> Kafka Processing");

    let mut fbb = FlatBufferBuilder::new();

    loop {
        match consumer.recv().await {
            Err(e) => error!("Kafka error: {}", e),
            Ok(m) => {
                let now = Instant::now();

                // Process the Data
                let raw_udpjson: RawUdpJson = serde_json::from_slice(m.payload().unwrap()).unwrap();

                process_udp_to_kafka(
                    &mut fbb,
                    &raw_udpjson.packet_data,
                    &raw_udpjson.src,
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
                consumer.commit_message(&m, CommitMode::Async).unwrap();

                let elapsed = now.elapsed();
                debug!(
                    "Packet IP: {} - Processing time: {:.3}us",
                    raw_udpjson.src,
                    elapsed.as_micros()
                );
                histogram!(PROCESSING_TIME).record(elapsed.as_secs_f64());
            }
        }
    }
}
