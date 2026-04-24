//! # `data-stream-processor`
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
//! `data-stream-processor` then converts these received messages to flatbuffers-encoded messages,
//! and then sends them to an output topic (usually `_rawEvents`).

pub mod data_processing;
pub mod gps_time;
pub mod metrics_logger;
pub mod udp_message;

use crate::data_processing::process_udp_to_kafka;

use clap::Parser;
use rdkafka::client::ClientContext;
use rdkafka::config::{ClientConfig, RDKafkaLogLevel};
use rdkafka::consumer::stream_consumer::StreamConsumer;
use rdkafka::consumer::{BaseConsumer, CommitMode, Consumer, ConsumerContext, Rebalance};
use rdkafka::error::KafkaResult;
use rdkafka::message::Message;
use rdkafka::producer::{DefaultProducerContext, ThreadedProducer};
use rdkafka::topic_partition_list::TopicPartitionList;
use serde::Deserialize;

use log::{debug, error, info};
use std::fs::File;
use std::path::Path;
use std::time::Instant;

// A context can be used to change the behavior of producers and consumers by adding callbacks
// that will be executed by librdkafka.
// This particular context sets up custom callbacks to log rebalancing events.
struct CustomContext;

impl ClientContext for CustomContext {}

impl ConsumerContext for CustomContext {
    fn pre_rebalance(&self, _: &BaseConsumer<CustomContext>, rebalance: &Rebalance) {
        info!("Pre rebalance {:?}", rebalance);
    }

    fn post_rebalance(&self, _: &BaseConsumer<CustomContext>, rebalance: &Rebalance) {
        info!("Post rebalance {:?}", rebalance);
    }

    fn commit_callback(&self, result: KafkaResult<()>, _offsets: &TopicPartitionList) {
        info!("Committing offsets: {:?}", result);
    }
}

// A type alias with your custom consumer can be created for convenience.
type LoggingConsumer = StreamConsumer<CustomContext>;

/// Command-line arguments for the `data-stream-processor`.
#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
pub struct Args {
    /// Port to use to receive data from
    #[arg(short = 'p', long, default_value_t = 5005)]
    pub port: u32,

    /// Ip address of the host, address to bind to
    #[arg(short = 's', long, default_value = "192.168.1.1")]
    pub host_ip: String,

    /// url of the kafka broker
    #[arg(short = 'k', long, default_value = "te7gull.te.rl.ac.uk:19092")]
    pub dest_kafka_broker: String,

    /// Kafka topic to send the data to
    #[arg(short = 'd', long)]
    pub dest_kafka_topic: String,

    /// url of the kafka broker
    #[arg(long, default_value = "te7gull.te.rl.ac.uk:19092")]
    pub src_kafka_broker: String,

    /// Kafka topic to get data from
    #[arg(short = 't', long, default_value = "")]
    pub src_kafka_topic: String,

    /// Script operating mode
    /// 0 - Consume UDP packets from Kafka SRC topic, process and send back to kafka
    /// 1 - Gets UDP packets from a local socket binding, processes and kafkas
    /// This script is mainly designed to function in the Kafka-> Kafka configuration
    /// With the UDP->Kafka rust buffering via kafka. This gives some failover, and potential throughput options
    ///
    #[arg(short = 'm', long, verbatim_doc_comment, default_value_t = 0)]
    pub mode: u32,

    // Consumer group to use when connecting to Kafka
    #[arg(short = 'g', long, default_value = "default_rust_proc")]
    pub consumer_grp: String,

    // Filepath to the wiring configuration file (csv)
    #[arg(short = 'w', long)]
    pub wiring_csv_path: String,
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

/// Listen to the input Kafka topic and produce messages onto the output Kafka topic forever.
pub async fn kafka_udp_process(cmd_args: Args, wiring_config: Vec<WiringConfigRecord>) -> ! {
    info!("Start UDP processing");
    info!("Configuration: {:#?}", cmd_args);

    let context = CustomContext;

    let consumer: LoggingConsumer = ClientConfig::new()
        .set("group.id", cmd_args.consumer_grp)
        .set("bootstrap.servers", cmd_args.src_kafka_broker)
        .set("enable.partition.eof", "false")
        .set("session.timeout.ms", "6000")
        .set("enable.auto.commit", "true")
        .set("auto.offset.reset", "smallest")
        .set_log_level(RDKafkaLogLevel::Debug)
        .create_with_context(context)
        .expect("Consumer creation failed");

    let producer: &ThreadedProducer<DefaultProducerContext> = &ClientConfig::new()
        .set("bootstrap.servers", cmd_args.dest_kafka_broker)
        .set("message.timeout.ms", "5000")
        .create()
        .expect("Producer creation error");

    consumer
        .subscribe(&[&cmd_args.src_kafka_topic])
        .expect("Can't subscribe to specified topics");

    info!("Mode 0 - Kafka -> Kafka Processing");

    loop {
        match consumer.recv().await {
            Err(e) => error!("Kafka error: {}", e),
            Ok(m) => {
                let now = Instant::now();

                // Process the Data
                let raw_udpjson: RawUdpJson = serde_json::from_slice(m.payload().unwrap()).unwrap();

                let kafka_fbs = process_udp_to_kafka(
                    &raw_udpjson.packet_data,
                    &raw_udpjson.src,
                    &wiring_config,
                );
                consumer.commit_message(&m, CommitMode::Async).unwrap();

                debug!("Num Kafka Messages to Prod: {}", kafka_fbs.len());

                for flatbuffer in kafka_fbs {
                    let result = producer.send(
                        rdkafka::producer::BaseRecord::to(&cmd_args.dest_kafka_topic)
                            .key("")
                            .payload(&flatbuffer),
                    );

                    if let Err(e) = result {
                        error!("Kafka error: {:?}", e);
                    }
                }

                let elapsed = now.elapsed();
                debug!("PK_IP: {} - PROCt: {:?} ", raw_udpjson.src, elapsed);
            }
        }
    }
}
