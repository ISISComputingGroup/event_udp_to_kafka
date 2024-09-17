mod kafka;
mod config_reader;
mod metrics_logger;
mod data_processing;

//use std::io::Error;
use std::{fmt::Write, num::ParseIntError};
pub use crate::metrics_logger::demo;
//pub use crate::data_processing::header_decoder;
pub use crate::data_processing::process_udp_to_kafka;

use rdkafka::client::ClientContext;
use rdkafka::config::{ClientConfig, RDKafkaLogLevel};
use rdkafka::consumer::stream_consumer::StreamConsumer;
// use rdkafka::consumer::{BaseConsumer, CommitMode, Consumer, ConsumerContext, Rebalance};
use rdkafka::error::KafkaResult;
use rdkafka::message::{Headers, Message};
use rdkafka::topic_partition_list::TopicPartitionList;
use rdkafka::util::get_rdkafka_version;

use futures::{SinkExt, StreamExt, pin_mut};
use kafkas::{
    topic_name, Error, Kafka, KafkaOptions, Producer, ProducerOptions, Consumer, ConsumerRecord, ConsumerOptions,
    Record, SerializeMessage,
    TimestampType, TokioExecutor, NO_PARTITION_LEADER_EPOCH, NO_PRODUCER_EPOCH, NO_PRODUCER_ID,
    NO_SEQUENCE,
};
use bytes::Bytes;
use serde::Deserialize;
use clap::Parser;


#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// Port to use to receive data from
    #[arg(short='p', long, default_value_t = 5005)]
    port: u32,

    /// Ip address of the host, address to bind to
    #[arg(short='s', long, default_value = "192.168.1.1")]
    host_ip: String,

    /// url of the kafka broker
    #[arg(short='k', long, default_value = "te7gull.te.rl.ac.uk")]
    dest_kafka_broker: String,

    /// Port to use for kafka communication - defaults for Redpanda kafka
    #[arg(short='b', long, default_value_t = 19092)]
    dest_kafka_port: u32,

    /// Kafka topic to send the data to
    #[arg(short='d', long)]
    dest_kafka_topic: String,

    /// url of the kafka broker
    #[arg(long, default_value = "te7gull.te.rl.ac.uk")]
    src_kafka_broker: String,

    /// Port to use for kafka communication - defaults for Redpanda kafka
    #[arg(long, default_value_t = 19092)]
    src_kafka_port: u32,

    /// Kafka topic to get data from
    #[arg(short='t', long, default_value="")]
    src_kafka_topic: String,

    /// Script operating mode
    /// 0 - Consume UDP packets from Kafka SRC topic, process and send back to kafka
    /// 1 - Gets UDP packets from a local socket binding, processes and kafkas
    /// This script is mainly designed to function in the Kafka-> Kafka configuration
    /// With the UDP->Kafka rust buffering via kafka. This gives some failover, and potential throughput options
    ///
    #[arg(short='m', long, verbatim_doc_comment, default_value_t = 0)]
    mode: u32,

    // Consumer group to use when connecting to Kafka
    #[arg(short='t', long, default_value="default_rust_proc")]
    consumer_grp: String,
}

#[derive(Deserialize)]
struct rawUdpJSON {
    src: String,
    packet_data:  String,
    // add the other fields if you need them
}

pub fn decode_hex(s: &str) -> Result<Vec<u8>, ParseIntError> {
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16))
        .collect()
}

struct KafkaData {
    value: Option<Bytes>,
}

impl KafkaData {
    fn new(value: &str) -> Self {
        Self {
            value: Some(Bytes::copy_from_slice(value.as_bytes())),
        }
    }
}

impl SerializeMessage for KafkaData {
    fn partition(&self) -> Option<i32> {
        None
    }

    fn key(&self) -> Option<&Bytes> {
        None
    }

    fn value(&self) -> Option<&Bytes> {
        self.value.as_ref()
    }

    fn serialize_message(input: Self) -> kafkas::Result<Record> {
        Ok(Record {
            transactional: false,
            control: false,
            partition_leader_epoch: NO_PARTITION_LEADER_EPOCH,
            producer_id: NO_PRODUCER_ID,
            producer_epoch: NO_PRODUCER_EPOCH,
            timestamp_type: TimestampType::Creation,
            offset: -1,
            sequence: NO_SEQUENCE,
            timestamp: 0,
            key: None,
            value: input.value,
            headers: indexmap::IndexMap::new(),
        })
    }
}

async fn kafka_udp_process(cmd_args: Args) -> Result<(), Box<Error>>{
    println!("Mode 0 - Kafka -> Kafka Processing");
    let kafka_broker_src = format!("{}:{}", cmd_args.src_kafka_broker, cmd_args.src_kafka_port);
    let kafka_broker_dest = format!("{}:{}", cmd_args.dest_kafka_broker, cmd_args.dest_kafka_port);

    println!("Kafka SRC: {}", &kafka_broker_src);
    println!("Kafka Dest: {}", &kafka_broker_dest);

    //let kafka_broker_src: String = "localhost:19092,localhost:29092,localhost:39092".to_string();

    let kafka_client_src = Kafka::new(kafka_broker_src, KafkaOptions::default(), TokioExecutor).await?;
    let kafka_client_dest = Kafka::new(kafka_broker_dest, KafkaOptions::default(), TokioExecutor).await?;

    //define consumer
    let mut consumer_options = ConsumerOptions::new(&cmd_args.consumer_grp);
    consumer_options.auto_commit_enabled = false;
    let mut consumer = Consumer::new(kafka_client_src, consumer_options).await?;
    let consume_stream = consumer.subscribe::<&str, ConsumerRecord>(vec![&cmd_args.src_kafka_topic]).await?;
    pin_mut!(consume_stream);

    //define producer
    // let (mut tx, mut rx) = futures::channel::mpsc::unbounded();
    // tokio::task::spawn(Box::pin(async move {
    //     while let Some(fut) = rx.next().await {
    //         if let Err(e) = fut.await {
    //             error!("{e}");
    //         }
    //     }
    // }));

    let producer = Producer::new(kafka_client_dest, ProducerOptions::default()).await?;
    let dest_topic = topic_name(cmd_args.dest_kafka_topic);

    while let Some(records) = consume_stream.next().await {
        for record in records {
            if let Some(value) = record.value {
                let raw_udpjson: rawUdpJSON = serde_json::from_slice(&value).unwrap();
               // println!("Got MSG To PROC - SRC IP ADR: {}", raw_udpjson.src);
                let kafka_fb = process_udp_to_kafka(&raw_udpjson.packet_data);

            }
        }
        // needed only when `auto_commit_enabled` is false
        consumer.commit_async().await?;
    }
    println!("Done looping");
    Ok(())

}

#[tokio::main]
async fn main() -> Result<(), Box<Error>> {
    println!("DSG - Rust Data Processor");
    println!("Processing UDP data into the flatbuffers since 2024");
    let args = Args::parse();


    let bytes: Vec<u8> = Vec::new();

    kafka_udp_process(args).await?;
    Ok(())
}
