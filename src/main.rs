mod kafka;
mod config_reader;
mod metrics_logger;
mod data_processing;

//use std::io::Error;
use std::error::Error;
use std::{fmt::Write, num::ParseIntError};
pub use crate::metrics_logger::demo;
//pub use crate::data_processing::header_decoder;
pub use crate::data_processing::process_udp_to_kafka;

use rdkafka::client::ClientContext;
use rdkafka::config::{ClientConfig, RDKafkaLogLevel};
use rdkafka::consumer::stream_consumer::StreamConsumer;
use rdkafka::consumer::{BaseConsumer, CommitMode, Consumer, ConsumerContext, Rebalance};
use rdkafka::producer::{DefaultProducerContext, FutureProducer, FutureRecord, ThreadedProducer};
use rdkafka::error::KafkaResult;
use rdkafka::message::{Headers, Message};
use rdkafka::topic_partition_list::TopicPartitionList;
use rdkafka::util::get_rdkafka_version;
use std::time::Duration;
use bytes::Bytes;
use serde::Deserialize;
use clap::{Arg, Parser};
extern crate csv;

use std::fs::File;
use std::path::Path;

// A context can be used to change the behavior of producers and consumers by adding callbacks
// that will be executed by librdkafka.
// This particular context sets up custom callbacks to log rebalancing events.
struct CustomContext;

impl ClientContext for CustomContext {}

impl ConsumerContext for CustomContext {
    fn pre_rebalance(&self, rebalance: &Rebalance) {
        println!("Pre rebalance {:?}", rebalance);
    }

    fn post_rebalance(&self, rebalance: &Rebalance) {
        println!("Post rebalance {:?}", rebalance);
    }

    fn commit_callback(&self, result: KafkaResult<()>, _offsets: &TopicPartitionList) {
        println!("Committing offsets: {:?}", result);
    }
}

// A type alias with your custom consumer can be created for convenience.
type LoggingConsumer = StreamConsumer<CustomContext>;


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
    #[arg(short='k', long, default_value = "te7gull.te.rl.ac.uk:19092")]
    dest_kafka_broker: String,

    /// Kafka topic to send the data to
    #[arg(short='d', long)]
    dest_kafka_topic: String,

    /// url of the kafka broker
    #[arg(long, default_value = "te7gull.te.rl.ac.uk:19092")]
    src_kafka_broker: String,

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
    #[arg(short='g', long, default_value="default_rust_proc")]
    consumer_grp: String,

    // Filepath to the wiring configuration file (csv)
    #[arg(short='w', long)]
    wiring_csv_path: String,
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

#[derive(Debug, serde::Deserialize)]
struct wiring_config_record {
    BRD_NUM: u8,
    BRD_Ref: String,
    BRD_Type: String,
    Packet_Type: String,
    SW_Pos: u8,
    StreamingIP: String,
    CH: u8,
    Mantid_DetectorID_Start: u32,
    Mantid_Detector_ID_Lenght: u32,
    Comment: String,

}

fn read_csv<P: AsRef<Path>>(filename: P) -> Vec<wiring_config_record>{
    let file = File::open(filename).unwrap();
    let mut rdr = csv::Reader::from_reader(file);

    let mut csv_config = Vec::new();

    for result in rdr.deserialize() {
        let line: wiring_config_record = result.unwrap();
        println!("{:?}", line);
        csv_config.push(line);
    }
    csv_config
}


async fn kafka_udp_process(cmd_args: Args, wiring_config: Vec<wiring_config_record>) {
    println!("Do UDP processing");
    println!("{:#?}", cmd_args);
    let kafka_broker = cmd_args.src_kafka_broker;
    let kafka_broker_dest = cmd_args.dest_kafka_broker;
    let dest_topic_name = cmd_args.dest_kafka_topic.as_str();

    let context = CustomContext;

    let consumer: LoggingConsumer = ClientConfig::new()
        .set("group.id", cmd_args.consumer_grp)
        .set("bootstrap.servers", &kafka_broker)
        .set("enable.partition.eof", "false")
        .set("session.timeout.ms", "6000")
        .set("enable.auto.commit", "true")
        //.set("statistics.interval.ms", "30000")
        .set("auto.offset.reset", "smallest")
        .set_log_level(RDKafkaLogLevel::Debug)
        .create_with_context(context)
        .expect("Consumer creation failed");

    let producer: &ThreadedProducer<DefaultProducerContext> = &ClientConfig::new()
        .set("bootstrap.servers", &kafka_broker_dest)
        .set("message.timeout.ms", "5000")
        .create()
        .expect("Producer creation error");

    consumer
        .subscribe(&[cmd_args.src_kafka_topic.as_str()])
        .expect("Can't subscribe to specified topics");
    println!("Mode 0 - Kafka -> Kafka Processing");
    loop {
        match consumer.recv().await {
            Err(e) => println!("Kafka error: {}", e),
            Ok(m) => {
                let payload = match m.payload_view::<str>() {
                    None => "",
                    Some(Ok(s)) => s,
                    Some(Err(e)) => {
                        println!("Error while deserializing message payload: {:?}", e);
                        ""
                    }
                };
                use std::time::Instant;
                let now = Instant::now();

                // Process the Data
                let raw_udpjson: rawUdpJSON = serde_json::from_slice(m.payload().unwrap()).unwrap();

                let kafka_fbs = process_udp_to_kafka(&raw_udpjson.packet_data, &raw_udpjson.src, &wiring_config);
                consumer.commit_message(&m, CommitMode::Async).unwrap();

                println!("Num Kafka Messages to Prod: {}", kafka_fbs.len());

                for flatbuffer in kafka_fbs{
                    let produce_error = producer.send(
                        rdkafka::producer::BaseRecord::to(dest_topic_name)
                            .key("")
                            .payload(&flatbuffer),
                        //   Duration::from_secs(0),
                    );
                }

                    // let futures = kafka_fbs.iter()
                    //     .map(|i| async move {
                    //         // The send operation on the topic returns a future, which will be
                    //         // completed once the result or failure from Kafka is received.
                    //         let delivery_status = producer
                    //             .send::<Vec<u8>, _, _>(
                    //                 FutureRecord::to(dest_topic_name)
                    //                     .payload(i),
                    //                 //    .key(&format!("Key {}", i)),
                    //                 // .headers(OwnedHeaders::new().insert(Header {
                    //                 //     key: "header_key",
                    //                 //     value: Some("header_value"),
                    //                 // })),
                    //                 Duration::from_secs(0),
                    //             )
                    //             .await;
                    //
                    //         // This will be executed when the result is received.
                    //         //println!("Delivery status for message - received");
                    //         delivery_status
                    //     })
                    //     .collect::<Vec<_>>();
                    //
                    // // This loop will wait until all delivery statuses have been received.
                    // for future in futures {
                    //     future.await.expect("TODO: panic message");
                    //     //println!("Future completed. Result: {:?}", future.await);
                    // }

                let elapsed = now.elapsed();
                //println!("Elapsed: {:.2?}", elapsed, );
                println!("PK_IP: {} - PROCt: {:?} ", raw_udpjson.src, elapsed);

            }
        }
    }
}

#[tokio::main]
async fn main(){
    println!("DSG - Rust Data Processor");
    println!("Processing UDP data into the flatbuffers since 2024");
    let args = Args::parse();


    let bytes: Vec<u8> = Vec::new();


    let filename = "C:\\GitLab\\rust-data-stream-processor\\src\\config\\wiring.csv";
    let filename = args.wiring_csv_path.as_str();
    let csv_data: Vec<wiring_config_record> = read_csv(args.wiring_csv_path.as_str());

    kafka_udp_process(args, csv_data).await;
}
