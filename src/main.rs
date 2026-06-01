use clap::Parser;
use event_udp_to_kafka::{Args, WiringConfigRecord, kafka_udp_process, read_csv};

#[tokio::main]
async fn main() {
    println!("DSG - Rust Data Processor");
    println!("Processing UDP data into the flatbuffers since 2024");
    env_logger::init();
    let args = Args::parse();

    let filename = args.wiring_csv_path.as_str();
    let csv_data: Vec<WiringConfigRecord> = read_csv(filename);

    kafka_udp_process(args, csv_data).await;
}
