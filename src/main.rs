mod kafka;
mod config_reader;
mod metrics_logger;
mod data_processing;

pub use crate::metrics_logger::demo;
pub use crate::data_processing::header_decoder;

fn main() {
    println!("Hello, world!");
    demo();
    let bytes: Vec<u8> = Vec::new();
    header_decoder(bytes);
}
