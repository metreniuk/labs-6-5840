mod common;
mod map_reduce_apps;
mod map_reduce_parallel;
mod map_reduce_seq;

use clap::Parser;
use common::MapReduce;

use map_reduce_apps::WordCount;
use map_reduce_seq::SequentialMapReduce;

#[derive(Parser, Debug)]
#[command(version, about, long_about=None)]
struct Args {
    #[arg(short, long)]
    input_dir: String,
    #[arg(short, long)]
    output_file: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<(), anyhow::Error> {
    let Args { input_dir, .. } = Args::parse();

    let wc = WordCount {};

    let mr = SequentialMapReduce::new(input_dir, Box::new(wc));

    mr.run().await?;
    Ok(())
}
