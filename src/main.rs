mod common;
mod map_reduce_apps;
mod map_reduce_parallel;
mod map_reduce_seq;
mod thread_pool;

use std::{thread::sleep, time::Duration};

use clap::Parser;
use common::MapReduce;

use map_reduce_apps::WordCount;
use map_reduce_parallel::ParallelMapReduce;
use map_reduce_seq::SequentialMapReduce;
use thread_pool::ThreadPool;

#[derive(Parser, Debug)]
#[command(version, about, long_about=None)]
struct Args {
    #[arg(short, long)]
    input_dir: String,
    #[arg(short, long)]
    output_file: String,
}

// #[tokio::main]
// async fn main() -> anyhow::Result<(), anyhow::Error> {
//     let Args { input_dir, .. } = Args::parse();

//     let wc = WordCount {};

//     let mr_seq = SequentialMapReduce::new(input_dir.clone(), Box::new(wc));

//     println!("[start] Sequential MR");
//     mr_seq.run().await?;
//     println!("[end] Sequential MR");

//     let wc_2 = WordCount {};
//     let mr_parallel = ParallelMapReduce::new(input_dir, Box::new(wc_2));
//     println!("[start] Parallel MR");
//     mr_parallel.run().await?;
//     println!("[end] Parallel MR");
//     Ok(())
// }

fn main() {
    let pool = ThreadPool::new(4);

    for i in 0..8 {
        pool.execute(move || {
            sleep(Duration::from_secs(1));
            println!("Processing task {}", i);
        });
    }

    // Wait for all tasks to complete
    sleep(Duration::from_secs(10));

    println!("All tasks completed.");
}
