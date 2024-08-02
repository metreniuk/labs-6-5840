mod map_reduce;
mod map_reduce_apps;

use clap::Parser;
use std::{collections::HashMap, fs};

use map_reduce::SequentialMapReduce;
use map_reduce_apps::WordCount;

#[derive(Parser, Debug)]
#[command(version, about, long_about=None)]
struct Args {
    #[arg(short, long)]
    input_dir: String,
    #[arg(short, long)]
    output_file: String,
}

fn main() -> anyhow::Result<(), anyhow::Error> {
    let Args {
        output_file,
        input_dir,
    } = Args::parse();
    let mut input: HashMap<String, String> = HashMap::new();
    for entry in fs::read_dir(input_dir)? {
        let entry = entry.unwrap();
        let path = entry.path();

        if path.is_file() {
            let filename = path.file_name().unwrap().to_str().unwrap();
            let contents = fs::read_to_string(&path)?;
            println!("read from: {:?}", path);
            input.insert(filename.to_string(), contents);
        }
    }

    let wc = WordCount {};

    let mr = SequentialMapReduce::new(input, Box::new(wc));

    let output = mr.run();

    let mut output_values: Vec<_> = output.into_iter().collect();

    output_values.sort_by(|a, b| {
        b.1.parse::<u64>()
            .unwrap()
            .cmp(&a.1.parse::<u64>().unwrap())
    });

    let mut lines = String::from("");
    for (key, o_value) in output_values {
        lines.push_str(&format!("{} {}\n", key, o_value));
    }
    fs::write(&output_file, lines)?;
    println!("write to: {:?}", output_file);
    Ok(())
}
