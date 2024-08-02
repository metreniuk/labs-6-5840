mod map_reduce;
mod map_reduce_apps;

use std::{collections::HashMap, fs};

use map_reduce::SequentialMapReduce;
use map_reduce_apps::WordCount;

fn main() {
    let i_filename = "input/pg-being_ernest.txt";
    let o_filename = "output/mr-being_ernest.txt";
    let contents =
        fs::read_to_string(i_filename).expect(&format!("failed to read file {}", i_filename));
    let mut input = HashMap::new();
    input.insert(i_filename.to_string(), contents);
    let wc = WordCount {};

    let mr = SequentialMapReduce::new(input, Box::new(wc));

    let output = mr.run();

    for (key, o_value) in output {
        fs::write(o_filename, format!("{} {}\n", key, o_value))
            .expect(&format!("failed to write to file {}", o_filename));
        // println!("{} {}\n", key, o_value);
    }
}
