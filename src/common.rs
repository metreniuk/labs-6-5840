use async_trait::async_trait;
use std::{collections::HashMap, fs};

/// {filename: file_contents}
// pub type Input = Vec<String>;
/// dir
pub type Input = String;
/// {key: reduce_output}
pub type Output = HashMap<String, String>;

pub enum Task {
    // filename
    Map(String, String),
    // src worker ID
    Reduce(String),
}

pub trait MapReduceApp: Send + Sync {
    fn map(&self, filename: String, contents: String) -> Vec<(String, String)>;
    fn reduce(&self, key: String, values: Vec<String>) -> String;
}

#[async_trait]
pub trait MapReduce {
    fn new(input: Input, mr_app: Box<dyn MapReduceApp>) -> Self;
    async fn run(self) -> anyhow::Result<()>;
}

pub fn read_files_from_dir(input_dir: String) -> anyhow::Result<Vec<(String, String)>> {
    let mut input: Vec<_> = Vec::new();
    for entry in fs::read_dir(input_dir)? {
        let entry = entry.unwrap();
        let path = entry.path();

        if path.is_file() {
            let filename = path.file_name().unwrap().to_str().unwrap();
            input.push((filename.to_string(), path.to_str().unwrap().to_owned()));
        }
    }
    Ok(input)
}
