use async_trait::async_trait;
use std::collections::HashMap;

/// {filename: file_contents}
// pub type Input = Vec<String>;
/// dir
pub type Input = String;
/// {key: reduce_output}
pub type Output = HashMap<String, String>;

pub trait MapReduceApp: Send {
    fn map(&self, filename: String, contents: String) -> Vec<(String, String)>;
    fn reduce(&self, key: String, values: Vec<String>) -> String;
}

#[async_trait]
pub trait MapReduce {
    fn new(input: Input, mr_app: Box<dyn MapReduceApp>) -> Self;
    async fn run(self) -> anyhow::Result<()>;
}
