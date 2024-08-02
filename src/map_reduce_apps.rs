use crate::map_reduce::MapReduceApp;
use regex::Regex;

pub struct WordCount {}

impl MapReduceApp for WordCount {
    fn map(&self, _filename: String, contents: String) -> Vec<(String, String)> {
        let words_regex = Regex::new(r"\b[a-zA-Z0-9]+\b").expect("invalid regex");
        let kva: Vec<_> = words_regex
            .find_iter(&contents)
            .into_iter()
            .map(|w| (w.as_str().to_lowercase().to_string(), String::from("1")))
            .collect();
        kva
    }

    fn reduce(&self, _key: String, values: Vec<String>) -> String {
        values.iter().len().to_string()
    }
}
