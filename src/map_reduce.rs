use std::collections::HashMap;

/// {filename: file_contents}
type Input = HashMap<String, String>;
/// {key: reduce_output}
type Output = HashMap<String, String>;

pub trait MapReduceApp {
    fn map(&self, filename: String, contents: String) -> Vec<(String, String)>;
    fn reduce(&self, key: String, values: Vec<String>) -> String;
}

pub struct SequentialMapReduce {
    input: Input,
    mr_app: Box<dyn MapReduceApp>,
}

impl SequentialMapReduce {
    pub fn new(input: Input, mr_app: Box<dyn MapReduceApp>) -> Self {
        Self { input, mr_app }
    }
    pub fn run(self) -> Output {
        // println!("input {:?}", self.input.clone());
        let intermediate_key_values =
            self.input
                .into_iter()
                .fold(Vec::new(), |mut acc, (filename, contents)| {
                    let key_values = self.mr_app.map(filename, contents);
                    acc.extend(key_values);
                    acc
                });

        // println!("intermediate_key_values {:?}", intermediate_key_values);

        let grouped_key_values: HashMap<String, Vec<String>> = intermediate_key_values
            .into_iter()
            .fold(HashMap::new(), |mut acc, (key, value)| {
                acc.entry(key)
                    .and_modify(|e| e.push(value.clone()))
                    .or_insert(vec![value]);
                acc
            });
        // println!("gropued {:?}", grouped_key_values);
        let output = grouped_key_values
            .into_iter()
            .map(|(key, ivalues)| (key.clone(), self.mr_app.reduce(key, ivalues)))
            .collect();

        output
    }
}
