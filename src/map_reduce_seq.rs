use std::{collections::HashMap, fs};

use async_trait::async_trait;

use crate::common::{Input, MapReduce, MapReduceApp, Output};

pub struct SequentialMapReduce {
    input: Input,
    mr_app: Box<dyn MapReduceApp>,
}

impl SequentialMapReduce {
    pub fn run_sync(self) -> anyhow::Result<Output> {
        let mut input: HashMap<String, String> = HashMap::new();
        for entry in fs::read_dir(self.input)? {
            let entry = entry.unwrap();
            let path = entry.path();

            if path.is_file() {
                let filename = path.file_name().unwrap().to_str().unwrap();
                let contents = fs::read_to_string(&path)?;
                println!("read from: {:?}", path);
                input.insert(filename.to_string(), contents);
            }
        }
        // println!("input {:?}", self.input.clone());
        let intermediate_key_values =
            input
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

        Ok(output)
    }
}

#[async_trait]
impl MapReduce for SequentialMapReduce {
    fn new(input_dir: Input, mr_app: Box<dyn MapReduceApp>) -> Self {
        Self {
            input: input_dir,
            mr_app,
        }
    }

    async fn run(self) -> anyhow::Result<()> {
        let output = self.run_sync()?;
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
        let output_file = "output-seq.txt";
        fs::write(output_file, lines)?;
        println!("write to: {:?}", output_file);
        Ok(())
    }
}
