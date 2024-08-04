use std::{collections::HashMap, fs, path::Path};

use async_trait::async_trait;
use uuid::Uuid;

use crate::common::{Input, MapReduce, MapReduceApp};

struct ParallelMapReduce {
    input: Input,
    mr_app: Box<dyn MapReduceApp>,
}

#[async_trait]
impl MapReduce for ParallelMapReduce {
    fn new(input: Input, mr_app: Box<dyn MapReduceApp>) -> Self {
        Self { input, mr_app }
    }

    async fn run(self) -> anyhow::Result<()> {
        let coord = Coordinator {};
        coord.start(self.input, 2, self.mr_app)?;

        Ok(())
    }
}

struct Coordinator {}

impl Coordinator {
    pub fn start(
        &self,
        dir: String,
        _workers_count: u32,
        mr_app: Box<dyn MapReduceApp>,
    ) -> anyhow::Result<()> {
        let files = read_files_from_dir(dir)?;
        let mut workers: Vec<Worker> = vec![];

        for _ in files.iter() {
            let worker = Worker {
                id: Uuid::new_v4().to_string(),
            };
            workers.push(worker);
        }

        for (worker, (filename, path)) in workers.iter().zip(files.iter()) {
            worker.run(Task::Map(filename.clone(), path.clone()), &mr_app)?;
        }

        for worker in workers {
            worker.run(Task::Reduce(worker.id.clone()), &mr_app)?;
        }

        Ok(())
    }
}

struct Worker {
    id: String,
}

impl Worker {
    pub fn run(&self, task: Task, mr_app: &Box<dyn MapReduceApp>) -> anyhow::Result<()> {
        match task {
            Task::Map(filename, path) => {
                let contents = fs::read_to_string(&path)?;
                let key_values = mr_app.map(filename.clone(), contents);
                let i_path_str = self.construct_file_path(filename, self.id.clone());
                let i_path = Path::new(&i_path_str);
                let lines = key_values.iter().fold(String::from(""), |mut acc, (k, v)| {
                    acc.push_str(&format!("{},{}\n", k, v));
                    acc
                });
                fs::write(i_path, lines)?;
            }
            Task::Reduce(src_worker_id) => {
                let filenames = read_files_from_dir(src_worker_id.clone())?;
                let (filename, _) = filenames.first().unwrap().clone();
                let contents =
                    fs::read_to_string(self.construct_file_path(filename, src_worker_id))?;
                let intermediate_key_values: Vec<(&str, &str)> = contents
                    .lines()
                    .map(|line| line.split_once(",").unwrap())
                    .collect();
                let grouped_key_values: HashMap<String, Vec<String>> = intermediate_key_values
                    .into_iter()
                    .fold(HashMap::new(), |mut acc, (key, value)| {
                        acc.entry(key.to_string())
                            .and_modify(|e| e.push(value.to_string()))
                            .or_insert(vec![value.to_string()]);
                        acc
                    });

                let output: HashMap<String, String> = grouped_key_values
                    .into_iter()
                    .map(|(key, ivalues)| (key.clone(), mr_app.reduce(key, ivalues)))
                    .collect();

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
                let output_file = format!("output-{}", Uuid::new_v4());
                fs::write(&output_file, lines)?;
            }
        };
        Ok(())
    }

    fn construct_file_path(&self, filename: String, worker_id: String) -> String {
        format!("{}/{}", worker_id, filename)
    }
}

enum Task {
    // filename
    Map(String, String),
    // src worker ID
    Reduce(String),
}

fn read_files_from_dir(input_dir: String) -> anyhow::Result<Vec<(String, String)>> {
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
