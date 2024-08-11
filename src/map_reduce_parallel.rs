use async_trait::async_trait;
use futures;
use std::{
    collections::HashMap,
    fs,
    path::Path,
    sync::mpsc,
    thread::{self, sleep, JoinHandle},
    time::Duration,
};
use uuid::Uuid;

use crate::common::{Input, MapReduce, MapReduceApp};

pub struct ParallelMapReduce {
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
        coord.start_naive(self.input, 2, self.mr_app)?;

        Ok(())
    }
}

struct Coordinator {}

impl Coordinator {
    pub fn start_naive(
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

        for (worker, (filename, path)) in workers.iter_mut().zip(files.iter()) {
            worker.run(Task::Map(filename.clone(), path.clone()), &mr_app)?;
        }

        for mut worker in workers {
            worker.run(Task::Reduce(worker.id.clone()), &mr_app)?;
        }

        self.combine_all_out_files()?;

        Ok(())
    }

    pub fn start_channels(
        &self,
        dir: String,
        workers_count: usize,
        mr_app: Box<dyn MapReduceApp>,
    ) -> anyhow::Result<()> {
        let files = read_files_from_dir(dir)?;
        let mut workers: Vec<Worker> = vec![];

        for _ in 0..workers_count {
            let worker = Worker {
                id: Uuid::new_v4().to_string(),
            };
            workers.push(worker);
        }

        let (tx, mut rx) = mpsc::channel();

        let handle = thread::spawn(move || {
            for (filename, path) in files.iter() {
                tx.send(Task::Map(filename.clone(), path.clone()));
            }
            drop(tx);
        });

        handle.join().unwrap();

        for mut worker in workers {
            worker.run(Task::Reduce(worker.id.clone()), &mr_app)?;
        }

        self.combine_all_out_files()?;

        Ok(())
    }

    fn combine_all_out_files(&self) -> anyhow::Result<()> {
        let files = read_files_from_dir("output/output".to_string())?;
        let mut kva: HashMap<String, usize> = HashMap::new();
        for (_, path) in files {
            let f_contents = fs::read_to_string(path)?;
            f_contents
                .lines()
                .map(|line| line.split_once(" ").unwrap())
                .for_each(|(k, v)| {
                    let entry = kva.entry(k.to_string()).or_insert(0);
                    *entry += v.parse::<usize>().unwrap();
                });
        }
        let mut output_values: Vec<_> = kva.into_iter().collect();

        output_values.sort_by(|a, b| b.1.cmp(&a.1));

        let mut lines = String::from("");
        for (key, value) in output_values {
            lines.push_str(&format!("{} {}\n", key, value));
        }
        let output_file = "output-parallel.txt";
        fs::write(&output_file, lines)?;
        println!("reduce write: {}", output_file);

        Ok(())
    }
}

struct Worker {
    id: String,
    // thread: Option<JoinHandle<()>>,
}

impl Worker {
    pub fn run(&self, task: Task, mr_app: &Box<dyn MapReduceApp>) -> anyhow::Result<()> {
        match task {
            Task::Map(filename, path) => {
                let contents = fs::read_to_string(&path)?;
                println!("map read: {}", path);
                let key_values = mr_app.map(filename.clone(), contents);
                let i_path_str = self.construct_i_file_path(filename, self.id.clone());
                let i_path = Path::new(&i_path_str);
                let lines = key_values.iter().fold(String::from(""), |mut acc, (k, v)| {
                    acc.push_str(&format!("{},{}\n", k, v));
                    acc
                });
                sleep(Duration::from_secs(1));
                fs::create_dir_all(self.construct_worker_dir(self.id.clone()))?;
                fs::write(i_path, lines)?;
                println!("map write: {}", i_path.to_str().unwrap());
            }
            Task::Reduce(src_worker_id) => {
                let filenames =
                    read_files_from_dir(self.construct_worker_dir(src_worker_id.clone()))?;
                println!(
                    "reduce read: {}",
                    self.construct_worker_dir(src_worker_id.clone())
                );
                let (filename, _) = filenames.first().unwrap().clone();
                println!(
                    "reduce read 2: {}",
                    self.construct_i_file_path(filename.clone(), src_worker_id.clone())
                );
                let contents =
                    fs::read_to_string(self.construct_i_file_path(filename, src_worker_id))?;
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
                let output_file = self.construct_o_file_path(Uuid::new_v4().to_string());

                sleep(Duration::from_secs(1));

                fs::create_dir_all("output/output")?;
                fs::write(&output_file, lines)?;
                println!("reduce write: {}", output_file);
            }
        };
        Ok(())
    }

    fn construct_i_file_path(&self, filename: String, worker_id: String) -> String {
        format!("{}/{}", self.construct_worker_dir(worker_id), filename)
    }

    fn construct_o_file_path(&self, id: String) -> String {
        format!("output/output/{}", id)
    }

    fn construct_worker_dir(&self, worker_id: String) -> String {
        format!("output/worker-{}", worker_id)
    }
}

struct ThreadPool {
    workers: Vec<Worker>,
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
