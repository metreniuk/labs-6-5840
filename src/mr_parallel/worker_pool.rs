use std::collections::HashMap;
use std::fs;
use std::path::Path;
use std::sync::Arc;
use std::thread::{self, sleep};
use std::time::Duration;
use uuid::Uuid;

use async_channel::{Receiver, Sender};

use crate::common::{read_files_from_dir, MapReduceApp, Task};

pub struct WorkerPool {
    pub workers: Vec<Worker>,
    sender: Sender<Job>,
    app: Arc<Box<dyn MapReduceApp>>,
}

type Job = Box<dyn FnOnce(String) + Send + 'static>;

impl WorkerPool {
    pub fn new(size: usize, app: Box<dyn MapReduceApp>) -> WorkerPool {
        assert!(size > 0);

        let (sender, receiver) = async_channel::bounded(size);
        let receiver = Arc::new(receiver);
        let mut workers = Vec::with_capacity(size);

        for _ in 0..size {
            workers.push(Worker::new(Arc::clone(&receiver)));
        }

        WorkerPool {
            workers,
            sender,
            app: Arc::new(app),
        }
    }

    fn execute<F>(&self, f: F)
    where
        F: FnOnce(String) + Send + 'static,
    {
        let job = Box::new(f);
        self.sender.send_blocking(job).unwrap();
    }

    pub fn wait(&self) {
        loop {
            if self.sender.is_empty() {
                return;
            }
            // TODO: fix with a flag
            sleep(Duration::from_millis(2000));
        }
    }

    pub fn run_task(&self, task: Task) {
        let app_clone = Arc::clone(&self.app);
        self.execute(move |worker_id| {
            match task {
                Task::Map(filename, path) => {
                    let contents =
                        fs::read_to_string(&path).expect(&format!("failed to read {:?}", path));
                    println!("map read: {}", path);
                    let key_values = app_clone.map(filename.clone(), contents);
                    let i_path_str = construct_i_file_path(filename, worker_id.clone());
                    let i_path = Path::new(&i_path_str);
                    let lines = key_values.iter().fold(String::from(""), |mut acc, (k, v)| {
                        acc.push_str(&format!("{},{}\n", k, v));
                        acc
                    });
                    sleep(Duration::from_millis(500));
                    println!("wake up: {}", construct_worker_dir(worker_id.clone()));
                    fs::create_dir_all(construct_worker_dir(worker_id.clone()))
                        .expect("failed to create dir");
                    fs::write(i_path, lines)
                        .expect(&format!("failed to write file to {:?}", i_path));
                    println!("map write: {}", i_path.to_str().unwrap());
                }
                Task::Reduce(src_worker_id) => {
                    let filenames =
                        read_files_from_dir(construct_worker_dir(src_worker_id.clone())).unwrap();
                    println!(
                        "reduce read {} files from {}",
                        filenames.len(),
                        construct_worker_dir(src_worker_id.clone()),
                    );
                    let mut contents = String::new();

                    for (_, path) in filenames {
                        let file_contents = fs::read_to_string(path)
                            // let file_contents = fs::read_to_string(construct_i_file_path(
                            //     filename,
                            //     src_worker_id.clone(),
                            // ))
                            .unwrap();
                        contents.push_str(&file_contents);
                    }

                    let intermediate_key_values: Vec<(&str, &str)> = contents
                        .lines()
                        .map(|line| line.split_once(",").unwrap())
                        .collect();

                    println!("contents {:?}", intermediate_key_values.len());

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
                        .map(|(key, ivalues)| (key.clone(), app_clone.reduce(key, ivalues)))
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
                    let output_file = construct_o_file_path(src_worker_id);

                    sleep(Duration::from_millis(500));

                    fs::create_dir_all("output/output").unwrap();
                    fs::write(&output_file, lines).unwrap();
                    println!("reduce write: {}", output_file);
                }
            };
        });
    }
}

fn construct_i_file_path(filename: String, worker_id: String) -> String {
    format!("{}/{}", construct_worker_dir(worker_id), filename)
}

fn construct_o_file_path(id: String) -> String {
    format!("output/output/{}", id)
}

fn construct_worker_dir(worker_id: String) -> String {
    format!("output/worker-{}", worker_id)
}

pub struct Worker {
    pub id: String,
    // thread: Option<thread::JoinHandle<()>>,
}

impl Worker {
    pub fn new(receiver: Arc<Receiver<Job>>) -> Worker {
        let id = Uuid::new_v4().to_string();
        let id_clone = id.clone();
        thread::spawn(move || loop {
            match receiver.recv_blocking() {
                Ok(job) => {
                    job(id_clone.clone());
                }
                Err(err) => {
                    println!("recv err {:?}", err);
                }
            };
        });

        Worker {
            id: id.clone(),
            // thread: Some(thread),
        }
    }
}
