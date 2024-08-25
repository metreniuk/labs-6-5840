use std::{collections::HashMap, fs};

use crate::common::{read_files_from_dir, MapReduceApp, Task};

use super::worker_pool::WorkerPool;

pub struct Coordinator {}

impl Coordinator {
    // pub fn start_naive(
    //     &self,
    //     dir: String,
    //     _workers_count: u32,
    //     mr_app: Box<dyn MapReduceApp>,
    // ) -> anyhow::Result<()> {
    //     let files = read_files_from_dir(dir)?;
    //     let mut workers: Vec<Worker> = vec![];

    //     for _ in files.iter() {
    //         let worker = Worker {
    //             id: Uuid::new_v4().to_string(),
    //         };
    //         workers.push(worker);
    //     }

    //     for (worker, (filename, path)) in workers.iter_mut().zip(files.iter()) {
    //         worker.run(Task::Map(filename.clone(), path.clone()), &mr_app)?;
    //     }

    //     for mut worker in workers {
    //         worker.run(Task::Reduce(worker.id.clone()), &mr_app)?;
    //     }

    //     self.combine_all_out_files()?;

    //     Ok(())
    // }

    pub fn start_pool(
        &self,
        dir: String,
        workers_count: usize,
        mr_app: Box<dyn MapReduceApp>,
    ) -> anyhow::Result<()> {
        let files = read_files_from_dir(dir)?;
        let pool = WorkerPool::new(workers_count, mr_app);

        for (filename, path) in files {
            pool.run_task(Task::Map(filename.clone(), path.clone()))
        }

        pool.wait();

        for worker in &pool.workers {
            pool.run_task(Task::Reduce(worker.id.clone()));
        }

        pool.wait();

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
