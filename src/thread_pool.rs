use futures_lite::{future, prelude::*};
use std::sync::{mpsc, Arc, Mutex};
use std::thread::{self, sleep};
use std::time::Duration;

use async_channel::{Receiver, RecvError, Sender};

pub struct ThreadPool {
    workers: Vec<Worker>,
    sender: Sender<Job>,
}

type Job = Box<dyn FnOnce() + Send + 'static>;

impl ThreadPool {
    pub fn new(size: usize) -> ThreadPool {
        assert!(size > 0);

        let (sender, receiver) = async_channel::bounded(size);
        let receiver = Arc::new(receiver);
        let mut workers = Vec::with_capacity(size);

        for _ in 0..size {
            workers.push(Worker::new(Arc::clone(&receiver)));
        }

        ThreadPool { workers, sender }
    }

    pub fn execute<F>(&self, f: F)
    where
        F: FnOnce() + Send + 'static,
    {
        let job = Box::new(f);
        future::block_on(self.sender.send(job)).unwrap();
    }
}

struct Worker {
    thread: Option<thread::JoinHandle<()>>,
}

impl Worker {
    fn new(receiver: Arc<Receiver<Job>>) -> Worker {
        let thread = thread::spawn(move || loop {
            match future::block_on(receiver.recv()) {
                Ok(job) => {
                    job();
                }
                Err(err) => {
                    println!("recv err {:?}", err);
                }
            };
        });

        Worker {
            thread: Some(thread),
        }
    }
}
