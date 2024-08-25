mod coordinator;
mod worker_pool;

use crate::common::{Input, MapReduce, MapReduceApp};
use async_trait::async_trait;
use coordinator::Coordinator;

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
        coord.start_pool(self.input, 2, self.mr_app)?;

        Ok(())
    }
}
