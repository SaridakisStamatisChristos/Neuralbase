use crossbeam_deque::{Injector, Steal, Stealer, Worker};
use std::ops::Range;
use std::sync::{Arc, Mutex};
use std::thread;

type ChunkResults<R> = Arc<Mutex<Vec<(usize, Vec<R>)>>>;

#[derive(Debug, Clone)]
pub struct MorselScheduler {
    pub morsel_size: usize,
    pub worker_threads: usize,
}

impl MorselScheduler {
    pub fn new(morsel_size: usize) -> Self {
        let workers = num_cpus::get_physical().max(1);
        Self {
            morsel_size: morsel_size.max(1),
            worker_threads: workers,
        }
    }

    /// Create a scheduler with a fixed number of worker threads.
    /// Useful for deterministic parallelism control and operational tuning.
    pub fn with_workers(morsel_size: usize, worker_threads: usize) -> Self {
        Self {
            morsel_size: morsel_size.max(1),
            worker_threads: worker_threads.max(1),
        }
    }

    pub fn parallel_map_ranges<R, F>(&self, total_len: usize, task: F) -> Result<Vec<R>, String>
    where
        R: Send,
        F: Fn(Range<usize>) -> Vec<R> + Sync,
    {
        if total_len == 0 {
            return Ok(Vec::new());
        }

        let injector = Injector::new();
        let mut start = 0;
        while start < total_len {
            let end = (start + self.morsel_size).min(total_len);
            injector.push(start..end);
            start = end;
        }

        let workers = (0..self.worker_threads)
            .map(|_| Worker::new_fifo())
            .collect::<Vec<Worker<Range<usize>>>>();
        let stealers = workers.iter().map(Worker::stealer).collect::<Vec<_>>();
        let results: ChunkResults<R> = Arc::new(Mutex::new(Vec::<(usize, Vec<R>)>::new()));
        let injector_ref = &injector;

        thread::scope(|scope| {
            for worker in workers {
                let shared_results = Arc::clone(&results);
                let local_stealers = stealers.clone();
                let task_ref = &task;

                scope.spawn(move || {
                    worker_loop(
                        worker,
                        injector_ref,
                        &local_stealers,
                        task_ref,
                        &shared_results,
                    );
                });
            }
        });

        let mut chunks = results
            .lock()
            .map_err(|_| "failed to lock work-stealing results".to_string())?
            .drain(..)
            .collect::<Vec<(usize, Vec<R>)>>();

        chunks.sort_by_key(|(range_start, _)| *range_start);
        Ok(chunks.into_iter().flat_map(|(_, out)| out).collect())
    }
}

fn worker_loop<R, F>(
    worker: Worker<Range<usize>>,
    injector: &Injector<Range<usize>>,
    stealers: &[Stealer<Range<usize>>],
    task: &F,
    results: &ChunkResults<R>,
) where
    R: Send,
    F: Fn(Range<usize>) -> Vec<R> + Sync,
{
    loop {
        let next = worker
            .pop()
            .or_else(|| steal_work(&worker, injector, stealers));
        let Some(range) = next else {
            break;
        };

        let out = task(range.clone());
        if let Ok(mut guard) = results.lock() {
            guard.push((range.start, out));
        }
    }
}

fn steal_work(
    worker: &Worker<Range<usize>>,
    injector: &Injector<Range<usize>>,
    stealers: &[Stealer<Range<usize>>],
) -> Option<Range<usize>> {
    loop {
        match injector.steal_batch_and_pop(worker) {
            Steal::Success(range) => return Some(range),
            Steal::Retry => continue,
            Steal::Empty => break,
        }
    }

    for stealer in stealers {
        match stealer.steal() {
            Steal::Success(range) => return Some(range),
            Steal::Retry => continue,
            Steal::Empty => {}
        }
    }

    None
}
