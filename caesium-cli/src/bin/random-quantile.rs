extern crate caesium_core;
extern crate clap;
extern crate rand;

use caesium_core::time::timer::Timer;
use clap::{App, Arg};
use sketch::RandomSketch;
use std::fs::File;
use std::io;
use std::io::{BufRead, BufReader};
use std::num::ParseIntError;

fn main() -> Result<(), Error> {
    let args = parse_args()?;
    let data = read_data_file(&args.data_path)?;

    for i in 0..args.num_trials {
        println!("Trial {}", i);
        let mut timer = Timer::new();
        let s = build_sketch(&data, &mut timer);
        println!("Inserted {} values", s.count());
        summarize_time(&timer);
        println!("================")
    }
    Ok(())
}

#[derive(Debug)]
struct Args {
    data_path: String,
    num_trials: usize,
}

fn parse_args() -> Result<Args, Error> {
    let matches = App::new("RANDOM quantile tool")
        .about("Time inserts into RANDOM sketch implementation")
        .arg(
            Arg::with_name("DATA_PATH")
                .index(1)
                .required(true)
                .help("Path to data file, one unsigned 32-bit integer per line"),
        ).arg(
            Arg::with_name("NUM_TRIALS")
                .long("num-trials")
                .short("t")
                .takes_value(true)
                .help("Number of trials to run (default 1)"),
        ).get_matches();

    let data_path = matches.value_of("DATA_PATH").unwrap().to_string();
    let num_trials = matches
        .value_of("NUM_TRIALS")
        .unwrap_or("1")
        .parse::<usize>()?;
    Ok(Args {
        data_path,
        num_trials,
    })
}

fn read_data_file(path: &str) -> Result<Vec<u32>, Error> {
    let file = File::open(path)?;
    let reader = BufReader::new(file);
    let values = reader
        .lines()
        .filter_map(|result| {
            result
                .map_err(|e| Error::IOError(e))
                .and_then(|l| l.parse::<u32>().map_err(From::from))
                .ok()
        }).collect();
    Ok(values)
}

fn build_sketch(data: &[u32], timer: &mut Timer) -> RandomSketch {
    let mut s = RandomSketch::new();
    timer.start();
    for v in data.iter() {
        s.insert(*v);
    }
    timer.stop();
    s
}

fn summarize_time(timer: &Timer) {
    let d = timer.duration().expect("Could not retrieve timer duration");
    let ms = (d.as_secs() * 1_000) + (d.subsec_nanos() / 1_000_000) as u64;
    println!("total insert time (ms) = {}", ms);
}

#[derive(Debug)]
enum Error {
    IOError(io::Error),
    ParseIntError(ParseIntError),
}

impl From<io::Error> for Error {
    fn from(err: io::Error) -> Error {
        Error::IOError(err)
    }
}

impl From<ParseIntError> for Error {
    fn from(err: ParseIntError) -> Error {
        Error::ParseIntError(err)
    }
}

mod sketch {
    use rand;
    use rand::rngs::SmallRng;
    use rand::{FromEntropy, Rng};
    use std::collections::HashMap;

    pub const BUFSIZE: usize = 256;
    pub const BUFCOUNT: usize = 8;

    #[derive(Clone)]
    pub struct Sampler {
        count: usize,
        sample_idx: usize,
        val: u32,
        max_weight: usize,
        generator: SmallRng,
    }

    impl Sampler {
        pub fn new() -> Sampler {
            Sampler {
                count: 0,
                sample_idx: 0,
                val: 0,
                max_weight: 1,
                generator: SmallRng::from_entropy(),
            }
        }

        pub fn set_max_weight(&mut self, max_weight: usize) {
            debug_assert!(max_weight > 0, "Max weight must be positive");
            self.max_weight = max_weight;
        }

        pub fn sample(&mut self, val: u32) -> Option<u32> {
            if self.sample_idx == self.count {
                self.val = val;
            }

            self.count += 1;
            if self.count == self.max_weight {
                self.count = 0;
                self.sample_idx = self.generator.gen_range(0, self.max_weight);
                Some(self.val)
            } else {
                None
            }
        }
    }

    #[derive(Clone)]
    pub struct RandomSketch {
        sampler: Sampler,
        current_buffer: usize,
        count: usize,
        buffers: [[u32; BUFSIZE]; BUFCOUNT],
        lengths: [usize; BUFCOUNT],
        levels: [usize; BUFCOUNT],
        active_level: usize,
        level_limit: usize,
    }

    impl RandomSketch {
        pub fn new() -> RandomSketch {
            RandomSketch {
                sampler: Sampler::new(),
                current_buffer: 0,
                count: 0,
                buffers: [[0; BUFSIZE]; BUFCOUNT],
                lengths: [0; BUFCOUNT],
                levels: [0; BUFCOUNT],
                active_level: 0,
                level_limit: RandomSketch::calc_level_limit(0),
            }
        }

        pub fn insert(&mut self, val: u32) {
            self.count += 1;
            if let Some(val) = self.sampler.sample(val) {
                self.insert_sampled(val);
            }
        }

        pub fn count(&self) -> usize {
            self.count
        }

        fn insert_sampled(&mut self, val: u32) {
            self.update_active_level();
            let idx = self.choose_insert_buffer();
            let len = self.lengths[idx];
            debug_assert!(len < BUFSIZE);
            self.buffers[idx][len] = val;
            self.lengths[idx] += 1;
            self.current_buffer = idx;
        }

        fn choose_insert_buffer(&mut self) -> usize {
            if self.lengths[self.current_buffer] < BUFSIZE {
                self.current_buffer
            } else if let Some(idx) = self.find_empty_buffer() {
                idx
            } else {
                self.merge_two_buffers()
            }
        }

        fn merge_two_buffers(&mut self) -> usize {
            if let Some((b1, b2)) = self.find_buffers_to_merge() {
                self.compact_and_return_empty(b1, b2)
            } else {
                panic!("Could not find two buffers to merge!");
            }
        }

        fn compact_and_return_empty(&mut self, b1: usize, b2: usize) -> usize {
            debug_assert!(self.lengths[b1] == BUFSIZE);
            debug_assert!(self.lengths[b2] == BUFSIZE);

            let mut tmp = [0; BUFSIZE * 2];
            tmp[..BUFSIZE].copy_from_slice(&self.buffers[b1][..]);
            tmp[BUFSIZE..BUFSIZE * 2].copy_from_slice(&self.buffers[b2][..]);
            tmp.sort_unstable();

            // Write surviving values to b2
            let mut sel = rand::random::<bool>();
            let mut idx = 0;
            for &val in tmp.iter() {
                if sel {
                    self.buffers[b2][idx] = val;
                    idx += 1;
                }
                sel = !sel;
            }
            self.levels[b2] += 1;

            // Empty and return b1
            self.lengths[b1] = 0;
            self.levels[b1] = self.active_level;
            b1
        }

        fn find_empty_buffer(&self) -> Option<usize> {
            self.lengths.iter().position(|&len| len == 0)
        }

        fn find_buffers_to_merge(&self) -> Option<(usize, usize)> {
            debug_assert!(self.lengths.iter().all(|&len| len == BUFSIZE));
            let mut level_map = HashMap::with_capacity(BUFCOUNT);
            let mut best_match = None;
            for (b1, level) in self.levels.iter().enumerate() {
                if let Some(b2) = level_map.insert(level, b1) {
                    best_match = match best_match {
                        None => Some((level, b1, b2)),
                        Some((old_level, _, _)) if level < old_level => Some((level, b1, b2)),
                        Some(current_best) => Some(current_best),
                    }
                }
            }
            best_match.map(|(_, b1, b2)| (b1, b2))
        }

        fn update_active_level(&mut self) {
            if self.count > self.level_limit {
                self.active_level += 1;
                self.sampler.set_max_weight(1 << self.active_level);
                self.level_limit = RandomSketch::calc_level_limit(self.active_level);
            }
        }

        fn calc_level_limit(level: usize) -> usize {
            (1 << (level + BUFCOUNT - 2)) * BUFSIZE
        }
    }
}
