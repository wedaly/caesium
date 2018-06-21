use quantile::mergable::MergableSketch;
use query::error::QueryError;
use query::ops::{OpOutput, QueryOp};
use std::cmp::Ordering;
use std::cmp::{max, min};
use std::collections::BinaryHeap;
use time::TimeWindow;

pub struct CombineOp<'a> {
    inputs: Vec<Box<QueryOp + 'a>>,
    outputs: Vec<OpOutput>,
    processed: bool,
}

impl<'a> CombineOp<'a> {
    pub fn new(inputs: Vec<Box<QueryOp + 'a>>) -> CombineOp {
        CombineOp {
            inputs,
            outputs: Vec::new(),
            processed: false,
        }
    }

    fn process_inputs(&mut self) -> Result<(), QueryError> {
        let mut combiner = Combiner::new();
        for mut input in self.inputs.iter_mut() {
            loop {
                match input.get_next()? {
                    OpOutput::Sketch(window, sketch) => {
                        combiner.insert(window, sketch);
                    }
                    OpOutput::End => {
                        break;
                    }
                    _ => return Err(QueryError::InvalidInput),
                }
            }
        }
        self.outputs = combiner.results();
        self.outputs.reverse(); // because we `pop()` in reverse order
        Ok(())
    }
}

impl<'a> QueryOp for CombineOp<'a> {
    fn get_next(&mut self) -> Result<OpOutput, QueryError> {
        if !self.processed {
            self.process_inputs()?;
            self.processed = true;
        }

        match self.outputs.pop() {
            Some(out) => Ok(out),
            None => Ok(OpOutput::End),
        }
    }
}

struct Combiner {
    heap: BinaryHeap<HeapItem>,
}

impl Combiner {
    fn new() -> Combiner {
        Combiner {
            heap: BinaryHeap::new(),
        }
    }

    fn insert(&mut self, window: TimeWindow, sketch: MergableSketch) {
        self.heap.push(HeapItem { window, sketch })
    }

    fn results(mut self) -> Vec<OpOutput> {
        let mut results = Vec::new();
        loop {
            match (self.heap.pop(), self.heap.pop()) {
                (Some(x), Some(y)) => {
                    if x.window.end() <= y.window.start() || x.window.start() >= y.window.end() {
                        results.push(x.to_output());
                        self.heap.push(y);
                    } else {
                        let merged_start = min(x.window.start(), y.window.start());
                        let merged_end = max(x.window.end(), y.window.end());
                        let mut merged_sketch = x.sketch;
                        merged_sketch.merge(&y.sketch);
                        let merged_item = HeapItem {
                            window: TimeWindow::new(merged_start, merged_end),
                            sketch: merged_sketch,
                        };
                        self.heap.push(merged_item);
                    }
                }
                (Some(x), None) => {
                    results.push(x.to_output());
                }
                (None, None) => {
                    break;
                }
                (None, Some(_)) => panic!("Item in heap after popping last item"),
            }
        }
        results
    }
}

struct HeapItem {
    window: TimeWindow,
    sketch: MergableSketch,
}

impl HeapItem {
    fn to_output(self) -> OpOutput {
        OpOutput::Sketch(self.window, self.sketch)
    }
}

impl Eq for HeapItem {}

impl Ord for HeapItem {
    fn cmp(&self, other: &HeapItem) -> Ordering {
        // Order desc by window start time, so max heap will prioritize earliest start times
        self.window.start().cmp(&other.window.start()).reverse()
    }
}

impl PartialOrd for HeapItem {
    fn partial_cmp(&self, other: &HeapItem) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for HeapItem {
    fn eq(&self, other: &HeapItem) -> bool {
        self.window.start() == other.window.start()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use quantile::writable::WritableSketch;

    #[test]
    fn it_merges_sketches_when_combining() {
        let mut combiner = Combiner::new();
        let mut w1 = WritableSketch::new();
        let mut w2 = WritableSketch::new();
        for i in 0..10 {
            w1.insert(i as u64);
            w2.insert(i as u64);
        }
        let s1 = w1.to_serializable().to_mergable();
        let s2 = w2.to_serializable().to_mergable();
        combiner.insert(TimeWindow::new(0, 10), s1);
        combiner.insert(TimeWindow::new(5, 15), s2);
        let results = combiner.results();
        assert_eq!(results.len(), 1);
        let sketch = match results.first() {
            Some(OpOutput::Sketch(_, sketch)) => sketch,
            _ => panic!("Expected sketch output"),
        };
        assert_eq!(sketch.count(), 20);
    }

    #[test]
    fn it_combines_empty_inputs() {
        let combiner = Combiner::new();
        assert_eq!(combiner.results().len(), 0);
    }

    #[test]
    fn it_combines_single_input() {
        let mut combiner = Combiner::new();
        insert_windows(&mut combiner, vec![TimeWindow::new(0, 10)]);
        assert_windows(combiner, vec![TimeWindow::new(0, 10)]);
    }

    #[test]
    fn it_combines_two_overlapping_inputs_first_before_second() {
        let mut combiner = Combiner::new();
        insert_windows(
            &mut combiner,
            vec![TimeWindow::new(0, 10), TimeWindow::new(5, 15)],
        );
        assert_windows(combiner, vec![TimeWindow::new(0, 15)]);
    }

    #[test]
    fn it_combines_two_overlapping_inputs_second_before_first() {
        let mut combiner = Combiner::new();
        insert_windows(
            &mut combiner,
            vec![TimeWindow::new(5, 15), TimeWindow::new(0, 10)],
        );
        assert_windows(combiner, vec![TimeWindow::new(0, 15)]);
    }

    #[test]
    fn it_combines_two_overlapping_inputs_first_contains_second() {
        let mut combiner = Combiner::new();
        insert_windows(
            &mut combiner,
            vec![TimeWindow::new(0, 20), TimeWindow::new(5, 10)],
        );
        assert_windows(combiner, vec![TimeWindow::new(0, 20)]);
    }

    #[test]
    fn it_combines_two_overlapping_inputs_second_contains_first() {
        let mut combiner = Combiner::new();
        insert_windows(
            &mut combiner,
            vec![TimeWindow::new(5, 10), TimeWindow::new(0, 20)],
        );
        assert_windows(combiner, vec![TimeWindow::new(0, 20)]);
    }

    #[test]
    fn it_combines_two_nonadjacent_inputs_first_before_second() {
        let mut combiner = Combiner::new();
        insert_windows(
            &mut combiner,
            vec![TimeWindow::new(0, 10), TimeWindow::new(20, 30)],
        );
        assert_windows(
            combiner,
            vec![TimeWindow::new(0, 10), TimeWindow::new(20, 30)],
        );
    }

    #[test]
    fn it_combines_two_nonadjacent_inputs_first_after_second() {
        let mut combiner = Combiner::new();
        insert_windows(
            &mut combiner,
            vec![TimeWindow::new(20, 30), TimeWindow::new(0, 10)],
        );
        assert_windows(
            combiner,
            vec![TimeWindow::new(0, 10), TimeWindow::new(20, 30)],
        );
    }

    #[test]
    fn it_combines_two_adjacent_inputs_first_before_second() {
        let mut combiner = Combiner::new();
        insert_windows(
            &mut combiner,
            vec![TimeWindow::new(0, 10), TimeWindow::new(10, 20)],
        );
        assert_windows(
            combiner,
            vec![TimeWindow::new(0, 10), TimeWindow::new(10, 20)],
        );
    }

    #[test]
    fn it_combines_two_adjacent_inputs_first_after_second() {
        let mut combiner = Combiner::new();
        insert_windows(
            &mut combiner,
            vec![TimeWindow::new(10, 20), TimeWindow::new(0, 10)],
        );
        assert_windows(
            combiner,
            vec![TimeWindow::new(0, 10), TimeWindow::new(10, 20)],
        );
    }

    #[test]
    fn it_combines_three_inputs_non_overlapping() {
        let mut combiner = Combiner::new();
        insert_windows(
            &mut combiner,
            vec![
                TimeWindow::new(0, 10),
                TimeWindow::new(30, 40),
                TimeWindow::new(20, 30),
            ],
        );
        assert_windows(
            combiner,
            vec![
                TimeWindow::new(0, 10),
                TimeWindow::new(20, 30),
                TimeWindow::new(30, 40),
            ],
        );
    }

    #[test]
    fn it_combines_three_inputs_first_two_overlap() {
        let mut combiner = Combiner::new();
        insert_windows(
            &mut combiner,
            vec![
                TimeWindow::new(0, 10),
                TimeWindow::new(5, 30),
                TimeWindow::new(40, 50),
            ],
        );
        assert_windows(
            combiner,
            vec![TimeWindow::new(0, 30), TimeWindow::new(40, 50)],
        );
    }

    #[test]
    fn it_combines_three_inputs_second_two_overlap() {
        let mut combiner = Combiner::new();
        insert_windows(
            &mut combiner,
            vec![
                TimeWindow::new(0, 10),
                TimeWindow::new(25, 40),
                TimeWindow::new(20, 30),
            ],
        );
        assert_windows(
            combiner,
            vec![TimeWindow::new(0, 10), TimeWindow::new(20, 40)],
        );
    }

    #[test]
    fn it_combines_three_inputs_all_overlap() {
        let mut combiner = Combiner::new();
        insert_windows(
            &mut combiner,
            vec![
                TimeWindow::new(0, 10),
                TimeWindow::new(5, 15),
                TimeWindow::new(10, 20),
            ],
        );
        assert_windows(combiner, vec![TimeWindow::new(0, 20)]);
    }

    #[test]
    fn it_combines_duplicate_windows() {
        let mut combiner = Combiner::new();
        insert_windows(
            &mut combiner,
            vec![
                TimeWindow::new(0, 30),
                TimeWindow::new(30, 60),
                TimeWindow::new(0, 30),
                TimeWindow::new(30, 60),
            ],
        );
        assert_windows(
            combiner,
            vec![TimeWindow::new(0, 30), TimeWindow::new(30, 60)],
        );
    }

    fn insert_windows(combiner: &mut Combiner, windows: Vec<TimeWindow>) {
        for window in windows.iter() {
            combiner.insert(*window, MergableSketch::empty());
        }
    }

    fn assert_windows(combiner: Combiner, expected: Vec<TimeWindow>) {
        let results = combiner.results();
        let windows: Vec<TimeWindow> = results
            .iter()
            .filter_map(|output| match output {
                OpOutput::Sketch(window, _) => Some(*window),
                _ => None,
            })
            .collect();
        assert_eq!(windows, expected);
    }
}
