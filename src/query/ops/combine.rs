use quantile::writable::WritableSketch;
use query::error::QueryError;
use query::ops::{OpOutput, QueryOp};
use std::cmp::{max, min, Ordering};
use std::collections::BinaryHeap;
use std::ops::DerefMut;
use time::TimeWindow;

pub struct CombineOp<'a> {
    inputs: Vec<Box<QueryOp + 'a>>,
    state: Option<State>,
}

impl<'a> CombineOp<'a> {
    pub fn new(inputs: Vec<Box<QueryOp + 'a>>) -> CombineOp {
        CombineOp {
            inputs,
            state: Some(State::initial()),
        }
    }
}

impl<'a> QueryOp for CombineOp<'a> {
    fn get_next(&mut self) -> Result<OpOutput, QueryError> {
        loop {
            let state = self.state.take().expect("Expected state to be nonempty");
            let (next_state, action) = state.transition(&mut self.inputs)?;
            self.state = Some(next_state);
            match action {
                Action::NoOutput => {
                    continue;
                }
                Action::OutputEnd => {
                    return Ok(OpOutput::End);
                }
                Action::OutputSketch(window, sketch) => {
                    return Ok(OpOutput::Sketch(window, sketch));
                }
            }
        }
    }
}

struct HeapItem {
    input_idx: usize,
    window: TimeWindow,
    sketch: WritableSketch,
}

impl HeapItem {
    fn from_input<'a>(
        input_idx: usize,
        input: &'a mut QueryOp,
    ) -> Result<Option<HeapItem>, QueryError> {
        match input.get_next()? {
            OpOutput::Sketch(window, sketch) => {
                let item = HeapItem {
                    input_idx,
                    window,
                    sketch,
                };
                Ok(Some(item))
            }
            OpOutput::End => Ok(None),
            _ => return Err(QueryError::InvalidInput),
        }
    }

    fn overlaps(&self, other: &HeapItem) -> bool {
        self.window.overlaps(&other.window)
    }

    fn merge(self, other: HeapItem) -> HeapItem {
        let min_start = min(self.window.start(), other.window.start());
        let max_end = max(self.window.end(), other.window.end());
        HeapItem {
            input_idx: self.input_idx,
            window: TimeWindow::new(min_start, max_end),
            sketch: self.sketch.merge(other.sketch),
        }
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

enum Action {
    NoOutput,
    OutputEnd,
    OutputSketch(TimeWindow, WritableSketch),
}

enum State {
    Empty,
    Selecting(BinaryHeap<HeapItem>),
    Combining(HeapItem, BinaryHeap<HeapItem>),
    Done,
}

impl State {
    fn initial() -> State {
        State::Empty
    }

    fn transition<'a>(
        self,
        inputs: &mut Vec<Box<QueryOp + 'a>>,
    ) -> Result<(State, Action), QueryError> {
        match self {
            State::Empty => State::transition_empty(inputs),
            State::Selecting(heap) => State::transition_selecting(inputs, heap),
            State::Combining(item, heap) => State::transition_combining(inputs, item, heap),
            State::Done => Ok((State::Done, Action::OutputEnd)),
        }
    }

    fn transition_empty<'a>(
        inputs: &mut Vec<Box<QueryOp + 'a>>,
    ) -> Result<(State, Action), QueryError> {
        let mut heap = BinaryHeap::with_capacity(inputs.len());
        for (input_idx, input_op) in inputs.iter_mut().enumerate() {
            if let Some(item) = HeapItem::from_input(input_idx, input_op.deref_mut())? {
                heap.push(item);
            }
        }
        let next_state = State::Selecting(heap);
        Ok((next_state, Action::NoOutput))
    }

    fn transition_selecting<'a>(
        mut inputs: &mut Vec<Box<QueryOp + 'a>>,
        mut heap: BinaryHeap<HeapItem>,
    ) -> Result<(State, Action), QueryError> {
        let next_item = heap.pop();
        match next_item {
            Some(item) => {
                State::replace_into_heap(&mut inputs, item.input_idx, &mut heap)?;
                Ok((State::Combining(item, heap), Action::NoOutput))
            }
            None => Ok((State::Done, Action::OutputEnd)),
        }
    }

    fn transition_combining<'a>(
        mut inputs: &mut Vec<Box<QueryOp + 'a>>,
        stored_item: HeapItem,
        mut heap: BinaryHeap<HeapItem>,
    ) -> Result<(State, Action), QueryError> {
        let next_item = heap.pop();
        match next_item {
            Some(item) => {
                State::replace_into_heap(&mut inputs, item.input_idx, &mut heap)?;
                if item.overlaps(&stored_item) {
                    let next_state = State::Combining(stored_item.merge(item), heap);
                    Ok((next_state, Action::NoOutput))
                } else {
                    let next_state = State::Combining(item, heap);
                    let action = Action::OutputSketch(stored_item.window, stored_item.sketch);
                    Ok((next_state, action))
                }
            }
            None => {
                let action = Action::OutputSketch(stored_item.window, stored_item.sketch);
                Ok((State::Done, action))
            }
        }
    }

    fn replace_into_heap<'a>(
        inputs: &mut Vec<Box<QueryOp + 'a>>,
        input_idx: usize,
        heap: &mut BinaryHeap<HeapItem>,
    ) -> Result<(), QueryError> {
        let replace_input = inputs
            .get_mut(input_idx)
            .expect("Could not retrieve input")
            .deref_mut();
        if let Some(item) = HeapItem::from_input(input_idx, replace_input)? {
            heap.push(item);
        };
        Ok(())
    }
}
