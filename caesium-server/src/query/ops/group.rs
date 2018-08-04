use caesium_core::quantile::writable::WritableSketch;
use caesium_core::time::timestamp::{days, hours};
use caesium_core::time::window::TimeWindow;
use query::error::QueryError;
use query::ops::{OpOutput, QueryOp};
use std::cmp::{max, min};

pub struct GroupOp<'a> {
    input: Box<QueryOp + 'a>,
    group_type: GroupType,
    state: Option<State>,
}

impl<'a> GroupOp<'a> {
    pub fn new(group_type: GroupType, input: Box<QueryOp + 'a>) -> Result<GroupOp<'a>, QueryError> {
        let op = GroupOp {
            group_type,
            input,
            state: Some(State::initial()),
        };
        Ok(op)
    }
}

impl<'a> QueryOp for GroupOp<'a> {
    fn get_next(&mut self) -> Result<OpOutput, QueryError> {
        let group_type = self.group_type;
        loop {
            let state = self.state.take().expect("Expected state to be nonempty");
            let (next_state, action) = state.transition(group_type, &mut *self.input)?;
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

#[derive(Copy, Clone)]
pub enum GroupType {
    Seconds,
    Hours,
    Days,
}

type GroupId = u64;

impl GroupType {
    pub fn from_str(s: &str) -> Result<GroupType, QueryError> {
        match s {
            "seconds" => Ok(GroupType::Seconds),
            "hours" => Ok(GroupType::Hours),
            "days" => Ok(GroupType::Days),
            _ => Err(QueryError::InvalidArgValue(
                "Group must be either seconds, hours, or days",
            )),
        }
    }

    fn calculate_group_id(&self, window: TimeWindow) -> GroupId {
        let start_ts = window.start();
        match self {
            GroupType::Seconds => start_ts,
            GroupType::Hours => hours(start_ts),
            GroupType::Days => days(start_ts),
        }
    }
}

enum Action {
    NoOutput,
    OutputEnd,
    OutputSketch(TimeWindow, WritableSketch),
}

enum State {
    Empty,
    Merging(GroupId, TimeWindow, WritableSketch),
    Done,
}

impl State {
    fn initial() -> State {
        State::Empty
    }

    fn transition<'a>(
        self,
        group_type: GroupType,
        input: &'a mut QueryOp,
    ) -> Result<(State, Action), QueryError> {
        match self {
            State::Empty => State::transition_empty(group_type, input),
            State::Merging(group_id, window, sketch) => {
                State::transition_merging(group_id, window, sketch, group_type, input)
            }
            State::Done => Ok((State::Done, Action::OutputEnd)),
        }
    }

    fn transition_empty<'a>(
        group_type: GroupType,
        input: &'a mut QueryOp,
    ) -> Result<(State, Action), QueryError> {
        match input.get_next()? {
            OpOutput::End => Ok((State::Done, Action::OutputEnd)),
            OpOutput::Sketch(window, sketch) => {
                let group_id = group_type.calculate_group_id(window);
                let next_state = State::Merging(group_id, window, sketch);
                Ok((next_state, Action::NoOutput))
            }
            _ => Err(QueryError::InvalidInput),
        }
    }

    fn transition_merging<'a>(
        prev_group_id: GroupId,
        prev_window: TimeWindow,
        prev_sketch: WritableSketch,
        group_type: GroupType,
        input: &'a mut QueryOp,
    ) -> Result<(State, Action), QueryError> {
        match input.get_next()? {
            OpOutput::End => {
                let action = Action::OutputSketch(prev_window, prev_sketch);
                Ok((State::Done, action))
            }
            OpOutput::Sketch(window, sketch) => {
                let next_group_id = group_type.calculate_group_id(window);
                if next_group_id == prev_group_id {
                    let min_start = min(window.start(), prev_window.start());
                    let max_end = max(window.end(), prev_window.end());
                    let merged_window = TimeWindow::new(min_start, max_end);
                    let next_state =
                        State::Merging(next_group_id, merged_window, sketch.merge(prev_sketch));
                    Ok((next_state, Action::NoOutput))
                } else {
                    let next_state = State::Merging(next_group_id, window, sketch);
                    let action = Action::OutputSketch(prev_window, prev_sketch);
                    Ok((next_state, action))
                }
            }
            _ => Err(QueryError::InvalidInput),
        }
    }
}
