use quantile::mergable::MergableSketch;
use query::error::QueryError;
use query::ops::{OpOutput, QueryOp};
use std::cmp::{max, min};
use storage::datasource::{DataCursor, DataSource};
use time::{days, hours, TimeStamp, TimeWindow};

pub struct FetchOp<'a> {
    cursor: Box<DataCursor + 'a>,
    group_type: GroupType,
    state: Option<State>,
}

impl<'a> FetchOp<'a> {
    pub fn new(
        metric: String,
        source: &'a DataSource,
        group_type: GroupType,
        start_ts: Option<TimeStamp>,
        end_ts: Option<TimeStamp>,
    ) -> Result<FetchOp<'a>, QueryError> {
        let cursor = source.fetch_range(&metric, start_ts, end_ts)?;
        let op = FetchOp {
            cursor,
            group_type,
            state: Some(State::initial()),
        };
        Ok(op)
    }
}

impl<'a> QueryOp for FetchOp<'a> {
    fn get_next(&mut self) -> Result<OpOutput, QueryError> {
        let group_type = self.group_type;
        loop {
            let state = self.state.take().expect("Expected state to be nonempty");
            let (next_state, action) = state.transition(group_type, &mut *self.cursor)?;
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
    OutputSketch(TimeWindow, MergableSketch),
}

enum State {
    Empty,
    Merging(GroupId, TimeWindow, MergableSketch),
    Done,
}

impl State {
    fn initial() -> State {
        State::Empty
    }

    fn transition(
        self,
        group_type: GroupType,
        cursor: &mut DataCursor,
    ) -> Result<(State, Action), QueryError> {
        match self {
            State::Empty => State::transition_empty(group_type, cursor),
            State::Merging(group_id, window, sketch) => {
                State::transition_merging(group_id, window, sketch, group_type, cursor)
            }
            State::Done => Ok((State::Done, Action::OutputEnd)),
        }
    }

    fn transition_empty(
        group_type: GroupType,
        cursor: &mut DataCursor,
    ) -> Result<(State, Action), QueryError> {
        match cursor.get_next()? {
            None => Ok((State::Done, Action::OutputEnd)),
            Some(row) => {
                let group_id = group_type.calculate_group_id(row.window);
                let next_state = State::Merging(group_id, row.window, row.sketch);
                Ok((next_state, Action::NoOutput))
            }
        }
    }

    fn transition_merging(
        group_id: GroupId,
        window: TimeWindow,
        mut sketch: MergableSketch,
        group_type: GroupType,
        cursor: &mut DataCursor,
    ) -> Result<(State, Action), QueryError> {
        match cursor.get_next()? {
            None => {
                let action = Action::OutputSketch(window, sketch);
                Ok((State::Done, action))
            }
            Some(row) => {
                let next_group_id = group_type.calculate_group_id(row.window);
                if next_group_id == group_id {
                    let min_start = min(row.window.start(), window.start());
                    let max_end = max(row.window.end(), window.end());
                    let merged_window = TimeWindow::new(min_start, max_end);
                    sketch.merge(&row.sketch);
                    let next_state = State::Merging(group_id, merged_window, sketch);
                    Ok((next_state, Action::NoOutput))
                } else {
                    let next_state = State::Merging(next_group_id, row.window, row.sketch);
                    let action = Action::OutputSketch(window, sketch);
                    Ok((next_state, action))
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use storage::datasource::DataRow;
    use storage::mock::MockDataSource;

    #[test]
    fn it_handles_empty_input() {
        let db = MockDataSource::new();
        let op = FetchOp::new("foo".to_string(), &db, GroupType::Seconds, None, None).unwrap();
        assert_outputs(op, vec![]);
    }

    #[test]
    fn it_groups_by_second() {
        let db = db_with_windows(
            &"foo",
            vec![
                TimeWindow::new(15, 30),
                TimeWindow::new(15, 35),
                TimeWindow::new(20, 40),
            ],
        );
        let op = FetchOp::new("foo".to_string(), &db, GroupType::Seconds, None, None).unwrap();
        assert_outputs(op, vec![TimeWindow::new(15, 35), TimeWindow::new(20, 40)]);
    }

    #[test]
    fn it_groups_by_hour() {
        let db = db_with_windows(
            &"foo",
            vec![
                TimeWindow::new(20, 45),
                TimeWindow::new(3600, 3605),
                TimeWindow::new(4000, 4500),
                TimeWindow::new(7250, 7500),
            ],
        );
        let op = FetchOp::new("foo".to_string(), &db, GroupType::Hours, None, None).unwrap();
        assert_outputs(
            op,
            vec![
                TimeWindow::new(20, 45),
                TimeWindow::new(3600, 4500),
                TimeWindow::new(7250, 7500),
            ],
        );
    }

    #[test]
    fn it_groups_by_day() {
        let db = db_with_windows(
            &"foo",
            vec![
                TimeWindow::new(20, 45),
                TimeWindow::new(3600, 3605),
                TimeWindow::new(4000, 4500),
                TimeWindow::new(7250, 7500),
                TimeWindow::new(87000, 90000),
                TimeWindow::new(92450, 123000),
            ],
        );
        let op = FetchOp::new("foo".to_string(), &db, GroupType::Days, None, None).unwrap();
        assert_outputs(
            op,
            vec![TimeWindow::new(20, 7500), TimeWindow::new(87000, 123000)],
        );
    }

    #[test]
    fn it_filters_start_and_end_range() {
        let db = db_with_windows(
            &"foo",
            vec![
                TimeWindow::new(20, 45),
                TimeWindow::new(3600, 3605),
                TimeWindow::new(4000, 4500),
                TimeWindow::new(7250, 7500),
                TimeWindow::new(87000, 90000),
                TimeWindow::new(92450, 123000),
            ],
        );
        let op = FetchOp::new(
            "foo".to_string(),
            &db,
            GroupType::Seconds,
            Some(3700),
            Some(7400),
        ).unwrap();
        assert_outputs(op, vec![TimeWindow::new(4000, 4500)]);
    }

    fn db_with_windows(metric: &str, windows: Vec<TimeWindow>) -> MockDataSource {
        let mut db = MockDataSource::new();
        for window in windows.iter() {
            let row = DataRow {
                window: *window,
                sketch: MergableSketch::empty(),
            };
            db.add_row(metric, row)
        }
        db
    }

    fn assert_outputs(mut op: FetchOp, expected: Vec<TimeWindow>) {
        let mut outputs = Vec::new();
        for _ in 0..100 {
            match op.get_next().unwrap() {
                OpOutput::End => {
                    break;
                }
                OpOutput::Sketch(window, _) => {
                    outputs.push(window);
                }
                OpOutput::Quantile(_, _) => {
                    panic!("Unexpected output");
                }
            }
        }
        assert_eq!(outputs, expected);
    }
}
