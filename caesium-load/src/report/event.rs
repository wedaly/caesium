use time::{get_time, Timespec};

pub enum Event {
    MetricSentEvent {
        event_ts: Timespec,
    },
    SketchSentEvent {
        event_ts: Timespec,
    },
    ErrorEvent {
        event_ts: Timespec,
    },
    QuerySentEvent {
        event_ts: Timespec,
        worker_id: usize,
        query_id: usize,
    },
    QueryBytesReceivedEvent {
        event_ts: Timespec,
        worker_id: usize,
        query_id: usize,
    },
}

impl Event {
    pub fn metric_inserted_event() -> Event {
        Event::MetricSentEvent {
            event_ts: get_time(),
        }
    }

    pub fn sketch_sent_event() -> Event {
        Event::SketchSentEvent {
            event_ts: get_time(),
        }
    }

    pub fn error_event() -> Event {
        Event::ErrorEvent {
            event_ts: get_time(),
        }
    }

    pub fn query_sent_event(worker_id: usize, query_id: usize) -> Event {
        Event::QuerySentEvent {
            event_ts: get_time(),
            worker_id,
            query_id,
        }
    }

    pub fn query_bytes_received_event(worker_id: usize, query_id: usize) -> Event {
        Event::QueryBytesReceivedEvent {
            event_ts: get_time(),
            worker_id,
            query_id,
        }
    }

    pub fn get_ts(&self) -> Timespec {
        match self {
            Event::MetricSentEvent { event_ts } => *event_ts,
            Event::SketchSentEvent { event_ts } => *event_ts,
            Event::ErrorEvent { event_ts } => *event_ts,
            Event::QuerySentEvent {
                event_ts,
                worker_id: _,
                query_id: _,
            } => *event_ts,
            Event::QueryBytesReceivedEvent {
                event_ts,
                worker_id: _,
                query_id: _,
            } => *event_ts,
        }
    }
}
