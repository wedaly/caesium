use time::{get_time, Timespec};

pub enum Event {
    InsertEvent {
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
    pub fn insert_event() -> Event {
        Event::InsertEvent {
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
            Event::InsertEvent { event_ts } => *event_ts,
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
