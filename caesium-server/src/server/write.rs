use bytes::Bytes;
use mio::net::TcpListener;
use mio::{Events, Poll, PollOpt, Ready, Token};
use server::write::connection::{Connection, ConnectionState};
use server::write::worker::spawn_worker;
use slab::Slab;
use std::io;
use std::net::SocketAddr;
use std::sync::mpsc::{channel, Sender};
use std::sync::{Arc, Mutex};
use storage::store::MetricStore;

const MAX_NUM_EVENTS: usize = 1024;

pub struct WriteServer {
    listener: TcpListener,
    tx: Sender<Bytes>,
    connections: Slab<Option<Connection>>,
}

impl WriteServer {
    pub fn new(
        addr: &SocketAddr,
        num_workers: usize,
        db_ref: Arc<MetricStore>,
    ) -> Result<WriteServer, io::Error> {
        assert!(num_workers > 0);
        let listener = TcpListener::bind(addr)?;
        let (tx, rx) = channel();
        let rx_ref = Arc::new(Mutex::new(rx));
        for idx in 0..num_workers {
            spawn_worker(idx, rx_ref.clone(), db_ref.clone());
        }
        Ok(WriteServer {
            listener,
            tx,
            connections: Slab::new(),
        })
    }

    pub fn local_addr(&self) -> Result<SocketAddr, io::Error> {
        self.listener.local_addr()
    }

    pub fn run(mut self) -> Result<(), io::Error> {
        let poll = Poll::new()?;
        let listener_id = self.connections.insert(None);
        poll.register(
            &self.listener,
            Token(listener_id),
            Ready::readable(),
            PollOpt::edge(),
        )?;
        let mut events = Events::with_capacity(MAX_NUM_EVENTS);
        info!("Listening for inserts on {}", self.local_addr()?);
        loop {
            poll.poll(&mut events, None)?;
            for event in events.iter() {
                match event.token() {
                    Token(t) if t == listener_id => {
                        self.handle_new_connections(&poll);
                    }
                    Token(t) => {
                        self.handle_read_ready(t);
                    }
                }
            }
        }
    }

    fn handle_new_connections(&mut self, poll: &Poll) {
        loop {
            match self.listener.accept() {
                Ok((stream, _)) => {
                    let entry = self.connections.vacant_entry();
                    let conn_id = entry.key();
                    let tok = Token(conn_id);
                    match poll.register(&stream, tok, Ready::readable(), PollOpt::edge()) {
                        Ok(_) => {
                            let conn = Connection::new(stream);
                            entry.insert(Some(conn));
                        }
                        Err(err) => {
                            error!("Could not register new connection: {:?}", err);
                        }
                    }
                }
                Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                    break;
                }
                Err(err) => {
                    error!("Could not accept new connection: {:?}", err);
                }
            }
        }
    }

    fn handle_read_ready(&mut self, conn_id: usize) {
        let mut conn = self
            .connections
            .get_mut(conn_id)
            .expect("Could not retrieve connection")
            .take()
            .expect("Connection entry should not be None");
        match conn.read_until_blocked() {
            Ok(conn_state) => match conn.output_messages(&self.tx) {
                Ok(_) => {
                    if let ConnectionState::Open = conn_state {
                        let conn_entry = self
                            .connections
                            .get_mut(conn_id)
                            .expect("Could not retrieve connection");
                        *conn_entry = Some(conn);
                    }
                }
                Err(err) => {
                    error!("Error sending insert msg to workers: {:?}", err);
                }
            },
            Err(err) => {
                error!("Error handling read: {:?}", err);
            }
        }
    }
}

mod connection {
    use bytes::{Buf, Bytes, BytesMut, IntoBuf};
    use mio::net::TcpStream;
    use std::io;
    use std::io::Read;
    use std::mem::size_of;
    use std::sync::mpsc::SendError;
    use std::sync::mpsc::Sender;

    const INITIAL_BUFSIZE: usize = 4096;

    pub enum ConnectionState {
        Open,
        Closed,
    }

    pub struct Connection {
        stream: TcpStream,
        buf: BytesMut,
        next_frame_len: Option<u64>,
    }

    impl Connection {
        pub fn new(stream: TcpStream) -> Connection {
            Connection {
                stream,
                buf: BytesMut::with_capacity(INITIAL_BUFSIZE),
                next_frame_len: None,
            }
        }

        pub fn read_until_blocked(&mut self) -> Result<ConnectionState, io::Error> {
            let mut tmp = [0; 1024];
            loop {
                match self.stream.read(&mut tmp[..]) {
                    Ok(0) => {
                        return Ok(ConnectionState::Closed);
                    }
                    Ok(n) => {
                        self.buf.extend_from_slice(&tmp[..n]);
                    }
                    Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                        return Ok(ConnectionState::Open);
                    }
                    Err(err) => {
                        return Err(err);
                    }
                }
            }
        }

        pub fn output_messages(&mut self, tx: &Sender<Bytes>) -> Result<(), SendError<Bytes>> {
            loop {
                let frame_len = match self.next_frame_len.take() {
                    Some(len) => len,
                    None => {
                        if self.buf.len() > size_of::<u64>() {
                            let mut len_buf = self.buf.split_to(size_of::<u64>()).into_buf();
                            len_buf.get_u64_be()
                        } else {
                            return Ok(());
                        }
                    }
                };

                if self.buf.len() < frame_len as usize {
                    self.next_frame_len = Some(frame_len);
                    return Ok(());
                } else {
                    let frame_buf = self.buf.split_to(frame_len as usize).freeze();
                    tx.send(frame_buf)?;
                }
            }
        }
    }
}

mod worker {
    use bytes::Bytes;
    use caesium_core::encode::Decodable;
    use caesium_core::protocol::messages::InsertMessage;
    use std::sync::mpsc::Receiver;
    use std::sync::{Arc, Mutex};
    use std::thread;
    use storage::error::StorageError;
    use storage::store::MetricStore;

    pub fn spawn_worker(id: usize, rx_lock: Arc<Mutex<Receiver<Bytes>>>, db_ref: Arc<MetricStore>) {
        thread::spawn(move || process_messages(id, rx_lock, db_ref));
    }

    fn process_messages(id: usize, rx_lock: Arc<Mutex<Receiver<Bytes>>>, db_ref: Arc<MetricStore>) {
        let db = &*db_ref;
        loop {
            let recv_result = rx_lock
                .lock()
                .expect("Could not acquire lock on worker msg queue")
                .recv();
            match recv_result {
                Ok(buf) => {
                    debug!("Processing insert in worker thread with id {}", id);
                    if let Err(err) = handle_insert(buf, db) {
                        error!(
                            "Could not process insert task (worker id {}): {:?}",
                            id, err
                        );
                    }
                }
                Err(err) => {
                    error!("Error receiving worker msg: {:?}", err);
                }
            }
        }
    }

    fn handle_insert(buf: Bytes, db: &MetricStore) -> Result<(), StorageError> {
        let mut buf_slice: &[u8] = &buf;
        let msg = InsertMessage::decode(&mut buf_slice)?;
        db.insert(&msg.metric, msg.window, msg.sketch)
    }
}
