use dashmap::DashMap;
use nohash_hasher::BuildNoHashHasher;
use parking_lot::Mutex;
use std::{
    net::SocketAddr,
    sync::atomic::{AtomicUsize, Ordering},
    time::{Duration, Instant, SystemTime},
};

use crate::message::BufferKind;

pub struct Packet {
    pub data: Box<[u8]>,
    pub timestamp: Duration,
    pub up: bool,
}

struct Connection {
    creation: Instant,
    creation_sys: SystemTime,
    address: SocketAddr,
    packets: Vec<Packet>,
}

pub struct FinishedConnection {
    pub id: u64,
    pub creation: SystemTime,
    pub whole_time: Duration,
    pub address: SocketAddr,
    pub packets: Vec<Packet>,
}

#[derive(Default)]
pub struct StatTracker {
    bytes_tx: AtomicUsize,
    bytes_rx: AtomicUsize,
    pkt_tx: AtomicUsize,
    pkt_rx: AtomicUsize,
    total_conns: AtomicUsize,
    total_suspends: AtomicUsize,
    total_resumes: AtomicUsize,
    total_keepalives: AtomicUsize,
    active_conns: DashMap<u64, Connection, BuildNoHashHasher<u64>>,
    past_conns: Mutex<Vec<FinishedConnection>>,
}

impl StatTracker {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn new_connection(&self, connection_id: u64, address: SocketAddr) {
        self.total_conns.fetch_add(1, Ordering::Relaxed);
        self.active_conns.entry(connection_id).or_insert_with(|| Connection {
            creation: Instant::now(),
            creation_sys: SystemTime::now(),
            packets: Vec::new(),
            address,
        });
    }

    pub fn end_connection(&self, connection_id: u64) {
        let Some((_, c)) = self.active_conns.remove(&connection_id) else {
            return;
        };

        let finished = FinishedConnection {
            id: connection_id,
            creation: c.creation_sys,
            address: c.address,
            whole_time: c.creation.elapsed(),
            packets: c.packets,
        };

        self.past_conns.lock().push(finished);
    }

    pub fn suspend_connection(&self, _connection_id: u64) {
        self.total_suspends.fetch_add(1, Ordering::Relaxed);
    }

    pub fn resume_connection(&self, _connection_id: u64) {
        self.total_resumes.fetch_add(1, Ordering::Relaxed);
    }

    pub fn keepalive(&self, _connection_id: u64) {
        self.total_keepalives.fetch_add(1, Ordering::Relaxed);
    }

    pub fn data_downstream(&self, connection_id: u64, buf: &BufferKind) {
        self.bytes_tx.fetch_add(buf.len(), Ordering::Relaxed);
        self.pkt_tx.fetch_add(1, Ordering::Relaxed);

        self.record_pkt(connection_id, buf, false);
    }

    pub fn data_upstream(&self, connection_id: u64, buf: &BufferKind) {
        self.bytes_rx.fetch_add(buf.len(), Ordering::Relaxed);
        self.pkt_rx.fetch_add(1, Ordering::Relaxed);

        self.record_pkt(connection_id, buf, true);
    }

    #[inline]
    fn record_pkt(&self, connection_id: u64, buf: &BufferKind, up: bool) {
        let Some(mut c) = self.active_conns.get_mut(&connection_id) else {
            return;
        };

        let timestamp = c.creation.elapsed();

        c.packets.push(Packet {
            data: Box::from(&buf[..]),
            timestamp,
            up,
        });
    }

    pub fn clear_past(&self) {
        self.past_conns.lock().clear();
    }

    pub fn clear_past_older_than(&self, dur: Duration) {
        self.past_conns
            .lock()
            .retain(|c| (c.creation + c.whole_time).elapsed().unwrap_or_default() <= dur);
    }

    pub fn clear_active(&self) {
        self.active_conns.clear();
    }

    pub fn clear_all(&self) {
        self.clear_past();
        self.clear_active();
    }

    pub fn take_all_past(&self) -> Vec<FinishedConnection> {
        std::mem::take(&mut *self.past_conns.lock())
    }
}
