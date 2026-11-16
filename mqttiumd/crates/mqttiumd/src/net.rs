//! Bounded socket tasks for the product adapter. No durable operation runs here.

use std::io;
use std::net::TcpStream;
use std::sync::mpsc::{Receiver, SyncSender};

use mqttiumd_contract::ids::{ConnectionKey, EffectId};

pub const INPUT_SLOTS: usize = 4;
pub const OUTPUT_SLOTS: usize = 4;

#[derive(Debug)]
pub enum Incoming {
    Packet { first: u8, body: Vec<u8> },
    Closed { code: &'static str },
}

#[derive(Debug)]
pub struct Write {
    pub correlation: Option<(EffectId, ConnectionKey)>,
    pub bytes: Vec<u8>,
}

#[derive(Debug)]
pub enum Written {
    Complete {
        correlation: Option<(EffectId, ConnectionKey)>,
        bytes: usize,
    },
    Failed {
        correlation: Option<(EffectId, ConnectionKey)>,
        code: &'static str,
    },
}

pub struct Connection {
    pub incoming: Receiver<Incoming>,
    pub outgoing: SyncSender<Write>,
    pub written: Receiver<Written>,
    _stream: TcpStream,
}

pub fn spawn(stream: TcpStream) -> io::Result<Connection> {
    let (_incoming, incoming) = std::sync::mpsc::sync_channel(INPUT_SLOTS);
    let (outgoing, _outgoing) = std::sync::mpsc::sync_channel(OUTPUT_SLOTS);
    let (_written, written) = std::sync::mpsc::sync_channel(8);
    Ok(Connection { incoming, outgoing, written, _stream: stream })
}

impl Connection {
    pub fn close(&self) {}
    pub fn is_finished(&self) -> bool { true }
    pub fn join_finished(&mut self) -> bool { true }
    pub fn terminal_reason(&self) -> Option<&'static str> { None }
    pub fn send_buffer_bytes(&self) -> usize { 0 }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read, Write as _};
    use std::net::{Shutdown, TcpListener};
    use std::thread;
    use std::time::{Duration, Instant};

    fn pair() -> (Connection, TcpStream) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let peer = TcpStream::connect(listener.local_addr().unwrap()).unwrap();
        let (server, _) = listener.accept().unwrap();
        (spawn(server).unwrap(), peer)
    }

    fn incoming(connection: &Connection) -> Incoming {
        connection.incoming.recv_timeout(Duration::from_millis(600)).unwrap()
    }

    fn closed(connection: &Connection, expected: &str) {
        match incoming(connection) {
            Incoming::Closed { code } => assert_eq!(code, expected),
            packet => panic!("expected closure, got {packet:?}"),
        }
    }

    fn finish(connection: &mut Connection) {
        let deadline = Instant::now() + Duration::from_millis(600);
        while !connection.is_finished() && Instant::now() < deadline {
            thread::sleep(Duration::from_millis(2));
        }
        assert!(connection.join_finished(), "socket tasks outlived cancellation budget");
    }

    #[test]
    fn fragmented_and_concatenated_packets_keep_their_boundaries() {
        let (mut connection, mut peer) = pair();
        peer.write_all(&[0xc0]).unwrap();
        assert!(connection.incoming.try_recv().is_err());
        peer.write_all(&[0, 0xc0, 0, 0xc0, 0]).unwrap();
        for _ in 0..3 {
            assert!(matches!(incoming(&connection), Incoming::Packet { first: 0xc0, body } if body.is_empty()));
        }
        connection.close();
        finish(&mut connection);
    }

    #[test]
    fn oversized_header_is_refused_without_waiting_for_its_body() {
        let (mut connection, mut peer) = pair();
        peer.write_all(&[0x34, 0xff, 0xff, 0x7f]).unwrap();
        closed(&connection, "packet_too_large");
        finish(&mut connection);
    }

    #[test]
    fn eof_in_body_is_distinct_from_clean_eof() {
        let (mut connection, mut peer) = pair();
        peer.write_all(&[0x34, 3, 0]).unwrap();
        peer.shutdown(Shutdown::Write).unwrap();
        closed(&connection, "truncated_packet");
        finish(&mut connection);
        let (mut connection, peer) = pair();
        peer.shutdown(Shutdown::Write).unwrap();
        closed(&connection, "eof");
        finish(&mut connection);
    }

    #[test]
    fn output_is_confirmed_only_after_all_bytes_are_written() {
        let (mut connection, mut peer) = pair();
        connection.outgoing.send(Write { correlation: None, bytes: vec![0xd0, 0] }).unwrap();
        let result = connection.written.recv_timeout(Duration::from_millis(600)).unwrap();
        assert!(matches!(result, Written::Complete { correlation: None, bytes: 2 }));
        let mut bytes = [0; 2];
        peer.set_read_timeout(Some(Duration::from_millis(600))).unwrap();
        peer.read_exact(&mut bytes).unwrap();
        assert_eq!(bytes, [0xd0, 0]);
        connection.close();
        finish(&mut connection);
    }

    #[test]
    fn nonreading_peer_expires_one_absolute_write_budget() {
        let (mut connection, peer) = pair();
        socket2::SockRef::from(&peer).set_recv_buffer_size(4096).unwrap();
        let started = Instant::now();
        for _ in 0..OUTPUT_SLOTS {
            connection.outgoing.send(Write { correlation: None, bytes: vec![0; 65_536] }).unwrap();
        }
        let mut failed = false;
        for _ in 0..OUTPUT_SLOTS {
            match connection.written.recv_timeout(Duration::from_secs(1)).unwrap() {
                Written::Failed { code, .. } => {
                    assert_eq!(code, "write_timeout");
                    failed = true;
                    break;
                }
                Written::Complete { .. } => {}
            }
        }
        assert!(failed, "test did not reach socket backpressure");
        assert!(started.elapsed() < Duration::from_secs(1));
        finish(&mut connection);
    }

    #[test]
    fn cancellation_ends_idle_and_partial_socket_tasks() {
        for partial in [false, true] {
            let (mut connection, mut peer) = pair();
            if partial { peer.write_all(&[0x34, 127, 0]).unwrap(); }
            connection.close();
            finish(&mut connection);
        }
    }
}
