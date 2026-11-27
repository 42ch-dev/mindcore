//! The Unix-socket transport: one Connect envelope per length-prefixed frame.
//!
//! `spoke-connect` owns the [`Transport`] trait and ships only an in-memory loopback; a
//! byte-stream carrier owns its own delimiting, so the framing lives here. One frame is a 4-byte
//! big-endian length followed by exactly that many bytes of body, and one frame is one envelope.
//!
//! Both ends of a session use the same framing: [`UnixSocketTransport::connect`] dials and
//! [`UnixSocketTransport::from_stream`] wraps an accepted stream. `send` and `recv` take `&self`,
//! so the stream is split — the write half behind a mutex (prefix and body written under one
//! lock, so two concurrent sends never interleave) and the read half behind its own.
//!
//! Framing faults travel in [`TransportError::Io`], because `spoke-connect` has exactly `Closed`
//! and `Io` and the daemon adds no variant. A peer closing at a frame boundary answers `Closed`;
//! one closing inside a prefix or a body answers `Io` (`truncated frame`). A length of `0` or
//! above [`MAX_FRAME`] is a protocol fault: the body is not read, the transport closes itself, and
//! the fault never reaches the responder as a message.

use std::path::Path;

use async_trait::async_trait;
use spoke_connect::remote::{Transport, TransportError};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::unix::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::UnixStream;
use tokio::sync::{watch, Mutex};

/// The largest body one frame may carry: 16 MiB (16,777,216 bytes).
///
/// A prefix of `0` or above this is a protocol fault — the body is not read and the transport
/// closes itself — so `send` refuses such an envelope rather than writing it.
pub const MAX_FRAME: u32 = 16 * 1024 * 1024;

/// One end of a Unix-socket session, framed as length-prefixed envelopes.
#[derive(Debug)]
pub struct UnixSocketTransport {
    /// The read half. Its own mutex, so a pending `recv` and a `send` do not contend.
    reader: Mutex<OwnedReadHalf>,
    /// The write half. Held across the prefix and the body so frames stay whole.
    writer: Mutex<OwnedWriteHalf>,
    /// `true` once the transport is closed. The sender keeps the value, so a late
    /// [`UnixSocketTransport::closed`] still resolves.
    closed: watch::Sender<bool>,
}

impl UnixSocketTransport {
    /// Dials `path` — the client end of a session.
    pub async fn connect(path: &Path) -> Result<Self, TransportError> {
        let stream = UnixStream::connect(path)
            .await
            .map_err(|error| TransportError::Io(format!("connect {}: {error}", path.display())))?;
        Ok(Self::from_stream(stream))
    }

    /// Wraps an accepted stream — the server end of a session.
    #[must_use]
    pub fn from_stream(stream: UnixStream) -> Self {
        let (reader, writer) = stream.into_split();
        Self {
            reader: Mutex::new(reader),
            writer: Mutex::new(writer),
            closed: watch::Sender::new(false),
        }
    }

    /// Resolves once this transport is closed.
    ///
    /// The connection task awaits this and then drops its responder: the peer hung up, the
    /// responder ended the session, or the daemon is draining.
    pub async fn closed(&self) {
        let mut closed = self.closed.subscribe();
        // The sender lives in `self`, so this only errors if the transport itself is gone.
        let _ = closed.wait_for(|closed| *closed).await;
    }

    fn is_closed(&self) -> bool {
        *self.closed.borrow()
    }

    /// Marks the transport closed and wakes every waiter. Answers whether this call closed it.
    fn mark_closed(&self) -> bool {
        !self.closed.send_replace(true)
    }

    /// Reads one frame: the 4-byte prefix, then exactly that many body bytes.
    async fn read_frame(&self, reader: &mut OwnedReadHalf) -> Result<Vec<u8>, TransportError> {
        // Read by hand rather than `read_exact`: an EOF after some prefix bytes is a truncated
        // frame, while one before any is the peer closing at a frame boundary.
        let mut prefix = [0u8; 4];
        let mut filled = 0;
        while filled < prefix.len() {
            let read = reader
                .read(&mut prefix[filled..])
                .await
                .map_err(|error| TransportError::Io(format!("read frame prefix: {error}")))?;
            if read == 0 {
                return if filled == 0 {
                    Err(TransportError::Closed)
                } else {
                    Err(TransportError::Io(format!(
                        "truncated frame: {filled} of 4 prefix bytes"
                    )))
                };
            }
            filled += read;
        }

        let len = u32::from_be_bytes(prefix);
        if len == 0 || len > MAX_FRAME {
            // The session is unusable: mark it closed and leave the body in the socket. The write
            // half is not shut down here — a concurrent `send` may hold it blocked on a peer that
            // stopped reading — the connection task's drop after `closed()` tears the socket down.
            self.mark_closed();
            return Err(TransportError::Io(format!(
                "frame length {len} outside 1..={MAX_FRAME}; body not read"
            )));
        }

        let mut body = vec![0u8; len as usize];
        reader.read_exact(&mut body).await.map_err(|error| {
            if error.kind() == std::io::ErrorKind::UnexpectedEof {
                TransportError::Io(format!("truncated frame: body of {len} bytes cut short"))
            } else {
                TransportError::Io(format!("read frame body: {error}"))
            }
        })?;
        Ok(body)
    }
}

#[async_trait]
impl Transport for UnixSocketTransport {
    async fn send(&self, envelope: &[u8]) -> Result<(), TransportError> {
        if self.is_closed() {
            return Err(TransportError::Closed);
        }
        let len = u32::try_from(envelope.len()).map_err(|_| {
            TransportError::Io(format!(
                "frame length {} does not fit the 4-byte prefix",
                envelope.len()
            ))
        })?;
        if len == 0 || len > MAX_FRAME {
            return Err(TransportError::Io(format!(
                "frame length {len} outside 1..={MAX_FRAME}; nothing sent"
            )));
        }

        // Prefix and body under one lock: a concurrent send waits for both.
        let mut writer = self.writer.lock().await;
        // `close` may have returned while this send waited for the lock; it must not write now.
        if self.is_closed() {
            return Err(TransportError::Closed);
        }
        writer
            .write_all(&len.to_be_bytes())
            .await
            .map_err(|error| TransportError::Io(format!("write frame prefix: {error}")))?;
        writer
            .write_all(envelope)
            .await
            .map_err(|error| TransportError::Io(format!("write frame body: {error}")))?;
        Ok(())
    }

    async fn recv(&self) -> Result<Vec<u8>, TransportError> {
        if self.is_closed() {
            return Err(TransportError::Closed);
        }
        let mut closed = self.closed.subscribe();
        let mut reader = self.reader.lock().await;
        // A pending recv fails fast when the daemon closes the transport. Dropping the read
        // future can only lose bytes no later recv can be asked for: close is terminal.
        tokio::select! {
            frame = self.read_frame(&mut reader) => frame,
            _ = closed.wait_for(|closed| *closed) => Err(TransportError::Closed),
        }
    }

    async fn close(&self) -> Result<(), TransportError> {
        if !self.mark_closed() {
            return Ok(());
        }
        // Wake a pending `recv` first (done above), then give the peer a clean frame boundary. A
        // send already holding the write half is left alone: waiting for a peer that stopped
        // reading would block drain, and dropping the halves shuts the write side anyway.
        if let Ok(mut writer) = self.writer.try_lock() {
            let _ = writer.shutdown().await;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use tokio::net::UnixListener;

    /// A listener bound to a path inside a tempdir that outlives it.
    fn bound() -> (tempfile::TempDir, std::path::PathBuf, UnixListener) {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("mindcore.sock");
        let listener = UnixListener::bind(&path).expect("bind");
        (dir, path, listener)
    }

    /// A dialed session and its accepted end.
    async fn session() -> (UnixSocketTransport, UnixSocketTransport) {
        let (_dir, path, listener) = bound();
        let accept = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("accept");
            UnixSocketTransport::from_stream(stream)
        });
        let dialer = UnixSocketTransport::connect(&path).await.expect("connect");
        let accepted = accept.await.expect("accept task");
        // The tempdir goes with `bound`; the connected pair does not need the name.
        (dialer, accepted)
    }

    #[tokio::test]
    async fn one_byte_one_kib_and_one_mib_envelopes_round_trip() {
        let (dialer, accepted) = session().await;

        for size in [1usize, 1024, 1024 * 1024] {
            let envelope: Vec<u8> = (0..size).map(|offset| (offset % 251) as u8).collect();
            // Send and receive together: a body larger than the socket buffer only completes
            // when the peer drains it.
            let (sent, received) = tokio::join!(dialer.send(&envelope), accepted.recv());
            sent.expect("send");
            assert_eq!(
                received.expect("recv"),
                envelope,
                "a {size}-byte envelope arrives whole"
            );
        }
    }

    #[tokio::test]
    async fn two_concurrent_sends_arrive_as_two_whole_frames() {
        let (dialer, accepted) = session().await;
        let big = vec![7u8; 64 * 1024];
        let small = vec![9u8; 3];

        // All three at once: a body larger than the socket buffer only drains because the
        // receives run alongside the sends.
        let (first, second, (one, two)) =
            tokio::join!(dialer.send(&big), dialer.send(&small), async {
                (accepted.recv().await, accepted.recv().await)
            });
        first.expect("first send");
        second.expect("second send");

        // The order is whichever send took the write lock first; the frames are whole either way.
        let mut frames = vec![one.expect("first frame"), two.expect("second frame")];
        frames.sort_by_key(Vec::len);
        assert_eq!(frames, vec![small, big], "each send is one intact frame");
    }

    #[tokio::test]
    async fn a_prefix_cut_short_is_an_io_error_not_a_panic() {
        let (mut peer, accepted) = UnixStream::pair().expect("pair");
        let transport = UnixSocketTransport::from_stream(accepted);

        peer.write_all(&[0x00, 0x00]).await.expect("half a prefix");
        peer.shutdown().await.expect("hang up");

        let error = transport.recv().await.expect_err("truncated prefix");
        assert!(
            matches!(&error, TransportError::Io(message) if message.contains("truncated")),
            "half a prefix then EOF is Io: {error:?}"
        );
    }

    #[tokio::test]
    async fn a_zero_or_oversize_prefix_is_io_and_leaves_the_body_unread() {
        for prefix in [0u32, MAX_FRAME + 1] {
            let (mut peer, accepted) = UnixStream::pair().expect("pair");
            let transport = UnixSocketTransport::from_stream(accepted);

            peer.write_all(&prefix.to_be_bytes()).await.expect("prefix");
            peer.write_all(b"body that must stay unread")
                .await
                .expect("body");

            let error = transport.recv().await.expect_err("protocol fault");
            let named = prefix.to_string();
            assert!(
                matches!(&error, TransportError::Io(message) if message.contains(&named)),
                "the fault names the length {prefix}: {error:?}"
            );
            assert!(
                matches!(transport.recv().await, Err(TransportError::Closed)),
                "a framing fault closes the transport, so the body is never delivered"
            );
        }
    }

    #[tokio::test]
    async fn close_is_idempotent_and_later_send_and_recv_fail() {
        let (dialer, accepted) = session().await;

        dialer.close().await.expect("close");
        dialer.close().await.expect("second close");

        assert!(
            matches!(dialer.send(b"late").await, Err(TransportError::Closed)),
            "a closed transport fails send"
        );
        assert!(
            matches!(dialer.recv().await, Err(TransportError::Closed)),
            "a closed transport fails recv"
        );
        assert!(
            matches!(accepted.recv().await, Err(TransportError::Closed)),
            "the peer sees the close as a frame boundary"
        );
    }

    #[tokio::test]
    async fn the_closed_signal_fires_on_close() {
        let (dialer, _accepted) = session().await;
        let dialer = std::sync::Arc::new(dialer);

        let waiting = tokio::spawn({
            let dialer = std::sync::Arc::clone(&dialer);
            async move { dialer.closed().await }
        });
        dialer.close().await.expect("close");

        tokio::time::timeout(std::time::Duration::from_secs(5), waiting)
            .await
            .expect("the connection task's wait resolves on close")
            .expect("closed signal task");
    }
}
