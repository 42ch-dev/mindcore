//! The Unix-socket transport: one Connect envelope per length-prefixed frame.
//!
//! `spoke-connect` owns the [`Transport`] trait and ships only an in-memory loopback; a
//! byte-stream carrier owns its own delimiting, so the framing lives here. One frame is a 4-byte
//! big-endian length followed by exactly that many bytes of body, and one frame is one envelope.
//!
//! Both ends of a session use the same framing: [`UnixSocketTransport::connect`] dials and
//! [`UnixSocketTransport::from_stream`] wraps an accepted stream. `send` and `recv` take `&self`,
//! so the stream is split: the write half behind a mutex (prefix and body written under one lock,
//! so two concurrent sends never interleave) and the read half behind its own.
//!
//! Teardown never waits for either half. `spoke-connect`'s trait hands out `&self`, and on this
//! tokio `&UnixStream` is not `AsyncWrite` — only `UnixStream` and the owned halves are — so a
//! shutdown through a borrow is unavailable and any shutdown routed through a half's mutex would
//! block behind a writer parked on a peer that stopped reading. The socket's own descriptor is
//! captured before the split and `shutdown(2)` is issued on it directly: the call is immediate,
//! needs neither half's mutex, and interrupts both a blocked `write_all` (which then fails) and a
//! blocked read. Teardown is serialized by a lock of its own, never held by `send` or `recv`, so a
//! returning `close` means the shutdown has finished rather than merely started.
//!
//! Framing faults travel in [`TransportError::Io`], because `spoke-connect` has exactly `Closed`
//! and `Io` and the daemon adds no variant. A peer closing at a frame boundary answers `Closed`;
//! one closing inside a prefix or a body answers `Io` (`truncated frame`). Every framing fault is
//! terminal: the transport marks itself closed, wakes [`UnixSocketTransport::closed`] and shuts the
//! socket down, so the bytes after the fault are never read and the fault never reaches the
//! responder as a message. A length of `0` or above [`MAX_FRAME`] is such a fault; `send` refuses
//! one rather than writing a frame the peer must reject.

use std::os::fd::{AsRawFd, RawFd};
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use spoke_connect::remote::{Transport, TransportError};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::unix::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::UnixStream;
use tokio::sync::{watch, Mutex};

/// A transport that withholds the peer's first frame until the host says the session is ready.
///
/// `spoke-connect`'s responder starts its own task before `connect_responder` returns and installs
/// no handlers from the manifest, so a dialer that completes its hello at once can invoke an
/// advertised tool before the host has registered one and be answered `op_unsupported`. Holding the
/// first `recv` until registration is done closes that window from this side: the responder cannot
/// read the hello, so it cannot establish a session, so nothing can be invoked.
///
/// `send` and `close` are never gated. The responder's hello reply is one `send`, and a start that
/// fails must still be able to close the session it opened; only inbound traffic waits.
pub struct GatedTransport {
    inner: Arc<dyn Transport>,
    /// Which of the three states the gate is in. A `watch` rather than a one-shot, so a `recv` that
    /// starts after the gate resolved still sees the answer.
    gate: watch::Sender<GateState>,
}

/// The gate's three answers.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum GateState {
    /// The host has not said whether the session may start: a `recv` waits.
    Closed,
    /// Registration succeeded: inbound frames may be read.
    Open,
    /// Registration failed and the session is being closed: a `recv` reports `Closed` at once.
    ///
    /// This is a state, not an `open()`, because opening would release the read task into a
    /// transport that never carries a frame: the host that refused the registration will not serve
    /// the session, so the only honest answer is that it is over.
    Rejected,
}

impl GatedTransport {
    /// Wraps `inner`, with the gate closed.
    #[must_use]
    pub fn new(inner: Arc<dyn Transport>) -> Self {
        let (gate, _) = watch::channel(GateState::Closed);
        Self { inner, gate }
    }

    /// Opens the gate: the next `recv` reads its frame, and every later one does so at once.
    pub fn open(&self) {
        self.gate.send_replace(GateState::Open);
    }

    /// Tells a waiting `recv` that the session is over, so its task can end rather than wait for a
    /// frame this host will never serve.
    pub fn reject(&self) {
        self.gate.send_replace(GateState::Rejected);
    }
}

#[async_trait]
impl Transport for GatedTransport {
    async fn send(&self, envelope: &[u8]) -> Result<(), TransportError> {
        self.inner.send(envelope).await
    }

    async fn recv(&self) -> Result<Vec<u8>, TransportError> {
        let mut gate = self.gate.subscribe();
        let mut state = *gate.borrow_and_update();
        if state == GateState::Closed {
            // Waits only while the host has not decided; the sender lives in `self`, so a closed
            // channel means this transport is gone and the inner one answers for itself.
            if let Ok(resolved) = gate.wait_for(|state| *state != GateState::Closed).await {
                state = *resolved;
            }
        }
        if state == GateState::Rejected {
            return Err(TransportError::Closed);
        }
        self.inner.recv().await
    }

    async fn close(&self) -> Result<(), TransportError> {
        self.inner.close().await
    }
}

/// The largest body one frame may carry: 16 MiB (16,777,216 bytes).
///
/// A prefix of `0` or above this is a protocol fault — the body is not read and the transport
/// closes itself — so `send` refuses such an envelope rather than writing it.
pub const MAX_FRAME: u32 = 16 * 1024 * 1024;

/// One end of a Unix-socket session, framed as length-prefixed envelopes.
#[derive(Debug)]
pub struct UnixSocketTransport {
    /// The socket's descriptor, captured before the split. Both halves share it, and this struct
    /// owns both, so it stays open for the transport's whole life. Teardown uses it precisely
    /// because it is reachable without either half's mutex.
    fd: RawFd,
    /// The read half. Its own mutex, so a pending `recv` and a `send` do not contend.
    reader: Mutex<OwnedReadHalf>,
    /// The write half. Held across the prefix and the body so frames stay whole.
    writer: Mutex<OwnedWriteHalf>,
    /// `true` once the transport is closed. The sender keeps the value, so a late
    /// [`UnixSocketTransport::closed`] still resolves.
    closed: watch::Sender<bool>,
    /// Serializes teardown, so `close` cannot resolve while another caller's `shutdown(2)` is still
    /// in flight. Never held by `send` or `recv`, so a blocked writer cannot delay it.
    shutdown_lock: Mutex<()>,
    /// Whether `shutdown(2)` has run. Separate from `closed`, because a framing fault closes the
    /// transport *and* tears the socket down, and a later `close` must still find the teardown done.
    /// Written only under `shutdown_lock`, so `true` here always means the shutdown completed.
    shutdown_done: AtomicBool,
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
        let fd = stream.as_raw_fd();
        let (reader, writer) = stream.into_split();
        Self {
            fd,
            reader: Mutex::new(reader),
            writer: Mutex::new(writer),
            closed: watch::Sender::new(false),
            shutdown_lock: Mutex::new(()),
            shutdown_done: AtomicBool::new(false),
        }
    }

    /// Resolves once this transport is closed.
    ///
    /// The connection task awaits this and then drops its responder: the peer hung up, the
    /// responder ended the session, a framing fault, or the daemon is draining.
    pub async fn closed(&self) {
        let mut closed = self.closed.subscribe();
        // The sender lives in `self`, so this only errors if the transport itself is gone.
        let _ = closed.wait_for(|closed| *closed).await;
    }

    fn is_closed(&self) -> bool {
        *self.closed.borrow()
    }

    /// Marks the transport closed and wakes every waiter.
    fn mark_closed(&self) {
        self.closed.send_replace(true);
    }

    /// Shuts both directions of the socket down, at most once, without taking either half's lock.
    ///
    /// A blocked `write_all` on the write half fails once this runs, and a blocked read on the read
    /// half ends, so no task stays parked on a peer that stopped reading. A peer that already went
    /// away answers `NotConnected`, which is the state this call is asking for anyway.
    ///
    /// Serialized: a caller that returns from here — `close` most of all — knows the shutdown has
    /// completed, never merely that someone else started it.
    async fn shutdown_socket(&self) {
        let _teardown = self.shutdown_lock.lock().await;
        if self.shutdown_done.load(Ordering::SeqCst) {
            return;
        }
        // SAFETY: `fd` is the descriptor of the socket this struct's two halves were split from,
        // and this struct owns both, so it is open here. `shutdown` only signals the socket; it
        // neither closes nor reallocates the descriptor, so the halves and tokio's registration
        // stay valid afterwards.
        //
        // Two steps, not `SHUT_RDWR`: once the peer has half-closed, `SHUT_RDWR` answers `ENOTCONN`
        // and delivers no EOF, while `SHUT_WR` still does. The write step is the one that must
        // never be skipped — it is what tells the peer the stream ended and what makes a parked
        // `write_all` fail — so it goes first and is unaffected by a `SHUT_RD` refusal after it.
        unsafe {
            let _ = libc::shutdown(self.fd, libc::SHUT_WR);
            let _ = libc::shutdown(self.fd, libc::SHUT_RD);
        }
        self.shutdown_done.store(true, Ordering::SeqCst);
    }

    /// The terminal transition: mark closed, then release the socket.
    async fn terminate(&self) {
        self.mark_closed();
        self.shutdown_socket().await;
    }

    /// Reads one frame: the 4-byte prefix, then exactly that many body bytes.
    ///
    /// Every fault here is terminal — the transport is closed and the socket shut down before the
    /// error returns, so a truncated frame cannot leave a half-usable session behind.
    async fn read_frame(&self, reader: &mut OwnedReadHalf) -> Result<Vec<u8>, TransportError> {
        // Read by hand rather than `read_exact`: an EOF after some prefix bytes is a truncated
        // frame, while one before any is the peer closing at a frame boundary.
        let mut prefix = [0u8; 4];
        let mut filled = 0;
        while filled < prefix.len() {
            let read = match reader.read(&mut prefix[filled..]).await {
                Ok(read) => read,
                Err(error) => {
                    self.terminate().await;
                    return Err(TransportError::Io(format!("read frame prefix: {error}")));
                }
            };
            if read == 0 {
                return if filled == 0 {
                    // The peer closed on a frame boundary: an orderly end, not a framing fault, but
                    // still the end. Without `terminate` the closed signal never fires and the
                    // connection task waits on `closed()` forever, holding its fd and its responder.
                    self.terminate().await;
                    Err(TransportError::Closed)
                } else {
                    self.terminate().await;
                    Err(TransportError::Io(format!(
                        "truncated frame: {filled} of 4 prefix bytes"
                    )))
                };
            }
            filled += read;
        }

        let len = u32::from_be_bytes(prefix);
        if len == 0 || len > MAX_FRAME {
            self.terminate().await;
            return Err(TransportError::Io(format!(
                "frame length {len} outside 1..={MAX_FRAME}; body not read"
            )));
        }

        let mut body = vec![0u8; len as usize];
        match reader.read_exact(&mut body).await {
            Ok(_) => Ok(body),
            Err(error) => {
                self.terminate().await;
                Err(if error.kind() == std::io::ErrorKind::UnexpectedEof {
                    TransportError::Io(format!("truncated frame: body of {len} bytes cut short"))
                } else {
                    TransportError::Io(format!("read frame body: {error}"))
                })
            }
        }
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

        let mut writer = self.writer.lock().await;
        // `close` may have returned while this send waited for the lock; it must not write now.
        if self.is_closed() {
            return Err(TransportError::Closed);
        }
        // A shutdown tears the socket down under an in-flight write, so the write fails. When our
        // own close is what interrupted it, the cause is the close and `Closed` is the honest
        // answer; only a failure with the transport still open is an I/O fault.
        let written = async {
            writer.write_all(&len.to_be_bytes()).await?;
            writer.write_all(envelope).await
        }
        .await;
        if let Err(error) = written {
            // Part of a frame may be on the stream: the framing is now broken for this peer, so
            // the transport ends exactly as a failed read does rather than staying half usable.
            if self.is_closed() {
                return Err(TransportError::Closed);
            }
            self.terminate().await;
            return Err(TransportError::Io(format!("write frame: {error}")));
        }
        // A close during the write shuts the socket down; the write may have completed on a peer
        // that is now gone, and it must not be reported as a delivered frame.
        if self.is_closed() {
            return Err(TransportError::Closed);
        }
        Ok(())
    }

    async fn recv(&self) -> Result<Vec<u8>, TransportError> {
        if self.is_closed() {
            return Err(TransportError::Closed);
        }
        let mut closed = self.closed.subscribe();
        let mut reader = self.reader.lock().await;
        // A pending recv fails fast when the transport closes. Dropping the read future can only
        // lose bytes no later recv can be asked for: close is terminal.
        tokio::select! {
            frame = self.read_frame(&mut reader) => frame,
            _ = closed.wait_for(|closed| *closed) => Err(TransportError::Closed),
        }
    }

    async fn close(&self) -> Result<(), TransportError> {
        // Idempotent, and it waits: a repeat takes the teardown lock, finds the shutdown done by the
        // first call (or by a framing fault) and returns only then. So a `close` that resolves means
        // the socket is shut down, never that some other teardown is still under way.
        self.terminate().await;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::collections::HashMap;
    use std::time::Duration;

    use spoke_connect::remote::loopback_transport_pair;

    use tokio::io::AsyncReadExt;
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
        (dialer, accepted)
    }

    /// A peer that closes on a frame boundary is an orderly end, but it is still an end: `closed()`
    /// must fire, or the connection task waits forever on a session nobody will use again.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_peer_closing_at_a_frame_boundary_fires_the_closed_signal() {
        let (dialer, accepted) = session().await;

        dialer.send(b"last words").await.expect("send a frame");
        assert_eq!(
            accepted.recv().await.expect("the frame arrives"),
            b"last words".to_vec()
        );

        dialer.close().await.expect("close the dialer");

        match accepted.recv().await {
            Err(TransportError::Closed) => {}
            other => panic!("an orderly EOF answers Closed, not {other:?}"),
        }
        assert_closed(&accepted).await;
    }

    /// The transport reports itself closed — the signal the connection task awaits.
    async fn assert_closed(transport: &UnixSocketTransport) {
        tokio::time::timeout(Duration::from_secs(5), transport.closed())
            .await
            .expect("the closed signal fires");
    }

    /// The peer's next read is EOF, i.e. this side really released the socket rather than merely
    /// dropping a reference to it.
    async fn assert_peer_sees_eof(peer: &mut UnixStream) {
        let mut buf = [0u8; 4096];
        let read = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                match peer.read(&mut buf).await {
                    Ok(0) => return Ok(()),
                    Ok(_) => continue,
                    Err(error) => return Err(error),
                }
            }
        })
        .await
        .expect("the peer is not left waiting");
        read.expect("the peer reads EOF, not an error");
    }

    #[tokio::test]
    async fn one_byte_one_kib_and_one_mib_envelopes_round_trip() {
        let (dialer, accepted) = session().await;

        for size in [1usize, 1024, 1024 * 1024] {
            let envelope: Vec<u8> = (0..size).map(|offset| (offset % 251) as u8).collect();
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

        let (first, second, (one, two)) =
            tokio::join!(dialer.send(&big), dialer.send(&small), async {
                (accepted.recv().await, accepted.recv().await)
            });
        first.expect("first send");
        second.expect("second send");

        let mut frames = vec![one.expect("first frame"), two.expect("second frame")];
        frames.sort_by_key(Vec::len);
        assert_eq!(frames, vec![small, big], "each send is one intact frame");
    }

    #[tokio::test]
    async fn an_invalid_prefix_releases_the_socket_while_the_transport_is_still_held() {
        let (mut peer, accepted) = UnixStream::pair().expect("pair");
        let transport = UnixSocketTransport::from_stream(accepted);

        peer.write_all(&0u32.to_be_bytes()).await.expect("prefix");
        peer.write_all(b"body that must stay unread")
            .await
            .expect("body");

        let error = transport.recv().await.expect_err("protocol fault");
        assert!(
            matches!(&error, TransportError::Io(message) if message.contains('0')),
            "the fault names the length: {error:?}"
        );

        // The transport is still alive here — this is the F-001 case: the socket must be shut down
        // regardless, so the peer sees EOF and the connection task's wait resolves.
        assert_closed(&transport).await;
        assert_peer_sees_eof(&mut peer).await;
        assert!(
            matches!(transport.send(b"late").await, Err(TransportError::Closed)),
            "nothing is sent after a framing fault"
        );
        assert!(
            matches!(transport.recv().await, Err(TransportError::Closed)),
            "nothing is received after a framing fault"
        );
    }

    #[tokio::test]
    async fn an_oversize_prefix_releases_the_socket_while_the_transport_is_still_held() {
        let (mut peer, accepted) = UnixStream::pair().expect("pair");
        let transport = UnixSocketTransport::from_stream(accepted);
        let oversize = MAX_FRAME + 1;

        peer.write_all(&oversize.to_be_bytes())
            .await
            .expect("prefix");
        peer.write_all(b"body that must stay unread")
            .await
            .expect("body");

        let error = transport.recv().await.expect_err("protocol fault");
        let named = oversize.to_string();
        assert!(
            matches!(&error, TransportError::Io(message) if message.contains(&named)),
            "the fault names the length {oversize}: {error:?}"
        );

        assert_closed(&transport).await;
        assert_peer_sees_eof(&mut peer).await;
    }

    #[tokio::test]
    async fn a_prefix_cut_short_is_terminal() {
        let (mut peer, accepted) = UnixStream::pair().expect("pair");
        let transport = UnixSocketTransport::from_stream(accepted);

        peer.write_all(&[0x00, 0x00]).await.expect("half a prefix");
        peer.shutdown().await.expect("hang up");

        let error = transport.recv().await.expect_err("truncated prefix");
        assert!(
            matches!(&error, TransportError::Io(message) if message.contains("truncated")),
            "half a prefix then EOF is Io: {error:?}"
        );

        assert_closed(&transport).await;
        assert_peer_sees_eof(&mut peer).await;
        assert!(
            matches!(transport.send(b"late").await, Err(TransportError::Closed)),
            "a send after a truncated prefix fails"
        );
        assert!(
            matches!(transport.recv().await, Err(TransportError::Closed)),
            "a recv after a truncated prefix fails"
        );
    }

    #[tokio::test]
    async fn a_body_cut_short_is_terminal() {
        let (mut peer, accepted) = UnixStream::pair().expect("pair");
        let transport = UnixSocketTransport::from_stream(accepted);

        peer.write_all(&10u32.to_be_bytes()).await.expect("prefix");
        peer.write_all(b"abc").await.expect("part of the body");
        peer.shutdown().await.expect("hang up");

        let error = transport.recv().await.expect_err("truncated body");
        assert!(
            matches!(&error, TransportError::Io(message) if message.contains("truncated")),
            "a short body is Io: {error:?}"
        );

        assert_closed(&transport).await;
        assert_peer_sees_eof(&mut peer).await;
        assert!(
            matches!(transport.send(b"late").await, Err(TransportError::Closed)),
            "a send after a truncated body fails"
        );
        assert!(
            matches!(transport.recv().await, Err(TransportError::Closed)),
            "a recv after a truncated body fails"
        );
    }

    #[tokio::test]
    async fn close_interrupts_a_send_blocked_on_a_peer_that_does_not_read() {
        let (mut peer, accepted) = UnixStream::pair().expect("pair");
        let transport = std::sync::Arc::new(UnixSocketTransport::from_stream(accepted));

        // Larger than any socket buffer, so the send parks — and within `MAX_FRAME`, so it is a
        // legitimate frame rather than an immediate protocol refusal.
        let body = vec![7u8; 8 * 1024 * 1024];
        let sending = tokio::spawn({
            let transport = std::sync::Arc::clone(&transport);
            async move { transport.send(&body).await }
        });
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert!(!sending.is_finished(), "the send is blocked on the peer");

        transport.close().await.expect("close");
        let sent = tokio::time::timeout(Duration::from_secs(5), sending)
            .await
            .expect("the blocked send wakes on close")
            .expect("send task");
        assert!(
            matches!(&sent, Err(TransportError::Closed)),
            "a send interrupted by our own close answers Closed, not Io and not success: {sent:?}"
        );

        assert_closed(&transport).await;
        assert_peer_sees_eof(&mut peer).await;
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
            "the peer sees the close as the end of the stream"
        );
    }

    #[tokio::test]
    async fn close_waits_for_teardown_instead_of_returning_early() {
        let (dialer, _accepted) = session().await;
        let dialer = std::sync::Arc::new(dialer);

        // Stand in for a teardown already under way: hold the shutdown lock, so `close` has the work
        // left to do and nothing else has done it yet.
        let teardown = dialer.shutdown_lock.lock().await;
        let closing = tokio::spawn({
            let dialer = std::sync::Arc::clone(&dialer);
            async move { dialer.close().await }
        });

        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(
            !closing.is_finished(),
            "close must not resolve while the shutdown it owes has not run"
        );
        assert!(
            !dialer.shutdown_done.load(Ordering::SeqCst),
            "the socket is still open here"
        );

        drop(teardown);
        let closed = tokio::time::timeout(Duration::from_secs(5), closing)
            .await
            .expect("close resolves once the teardown runs")
            .expect("close task");
        closed.expect("close");
        assert!(
            dialer.shutdown_done.load(Ordering::SeqCst),
            "close returned only after the socket was shut down"
        );
    }

    #[tokio::test]
    async fn a_second_close_returns_only_after_the_first_teardown_finished() {
        let (dialer, _accepted) = session().await;
        let dialer = std::sync::Arc::new(dialer);

        let teardown = dialer.shutdown_lock.lock().await;
        let first = tokio::spawn({
            let dialer = std::sync::Arc::clone(&dialer);
            async move { dialer.close().await }
        });
        tokio::time::sleep(Duration::from_millis(100)).await;

        // The repeat must queue behind the first teardown, not report success on its own.
        let second = tokio::spawn({
            let dialer = std::sync::Arc::clone(&dialer);
            async move { dialer.close().await }
        });
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(
            !first.is_finished() && !second.is_finished(),
            "both closes wait"
        );

        drop(teardown);
        for (name, task) in [("first", first), ("second", second)] {
            let closed = tokio::time::timeout(Duration::from_secs(5), task)
                .await
                .unwrap_or_else(|_| panic!("the {name} close resolves"))
                .expect("close task");
            closed.unwrap_or_else(|error| panic!("the {name} close is Ok: {error:?}"));
        }
        assert!(
            dialer.shutdown_done.load(Ordering::SeqCst),
            "the shutdown ran once and both closes saw it done"
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

        tokio::time::timeout(Duration::from_secs(5), waiting)
            .await
            .expect("the connection task's wait resolves on close")
            .expect("closed signal task");
    }

    /// The gate is load-bearing: with it closed the responder never reads the dialer's hello, so the
    /// session never establishes — the exact window the daemon closes before registering handlers.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_closed_gate_withholds_the_hello() {
        use spoke_connect::remote::{
            connect_responder, loopback_transport_pair, ConnectResponderOptions, RemoteIdentity,
        };

        let pair = loopback_transport_pair();
        let gated = Arc::new(GatedTransport::new(Arc::new(pair.server)));
        let responder = connect_responder(ConnectResponderOptions {
            transport: Arc::clone(&gated) as Arc<dyn Transport>,
            identity: RemoteIdentity { seed: [0x51u8; 32] },
            manifest: serde_json::from_value(serde_json::json!({
                "schema_version": 1,
                "host_id": "gate",
                "roles": ["data-store"],
                "capabilities": [],
                "namespaces": [],
                "extensions": {},
            }))
            .expect("manifest"),
            allowlist: Vec::new(),
            peer_keys: HashMap::new(),
            ports: None,
            invoke_timeout_ms: Some(1_000),
        })
        .await;

        let client = pair.client;
        client
            .send(&serde_json::to_vec(&serde_json::json!({"op": "hello"})).expect("frame"))
            .await
            .expect("send a first frame");

        // Never opened: the handshake cannot make progress, so the state stays pre-established.
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        assert_ne!(
            responder.state(),
            spoke_connect::remote::ConnectResponderState::Established,
            "a closed gate must keep the session unestablished"
        );

        // Opening it releases the frame, and the responder's own state machine takes over.
        gated.open();
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        assert_ne!(
            responder.state(),
            spoke_connect::remote::ConnectResponderState::Handshaking,
            "an opened gate lets the responder read"
        );
        responder.close();
    }

    /// A `recv` parked on the closed gate must end when the host rejects the session. This is the
    /// leak the daemon closes on a registration failure: the responder's read task waits here, and
    /// `close` cannot reach a task that never entered the inner transport — so a rejection must
    /// resolve the gate, not leave it closed.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_rejected_gate_ends_a_parked_recv() {
        let pair = loopback_transport_pair();
        let gated = Arc::new(GatedTransport::new(Arc::new(pair.server)));
        let waiting = {
            let gated = Arc::clone(&gated);
            tokio::spawn(async move { gated.recv().await })
        };
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(
            !waiting.is_finished(),
            "the recv is parked on the closed gate"
        );

        gated.reject();
        let answered = tokio::time::timeout(Duration::from_secs(2), waiting)
            .await
            .expect("a rejected gate must resolve a parked recv")
            .expect("the recv task did not panic");
        assert_eq!(
            answered.err(),
            Some(TransportError::Closed),
            "the parked recv answers Closed, so its task can end"
        );
    }

    /// Opening the gate is not a substitute: a parked recv then waits on the inner transport, which
    /// a rejected host never writes to. Pinned so a later simplification cannot swap one for the
    /// other.
    #[tokio::test(flavor = "multi_thread")]
    async fn opening_the_gate_releases_the_recv_into_the_inner_transport() {
        let pair = loopback_transport_pair();
        let gated = Arc::new(GatedTransport::new(Arc::new(pair.server)));
        let waiting = {
            let gated = Arc::clone(&gated);
            tokio::spawn(async move { gated.recv().await })
        };
        tokio::time::sleep(Duration::from_millis(50)).await;
        gated.open();
        // The inner transport carries nothing, so the recv stays pending: it is no longer the gate
        // that holds it, and that is exactly why a rejection is a distinct state.
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(
            !waiting.is_finished(),
            "an opened gate hands the recv to the inner transport, which has no frame"
        );
        waiting.abort();
    }
}
