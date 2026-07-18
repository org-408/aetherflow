//! I/O as messages — the `Connection` surface (DRAFT / feature `net`).
//!
//! An implementation of the surface described in `docs/io-surface-design.md`, first realized on a
//! **portable reference backend**. The user never reads/writes the socket directly: instead they
//! **receive incoming bytes via `on_data` (= a message) and append outbound data to the
//! nonblocking [`Io`] handle**. Handlers are synchronous run-to-completion — no await, no function
//! coloring, no `Pin`.
//!
//! **This backend is a reference implementation** (a single thread spinning a nonblocking socket),
//! not the intended **busy-poll design (each core spinning its own socket, on Linux)**. Its purpose
//! is to compile & test the surface API on macOS and lock it down. The performance version
//! (integration into `System::listen` + a busy-poll reactor) will follow on Linux.
//!
//! ```no_run
//! use aetherflow::net::{serve, Connection, Io};
//! struct Echo;
//! impl Connection for Echo {
//!     fn on_data(&mut self, buf: &[u8], io: &mut Io) { io.write(buf); }
//! }
//! let server = serve("127.0.0.1:0", || Echo).unwrap();
//! // ... connect to server.local_addr() ...
//! server.shutdown();
//! ```

use std::io::{self, ErrorKind, Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream, ToSocketAddrs};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::Duration;

/// A connection handler. **One instance per connection**. State lives in its fields (single
/// ownership, no `Arc`/`Mutex` needed).
///
/// Only [`on_data`](Connection::on_data) is required. I/O is the "source of messages" = the runtime
/// reads the socket and calls `on_data`. The user never writes a read loop.
pub trait Connection: Send + 'static {
    /// Called right after the connection opens. Use it for the first step of a handshake, etc.
    /// (optional).
    fn on_open(&mut self, _io: &mut Io) {}

    /// Bytes arrived. `buf` **may arrive in pieces** (TCP is a stream). If you need frame
    /// boundaries, use [`FramedConnection`].
    fn on_data(&mut self, buf: &[u8], io: &mut Io);

    /// The connection closed (peer close, error, or `io.close()`). Use it for aggregation and
    /// cleanup (optional).
    fn on_close(&mut self) {}
}

/// The outbound handle. `write` is **nonblocking** (append to an internal buffer; the reactor
/// writes it out to the socket). It never awaits. On the basic path it never fails (when full, it
/// buffers). Strict flow control is a separate concern for a higher-level API (not implemented in
/// this draft).
#[derive(Default)]
pub struct Io {
    out: Vec<u8>,
    close: bool,
}

impl Io {
    /// Queue outbound data (nonblocking). The actual socket write is performed by the reactor.
    pub fn write(&mut self, bytes: &[u8]) {
        self.out.extend_from_slice(bytes);
    }

    /// Schedule the connection to close once the outbound buffer has been fully flushed.
    pub fn close(&mut self) {
        self.close = true;
    }

    /// Number of bytes not yet sent out (for tests and observation).
    pub fn pending(&self) -> usize {
        self.out.len()
    }
}

// ───────────────────────── framing (move the boilerplate protocol state machine into the runtime) ─────────────────

/// A rule for carving frames out of a byte stream. If `decode` can take one whole frame from the
/// front, it returns `Some(frame)` and consumes those bytes from `buf`. If there isn't enough, it
/// returns `None` (wait for the next `on_data`).
pub trait Codec: Default + Send + 'static {
    fn decode(&mut self, buf: &mut Vec<u8>) -> Option<Vec<u8>>;
}

/// A 4-byte big-endian length prefix followed by the payload.
#[derive(Default)]
pub struct LengthPrefixed;

impl Codec for LengthPrefixed {
    fn decode(&mut self, buf: &mut Vec<u8>) -> Option<Vec<u8>> {
        if buf.len() < 4 {
            return None;
        }
        let n = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;
        if buf.len() < 4 + n {
            return None;
        }
        let frame = buf[4..4 + n].to_vec();
        buf.drain(..4 + n);
        Some(frame)
    }
}

/// Newline (`\n`) delimited. The returned frame excludes the newline (a trailing `\r` is also
/// stripped).
#[derive(Default)]
pub struct Lines;

impl Codec for Lines {
    fn decode(&mut self, buf: &mut Vec<u8>) -> Option<Vec<u8>> {
        let pos = buf.iter().position(|&b| b == b'\n')?;
        let mut line = buf[..pos].to_vec();
        if line.last() == Some(&b'\r') {
            line.pop();
        }
        buf.drain(..=pos); // consume the newline too
        Some(line)
    }
}

/// A handler that receives data one frame at a time. `on_frame` is **always called with one whole
/// frame** = the hand-written state machine disappears (the shallow surface of design.md §2.5).
pub trait FramedConnection: Send + 'static {
    type Codec: Codec;
    fn on_open(&mut self, _io: &mut Io) {}
    fn on_frame(&mut self, frame: &[u8], io: &mut Io);
    fn on_close(&mut self) {}
}

/// An adapter that turns a [`FramedConnection`] into a plain [`Connection`].
/// `serve(addr, || Framed::new(Proto))`.
pub struct Framed<F: FramedConnection> {
    inner: F,
    codec: F::Codec,
    buf: Vec<u8>,
}

impl<F: FramedConnection> Framed<F> {
    pub fn new(inner: F) -> Self {
        Self {
            inner,
            codec: F::Codec::default(),
            buf: Vec::new(),
        }
    }
}

impl<F: FramedConnection> Connection for Framed<F> {
    fn on_open(&mut self, io: &mut Io) {
        self.inner.on_open(io);
    }
    fn on_data(&mut self, chunk: &[u8], io: &mut Io) {
        self.buf.extend_from_slice(chunk);
        while let Some(frame) = self.codec.decode(&mut self.buf) {
            self.inner.on_frame(&frame, io);
        }
    }
    fn on_close(&mut self) {
        self.inner.on_close();
    }
}

// ───────────────────────── reference reactor (portability first; not the busy-poll performance version) ─────────────────────

/// A handle to a running server. `local_addr()` gives the listen address; `shutdown()` stops all
/// reactors. In thread-per-core mode ([`serve_on_cores`]) it holds multiple reactor threads.
pub struct ServerHandle {
    addr: SocketAddr,
    stop: Arc<AtomicBool>,
    joins: Vec<JoinHandle<()>>,
}

impl ServerHandle {
    /// The listen address (as passed to `serve*`; for `:0`, the actually assigned port).
    pub fn local_addr(&self) -> SocketAddr {
        self.addr
    }

    /// Stop all reactors and join them.
    pub fn shutdown(mut self) {
        self.stop.store(true, Ordering::Release);
        for j in self.joins.drain(..) {
            let _ = j.join();
        }
    }
}

impl Drop for ServerHandle {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        for j in self.joins.drain(..) {
            let _ = j.join();
        }
    }
}

struct Conn<C: Connection> {
    stream: TcpStream,
    handler: C,
    io: Io,
}

impl<C: Connection> Conn<C> {
    /// Write as much of the outbound buffer to the socket as possible (nonblocking; leave the rest
    /// if it stalls). Returns Err on a fatal error such as the peer disconnecting.
    fn try_flush(&mut self) -> io::Result<()> {
        while !self.io.out.is_empty() {
            match self.stream.write(&self.io.out) {
                Ok(0) => break,
                Ok(n) => {
                    self.io.out.drain(..n);
                }
                Err(e) if e.kind() == ErrorKind::WouldBlock => break,
                Err(e) if e.kind() == ErrorKind::Interrupted => continue,
                Err(e) => return Err(e),
            }
        }
        Ok(())
    }

    /// Whether a close was scheduled and all outbound data has been flushed.
    fn closing_done(&self) -> bool {
        self.io.close && self.io.out.is_empty()
    }

    /// Common post-processing that runs after on_data / when there is nothing to read: flush the
    /// outbound data, and on a fatal error or a completed close call `on_close` and return true to
    /// signal "drop this connection".
    fn service(&mut self) -> bool {
        if self.try_flush().is_err() || self.closing_done() {
            self.handler.on_close();
            return true;
        }
        false
    }
}

/// How to spin the reactor. The same nonblocking code runs modestly on a dev machine and busy-polls
/// on a dedicated core.
///
/// The default (`Default`) favors portability and development = `busy_poll: false` (a short sleep
/// each iteration) and `pin_core: None`. Busy-poll + pinning is explicit opt-in (the low-latency
/// path on a dedicated core).
#[derive(Clone, Copy, Default)]
pub struct ServeOptions {
    /// `true`: keep spinning without sleeping = **busy-poll** (low latency, assumes a dedicated
    /// core; the measured performance path on Linux).
    /// `false` (default): a short sleep each iteration = reference/development use (don't burn CPU
    /// on a shared machine).
    pub busy_poll: bool,
    /// `Some(core)`: best-effort pin the reactor thread to that core (pairs with busy-poll).
    pub pin_core: Option<usize>,
    /// `true` **and on Linux**: instead of scanning all fds, use **epoll (readiness)** to service
    /// only the ready fds = O(ready). Combined with `busy_poll`, epoll_wait is called with
    /// timeout=0 (no park). Ignored on non-Linux (scan).
    ///
    /// **The default `false` (scan) is recommended.** In measurements (8 vCPU, AWS c7g), as long as
    /// the server core isn't saturated (up to ~256 connections), **scan busy-poll is faster** (the
    /// epoll_wait syscall becomes pure overhead). Epoll's O(ready) advantage should only start to
    /// matter at **very high fd counts (thousands of connections)**, but that is unproven (needs
    /// harness improvements). At present epoll clearly wins only on "the flatness of the tail at low
    /// connection counts". See `docs/io-surface-design.md` §7.5.
    pub epoll: bool,
}

/// Start a server (default options = reference/development use). Binds to `addr` and creates one
/// handler per connection via `factory()`. For the low-latency performance path, pass
/// [`ServeOptions`] to [`serve_with`].
pub fn serve<A, F, C>(addr: A, factory: F) -> io::Result<ServerHandle>
where
    A: ToSocketAddrs,
    F: Fn() -> C + Send + 'static,
    C: Connection,
{
    serve_with(addr, factory, ServeOptions::default())
}

/// [`serve`] with options. Busy-poll + core pinning turns it into the **low-latency path on a
/// dedicated core**.
///
/// It simply switches the same nonblocking reactor to "keep spinning without sleeping" via
/// `busy_poll` — extending the busy-spin philosophy to I/O. In the low-connection, low-latency
/// slice, continuously scanning the fds is optimal (epoll targets high connection counts = a
/// different slice).
pub fn serve_with<A, F, C>(addr: A, factory: F, opts: ServeOptions) -> io::Result<ServerHandle>
where
    A: ToSocketAddrs,
    F: Fn() -> C + Send + 'static,
    C: Connection,
{
    let listener = TcpListener::bind(addr)?;
    listener.set_nonblocking(true)?;
    let local = listener.local_addr()?;
    let stop = Arc::new(AtomicBool::new(false));
    let stop_thread = Arc::clone(&stop);

    let join = thread::Builder::new()
        .name("aether-net".into())
        .spawn(move || reactor_loop(listener, factory, stop_thread, opts))?;

    Ok(ServerHandle {
        addr: local,
        stop,
        joins: vec![join],
    })
}

/// **thread-per-core serve**: spin up one reactor thread on each core in `cores`, each holding its
/// own listener bound to the same `addr` via **SO_REUSEPORT**. The kernel distributes incoming
/// connections across the listeners, so connections spread over N cores and each core spins only its
/// own connections (= AetherFlow's thread-per-core philosophy applied to I/O; enables an N-to-N
/// scaling comparison against glommio's N executors).
///
/// `factory` creates one instance per connection, so it must be **`Clone`** (each reactor holds its
/// own). `opts.pin_core` is ignored; each reactor is pinned to `cores[i]`.
///
/// **Note**: because multiple listeners coexist via SO_REUSEPORT, `addr` **must be a concrete port**
/// (`:0` would give each listener a different port and defeat the distribution). Unix only
/// (SO_REUSEPORT).
pub fn serve_on_cores<A, F, C>(
    addr: A,
    cores: &[usize],
    factory: F,
    opts: ServeOptions,
) -> io::Result<ServerHandle>
where
    A: ToSocketAddrs,
    F: Fn() -> C + Send + Clone + 'static,
    C: Connection,
{
    assert!(!cores.is_empty(), "serve_on_cores needs at least 1 core");
    let addr: SocketAddr = addr
        .to_socket_addrs()?
        .next()
        .ok_or_else(|| io::Error::new(ErrorKind::InvalidInput, "no addr"))?;
    let stop = Arc::new(AtomicBool::new(false));
    let mut joins = Vec::with_capacity(cores.len());

    for &core in cores {
        let listener = reuseport_listener(addr)?;
        let stop_t = Arc::clone(&stop);
        let factory_t = factory.clone();
        let mut opts_t = opts;
        opts_t.pin_core = Some(core);
        let join = thread::Builder::new()
            .name(format!("aether-net-{core}"))
            .spawn(move || reactor_loop(listener, factory_t, stop_t, opts_t))?;
        joins.push(join);
    }

    Ok(ServerHandle { addr, stop, joins })
}

/// Bind a nonblocking listener to addr with SO_REUSEPORT + SO_REUSEADDR set (so multiple cores can
/// accept concurrently on the same addr).
fn reuseport_listener(addr: SocketAddr) -> io::Result<TcpListener> {
    use socket2::{Domain, Protocol, Socket, Type};
    let domain = if addr.is_ipv4() {
        Domain::IPV4
    } else {
        Domain::IPV6
    };
    let sock = Socket::new(domain, Type::STREAM, Some(Protocol::TCP))?;
    sock.set_reuse_address(true)?;
    #[cfg(unix)]
    sock.set_reuse_port(true)?;
    sock.set_nonblocking(true)?;
    sock.bind(&addr.into())?;
    sock.listen(1024)?;
    Ok(sock.into())
}

fn reactor_loop<F, C>(listener: TcpListener, factory: F, stop: Arc<AtomicBool>, opts: ServeOptions)
where
    F: Fn() -> C,
    C: Connection,
{
    // On Linux with epoll requested, use the readiness reactor. Otherwise the portable scan reactor.
    #[cfg(target_os = "linux")]
    if opts.epoll {
        epoll_reactor(listener, factory, stop, opts);
        return;
    }
    scan_reactor(listener, factory, stop, opts);
}

/// Portable scan reactor: scan all connections nonblockingly. Optimal for low connection counts and
/// low latency (no epoll_wait syscall). At high concurrency the O(connections) scan lengthens the
/// tail → on Linux, use epoll.
fn scan_reactor<F, C>(listener: TcpListener, factory: F, stop: Arc<AtomicBool>, opts: ServeOptions)
where
    F: Fn() -> C,
    C: Connection,
{
    if let Some(core) = opts.pin_core {
        crate::pinning::pin_current_thread_to(core);
    }
    let mut conns: Vec<Conn<C>> = Vec::new();
    let mut rbuf = [0u8; 4096];

    while !stop.load(Ordering::Acquire) {
        // 1) accept as many as we can
        loop {
            match listener.accept() {
                Ok((stream, _peer)) => {
                    if stream.set_nonblocking(true).is_err() {
                        continue;
                    }
                    let mut conn = Conn {
                        stream,
                        handler: factory(),
                        io: Io::default(),
                    };
                    conn.handler.on_open(&mut conn.io);
                    // Flush on_open's output. If it shouldn't be dropped, add it to the list.
                    if !conn.service() {
                        conns.push(conn);
                    }
                }
                Err(e) if e.kind() == ErrorKind::WouldBlock => break,
                Err(e) if e.kind() == ErrorKind::Interrupted => continue,
                Err(_) => break,
            }
        }

        // 2) read each connection → on_data → flush
        let mut i = 0;
        while i < conns.len() {
            let mut remove = false;
            match conns[i].stream.read(&mut rbuf) {
                Ok(0) => {
                    // peer closed
                    conns[i].handler.on_close();
                    remove = true;
                }
                Ok(n) => {
                    let conn = &mut conns[i];
                    conn.handler.on_data(&rbuf[..n], &mut conn.io);
                    remove = conn.service();
                }
                Err(e) if e.kind() == ErrorKind::WouldBlock => {
                    // Nothing to read. Just attempt to flush any queued outbound data.
                    remove = conns[i].service();
                }
                Err(e) if e.kind() == ErrorKind::Interrupted => {}
                Err(_) => {
                    conns[i].handler.on_close();
                    remove = true;
                }
            }
            if remove {
                conns.swap_remove(i);
            } else {
                i += 1;
            }
        }

        // busy-poll: keep spinning without sleeping (the low-latency path assuming a dedicated core).
        // reference/development: rest briefly each iteration (don't burn CPU on a shared machine).
        if opts.busy_poll {
            std::hint::spin_loop();
        } else {
            thread::sleep(Duration::from_micros(100));
        }
    }
}

/// Linux epoll (readiness) reactor: service only the ready fds = O(ready). Even at high concurrency
/// no connection starves waiting for a scan, so the tail tightens (the same idea as glommio's
/// io_uring). Under `busy_poll`, epoll_wait is called with timeout=0 (no park). Level-triggered (no
/// EPOLLET) = simple, since a partial read re-fires on the next iteration.
#[cfg(target_os = "linux")]
fn epoll_reactor<F, C>(listener: TcpListener, factory: F, stop: Arc<AtomicBool>, opts: ServeOptions)
where
    F: Fn() -> C,
    C: Connection,
{
    use std::collections::HashMap;
    use std::os::unix::io::AsRawFd;

    if let Some(core) = opts.pin_core {
        crate::pinning::pin_current_thread_to(core);
    }

    let epfd = unsafe { libc::epoll_create1(0) };
    assert!(epfd >= 0, "epoll_create1 failed");
    let listen_fd = listener.as_raw_fd();

    // Small helper to register/modify an fd in epoll (with EPOLLIN|EPOLLOUT).
    let ctl = |op: libc::c_int, fd: libc::c_int, want_out: bool| {
        let mut events = libc::EPOLLIN as u32;
        if want_out {
            events |= libc::EPOLLOUT as u32;
        }
        let mut ev = libc::epoll_event {
            events,
            u64: fd as u64,
        };
        unsafe { libc::epoll_ctl(epfd, op, fd, &mut ev) }
    };
    ctl(libc::EPOLL_CTL_ADD, listen_fd, false);

    let mut conns: HashMap<libc::c_int, Conn<C>> = HashMap::new();
    let mut events = vec![libc::epoll_event { events: 0, u64: 0 }; 1024];
    let mut rbuf = [0u8; 4096];
    let timeout = if opts.busy_poll { 0 } else { 10 };

    while !stop.load(Ordering::Acquire) {
        let n = unsafe {
            libc::epoll_wait(epfd, events.as_mut_ptr(), events.len() as libc::c_int, timeout)
        };
        if n < 0 {
            if std::io::Error::last_os_error().kind() == ErrorKind::Interrupted {
                continue;
            }
            break;
        }
        for ev in events.iter().take(n as usize) {
            let fd = ev.u64 as libc::c_int;

            if fd == listen_fd {
                // Accept as many as we can (level-triggered, so it re-fires if any remain).
                loop {
                    match listener.accept() {
                        Ok((stream, _)) => {
                            if stream.set_nonblocking(true).is_err() {
                                continue;
                            }
                            let cfd = stream.as_raw_fd();
                            let mut conn = Conn {
                                stream,
                                handler: factory(),
                                io: Io::default(),
                            };
                            conn.handler.on_open(&mut conn.io);
                            let _ = conn.try_flush();
                            if conn.closing_done() {
                                conn.handler.on_close();
                                continue;
                            }
                            ctl(libc::EPOLL_CTL_ADD, cfd, !conn.io.out.is_empty());
                            conns.insert(cfd, conn);
                        }
                        Err(e) if e.kind() == ErrorKind::WouldBlock => break,
                        Err(e) if e.kind() == ErrorKind::Interrupted => continue,
                        Err(_) => break,
                    }
                }
                continue;
            }

            let mut remove = false;
            if let Some(conn) = conns.get_mut(&fd) {
                let bits = ev.events;
                if bits & ((libc::EPOLLHUP | libc::EPOLLERR) as u32) != 0 {
                    conn.handler.on_close();
                    remove = true;
                } else {
                    if bits & (libc::EPOLLIN as u32) != 0 {
                        match conn.stream.read(&mut rbuf) {
                            Ok(0) => {
                                conn.handler.on_close();
                                remove = true;
                            }
                            Ok(cnt) => {
                                let c = &mut *conn;
                                c.handler.on_data(&rbuf[..cnt], &mut c.io);
                            }
                            Err(e) if e.kind() == ErrorKind::WouldBlock => {}
                            Err(e) if e.kind() == ErrorKind::Interrupted => {}
                            Err(_) => {
                                conn.handler.on_close();
                                remove = true;
                            }
                        }
                    }
                    if !remove {
                        if conn.try_flush().is_err() || conn.closing_done() {
                            conn.handler.on_close();
                            remove = true;
                        } else {
                            // Request EPOLLOUT if outbound data remains, otherwise EPOLLIN only.
                            ctl(libc::EPOLL_CTL_MOD, fd, !conn.io.out.is_empty());
                        }
                    }
                }
            }
            if remove {
                unsafe { libc::epoll_ctl(epfd, libc::EPOLL_CTL_DEL, fd, std::ptr::null_mut()) };
                conns.remove(&fd);
            }
        }

        if opts.busy_poll && n == 0 {
            std::hint::spin_loop();
        }
    }

    unsafe { libc::close(epfd) };
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;
    use std::sync::Mutex;

    fn connect(addr: SocketAddr) -> TcpStream {
        let s = TcpStream::connect(addr).unwrap();
        s.set_read_timeout(Some(Duration::from_secs(2))).unwrap();
        s
    }

    #[test]
    fn echo_roundtrip() {
        struct Echo;
        impl Connection for Echo {
            fn on_data(&mut self, buf: &[u8], io: &mut Io) {
                io.write(buf);
            }
        }
        let server = serve("127.0.0.1:0", || Echo).unwrap();
        let mut c = connect(server.local_addr());
        c.write_all(b"hello aether").unwrap();
        let mut buf = [0u8; 12];
        c.read_exact(&mut buf).unwrap();
        assert_eq!(&buf, b"hello aether");
        drop(c);
        server.shutdown();
    }

    #[test]
    fn on_open_greets_and_close_fires() {
        static CLOSED: AtomicUsize = AtomicUsize::new(0);
        struct Greeter;
        impl Connection for Greeter {
            fn on_open(&mut self, io: &mut Io) {
                io.write(b"hi\n");
            }
            fn on_data(&mut self, _buf: &[u8], _io: &mut Io) {}
            fn on_close(&mut self) {
                CLOSED.fetch_add(1, Ordering::SeqCst);
            }
        }
        let server = serve("127.0.0.1:0", || Greeter).unwrap();
        let mut c = connect(server.local_addr());
        let mut buf = [0u8; 3];
        c.read_exact(&mut buf).unwrap();
        assert_eq!(&buf, b"hi\n"); // on_open's output arrives
        drop(c); // peer closes → on_close
        let t = std::time::Instant::now();
        while CLOSED.load(Ordering::SeqCst) == 0 && t.elapsed() < Duration::from_secs(2) {
            thread::sleep(Duration::from_millis(2));
        }
        assert_eq!(CLOSED.load(Ordering::SeqCst), 1);
        server.shutdown();
    }

    #[test]
    fn framed_lines_delivers_whole_frames() {
        // Record each received frame (line) and echo it back uppercased. Even with split sends, one line = one frame.
        static FRAMES: Mutex<Vec<String>> = Mutex::new(Vec::new());
        struct Upper;
        impl FramedConnection for Upper {
            type Codec = Lines;
            fn on_frame(&mut self, frame: &[u8], io: &mut Io) {
                FRAMES.lock().unwrap().push(String::from_utf8_lossy(frame).into_owned());
                let mut up = frame.to_ascii_uppercase();
                up.push(b'\n');
                io.write(&up);
            }
        }
        let server = serve("127.0.0.1:0", || Framed::new(Upper)).unwrap();
        let mut c = connect(server.local_addr());

        // Deliberately send across a boundary: "foo\nba" then "r\n" → frames are "foo","bar"
        c.write_all(b"foo\nba").unwrap();
        thread::sleep(Duration::from_millis(20));
        c.write_all(b"r\n").unwrap();

        let mut got = Vec::new();
        let mut tmp = [0u8; 64];
        // read "FOO\nBAR\n" = 8 bytes
        let mut total = 0;
        while total < 8 {
            match c.read(&mut tmp) {
                Ok(0) => break,
                Ok(n) => {
                    got.extend_from_slice(&tmp[..n]);
                    total += n;
                }
                Err(_) => break,
            }
        }
        assert_eq!(&got, b"FOO\nBAR\n");
        assert_eq!(&*FRAMES.lock().unwrap(), &["foo".to_string(), "bar".to_string()]);
        drop(c);
        server.shutdown();
    }

    #[test]
    fn multicore_reuseport_echo() {
        // Echo on 2 cores (2 reactors + SO_REUSEPORT). Even when multiple clients land on different reactors, everyone gets their echo.
        #[derive(Clone)]
        struct Echo;
        impl Connection for Echo {
            fn on_data(&mut self, buf: &[u8], io: &mut Io) {
                io.write(buf);
            }
        }
        // A concrete port is required (:0 would give different ports under reuseport). A high fixed port to avoid collisions.
        let addr = "127.0.0.1:19911";
        let server = serve_on_cores(addr, &[0, 1], || Echo, ServeOptions::default()).unwrap();
        // Open 8 connections and verify each echoes (the kernel distributes them across the 2 listeners).
        let mut clients: Vec<TcpStream> = (0..8).map(|_| connect(server.local_addr())).collect();
        for (i, c) in clients.iter_mut().enumerate() {
            let msg = format!("hello-{i}");
            c.write_all(msg.as_bytes()).unwrap();
            let mut buf = vec![0u8; msg.len()];
            c.read_exact(&mut buf).unwrap();
            assert_eq!(buf, msg.as_bytes());
        }
        drop(clients);
        server.shutdown();
    }

    #[test]
    fn framed_length_prefixed() {
        struct EchoFrame;
        impl FramedConnection for EchoFrame {
            type Codec = LengthPrefixed;
            fn on_frame(&mut self, frame: &[u8], io: &mut Io) {
                // echo the received frame back with the same length prefix
                io.write(&(frame.len() as u32).to_be_bytes());
                io.write(frame);
            }
        }
        let server = serve("127.0.0.1:0", || Framed::new(EchoFrame)).unwrap();
        let mut c = connect(server.local_addr());
        let payload = b"aetherflow";
        c.write_all(&(payload.len() as u32).to_be_bytes()).unwrap();
        c.write_all(payload).unwrap();

        let mut lenb = [0u8; 4];
        c.read_exact(&mut lenb).unwrap();
        let n = u32::from_be_bytes(lenb) as usize;
        let mut body = vec![0u8; n];
        c.read_exact(&mut body).unwrap();
        assert_eq!(&body, payload);
        drop(c);
        server.shutdown();
    }
}
