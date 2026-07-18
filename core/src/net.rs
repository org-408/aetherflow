//! I/O as messages — `Connection` 表面(DRAFT / feature `net`)。
//!
//! `docs/io-surface-design.md` の表面を、まず **ポータブルな参照バックエンド**で実装したもの。
//! ユーザーは socket を read/write せず、**届いたバイトを `on_data`(=メッセージ)で受け、送信は
//! 非ブロッキングの [`Io`] handle へ append** する。ハンドラは同期 run-to-completion、await 無し・
//! 関数の色無し・`Pin` 無し。
//!
//! **このバックエンドは参照実装**(nonblocking socket を1スレッドで回すだけ)であって、本命の
//! **busy-poll(各コアが socket を回す・Linux)ではない**。目的は「表面 API を macOS で compile &
//! test して確定させる」こと。性能版(`System::listen` への統合 + busy-poll reactor)は Linux で後追い。
//!
//! ```no_run
//! use aetherflow::net::{serve, Connection, Io};
//! struct Echo;
//! impl Connection for Echo {
//!     fn on_data(&mut self, buf: &[u8], io: &mut Io) { io.write(buf); }
//! }
//! let server = serve("127.0.0.1:0", || Echo).unwrap();
//! // ... server.local_addr() へ接続 ...
//! server.shutdown();
//! ```

use std::io::{self, ErrorKind, Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream, ToSocketAddrs};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::Duration;

/// 接続ハンドラ。**接続1つにつき1インスタンス**。状態はフィールドに持つ(単一所有、`Arc`/`Mutex` 不要)。
///
/// 必須は [`on_data`](Connection::on_data) だけ。I/O は「メッセージの発生源」= ランタイムが socket を
/// 読んで `on_data` を呼ぶ。ユーザーは read を書かない。
pub trait Connection: Send + 'static {
    /// 接続が開いた直後。ハンドシェイクの初手などに使う(任意)。
    fn on_open(&mut self, _io: &mut Io) {}

    /// バイトが届いた。`buf` は**分割で届きうる**(TCP はストリーム)。フレーム境界が要るなら
    /// [`FramedConnection`] を使う。
    fn on_data(&mut self, buf: &[u8], io: &mut Io);

    /// 接続が閉じた(相手クローズ or エラー or `io.close()`)。集計・後始末に使う(任意)。
    fn on_close(&mut self) {}
}

/// 送信 handle。`write` は**非ブロッキング**(内部バッファへ append、reactor が socket へ書き出す)。
/// await しない。基本パスでは失敗しない(満杯時はバッファする)。厳密な flow control は上級 API で
/// 別途(このドラフトでは未実装)。
#[derive(Default)]
pub struct Io {
    out: Vec<u8>,
    close: bool,
}

impl Io {
    /// 送信データを積む(非ブロッキング)。実際の socket 書き込みは reactor が行う。
    pub fn write(&mut self, bytes: &[u8]) {
        self.out.extend_from_slice(bytes);
    }

    /// 送信バッファを出し切ってから接続を閉じるよう予約する。
    pub fn close(&mut self) {
        self.close = true;
    }

    /// まだ送り出していないバイト数(テスト・観測用)。
    pub fn pending(&self) -> usize {
        self.out.len()
    }
}

// ───────────────────────── framing(定型プロトコルの状態機械をランタイムに寄せる) ─────────────────

/// バイト列からフレームを切り出す規則。`decode` は先頭から1フレーム取れれば `Some(frame)` を返し、
/// その分を `buf` から消費する。足りなければ `None`(次の `on_data` を待つ)。
pub trait Codec: Default + Send + 'static {
    fn decode(&mut self, buf: &mut Vec<u8>) -> Option<Vec<u8>>;
}

/// 4バイト BE の長さプレフィックス + 本文。
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

/// 改行(`\n`)区切り。返すフレームには改行を含めない(末尾 `\r` も落とす)。
#[derive(Default)]
pub struct Lines;

impl Codec for Lines {
    fn decode(&mut self, buf: &mut Vec<u8>) -> Option<Vec<u8>> {
        let pos = buf.iter().position(|&b| b == b'\n')?;
        let mut line = buf[..pos].to_vec();
        if line.last() == Some(&b'\r') {
            line.pop();
        }
        buf.drain(..=pos); // 改行も消費
        Some(line)
    }
}

/// フレーム単位で受け取るハンドラ。`on_frame` は**必ず1フレーム丸ごと**で呼ばれる = 手書き状態機械が
/// 消える(design.md §2.5 の shallow surface)。
pub trait FramedConnection: Send + 'static {
    type Codec: Codec;
    fn on_open(&mut self, _io: &mut Io) {}
    fn on_frame(&mut self, frame: &[u8], io: &mut Io);
    fn on_close(&mut self) {}
}

/// [`FramedConnection`] を素の [`Connection`] に変換するアダプタ。`serve(addr, || Framed::new(Proto))`。
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

// ───────────────────────── 参照 reactor(移植性優先。busy-poll 性能版ではない) ─────────────────────

/// 起動中のサーバへのハンドル。`local_addr()` で実際の待受アドレス、`shutdown()` で停止。
pub struct ServerHandle {
    addr: SocketAddr,
    stop: Arc<AtomicBool>,
    join: Option<JoinHandle<()>>,
}

impl ServerHandle {
    /// 実際に bind されたアドレス(`:0` を渡した場合の割当ポート確認に使う)。
    pub fn local_addr(&self) -> SocketAddr {
        self.addr
    }

    /// reactor を止めて join する。
    pub fn shutdown(mut self) {
        self.stop.store(true, Ordering::Release);
        if let Some(j) = self.join.take() {
            let _ = j.join();
        }
    }
}

impl Drop for ServerHandle {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        if let Some(j) = self.join.take() {
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
    /// 送信バッファを可能な範囲で socket へ書き出す(nonblocking。詰まったら残す)。
    /// 相手切断などの致命エラーなら Err。
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

    /// close 予約済みで送信も出し切ったか。
    fn closing_done(&self) -> bool {
        self.io.close && self.io.out.is_empty()
    }

    /// on_data 後 / 読むもの無し時に共通で走る後処理: 送信を出し切り、致命エラー or close 完了なら
    /// `on_close` を呼んで「この接続を落とす」= true を返す。
    fn service(&mut self) -> bool {
        if self.try_flush().is_err() || self.closing_done() {
            self.handler.on_close();
            return true;
        }
        false
    }
}

/// reactor の回し方。同じ nonblocking コードを、開発機では控えめに、専有コアでは busy-poll で回す。
///
/// 既定(`Default`)は移植性・開発優先 = `busy_poll: false`(各周回で短く sleep)・`pin_core: None`。
/// busy-poll + ピン留めは明示 opt-in(専有コアの低レイテンシ経路)。
#[derive(Clone, Copy, Default)]
pub struct ServeOptions {
    /// `true`: sleep せず回し続ける = **busy-poll**(低レイテンシ・専有コア前提。Linux 実測の性能経路)。
    /// `false`(既定): 各周回で短く sleep = 参照/開発用(共有機で CPU を焼かない)。
    pub busy_poll: bool,
    /// `Some(core)`: reactor スレッドをそのコアへ best-effort ピン留め(busy-poll と対で効く)。
    pub pin_core: Option<usize>,
    /// `true` **かつ Linux**: 全 fd スキャンでなく **epoll(readiness)** で ready な fd だけ捌く。
    /// 高並行で tail を締める(scan は O(接続数)で高並行時に一部接続が待たされ tail 暴発)。
    /// `busy_poll` と併用すると epoll_wait を timeout=0 で回す(park しない低レイテンシ経路)。
    /// 非 Linux では無視(scan にフォールバック)。
    pub epoll: bool,
}

/// サーバを起動する(既定オプション = 参照/開発用)。`addr` に bind し、接続ごとに `factory()` で
/// ハンドラを1つ作る。低レイテンシの性能経路は [`serve_with`] に [`ServeOptions`] を渡す。
pub fn serve<A, F, C>(addr: A, factory: F) -> io::Result<ServerHandle>
where
    A: ToSocketAddrs,
    F: Fn() -> C + Send + 'static,
    C: Connection,
{
    serve_with(addr, factory, ServeOptions::default())
}

/// [`serve`] にオプションを付けた版。busy-poll + コアピン留めで**専有コアの低レイテンシ経路**にする。
///
/// 同じ nonblocking reactor を、`busy_poll` で「sleep せず回し続ける」に切り替えるだけ ── busy-spin の
/// 思想を I/O へ延長したもの。低コネクション・低レイテンシ slice では fd を舐め続けるのが最適
/// (epoll は高コネクション向け=別 slice)。
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
        join: Some(join),
    })
}

fn reactor_loop<F, C>(listener: TcpListener, factory: F, stop: Arc<AtomicBool>, opts: ServeOptions)
where
    F: Fn() -> C,
    C: Connection,
{
    // Linux で epoll 指定なら readiness reactor へ。それ以外は移植性 scan reactor。
    #[cfg(target_os = "linux")]
    if opts.epoll {
        epoll_reactor(listener, factory, stop, opts);
        return;
    }
    scan_reactor(listener, factory, stop, opts);
}

/// 移植性 scan reactor: 全接続を nonblocking で舐める。低コネクション・低レイテンシに最適
/// (epoll_wait の syscall が無い)。高並行では O(接続数)スキャンで tail が伸びる → Linux は epoll。
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
        // 1) accept できるだけ受ける
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
                    // on_open の送信を出す。落とすべきでなければ接続リストへ。
                    if !conn.service() {
                        conns.push(conn);
                    }
                }
                Err(e) if e.kind() == ErrorKind::WouldBlock => break,
                Err(e) if e.kind() == ErrorKind::Interrupted => continue,
                Err(_) => break,
            }
        }

        // 2) 各接続を読む → on_data → flush
        let mut i = 0;
        while i < conns.len() {
            let mut remove = false;
            match conns[i].stream.read(&mut rbuf) {
                Ok(0) => {
                    // 相手クローズ
                    conns[i].handler.on_close();
                    remove = true;
                }
                Ok(n) => {
                    let conn = &mut conns[i];
                    conn.handler.on_data(&rbuf[..n], &mut conn.io);
                    remove = conn.service();
                }
                Err(e) if e.kind() == ErrorKind::WouldBlock => {
                    // 読むものは無い。溜まった送信を出す試みだけする。
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

        // busy-poll: sleep せず回し続ける(専有コア前提の低レイテンシ経路)。
        // 参照/開発: 各周回で短く休む(共有機で CPU を焼かない)。
        if opts.busy_poll {
            std::hint::spin_loop();
        } else {
            thread::sleep(Duration::from_micros(100));
        }
    }
}

/// Linux epoll(readiness)reactor: ready な fd だけを捌く = O(ready)。高並行でも一部接続が
/// スキャン待ちで飢えないので tail が締まる(glommio の io_uring と同じ発想)。`busy_poll` 時は
/// epoll_wait を timeout=0 で回す(park しない)。level-triggered(EPOLLET なし)= 部分読みでも
/// 次周回で再発火するので単純。
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

    // fd を epoll に (EPOLLIN|EPOLLOUT で) 登録/変更する小ヘルパ。
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
                // 受けられるだけ accept(level-triggered なので残ればまた発火)。
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
                            // 送信残があれば EPOLLOUT を要求、無ければ EPOLLIN のみ。
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
        assert_eq!(&buf, b"hi\n"); // on_open の送信が届く
        drop(c); // 相手クローズ → on_close
        let t = std::time::Instant::now();
        while CLOSED.load(Ordering::SeqCst) == 0 && t.elapsed() < Duration::from_secs(2) {
            thread::sleep(Duration::from_millis(2));
        }
        assert_eq!(CLOSED.load(Ordering::SeqCst), 1);
        server.shutdown();
    }

    #[test]
    fn framed_lines_delivers_whole_frames() {
        // 受け取ったフレーム(行)を記録し、大文字化して返す。分割送信でも1行=1フレーム。
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

        // わざと境界をまたいで送る: "foo\nba" then "r\n" → フレームは "foo","bar"
        c.write_all(b"foo\nba").unwrap();
        thread::sleep(Duration::from_millis(20));
        c.write_all(b"r\n").unwrap();

        let mut got = Vec::new();
        let mut tmp = [0u8; 64];
        // "FOO\nBAR\n" = 8 bytes を読む
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
    fn framed_length_prefixed() {
        struct EchoFrame;
        impl FramedConnection for EchoFrame {
            type Codec = LengthPrefixed;
            fn on_frame(&mut self, frame: &[u8], io: &mut Io) {
                // 受けたフレームを同じ length-prefixed で返す
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
