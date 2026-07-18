//! 比較用: glommio の echo サーバ(Linux 専用)。AetherFlow busy-poll echo と**同じ client**
//! (`io_bench client`)で測る = フェア比較。core 0 に固定。
//!
//!   cargo run --release --example echo_glommio -- 127.0.0.1:9002 0

#[cfg(target_os = "linux")]
fn main() {
    use futures_lite::{AsyncReadExt, AsyncWriteExt};
    use glommio::net::TcpListener;
    use glommio::{LocalExecutorBuilder, Placement};

    let args: Vec<String> = std::env::args().collect();
    let addr = args.get(1).cloned().unwrap_or_else(|| "127.0.0.1:9002".into());
    let core: usize = args.get(2).and_then(|s| s.parse().ok()).unwrap_or(0);

    let handle = LocalExecutorBuilder::new(Placement::Fixed(core))
        .name("glommio-echo")
        .spawn(move || async move {
            let listener = TcpListener::bind(&addr).expect("bind");
            println!("glommio echo on {addr} (pinned core {core})");
            loop {
                match listener.accept().await {
                    Ok(mut stream) => {
                        glommio::spawn_local(async move {
                            let mut buf = [0u8; 4096];
                            loop {
                                match stream.read(&mut buf).await {
                                    Ok(0) | Err(_) => break,
                                    Ok(n) => {
                                        if stream.write_all(&buf[..n]).await.is_err() {
                                            break;
                                        }
                                    }
                                }
                            }
                        })
                        .detach();
                    }
                    Err(_) => break,
                }
            }
        })
        .expect("spawn executor");
    handle.join().expect("join");
}

#[cfg(not(target_os = "linux"))]
fn main() {
    eprintln!("echo_glommio is Linux-only (glommio uses io_uring). Run this on the AWS box.");
}
