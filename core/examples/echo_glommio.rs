//! For comparison: glommio's echo server (Linux-only). Measured with the **same client**
//! (`io_bench client`) as the AetherFlow busy-poll echo = fair comparison.
//!
//!   single core:   cargo run --release --example echo_glommio -- 127.0.0.1:9002 0
//!   multi core:    cargo run --release --example echo_glommio -- 127.0.0.1:9002 0,1,2,3
//!
//! Multi-core uses LocalExecutorPoolBuilder (N shards) with each shard binding the same addr
//! (glommio sets SO_REUSEPORT, so incoming connections are distributed). Compared N-to-N against
//! AetherFlow's serve_on_cores.

#[cfg(target_os = "linux")]
fn main() {
    use futures_lite::{AsyncReadExt, AsyncWriteExt};
    use glommio::net::TcpListener;
    use glommio::{CpuSet, LocalExecutorBuilder, LocalExecutorPoolBuilder, Placement, PoolPlacement};

    let args: Vec<String> = std::env::args().collect();
    let addr = args.get(1).cloned().unwrap_or_else(|| "127.0.0.1:9002".into());
    let cores: Vec<usize> = args
        .get(2)
        .map(|s| s.split(',').filter_map(|x| x.trim().parse().ok()).collect())
        .unwrap_or_else(|| vec![0]);

    async fn serve(addr: String) {
        let listener = TcpListener::bind(&*addr).expect("bind");
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
    }

    println!("glommio echo on {addr} (cores {cores:?})");
    if cores.len() == 1 {
        let addr2 = addr.clone();
        LocalExecutorBuilder::new(Placement::Fixed(cores[0]))
            .name("glommio-echo")
            .spawn(move || serve(addr2))
            .expect("spawn")
            .join()
            .expect("join");
    } else {
        let cpuset = CpuSet::online()
            .expect("cpuset")
            .filter(move |l| cores.contains(&l.cpu));
        let n = cpuset.len();
        LocalExecutorPoolBuilder::new(PoolPlacement::MaxSpread(n, Some(cpuset)))
            .on_all_shards(move || serve(addr.clone()))
            .expect("pool")
            .join_all();
    }
}

#[cfg(not(target_os = "linux"))]
fn main() {
    eprintln!("echo_glommio is Linux-only (glommio uses io_uring). Run this on the AWS box.");
}
