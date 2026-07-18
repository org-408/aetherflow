//! 比較用: glommio の echo サーバ(Linux 専用)。AetherFlow busy-poll echo と**同じ client**
//! (`io_bench client`)で測る = フェア比較。
//!
//!   単一コア:   cargo run --release --example echo_glommio -- 127.0.0.1:9002 0
//!   複数コア:   cargo run --release --example echo_glommio -- 127.0.0.1:9002 0,1,2,3
//!
//! 複数コアは LocalExecutorPoolBuilder(N shard)+ 各 shard が同じ addr に bind(glommio は
//! SO_REUSEPORT を張るので分散着信)。AetherFlow の serve_on_cores と N 対 N で比べる。

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
