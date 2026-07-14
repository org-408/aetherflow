# Stage 0 ベンチを AWS で回す(turnkey)

> macOS ではハードピン留めできないので、tail の権威ある数字は Linux で取る。**ハード購入は不要**
> ── AWS で時間借りして測って破棄する。2 段階:
> - **Tier 1(マーケ用・簡単):** 専有 vCPU で「クラウドで Tokio/kameo に圧勝」の数字。~$0.15・15分。
> - **Tier 2(HFT flex):** ベアメタル + コア隔離で「単桁µs の tail」。~$5・1〜2時間。
>
> **⚠ 終わったら必ず instance を Terminate**(特に metal は ~$2/時)。Billing alert も設定推奨。

---

## 共通の前提
- AWS アカウント、SSH 鍵ペア(EC2 コンソールで作成 → `.pem` を保存)。
- Security Group: 自分の IP から TCP 22(SSH)を許可。
- リージョンはどこでも可(東京 `ap-northeast-1` でよい)。
- **新規アカウントは vCPU quota が低いことがある**。Tier 1(2 vCPU)は大抵OK。Tier 2 の `c7g.metal`
  (64 vCPU)は "Running On-Demand Standard instances" の上限に当たることがある → Service Quotas から
  引き上げ申請(数分〜数時間)。

---

## Tier 1 — 専有 vCPU(マーケ用の数字)

**instance**: `c7g.large`(Graviton3 / ARM64 / 2 vCPU、compute-optimized=専有)。~$0.07/時。
**AMI**: Ubuntu Server 24.04 LTS (ARM64)。

```sh
# 1) EC2 で c7g.large / Ubuntu 24.04 ARM64 を起動、SSH
ssh -i your-key.pem ubuntu@<PUBLIC_IP>

# 2) ツール
sudo apt-get update && sudo apt-get install -y git build-essential
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
. "$HOME/.cargo/env"

# 3) clone してベンチ(private repo なら gh auth / token で clone)
git clone https://github.com/org-408/aetherflow.git
cd aetherflow
cargo bench -p aetherflow --bench latency
```

出力の `aether-spin` / `aether-backoff` / `tokio` / `kameo-ask` / `aether-ask` を読む。
**見たいもの**: 実 Linux・専有 vCPU で、macOS/Docker より tail が締まり、**backoff が全分位で tokio/kameo
を上回る**か。ここが「クラウドで速くて、しかもアイドルで CPU を焼かない(backoff)」のマーケ数字。

---

## Tier 2 — ベアメタル + コア隔離(HFT 級 tail)

**instance**: `c7g.metal`(Graviton3 ベアメタル、64 vCPU)。~$2.3/時。**終了忘れ厳禁。**
**AMI**: Ubuntu Server 24.04 LTS (ARM64)。

```sh
ssh -i your-key.pem ubuntu@<PUBLIC_IP>
sudo apt-get update && sudo apt-get install -y git build-essential linux-tools-common

# 1) コアを隔離(OS スケジューラから外す)。ここでは物理コア 2,3 を隔離。
sudo sed -i 's/^GRUB_CMDLINE_LINUX="/GRUB_CMDLINE_LINUX="isolcpus=2,3 nohz_full=2,3 rcu_nocbs=2,3 /' /etc/default/grub
sudo update-grub
sudo reboot            # 再起動後もう一度 SSH

# 2) 決定性チューニング(再起動後)
sudo cpupower frequency-set -g performance 2>/dev/null || true   # 周波数固定
cat /sys/devices/system/cpu/isolated                              # 2-3 と出れば隔離成功

# 3) Rust & clone(Tier 1 と同じ)
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
. "$HOME/.cargo/env"
git clone https://github.com/org-408/aetherflow.git && cd aetherflow

# 4) 隔離コア 2,3 に閉じ込めて測る。
#    taskset がプロセス全スレッドを 2,3 に限定 → isolcpus で他は 2,3 に来ない
#    = busy-spin スレッドが「誰にも邪魔されない専有物理コア」を得る。
#    (ランタイム内部の best-effort pin は taskset に負けて無害)
taskset -c 2,3 cargo bench -p aetherflow --bench latency
```

**見たいもの**: `aether-spin` の **p99.9 が中央値に潰れる(単桁µs)**。これが出れば LMAX/exchange-core の
領域=「その気になれば HFT 級も出る」の看板。仮想化(Docker on Mac)で 2.5ms 爆発してた尾が、
専有隔離コアで消えるはず、が仮説。

---

## 数字の読み方(何が「勝ち」か)
- **Tier 1 の勝ち条件(マーケ)**: `aether-backoff` が `tokio`/`kameo` を **p50/p99/p999 全部で上回る**。
  → 「普通のクラウドで、安全なまま、Tokio 系に全分位圧勝」= 広い市場に刺さる看板。
- **Tier 2 の勝ち条件(flex)**: `aether-spin` の **p99.9 が単桁〜十数µs** に収束。
  → exchange-core(p99.9 22µs)級。「HFT にも届く」の証明。
- どちらも出れば thesis は本物。**Tier 1 だけでも十分に事業の起点**になる(Tier 2 は天井の看板)。

## 片付け(重要)
```sh
# ローカル/コンソールで instance を Terminate。metal を止め忘れると日 ~$55。
```
Billing alerts(予算 $10 等)を先に設定しておくと安全。

## Tier 2 を AWS 以外で(推奨:quota 審査が無い / むしろ簡単・速い・安い)

AWS の quota 審査で止まったら **待たずに他所で撃つ**。手順は上の Tier 2 と同じ(bare-metal +
isolcpus + pin + bench)、違うのは"借りる先"だけ。

| プロバイダ | 特徴 | 価格例(2026) |
|---|---|---|
| **Latitude.sh(本命)** | 時間貸し bare-metal、**~5秒でプロビジョン**、セルフサーブ、**quota 審査なし** | f4.metal.small **$0.40/時**(12コア)/ 64コア $1.58/時 |
| **Vultr Bare Metal** | 時間貸し、綺麗な API、新規に寛容、セルフサーブ | 競争力ある時間課金 |
| ~~Equinix Metal~~ | **2026-06-30 で sunset(終了)** → 使わない | — |

**手順(Latitude/Vultr、AWS Tier2 と同一)**:
1. bare-metal を時間借り(Ubuntu 24.04、モダン CPU:EPYC/Xeon 等)。SSH 鍵登録。
2. `/etc/default/grub` に `isolcpus=2,3 nohz_full=2,3 rcu_nocbs=2,3` → `sudo update-grub` → reboot。
3. `sudo cpupower frequency-set -g performance`(turbo/変動オフ)、IRQ を隔離コアから退避。
4. `git clone … && cd aetherflow && taskset -c 2,3 cargo bench -p aetherflow --bench latency`。
5. **p99/p99.9 を読む**(隔離コアで tail が中央値へ潰れるか) → **終わったら即 Terminate**。
- **~$1・1〜2時間。AWS より安く速い**(quota 審査が無い)。**Tier2 の "対策" はこれ = AWS を待たない。**

> 位置づけ(2026-07): Tier2 は **公開の必須ゲートではない**(Tier1 実測で看板は証明済み)。ceiling-flex
> として、やりたい時に Latitude/Vultr で取り、bench notes に後追いで足す。公開を AWS quota に人質に取らせない。

## 関連
- `stage0-bench-notes.md` — これまでの実測(macOS / Docker / AWS Tier1)と解釈
- `direction-and-roadmap.md` #6 — Stage 0 の位置づけ
- `core/benches/latency.rs` — 測定コード / `core/scripts/bench-linux.sh` — Docker 版(参考)
