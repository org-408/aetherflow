#!/usr/bin/env bash
# Mac 上の Linux コンテナで Stage 0 ベンチを走らせる。
# macOS ネイティブではハードピン留めできないが、Linux コンテナ内なら sched_setaffinity が効くので、
# tail latency にピン留めの効果が出るかをローカルで確認できる。
#
# 前提: Docker Desktop(または互換ランタイム)が起動していること。
# 使い方: runtime/ から `./scripts/bench-linux.sh`
#
# 注意: コンテナ内 CPU は仮想化されている(特に Apple Silicon)。bare-metal Linux/Graviton ほど
# 正確ではないが、macOS ネイティブより実 Linux の挙動に近い。権威ある数字はやはり実機/VM で。

set -euo pipefail
cd "$(dirname "$0")/.."   # runtime/

# 専有コアを与えるとピン留めの尾が見やすい(コア数に応じて調整可)。
CPUSET="${CPUSET:-0-3}"

echo "== Stage 0 bench in Linux container (cpuset=${CPUSET}) =="
docker run --rm \
  --cpuset-cpus="${CPUSET}" \
  -v "$PWD":/work -w /work \
  -e CARGO_TARGET_DIR=/work/target-linux \
  rust:slim \
  bash -c "cargo bench --bench latency"
