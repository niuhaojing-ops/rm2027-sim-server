#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

if ! command -v python3 >/dev/null 2>&1; then
  echo "错误: 未找到 python3 (需要 Python >= 3.10)" >&2
  exit 1
fi

if ! command -v gst-launch-1.0 >/dev/null 2>&1 && ! command -v ffmpeg >/dev/null 2>&1; then
  echo "提示: 未检测到 gst-launch-1.0 或 ffmpeg，图传目录模式当前仅支持 .hevc/.h265 裸流文件（或传 --video-backend synthetic）。" >&2
fi

exec python3 "$ROOT_DIR/run.py" "$@"
