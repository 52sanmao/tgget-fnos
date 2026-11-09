#!/usr/bin/env bash
# 自动递增版本号（薄封装，实际逻辑在 bump_version.py）。
# 每次 push 触发 CI 编译时自动 patch+1，无需手动改版本号。
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

PYBIN="$(command -v python3 || command -v python || echo python)"
exec "$PYBIN" "$ROOT/tools/bump_version.py"
