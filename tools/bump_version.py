#!/usr/bin/env python3
"""自动递增版本号：读 VERSION 文件的 patch 位 +1，同步写入 __init__.py 与 manifest。

   0.9.5 -> 0.9.6 -> 0.9.7 -> ...

这是唯一的版本真源。每次 push 触发 CI 编译时自动 +1，无需手动改任何版本号。
"""
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
VERSION_FILE = ROOT / "VERSION"


def main() -> int:
    if not VERSION_FILE.exists():
        print("缺少 VERSION 文件", file=sys.stderr)
        return 1

    old = VERSION_FILE.read_text(encoding="utf-8").strip()

    # 拆成 x.y.z（兼容 x.y / x.y.z / 带 -build 后缀）
    base, sep, build = old.partition("-")
    parts = base.split(".")
    x = int(parts[0]) if len(parts) > 0 and parts[0].isdigit() else 0
    y = int(parts[1]) if len(parts) > 1 and parts[1].isdigit() else 0
    z = int(parts[2]) if len(parts) > 2 and parts[2].isdigit() else 0

    new = f"{x}.{y}.{z + 1}" + (f"-{build}" if sep else "")

    print(f"==> 版本递增：{old} -> {new}")

    # 统一用 LF 换行写回，避免 Windows 本地测试产生 CRLF 污染
    def _write(path: Path, text: str) -> None:
        path.write_text(text, encoding="utf-8", newline="\n")

    # 1. 写回 VERSION
    _write(VERSION_FILE, new + "\n")

    # 2. 写回 app/tgget/__init__.py
    init = ROOT / "app" / "tgget" / "__init__.py"
    if init.exists():
        s = init.read_text(encoding="utf-8")
        s = re.sub(r'__version__\s*=\s*"[^"]*"', f'__version__ = "{new}"', s, count=1)
        _write(init, s)

    # 3. 写回 manifest 的 version 字段与 changelog 前缀
    manifest = ROOT / "manifest"
    if manifest.exists():
        s = manifest.read_text(encoding="utf-8")
        s = re.sub(r"(?m)^version=.*$", f"version={new}", s, count=1)
        s = s.replace(f"v{old}", f"v{new}")
        _write(manifest, s)

    print("==> 版本号已同步到 VERSION / __init__.py / manifest")
    return 0


if __name__ == "__main__":
    sys.exit(main())
