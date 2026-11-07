"""磁盘信息：顶栏「文件存储」显示下载目录所在盘的剩余空间。

程序最终跑在飞牛 NAS 上，没有桌面环境，所以**不提供**文件浏览器、改名、删除、
「在资源管理器中打开」这类本地文件操作——下载下来的文件直接交给在线播放器看。
"""

from __future__ import annotations

import shutil
from pathlib import Path


class Files:
    def __init__(self, cfg) -> None:
        self.cfg = cfg

    @property
    def root(self) -> Path:
        return self.cfg.download_path.resolve()

    def disk(self) -> dict:
        usage = shutil.disk_usage(self.root)
        return {
            "root": str(self.root),
            "free": usage.free,
            "used": usage.used,
            "total": usage.total,
            "text": f"{_size(usage.free)} 可用 / {_size(usage.total)}",
        }


def _size(n: float) -> str:
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if n < 1024 or unit == "TB":
            return f"{n:.0f} {unit}" if unit == "B" else f"{n:.1f} {unit}"
        n /= 1024
    return f"{n:.1f} TB"
