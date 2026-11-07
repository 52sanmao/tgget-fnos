"""磁盘信息：顶栏「文件存储」显示下载目录所在盘的剩余空间；以及目录浏览
（设置页「保存目标文件夹」的文件夹选择器用）。

程序最终跑在飞牛 NAS 上，没有桌面环境，所以文件浏览只服务于「选择保存目录」，
不做改名、删除、「在资源管理器中打开」这类本地文件操作。
"""

from __future__ import annotations

import os
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

    # -- 目录浏览（保存目标文件夹选择器） ---------------------------------
    def browse_roots(self) -> list[str]:
        """返回可选作保存目录的「根」路径列表。

        优先级：用户在飞牛应用设置里授权的目录（TRIM_DATA_ACCESSIBLE_PATHS）>
        共享目录（TRIM_DATA_SHARE_PATHS）> 当前下载目录。
        """
        roots: list[str] = []
        seen = set()
        for env in ("TRIM_DATA_ACCESSIBLE_PATHS", "TRIM_DATA_SHARE_PATHS"):
            raw = os.environ.get(env, "")
            for part in raw.split(":"):
                part = part.strip()
                if not part:
                    continue
                p = str(Path(part).expanduser().resolve())
                if p not in seen:
                    seen.add(p)
                    roots.append(p)
        # 兜底：当前下载目录本身也可以被浏览/选择
        cur = str(self.root)
        if cur not in seen:
            roots.append(cur)
        return roots

    def browse(self, path: str) -> dict:
        """列出某目录下的子目录（供文件夹选择器逐级进入）。

        仅允许在授权目录/共享目录/当前下载目录范围内浏览，防止用户遍历任意路径。
        返回 {path, parent, dirs}，dirs 每项为 {name, path}。
        """
        target = Path(path).expanduser().resolve() if path else self.root
        # 根列表：把允许的范围归一化
        allowed = [Path(r).resolve() for r in self.browse_roots()]

        def _within(p: Path) -> bool:
            for r in allowed:
                try:
                    p.relative_to(r)
                    return True
                except ValueError:
                    continue
            return False

        if not _within(target):
            # 越界则回退到第一个允许的根
            target = allowed[0] if allowed else self.root

        dirs: list[dict] = []
        if target.is_dir():
            try:
                for child in sorted(target.iterdir()):
                    if not child.is_dir():
                        continue
                    if child.name.startswith("."):
                        continue
                    try:
                        full = child.resolve()
                    except OSError:
                        continue
                    if not _within(full):
                        continue
                    dirs.append({"name": child.name, "path": str(full)})
            except OSError:
                pass

        parent = target.parent if target != Path(target.anchor) else None
        # 父目录也要在允许范围内才允许返回
        if parent is not None and not _within(parent):
            parent = None

        return {
            "path": str(target),
            "parent": str(parent) if parent is not None else None,
            "dirs": dirs,
            "roots": [str(r) for r in allowed],
        }


def _size(n: float) -> str:
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if n < 1024 or unit == "TB":
            return f"{n:.0f} {unit}" if unit == "B" else f"{n:.1f} {unit}"
        n /= 1024
    return f"{n:.1f} TB"
