"""下载索引：记住「哪条消息落地成了哪个本地文件」。

用途：
1. **不重复下载** —— 已经下过的消息不再回源，换目录也只做硬链接；
2. **播放优先走本地** —— 拖进度条不会重新拉流（NAS 上省流量、秒开）；
3. **删除时同步清本地** —— 否则会留下孤儿文件（文件名已改成日期前缀，靠猜找不回来）；
4. **幽灵索引自愈** —— 若磁盘上的文件已被外部删除或大小为 0，查询时自动清除数据库失效记录，
   确保前端状态与增量进度游标（max_downloaded_id）永远反映真实的磁盘文件状态。

存储：SQLite 的 `files` 表（`~/.tgget/tgget.db`），主键 `(chat_id, msg_id)`。
"""

from __future__ import annotations

import threading
from pathlib import Path
from typing import Optional

from .config import HOME
from .db import DB, Database


class FileIndex:
    def __init__(self, path: Optional[Path] = None, db: Optional[Database] = None) -> None:
        if db is not None:
            self._db = db
            self._legacy = None
        elif path is not None:
            p = Path(path)
            # 传 .json 是老用法（测试里这么写）：换成同名 .db，保持隔离
            self._db = Database(p.with_suffix(".db") if p.suffix == ".json" else p)
            self._legacy = p if p.suffix == ".json" else None
        else:
            self._db = DB
            self._legacy = HOME / "files.json"
        self._lock = threading.Lock()
        self._imported = False

    # -- 内部 --------------------------------------------------------------
    @staticmethod
    def _key(peer: str, msg_id: int) -> str:
        return f"{peer}|{int(msg_id)}"

    def _maybe_import(self) -> None:
        if self._imported or self._legacy is None:
            return
        with self._lock:
            if self._imported:
                return
            self._imported = True
            try:
                self._db.import_files_json(self._legacy)
            except Exception:
                pass  # 迁移失败不该影响下载本身

    # -- 对外 --------------------------------------------------------------
    def put(self, peer: str, msg_id: int, path, size: int = 0, fid: str = "") -> None:
        self._maybe_import()
        p = Path(str(path or ""))
        try:
            if not p.is_file() or p.stat().st_size <= 0:
                return  # 严禁把不存在或 0 字节的幽灵文件写进索引表
        except OSError:
            return
        try:
            self._db.put_file(peer, msg_id, p, size or p.stat().st_size, fid)
        except Exception:
            pass  # 索引写不进去不该影响下载本身

    def get(self, peer: str, msg_id: int) -> Optional[Path]:
        """返回仍然存在于磁盘上且非空的本地文件；若文件已丢失则自动清理失效数据库记录。"""
        ent = self.info(peer, msg_id)
        if not ent:
            return None
        p = Path(str(ent.get("path") or ""))
        try:
            if p.is_file() and p.stat().st_size > 0:
                return p
        except OSError:
            pass
        # 磁盘文件已不存在或为 0 字节：自愈清除幽灵记录，让前端如实恢复为「未下载」状态
        try:
            self._db.delete_file(peer, msg_id)
        except Exception:
            pass
        return None

    def info(self, peer: str, msg_id: int) -> Optional[dict]:
        self._maybe_import()
        try:
            return self._db.get_file(peer, msg_id)
        except Exception:
            return None

    def find_fid(self, fid: str, exclude: str = "") -> Optional[Path]:
        """同一个文件（内容指纹相同）已经落在磁盘上的哪一份。

        用于「同一段视频被转发到多个频道」这类重复：网络上只有一份内容，
        没必要再走一遍流量。`exclude` 是 `chat_id|msg_id`，跳过自己那条。
        """
        if not fid:
            return None
        self._maybe_import()
        try:
            ent = self._db.find_file_by_fid(fid, exclude=exclude)
        except Exception:
            return None
        if not ent:
            return None
        p = Path(str(ent.get("path") or ""))
        try:
            if p.is_file() and p.stat().st_size > 0:
                return p
        except OSError:
            pass
        # 如果命中的那条旧记录文件已经丢了，顺手清掉再查下一条
        try:
            self._db.delete_file(str(ent.get("chat_id") or ""), int(ent.get("msg_id") or 0))
            ent2 = self._db.find_file_by_fid(fid, exclude=exclude)
            if ent2:
                p2 = Path(str(ent2.get("path") or ""))
                if p2.is_file() and p2.stat().st_size > 0:
                    return p2
        except Exception:
            pass
        return None

    def drop(self, peer: str, msg_id: int) -> list[str]:
        """把一条消息对应的本地文件（含 .part 半成品）删掉，返回删掉的路径。"""
        ent = self.info(peer, msg_id)
        removed: list[str] = []
        if ent:
            try:
                self._db.delete_file(peer, msg_id)
            except Exception:
                pass
            p = Path(str(ent.get("path") or ""))
            if p.name:
                for cand in (p, p.with_name(f"{p.name}.{int(msg_id)}.part"),
                             p.with_name(f"{p.name}.{int(msg_id)}.part.seg"),
                             p.with_suffix(p.suffix + ".part")):
                    try:
                        if cand.is_file():
                            cand.unlink()
                            removed.append(str(cand))
                    except OSError:
                        pass
        return removed

    def drop_many(self, peer: str, ids: list[int]) -> list[str]:
        out: list[str] = []
        for mid in ids:
            out.extend(self.drop(peer, mid))
        return out

    def stats(self) -> dict:
        self._maybe_import()
        try:
            return self._db.file_stats()
        except Exception:
            return {"indexed": 0, "alive": 0, "bytes": 0}

    def close(self) -> None:
        """关掉底层连接（测试用临时目录时必须调，否则 Windows 删不掉 .db 文件）。"""
        try:
            self._db.close()
        except Exception:
            pass


INDEX = FileIndex()
