"""本地消息库：一个 SQLite 文件，让「已下载的东西」在断网时也能看得见。

为什么不用原来的 JSON：`files.json` 是 `键 → 路径` 的字典，每次写要整个文件
重写，也没法按时间翻页、按类型过滤。消息一多就不能当数据库用了。

五张核心表：
- `chats`      对话列表（离线时左侧栏靠它）
- `messages`   消息本体：文字、发送者、媒体元信息（离线时气泡靠它）
- `files`      本地文件路径（消息 ⇄ 改名后的文件，1:0..1）
- `sync_state` 每个对话同步到哪儿了，用来只拉增量
- `oplogs`     所有操作与异常步骤日志（持久化追踪）

约定：
- 时间同时存 epoch（排序用）和原始 ISO 字符串（显示用，跟 Telethon 给的完全一致）。
- 同一线程复用一个连接（`threading.local`），WAL 模式 + 进程内 RLock 串行化写事务 + 自动退避重试，
  彻底避免转存任务、并发下载、日志写入同时发生时出现 `OperationalError: database is locked`。
"""

from __future__ import annotations

import json
import sqlite3
import threading
import time
from pathlib import Path
from typing import Callable, Optional, TypeVar

from .config import HOME

SCHEMA_VERSION = 2
DB_FILE = HOME / "tgget.db"

_T = TypeVar("_T")

_SCHEMA = """
CREATE TABLE IF NOT EXISTS chats (
    chat_id    TEXT PRIMARY KEY,
    title      TEXT DEFAULT '',
    username   TEXT,
    kind       TEXT DEFAULT 'user',
    has_photo  INTEGER DEFAULT 0,
    unread     INTEGER DEFAULT 0,
    pinned     INTEGER DEFAULT 0,
    preview    TEXT DEFAULT '',
    date       INTEGER DEFAULT 0,
    updated_at INTEGER DEFAULT 0
);
CREATE TABLE IF NOT EXISTS messages (
    chat_id    TEXT NOT NULL,
    msg_id     INTEGER NOT NULL,
    date       INTEGER DEFAULT 0,
    date_iso   TEXT DEFAULT '',
    sender     TEXT DEFAULT '',
    out        INTEGER DEFAULT 0,
    text       TEXT DEFAULT '',
    kind       TEXT DEFAULT 'text',
    file_id    TEXT DEFAULT '',
    filename   TEXT DEFAULT '',
    size       INTEGER DEFAULT 0,
    mime       TEXT DEFAULT '',
    width      INTEGER,
    height     INTEGER,
    duration   INTEGER,
    grouped_id INTEGER,
    has_media  INTEGER DEFAULT 0,
    deleted    INTEGER DEFAULT 0,
    synced_at  INTEGER DEFAULT 0,
    PRIMARY KEY (chat_id, msg_id)
);
CREATE INDEX IF NOT EXISTS idx_msg_chat_date ON messages (chat_id, date DESC);
CREATE INDEX IF NOT EXISTS idx_msg_chat_kind ON messages (chat_id, has_media, msg_id DESC);
CREATE TABLE IF NOT EXISTS files (
    chat_id  TEXT NOT NULL,
    msg_id   INTEGER NOT NULL,
    path     TEXT NOT NULL,
    size     INTEGER DEFAULT 0,
    fid      TEXT DEFAULT '',
    added_at INTEGER DEFAULT 0,
    PRIMARY KEY (chat_id, msg_id)
);
CREATE INDEX IF NOT EXISTS idx_files_fid ON files (fid);
CREATE TABLE IF NOT EXISTS sync_state (
    chat_id    TEXT PRIMARY KEY,
    newest_id  INTEGER DEFAULT 0,
    oldest_id  INTEGER DEFAULT 0,
    last_sync  INTEGER DEFAULT 0,
    total      INTEGER DEFAULT 0
);
CREATE TABLE IF NOT EXISTS meta (
    key   TEXT PRIMARY KEY,
    value TEXT
);
CREATE TABLE IF NOT EXISTS oplogs (
    id        INTEGER PRIMARY KEY AUTOINCREMENT,
    ts        INTEGER NOT NULL,
    level     TEXT DEFAULT 'info',
    category  TEXT DEFAULT 'system',
    action    TEXT DEFAULT '',
    peer      TEXT DEFAULT '',
    detail    TEXT DEFAULT '',
    meta_json TEXT DEFAULT ''
);
CREATE INDEX IF NOT EXISTS idx_oplogs_ts ON oplogs (ts DESC, id DESC);
CREATE INDEX IF NOT EXISTS idx_oplogs_cat ON oplogs (category, ts DESC);
"""


class Database:
    """极薄的一层 SQLite 封装：带进程内写锁与 busy 自动退避重试，杜绝并发锁库报错。"""

    def __init__(self, path: Optional[Path] = None) -> None:
        self.path = Path(path) if path else DB_FILE
        self._local = threading.local()
        self._init_lock = threading.Lock()
        self._write_lock = threading.RLock()
        self._init()

    # -- 连接与并发写保护 --------------------------------------------------
    def _conn(self) -> sqlite3.Connection:
        conn = getattr(self._local, "conn", None)
        if conn is None:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            conn = sqlite3.connect(str(self.path), timeout=30.0, check_same_thread=False)
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=NORMAL")
            conn.execute("PRAGMA temp_store=MEMORY")
            conn.execute("PRAGMA busy_timeout=30000")
            self._local.conn = conn
        return conn

    def _write(self, fn: Callable[[sqlite3.Connection], _T]) -> _T:
        """所有写操作统一走进程内 RLock + SQLite 事务 + 锁冲突自动退避重试。"""
        last_exc: Optional[ Exception] = None
        for attempt in range(6):
            try:
                with self._write_lock:
                    conn = self._conn()
                    with conn:
                        return fn(conn)
            except sqlite3.OperationalError as exc:
                last_exc = exc
                msg = str(exc).lower()
                if "locked" in msg or "busy" in msg:
                    time.sleep(0.05 * (attempt + 1))
                    continue
                raise
        raise last_exc  # type: ignore[misc]

    def _read(self, fn: Callable[[sqlite3.Connection], _T]) -> _T:
        """读操作也自带锁冲突退避重试。"""
        last_exc: Optional[Exception] = None
        for attempt in range(5):
            try:
                conn = self._conn()
                return fn(conn)
            except sqlite3.OperationalError as exc:
                last_exc = exc
                msg = str(exc).lower()
                if "locked" in msg or "busy" in msg:
                    time.sleep(0.04 * (attempt + 1))
                    continue
                raise
        raise last_exc  # type: ignore[misc]

    def _init(self) -> None:
        with self._init_lock, self._write_lock:
            conn = self._conn()
            conn.executescript(_SCHEMA)
            conn.execute(
                "INSERT OR REPLACE INTO meta (key, value) VALUES ('version', ?)",
                (str(SCHEMA_VERSION),),
            )
            conn.commit()

    def close(self) -> None:
        conn = getattr(self._local, "conn", None)
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass
            self._local.conn = None

    def __del__(self) -> None:
        try:
            self.close()
        except Exception:
            pass

    # -- 对话 --------------------------------------------------------------
    def upsert_chats(self, rows: list[dict]) -> int:
        if not rows:
            return 0
        now = int(time.time())
        sql = (
            "INSERT INTO chats (chat_id, title, username, kind, has_photo, unread, pinned,"
            " preview, date, updated_at) VALUES (?,?,?,?,?,?,?,?,?,?)"
            " ON CONFLICT(chat_id) DO UPDATE SET"
            " title=excluded.title, username=excluded.username, kind=excluded.kind,"
            " has_photo=excluded.has_photo, unread=excluded.unread, pinned=excluded.pinned,"
            " preview=excluded.preview, date=excluded.date, updated_at=excluded.updated_at"
        )
        vals = [
            (str(r.get("peer") or r.get("chat_id") or ""), str(r.get("title") or ""),
             r.get("username"), str(r.get("kind") or "user"), int(bool(r.get("has_photo"))),
             int(r.get("unread") or 0), int(bool(r.get("pinned"))), str(r.get("preview") or ""),
             int(r.get("date") or 0), now)
            for r in rows
            if (r.get("peer") or r.get("chat_id"))
        ]
        if not vals:
            return 0
        self._write(lambda conn: conn.executemany(sql, vals))
        return len(vals)

    def list_chats(self, limit: int = 200, query: str = "") -> list[dict]:
        sql = "SELECT * FROM chats"
        args: list = []
        q = (query or "").strip().lower()
        if q:
            sql += " WHERE lower(title) LIKE ? OR lower(coalesce(username,'')) LIKE ?"
            args += [f"%{q}%", f"%{q}%"]
        sql += " ORDER BY pinned DESC, date DESC LIMIT ?"
        args.append(max(1, int(limit)))
        rows = self._read(lambda conn: conn.execute(sql, args).fetchall())
        return [chat_row(r) for r in rows]

    def chat_count(self) -> int:
        return self._read(lambda conn: int(conn.execute("SELECT count(*) FROM chats").fetchone()[0]))

    def resolve_chat_id(self, peer: str) -> str:
        """把 `@username` / 链接 / 数字 id 都归一到 chat_id，查不到返回空串。"""
        s = str(peer or "").strip()
        if not s:
            return ""
        if s.lstrip("-").isdigit():
            return s
        from .tg import parse_link  # 放在函数里，避免模块级循环引用

        try:
            ref, _ = parse_link(s)
        except Exception:
            ref = s
        if ref.lstrip("-").isdigit():
            return ref
        u = ref.lstrip("@").lower()

        def _q(conn: sqlite3.Connection) -> str:
            row = conn.execute(
                "SELECT chat_id FROM chats WHERE lower(coalesce(username,''))=? LIMIT 1", (u,)
            ).fetchone()
            if row:
                return str(row["chat_id"])
            row = conn.execute(
                "SELECT chat_id FROM chats WHERE lower(title)=? LIMIT 1", (u,)
            ).fetchone()
            return str(row["chat_id"]) if row else ""

        return self._read(_q)

    # -- 消息 --------------------------------------------------------------
    def upsert_messages(self, chat_id: str, rows: list[dict]) -> int:
        """写入/更新一批消息。同一条消息再拉一次就覆盖（编辑过的文字能跟上）。"""
        if not rows:
            return 0
        now = int(time.time())
        sql = (
            "INSERT INTO messages (chat_id, msg_id, date, date_iso, sender, out, text, kind,"
            " file_id, filename, size, mime, width, height, duration, grouped_id, has_media,"
            " deleted, synced_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,0,?)"
            " ON CONFLICT(chat_id, msg_id) DO UPDATE SET"
            " date=excluded.date, date_iso=excluded.date_iso, sender=excluded.sender,"
            " out=excluded.out, text=excluded.text, kind=excluded.kind,"
            " file_id=excluded.file_id, filename=excluded.filename, size=excluded.size,"
            " mime=excluded.mime, width=excluded.width, height=excluded.height,"
            " duration=excluded.duration, grouped_id=excluded.grouped_id,"
            " has_media=excluded.has_media, deleted=0, synced_at=excluded.synced_at"
        )
        vals = []
        for r in rows:
            mid = int(r.get("id") or 0)
            if mid <= 0:
                continue
            ts, iso = _split_date(r.get("date"))
            vals.append((
                str(chat_id), mid, ts, iso, str(r.get("sender") or ""), int(bool(r.get("out"))),
                str(r.get("text") or "")[:4000], str(r.get("kind") or "text"),
                str(r.get("file_id") or ""), str(r.get("filename") or ""),
                int(r.get("size") or 0), str(r.get("mime") or ""),
                _opt_int(r.get("width")), _opt_int(r.get("height")),
                _opt_int(r.get("duration")), _opt_int(r.get("grouped_id")),
                int(bool(r.get("has_media"))), now,
            ))
        if not vals:
            return 0
        self._write(lambda conn: conn.executemany(sql, vals))
        return len(vals)

    def list_messages(self, chat_id: str, limit: int = 50, before_id: int = 0,
                      media_only: bool = True) -> tuple[list[dict], bool]:
        """按 msg_id 倒序翻页。返回 (行, 还有更多吗)。"""
        limit = max(1, int(limit))
        sql = ("SELECT * FROM messages WHERE chat_id=? AND deleted=0"
               + (" AND has_media=1" if media_only else ""))
        args: list = [str(chat_id)]
        if before_id:
            sql += " AND msg_id < ?"
            args.append(int(before_id))
        sql += " ORDER BY msg_id DESC LIMIT ?"
        args.append(limit + 1)
        rows = self._read(lambda conn: conn.execute(sql, args).fetchall())
        more = len(rows) > limit
        return [message_row(r) for r in rows[:limit]], more

    def get_message(self, chat_id: str, msg_id: int) -> Optional[dict]:
        row = self._read(lambda conn: conn.execute(
            "SELECT * FROM messages WHERE chat_id=? AND msg_id=?",
            (str(chat_id), int(msg_id)),
        ).fetchone())
        return message_row(row) if row else None

    def count_messages(self, chat_id: str, media_only: bool = False) -> int:
        sql = "SELECT count(*) FROM messages WHERE chat_id=? AND deleted=0"
        if media_only:
            sql += " AND has_media=1"
        return self._read(lambda conn: int(conn.execute(sql, (str(chat_id),)).fetchone()[0]))

    def mark_deleted(self, chat_id: str, ids: list[int]) -> int:
        if not ids:
            return 0
        pairs = [(str(chat_id), int(i)) for i in ids]

        def _do(conn: sqlite3.Connection) -> int:
            cur = conn.executemany(
                "UPDATE messages SET deleted=1 WHERE chat_id=? AND msg_id=?",
                pairs,
            )
            return cur.rowcount or 0

        return self._write(_do)

    def reconcile_window(self, chat_id: str, live_ids: list[int], offset_id: int = 0,
                         reached_end: bool = False, media_only: bool = False) -> list[int]:
        """在线拉取消息后与本地缓存比对：把云端已删除、但本地仍显示的消息标为 deleted=1。"""
        cid = str(chat_id)
        live_set = {int(x) for x in live_ids if int(x or 0) > 0}
        media_cond = " AND has_media=1" if media_only else ""

        def _scan(conn: sqlite3.Connection) -> list[int]:
            if not live_set:
                if reached_end:
                    if offset_id > 0:
                        rows = conn.execute(
                            f"SELECT msg_id FROM messages WHERE chat_id=? AND deleted=0 AND msg_id < ?{media_cond}",
                            (cid, int(offset_id)),
                        ).fetchall()
                    else:
                        rows = conn.execute(
                            f"SELECT msg_id FROM messages WHERE chat_id=? AND deleted=0{media_cond}",
                            (cid,),
                        ).fetchall()
                    return [int(r["msg_id"]) for r in rows]
                return []

            lo = min(live_set)
            if offset_id <= 0:
                if reached_end:
                    rows = conn.execute(
                        f"SELECT msg_id FROM messages WHERE chat_id=? AND deleted=0{media_cond}",
                        (cid,),
                    ).fetchall()
                else:
                    rows = conn.execute(
                        f"SELECT msg_id FROM messages WHERE chat_id=? AND deleted=0 AND msg_id >= ?{media_cond}",
                        (cid, lo),
                    ).fetchall()
            else:
                if reached_end:
                    rows = conn.execute(
                        f"SELECT msg_id FROM messages WHERE chat_id=? AND deleted=0 AND msg_id < ?{media_cond}",
                        (cid, int(offset_id)),
                    ).fetchall()
                else:
                    rows = conn.execute(
                        f"SELECT msg_id FROM messages WHERE chat_id=? AND deleted=0 AND msg_id >= ? AND msg_id < ?{media_cond}",
                        (cid, lo, int(offset_id)),
                    ).fetchall()
            return [int(r["msg_id"]) for r in rows if int(r["msg_id"]) not in live_set]

        gone = self._read(_scan)
        if gone:
            self.mark_deleted(cid, gone)
        return gone

    def forget_chat(self, chat_id: str) -> None:
        def _do(conn: sqlite3.Connection) -> None:
            conn.execute("DELETE FROM messages WHERE chat_id=?", (str(chat_id),))
            conn.execute("DELETE FROM files WHERE chat_id=?", (str(chat_id),))
            conn.execute("DELETE FROM sync_state WHERE chat_id=?", (str(chat_id),))

        self._write(_do)

    # -- 同步游标 ----------------------------------------------------------
    def touch_sync(self, chat_id: str, ids: list[int]) -> None:
        """记下这个对话已经同步到哪一段（newest/oldest + 总条数）。"""
        if not ids:
            return
        lo, hi = min(ids), max(ids)
        self._write(lambda conn: conn.execute(
            "INSERT INTO sync_state (chat_id, newest_id, oldest_id, last_sync, total)"
            " VALUES (?,?,?,?, (SELECT count(*) FROM messages WHERE chat_id=?))"
            " ON CONFLICT(chat_id) DO UPDATE SET"
            " newest_id=max(coalesce(sync_state.newest_id,0), excluded.newest_id),"
            " oldest_id=CASE WHEN coalesce(sync_state.oldest_id,0)=0 THEN excluded.oldest_id"
            "          ELSE min(sync_state.oldest_id, excluded.oldest_id) END,"
            " last_sync=excluded.last_sync,"
            " total=(SELECT count(*) FROM messages WHERE chat_id=excluded.chat_id)",
            (str(chat_id), hi, lo, int(time.time()), str(chat_id)),
        ))

    def sync_info(self, chat_id: str) -> dict:
        row = self._read(lambda conn: conn.execute(
            "SELECT * FROM sync_state WHERE chat_id=?", (str(chat_id),)
        ).fetchone())
        return dict(row) if row else {}

    # -- 文件（供 FileIndex 使用）-------------------------------------------
    def put_file(self, chat_id: str, msg_id: int, path, size: int = 0, fid: str = "") -> None:
        self._write(lambda conn: conn.execute(
            "INSERT INTO files (chat_id, msg_id, path, size, fid, added_at) VALUES (?,?,?,?,?,?)"
            " ON CONFLICT(chat_id, msg_id) DO UPDATE SET"
            " path=excluded.path, size=excluded.size, fid=excluded.fid",
            (str(chat_id), int(msg_id), str(path), int(size or 0), str(fid or ""),
             int(time.time())),
        ))

    def get_file(self, chat_id: str, msg_id: int) -> Optional[dict]:
        row = self._read(lambda conn: conn.execute(
            "SELECT * FROM files WHERE chat_id=? AND msg_id=?",
            (str(chat_id), int(msg_id)),
        ).fetchone())
        return dict(row) if row else None

    def find_file_by_fid(self, fid: str, exclude: str = "") -> Optional[dict]:
        if not fid:
            return None
        rows = self._read(lambda conn: conn.execute(
            "SELECT * FROM files WHERE fid=? ORDER BY added_at", (str(fid),)
        ).fetchall())
        for r in rows:
            if f"{r['chat_id']}|{r['msg_id']}" == exclude:
                continue
            return dict(r)
        return None

    def find_file_by_path(self, path) -> Optional[dict]:
        row = self._read(lambda conn: conn.execute(
            "SELECT * FROM files WHERE path=?", (str(path),)
        ).fetchone())
        return dict(row) if row else None

    def delete_file(self, chat_id: str, msg_id: int) -> Optional[dict]:
        ent = self.get_file(chat_id, msg_id)
        if ent:
            self._write(lambda conn: conn.execute(
                "DELETE FROM files WHERE chat_id=? AND msg_id=?",
                (str(chat_id), int(msg_id)),
            ))
        return ent

    def all_files(self) -> list[dict]:
        return self._read(lambda conn: [dict(r) for r in conn.execute("SELECT * FROM files").fetchall()])

    def max_downloaded_id(self, chat_id: str) -> int:
        """查询某个对话已经下载落盘过的最大消息 ID（供「新增下载」与「自动规则续接」使用）。"""
        cid = str(chat_id or "").strip()
        if not cid:
            return 0
        cands = {cid}
        if not cid.lstrip("-").isdigit():
            try:
                resolved = self.resolve_chat_id(cid)
                if resolved:
                    cands.add(str(resolved))
            except Exception:
                pass

        def _q(conn: sqlite3.Connection) -> int:
            best = 0
            for k in cands:
                row = conn.execute(
                    "SELECT max(msg_id) AS m FROM files WHERE chat_id=?", (k,)
                ).fetchone()
                if row and row["m"] and int(row["m"]) > best:
                    best = int(row["m"])
            return best

        return self._read(_q)

    # -- 操作与运行日志（持久化到 SQLite oplogs 表）-------------------------
    def add_log(self, level: str, category: str, action: str, detail: str,
                peer: str = "", meta: Optional[dict] = None) -> int:
        now = int(time.time())
        meta_s = json.dumps(meta, ensure_ascii=False, default=str) if meta else ""

        def _do(conn: sqlite3.Connection) -> int:
            cur = conn.execute(
                "INSERT INTO oplogs (ts, level, category, action, peer, detail, meta_json)"
                " VALUES (?,?,?,?,?,?,?)",
                (now, str(level or "info")[:16], str(category or "system")[:24],
                 str(action or "")[:64], str(peer or "")[:64],
                 str(detail or "")[:1000], meta_s[:2000]),
            )
            row_id = int(cur.lastrowid or 0)
            if row_id % 100 == 0:
                conn.execute(
                    "DELETE FROM oplogs WHERE id NOT IN (SELECT id FROM oplogs ORDER BY ts DESC, id DESC LIMIT 2000)"
                )
            return row_id

        try:
            return self._write(_do)
        except Exception:
            return 0

    def list_logs(self, limit: int = 200, level: str = "", category: str = "",
                  query: str = "") -> list[dict]:
        sql = "SELECT * FROM oplogs WHERE 1=1"
        args: list = []
        if level and level != "all":
            sql += " AND level=?"
            args.append(str(level))
        if category and category != "all":
            sql += " AND category=?"
            args.append(str(category))
        q = (query or "").strip().lower()
        if q:
            sql += " AND (lower(detail) LIKE ? OR lower(action) LIKE ? OR lower(peer) LIKE ?)"
            args.extend([f"%{q}%", f"%{q}%", f"%{q}%"])
        sql += " ORDER BY ts DESC, id DESC LIMIT ?"
        args.append(max(1, min(1000, int(limit or 200))))
        rows = self._read(lambda conn: conn.execute(sql, args).fetchall())
        out = []
        for r in rows:
            d = dict(r)
            try:
                d["meta"] = json.loads(d.pop("meta_json") or "{}") if d.get("meta_json") else {}
            except Exception:
                d["meta"] = {}
            out.append(d)
        return out

    def clear_logs(self) -> int:
        return self._write(lambda conn: int(conn.execute("DELETE FROM oplogs").rowcount or 0))

    def file_stats(self) -> dict:
        alive = 0
        total = 0
        size = 0
        for ent in self.all_files():
            total += 1
            try:
                p = Path(str(ent.get("path") or ""))
                if p.is_file():
                    alive += 1
                    size += p.stat().st_size
            except OSError:
                pass
        return {"indexed": total, "alive": alive, "bytes": size}

    # -- 导入旧数据 --------------------------------------------------------
    def import_files_json(self, path: Path) -> int:
        """把老版本 `~/.tgget/files.json` 一次性搬进来（只在 files 表为空时做）。"""
        if not path.is_file():
            return 0
        if self._read(lambda conn: conn.execute("SELECT count(*) FROM files").fetchone()[0]):
            return 0
        try:
            raw = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            return 0
        if not isinstance(raw, dict):
            return 0

        def _do(conn: sqlite3.Connection) -> int:
            n = 0
            for key, ent in raw.items():
                if not isinstance(ent, dict) or "|" not in str(key):
                    continue
                chat_id, _, mid = str(key).partition("|")
                try:
                    mid_i = int(mid)
                except ValueError:
                    continue
                conn.execute(
                    "INSERT OR REPLACE INTO files (chat_id, msg_id, path, size, fid, added_at)"
                    " VALUES (?,?,?,?,?,?)",
                    (chat_id, mid_i, str(ent.get("path") or ""), int(ent.get("size") or 0),
                     str(ent.get("fid") or ""), int(time.time())),
                )
                n += 1
            return n

        return self._write(_do)

    def stats(self) -> dict:
        def _q(conn: sqlite3.Connection) -> tuple[int, int, int, int]:
            chats = int(conn.execute("SELECT count(*) FROM chats").fetchone()[0])
            msgs = int(conn.execute("SELECT count(*) FROM messages").fetchone()[0])
            medias = int(conn.execute(
                "SELECT count(*) FROM messages WHERE has_media=1 AND deleted=0"
            ).fetchone()[0])
            oplogs = int(conn.execute("SELECT count(*) FROM oplogs").fetchone()[0])
            return chats, msgs, medias, oplogs

        chats, msgs, medias, oplogs = self._read(_q)
        out = self.file_stats()
        out.update({
            "chats": chats,
            "messages": msgs,
            "media_messages": medias,
            "oplogs": oplogs,
            "db_bytes": self.path.stat().st_size if self.path.exists() else 0,
        })
        return out


# -- 行 → 字典 ------------------------------------------------------------
def _split_date(value) -> tuple[int, str]:
    """把 ISO 字符串 / 时间戳拆成 (epoch 秒, 原始 ISO)。"""
    if value is None or value == "":
        return 0, ""
    if isinstance(value, (int, float)):
        try:
            return int(value), _iso_from_ts(float(value))
        except (OverflowError, OSError, ValueError):
            return int(value), ""
    s = str(value)
    try:
        from datetime import datetime

        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        return int(dt.timestamp()), s
    except ValueError:
        return 0, s


def _iso_from_ts(ts: float) -> str:
    from datetime import datetime

    try:
        return datetime.fromtimestamp(ts).isoformat()
    except (OverflowError, OSError, ValueError):
        return ""


def _opt_int(v):
    if v is None or v == "":
        return None
    try:
        return int(v)
    except (TypeError, ValueError):
        return None


def chat_row(row: sqlite3.Row) -> dict:
    return {
        "peer": row["chat_id"],
        "id": row["chat_id"],
        "title": row["title"] or "",
        "username": row["username"],
        "kind": row["kind"] or "user",
        "has_photo": bool(row["has_photo"]),
        "unread": int(row["unread"] or 0),
        "pinned": bool(row["pinned"]),
        "preview": row["preview"] or "",
        "date": int(row["date"] or 0),
        "cached": True,
    }


def message_row(row: sqlite3.Row) -> dict:
    """库里的行 → 前端认识的字典（字段名跟 tg.message_row 保持一致）。"""
    iso = row["date_iso"] or _iso_from_ts(row["date"] or 0)
    return {
        "id": int(row["msg_id"]),
        "date": iso,
        "text": row["text"] or "",
        "out": bool(row["out"]),
        "sender": row["sender"] or "",
        "grouped_id": row["grouped_id"],
        "has_media": bool(row["has_media"]),
        "kind": row["kind"] or "text",
        "filename": row["filename"] or "",
        "size": int(row["size"] or 0) or None,
        "mime": row["mime"] or "",
        "caption": (row["text"] or "")[:300],
        "duration": row["duration"],
        "width": row["width"],
        "height": row["height"],
        "file_id": row["file_id"] or "",
        "chat_id": row["chat_id"],
        "cached": True,
    }


DB = Database()
