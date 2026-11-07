"""演示模式：不连 Telegram，用假数据跑通整条链路。

用途：在没有 api_id/api_hash、或网络不通时验证控制中心界面与任务调度。
生成的文件是真实的（内容为占位字节），方便验证断点续传与落盘路径。
"""

from __future__ import annotations

import asyncio
import random
import time
from pathlib import Path
from typing import Callable, Optional

DEMO_DIALOGS = [
    {"id": 1, "peer": "-1001000000001", "title": "演示 · 影视资源频道", "username": "demo_movies",
     "kind": "channel", "unread": 12, "pinned": True, "date": 1762400000,
     "preview": "高清珍贵视频_2026.mp4"},
    {"id": 2, "peer": "-1001000000002", "title": "演示 · 学习资料群", "username": None,
     "kind": "group", "unread": 3, "pinned": False, "date": 1762390000,
     "preview": "第 12 讲课件.pdf"},
    {"id": 3, "peer": "777000", "title": "Telegram", "username": "telegram", "kind": "user",
     "unread": 0, "pinned": False, "date": 1762380000, "preview": "登录代码 12345"},
    {"id": 1, "peer": "1", "title": "我的收藏", "username": None, "kind": "saved",
     "unread": 0, "pinned": True, "date": 1762410000, "preview": "保存给自己的消息"},
]

_EXTS = [("mp4", "video", 220 * 1024 * 1024), ("jpg", "photo", 2 * 1024 * 1024),
         ("jpg", "photo", 3 * 1024 * 1024), ("mp4", "video", 64 * 1024 * 1024),
         ("zip", "file", 42 * 1024 * 1024), ("pdf", "file", 8 * 1024 * 1024),
         ("mp3", "audio", 9 * 1024 * 1024), ("jpg", "photo", 1 * 1024 * 1024)]

# 演示用的原始像素尺寸：宽/方/竖/超宽都放进来，好让相册的几种排布模板都能看到
_DEMO_DIMS = [(1920, 1080), (1080, 1440), (1200, 1200), (1080, 1920),
              (2560, 1080), (1440, 1080), (900, 1200)]


def _fake_messages(peer: str, limit: int, offset_id: int = 0) -> list[dict]:
    """生成一批假消息：媒体与纯文本混排，并带相册分组，和真实数据形状保持一致。"""
    from .tg import download_name

    rnd = random.Random(peer)
    out = []
    title = next((d["title"] for d in DEMO_DIALOGS if d["peer"] == str(peer)), "演示频道")
    total = 96
    group = None
    for i in range(1, total + 1):
        mid = 9000 - i
        if offset_id and mid >= offset_id:
            continue
        if i % 5 == 0:
            out.append({
                "id": mid, "chat_id": peer, "chat_title": title,
                "text": f"这是第 {i} 条纯文字消息，用来验证「消息」视图的排版与时间分组。",
                "caption": f"这是第 {i} 条纯文字消息",
                "kind": "text", "size": 0, "filename": "", "save_name": "",
                "date": f"2026-11-{(i % 28) + 1:02d}T{10 + i % 12:02d}:30:00+00:00",
                "out": i % 10 == 1, "sender": "演示频道" if i % 10 == 1 else "",
                "grouped_id": None, "has_media": False,
            })
            continue
        ext, kind, size = _EXTS[rnd.randrange(len(_EXTS))]
        if kind in ("photo", "image"):
            size = None  # 和真实 Telegram 一致：照片拿不到精确字节数，走 media_bytes 通道
        # 每 4 条媒体凑一个「相册」，让客户端的扩选提示能触发
        if i % 4 == 0:
            group = 5000 + i
        tw, th = _DEMO_DIMS[i % len(_DEMO_DIMS)] if kind in ("photo", "image", "video") else (None, None)
        # 真实 Telegram 里一个相册的几条消息时间完全相同（同一秒），演示也照这个来，
        # 这样「同一天加序号」的命名规则在演示模式下看得见
        stamp = i - (i % 4) if group and i % 4 in (0, 1, 2) else i
        row = {
            "id": mid,
            "chat_id": peer,
            "chat_title": title,
            "filename": f"示例文件_{mid}.{ext}",
            "size": size,
            "mime": {"mp4": "video/mp4", "zip": "application/zip", "pdf": "application/pdf",
                     "jpg": "image/jpeg", "mp3": "audio/mpeg"}[ext],
            "kind": kind,
            "date": f"2026-11-{(stamp % 28) + 1:02d}T{10 + stamp % 12:02d}:00:00+00:00",
            "caption": f"演示数据第 {i} 条（未连接 Telegram）",
            "text": f"演示数据第 {i} 条（未连接 Telegram）",
            "out": False,
            "sender": title,
            "grouped_id": group if i % 4 in (0, 1, 2) else None,
            "has_media": True,
            "duration": 92 if kind == "video" else None,
            "width": tw,
            "height": th,
        }
        row["save_name"] = download_name(row)
        out.append(row)
        if len(out) >= limit:
            break
    return out[:limit]


_PALETTE = {
    "video": ((15, 118, 110), (6, 78, 59)),
    "photo": ((56, 189, 248), (29, 78, 216)),
    "image": ((56, 189, 248), (29, 78, 216)),
    "audio": ((251, 191, 36), (180, 83, 9)),
    "sticker": ((244, 114, 182), (157, 23, 77)),
}


def _gradient_png(seed: str, kind: str, w: int = 160, h: int = 120) -> bytes:
    """生成一张带纹理的抽象图（渐变 + 正弦噪声 + 暗角），纯标准库实现。

    目的只是让演示模式的卡片看起来像真实缩略图，而不是一块纯色。
    """
    import hashlib
    import math
    import struct
    import zlib

    a, b = _PALETTE.get(kind, ((113, 113, 122), (63, 63, 70)))
    hsh = hashlib.sha1(seed.encode()).digest()
    off = [(hsh[i] % 36) - 18 for i in range(3)]
    p1 = 0.05 + (hsh[3] % 40) / 1000.0
    p2 = 0.09 + (hsh[4] % 50) / 1000.0
    p3 = 0.6 + (hsh[5] % 90) / 100.0

    raw = bytearray()
    cx, cy = w * 0.5, h * 0.42
    maxd = math.hypot(cx, cy)
    for y in range(h):
        raw.append(0)  # 每行 filter 类型：None
        for x in range(w):
            u = (x / (w - 1)) * 0.65 + (y / (h - 1)) * 0.35
            # 两层不同频率的正弦叠加，做出类似云/山的起伏
            n = (
                math.sin(x * p1 + hsh[6] % 7) * math.cos(y * p2)
                + 0.5 * math.sin((x * p2 + y * p1) * p3)
            ) * 0.5
            # 暗角：越靠边缘越暗，像照片
            vig = 1.0 - 0.45 * (math.hypot(x - cx, y - cy) / maxd) ** 2
            shade = (0.86 + 0.14 * n) * vig + 0.10
            for c in range(3):
                v = int((a[c] + (b[c] - a[c]) * u) * shade) + off[c]
                raw.append(0 if v < 0 else 255 if v > 255 else v)

    def chunk(tag: bytes, data: bytes) -> bytes:
        body = tag + data
        return struct.pack(">I", len(data)) + body + struct.pack(">I", zlib.crc32(body) & 0xFFFFFFFF)

    ihdr = struct.pack(">IIBBBBB", w, h, 8, 2, 0, 0, 0)
    return (
        b"\x89PNG\r\n\x1a\n"
        + chunk(b"IHDR", ihdr)
        + chunk(b"IDAT", zlib.compress(bytes(raw), 6))
        + chunk(b"IEND", b"")
    )


class DemoEntity(str):
    """既是 peer 字符串（便于 get_messages 使用），又带 title 属性。"""

    def __new__(cls, ref: str, title: str):
        obj = super().__new__(cls, ref)
        obj.title = title
        return obj


class DemoTelegram:
    """与 Telegram 类保持同构的最小实现。"""

    def __init__(self, emit: Callable[[str, dict], None]) -> None:
        from .tg import LoopThread

        self.emit = emit
        self.authorized = False
        self._deadline = 0.0
        self.loop = LoopThread().start()

    # -- 与真实客户端对齐 --------------------------------------------------
    async def _ensure(self):
        return self

    async def _state(self) -> dict:
        return {
            "connected": True,
            "authorized": self.authorized,
            "demo": True,
            "user": {"id": 1, "name": "演示账号", "username": "demo", "phone": "***"} if self.authorized else None,
        }

    def state(self) -> dict:
        return self.loop.call(self._state())

    def qr_start(self) -> dict:
        self._deadline = time.time() + 60
        return self._qr_payload()

    def _qr_payload(self) -> dict:
        svg = ""
        try:
            import io

            import qrcode
            import qrcode.image.svg as svgimg

            img = qrcode.make("tg://login?token=DEMO-DEMO-DEMO-DEMO", image_factory=svgimg.SvgPathImage, box_size=10)
            b = io.BytesIO()
            img.save(b)
            svg = b.getvalue().decode("utf-8")
        except Exception:
            pass
        return {"authorized": False, "url": "tg://login?token=DEMO-DEMO-DEMO-DEMO",
                "expires": None, "svg": svg, "demo": True,
                "hint": "演示模式：点「模拟扫码成功」跳过真实登录"}

    def qr_poll(self) -> dict:
        return {"authorized": self.authorized, "demo": True}

    def qr_password(self, password: str) -> dict:
        self.authorized = True
        return {"ok": True, **self.state()}

    def demo_authorize(self) -> dict:
        self.authorized = True
        self.emit("state", self.state())
        return {"authorized": True, **self.state()}

    def send_code(self, phone: str) -> dict:
        return {"ok": True, "type": "Demo", "hint": "演示模式不需要验证码"}

    def sign_in(self, phone: str, code: str, password: str = "") -> dict:
        self.authorized = True
        return {"ok": True, **self.state()}

    def logout(self) -> dict:
        self.authorized = False
        return {"ok": True}

    def dialogs(self, limit: int = 200, query: str = "", source: str = "auto") -> list[dict]:
        q = (query or "").lower()
        rows = [dict(d) for d in DEMO_DIALOGS if not q or q in d["title"].lower()]
        for d in rows:  # 演示模式每个对话都有「头像」，实际是一张渐变色块
            d["has_photo"] = True
        return rows

    def messages(self, peer: str, limit: int = 50, offset_id: int = 0, media_only: bool = True,
                 source: str = "auto") -> dict:
        from .tg import assign_save_names

        rows = _fake_messages(peer, 200, offset_id)
        scanned = len(rows)
        if media_only:
            rows = [r for r in rows if r.get("has_media")]
        rows = rows[:limit]
        assign_save_names(rows)
        nxt = rows[-1]["id"] if rows else 0
        return {"rows": rows, "next_offset": nxt, "has_more": scanned >= limit, "scanned": scanned,
                "source": "local" if source == "local" else "remote"}

    def media_meta(self, peer: str, msg_id: int) -> dict:
        row = next((r for r in _fake_messages(peer, 200) if r["id"] == msg_id), None)
        if row is None or not row.get("has_media"):
            raise ValueError("演示数据里没有这条媒体")
        return row

    def media_bytes(self, peer: str, msg_id: int) -> tuple[bytes, str]:
        row = self.media_meta(peer, msg_id)
        if row["kind"] not in ("photo", "image"):
            raise ValueError("演示模式只提供图片的完整内容")
        return _gradient_png(f"{peer}:{msg_id}", row["kind"], 900, 600), "image/png"

    def open_stream(self, peer: str, msg_id: int, start: int = 0, length: int = -1,
                    chunk: int = 512 * 1024):
        import threading

        row = self.media_meta(peer, msg_id)
        if row["kind"] in ("photo", "image"):
            data, _ = self.media_bytes(peer, msg_id)
            return iter([data[max(0, start):]]), threading.Event()
        raise ValueError("演示模式没有真实的视频/音频流，请在真实账号下播放")

    def thumb(self, peer: str, msg_id: int) -> bytes:
        """演示模式没有真实图片，按 id 生成一张渐变色块占位图，走通整条渲染链路。"""
        rows = _fake_messages(peer, 200)
        row = next((r for r in rows if r["id"] == msg_id), None)
        if row is None:
            return b""
        if row["kind"] in ("audio", "voice", "file", "text"):
            return b""  # 音频/文件/文本本来就没有缩略图，前端会退回图标
        return _gradient_png(f"{peer}:{msg_id}", row["kind"])

    def avatar(self, peer: str) -> bytes:
        """演示模式的对话头像：按 peer 生成一张方形渐变色块。"""
        row = next((d for d in DEMO_DIALOGS if str(d["peer"]) == str(peer)), None)
        kind = (row or {}).get("kind") or "group"
        return _gradient_png(f"avatar:{peer}", kind, 160, 160)

    def reconnect(self) -> dict:
        return self.state()

    # -- 转发 / 发送 / 编辑（演示版只走通结构）------------------------------
    def transfer_saved(self, peer: str, ids: list[int], target: str) -> dict:
        from .db import DB
        from .store import INDEX

        time.sleep(0.25)
        clean_ids = [int(i) for i in ids]
        INDEX.drop_many(peer, clean_ids)
        try:
            DB.mark_deleted(str(peer), clean_ids)
        except Exception:
            pass
        title = next((d["title"] for d in DEMO_DIALOGS if str(d["peer"]) == str(target)), str(target))
        return {
            "transferred": len(clean_ids),
            "deleted_saved": len(clean_ids),
            "moved_files": len(clean_ids),
            "target_peer": str(target),
            "target_title": title,
            "new_ids": [9900 + i for i in range(len(clean_ids))],
        }

    def forward(self, peer: str, ids: list[int], target: str, drop_author: bool = False) -> dict:
        time.sleep(0.3)
        return {"forwarded": len(ids), "failed": 0, "target": target, "errors": []}

    def send_files(self, peer: str, paths: list[str], caption: str = "") -> dict:
        return {"sent": len(paths), "failed": 0, "errors": []}

    def edit_message(self, peer: str, msg_id: int, text: str) -> dict:
        return {"id": msg_id, "text": text}

    def export_messages(self, peer: str, limit: int = 2000, fmt: str = "txt",
                        outdir: str = "") -> dict:
        from .config import Config

        rows = _fake_messages(peer, min(limit, 24))
        root = Config().download_path
        root.mkdir(parents=True, exist_ok=True)
        target = root / f"演示导出_{len(rows)}条.{fmt if fmt == 'docx' else 'txt'}"
        body = "演示对话导出\n\n" + "\n".join(f"[示例] {r['filename']} · {r['caption']}" for r in rows)
        if fmt == "docx":
            from .tg import _build_docx

            target.write_bytes(_build_docx("演示对话导出", body.splitlines()))
        else:
            target.write_text(body, encoding="utf-8")
        return {"path": str(target), "messages": len(rows), "bytes": target.stat().st_size, "format": fmt}

    # Downloader 会用到这两个接口，保持与真实客户端同构
    async def get_entity(self, ref):
        ref = str(ref)
        title = next((d["title"] for d in DEMO_DIALOGS if d["peer"] == ref), ref)
        return DemoEntity(ref, title)

    async def get_messages(self, entity, ids=None):
        rows = _fake_messages(str(entity), 60)
        if ids:
            want = set(int(i) for i in ids)
            rows = [m for m in rows if m["id"] in want]
        return rows

    def iter_messages(self, entity, limit=50, offset_id: int = 0, min_id: int = 0, filter=None):
        async def gen():
            cap = int(limit) if (limit is not None and int(limit or 0) > 0) else 60
            for m in _fake_messages(str(entity), cap):
                if min_id and int(m.get("id") or 0) <= int(min_id):
                    continue
                if offset_id and int(m.get("id") or 0) >= int(offset_id):
                    continue
                yield m

        return gen()

    async def download_message(self, message: dict, target_dir: Path, sem, check_cancel,
                               peer: str = "", progress=None, info=None, name=None,
                               force: bool = False) -> dict:
        info = dict(info or message)
        async with sem:
            if check_cancel():
                return {"ok": False, "cancelled": True, **info}
            from .store import INDEX
            from .tg import safe_name

            folder = target_dir / safe_name(info["chat_title"], "unknown")
            folder.mkdir(parents=True, exist_ok=True)
            prev = None if force else INDEX.get(peer or str(info.get("chat_id") or ""), info["id"])
            if prev is not None and prev.parent == folder:
                return {"ok": True, "skipped": True, **info,
                        "path": str(prev), "size": prev.stat().st_size}
            dest = folder / safe_name(name or info.get("save_name") or info["filename"], "file")
            part = dest.with_name(f"{dest.name}.{info['id']}.part")
            total = info.get("size") or 0
            done = part.stat().st_size if part.exists() else 0
            # 演示：只写前 6MB，让进度条动起来就够了
            cap = min(total, 6 * 1024 * 1024)
            step = 512 * 1024
            with open(part, "ab") as fh:
                while done < cap:
                    if check_cancel():
                        return {"ok": False, "cancelled": True, **info}
                    n = min(step, cap - done)
                    fh.write(b"\0" * n)
                    done += n
                    await asyncio.sleep(0.35)
                    if progress:
                        progress(info, done, total)
                    self.emit("task", {**info, "state": "running", "done": done, "total": total,
                                       "percent": round(done * 100 / total, 1) if total else None})
            part.replace(dest)
            INDEX.put(peer or str(info.get("chat_id") or ""), info["id"], dest, done)
            self.emit("task", {**info, "state": "done", "done": done, "total": total, "percent": 100,
                               "path": str(dest)})
            return {"ok": True, **info, "path": str(dest), "size": done}

    def delete_messages(self, peer: str, ids: list[int], cloud: bool = True) -> dict:
        """演示模式不真的删云端消息，只走通本地文件清理与返回值结构。"""
        from .db import DB
        from .store import INDEX

        clean_ids = [int(i) for i in ids]
        local = INDEX.drop_many(peer, clean_ids)
        try:
            DB.mark_deleted(str(peer), clean_ids)
        except Exception:
            pass
        return {"deleted": len(ids) if cloud else 0, "failed": 0,
                "local_files": len(local), "errors": [], "demo": True}

    def close(self) -> None:
        pass
