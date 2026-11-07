"""Telegram 辅助工具、链接解析、消息元信息提取与方案 C 文件命名分配器。"""

from __future__ import annotations

import asyncio
import hashlib
import io
import math
import mimetypes
import os
import queue
import re
import shutil
import threading
import time
import weakref
import zipfile
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Optional

from telethon import TelegramClient, types, utils
from telethon.errors import (
    FileReferenceExpiredError,
    PasswordHashInvalidError,
    PhoneCodeInvalidError,
    SessionPasswordNeededError,
)
from telethon.sessions import StringSession

from .config import HOME, Config
from .db import DB
from .store import INDEX


def _rt():
    """动态获取 tgget.tg 外观模块，确保外部或测试对 tg.DB / tg.INDEX / tg.HOME 的替换在所有子模块生效。"""
    import sys
    return sys.modules.get("tgget.tg") or sys.modules[__name__]


_BAD_CHARS = re.compile(r'[<>:"/\\|?*\x00-\x1f]')
_SAFE_MAX = 120


def _media_label(media) -> str:
    """对话列表里给无文字消息一个可读的占位。"""
    name = type(media).__name__
    if name == "MessageMediaPhoto":
        return "［图片］"
    if name == "MessageMediaDocument":
        doc = getattr(media, "document", None)
        for attr in getattr(doc, "attributes", []) or []:
            cls = type(attr).__name__
            if cls == "DocumentAttributeVideo":
                return "［视频］"
            if cls == "DocumentAttributeAudio":
                return "［音频］"
            if cls == "DocumentAttributeSticker":
                return "［贴纸］"
        return "［文件］"
    if name == "MessageMediaGeo":
        return "［位置］"
    return "［媒体］"


def safe_name(name: str, fallback: str = "file") -> str:
    name = _BAD_CHARS.sub("_", (name or "").strip()).strip(". ")
    if not name:
        name = fallback
    if len(name) > _SAFE_MAX:
        stem, dot, ext = name.rpartition(".")
        if dot and len(ext) <= 8:
            name = stem[: _SAFE_MAX - len(ext) - 1] + "." + ext
        else:
            name = name[:_SAFE_MAX]
    return name


def _unlink(*paths) -> None:
    """删掉若干临时文件，删不掉也不算错（Windows 上文件可能正被占用）。"""
    for p in paths:
        try:
            Path(p).unlink(missing_ok=True)
        except OSError:
            pass


def human_size(n: Optional[float]) -> str:
    if not n:
        return "0 B"
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if n < 1024 or unit == "TB":
            return f"{n:.1f} {unit}" if unit != "B" else f"{int(n)} B"
        n /= 1024
    return f"{n:.1f} TB"


# -- 防重复：进程内的单文件锁 + 本地复用 ------------------------------------
# 同一个消息 id 被两个任务同时选中时，两边都会拿不到索引、同时开始写同一个
# `.part`，最后必然有一个是坏的。这里按 (对话, 消息 id) 加协程锁，第二个
# 等第一个下完，然后走索引直接跳过。
# 使用 WeakValueDictionary：协程在 `async with _dl_lock(key):` 栈帧中持有强引用，
# 同一文件的并发协程会拿到同一把锁；所有协程退出后锁对象自动被 GC 回收，零内存泄漏。
_DL_LOCKS: weakref.WeakValueDictionary[str, asyncio.Lock] = weakref.WeakValueDictionary()


def _dl_lock(key: str) -> asyncio.Lock:
    lock = _DL_LOCKS.get(key)
    if lock is None:
        lock = asyncio.Lock()
        _DL_LOCKS[key] = lock
    return lock


def reuse_local(src: Path, dest: Path) -> str:
    """把一个已经下过的文件「搬」到新位置，不走网络。

    同一块盘优先建硬链接（0 字节、瞬时完成）；跨盘会 OSError 退回复制。
    返回 `link` / `copy` / 空串（失败）。
    """
    if not src.is_file():
        return ""
    if dest.exists():
        return ""
    try:
        dest.parent.mkdir(parents=True, exist_ok=True)
        os.link(src, dest)
        return "link"
    except OSError:
        pass
    try:
        shutil.copy2(src, dest)
        return "copy"
    except OSError:
        return ""


def entity_title(entity) -> str:
    """对话/用户的可读名字，取不到就退回 id。"""
    if entity is None:
        return ""
    for attr in ("title", "first_name", "username"):
        v = getattr(entity, attr, None)
        if v:
            return str(v)
    last = getattr(entity, "last_name", "") or ""
    if last:
        return str(last)
    eid = getattr(entity, "id", None)
    return str(eid) if eid is not None else ""


# -- 落盘命名 --------------------------------------------------------------
# 规则：只按**发布日期**命名 —— `2026-05-31.mp4`，不掺原文件名、不加消息 id。
#
# 为什么需要序号：Telegram 的一个「相册」并不是一条消息，而是一组独立消息
# （每条一个文件），它们的 `date` 完全一样（同一秒），只差消息 id。只按日期
# 命名，一个 8 图相册会全部叫 `2026-05-31.jpg` 互相覆盖。所以同一天的第 2 个
# 及以后的文件按消息顺序追加序号：`2026-05-31_2.jpg`、`2026-05-31_3.jpg`……
# 序号由 NameAllocator 在下载开始前统一分配（见 jobs.py），保证并发下载时
# 顺序不会乱。
_KIND_CN = {
    "photo": "照片", "image": "照片", "video": "视频", "audio": "音频",
    "voice": "语音", "sticker": "贴纸", "file": "文件",
}

# 断点续传的临时文件：`2026-05-31.mp4.6665.part`（末尾带消息 id，才不会串）
_PART_RE = re.compile(r"^(?P<base>.+)\.(?P<id>\d+)\.part$")


def _msg_datetime(info: dict) -> Optional[datetime]:
    """消息时间转成本机时区的 datetime（文件名里用本地时间更直观）。"""
    raw = info.get("date")
    if not raw:
        return None
    if isinstance(raw, (int, float)):
        try:
            return datetime.fromtimestamp(float(raw))
        except (OverflowError, OSError, ValueError):
            return None
    try:
        dt = datetime.fromisoformat(str(raw).replace("Z", "+00:00"))
    except ValueError:
        return None
    if dt.tzinfo is not None:
        try:
            dt = dt.astimezone()
        except Exception:
            pass
    return dt


def _media_ext(info: dict) -> str:
    """落盘用的扩展名：优先原文件名，其次 MIME，最后按类型兜底。"""
    kind = info.get("kind") or "file"
    ext = Path(str(info.get("filename") or "")).suffix.lstrip(".").lower()
    if not ext:
        ext = (mimetypes.guess_extension(info.get("mime") or "") or "").lstrip(".")
    if not ext:
        ext = {"photo": "jpg", "image": "jpg", "video": "mp4", "audio": "mp3",
               "voice": "ogg", "sticker": "webp"}.get(kind, "bin")
    if len(ext) > 8:  # 有些文件名后缀是乱码，别把整个名字当后缀
        ext = {"photo": "jpg", "image": "jpg", "video": "mp4"}.get(kind, "bin")
    return ext


def date_tag(info: dict) -> str:
    """消息的发布日期（本机时区），形如 `2026-05-31`；拿不到返回空串。"""
    dt = _msg_datetime(info)
    return dt.strftime("%Y-%m-%d") if dt else ""


def download_stem(info: dict, seq: int = 0) -> str:
    """落盘文件的主名（方案 C）：`2026-08-30_6787`（原始发布日期 + 消息 ID）。
    - 每条消息自带唯一消息 ID，同一天多次转存、多任务并发下载绝不重名或互相覆盖；
    - 按文件名排序时，先按日期排、同日内按消息 ID 升序（即相册原始顺序）排；
    - 若磁盘上恰好已有同名文件（seq >= 2），自动追加 `_2`、`_3`。
    """
    base = date_tag(info) or _KIND_CN.get(info.get("kind") or "file", "媒体")
    mid = int(info.get("id") or 0)
    stem = f"{base}_{mid}" if mid > 0 else base
    return f"{stem}_{seq}" if seq > 1 else stem


def download_name(info: dict, seq: int = 0) -> str:
    """消息 → 落盘文件名（方案 C：`YYYY-MM-DD_消息ID.ext`）。"""
    ext = _media_ext(info)
    return safe_name(f"{download_stem(info, seq)}.{ext}", f"{info.get('id') or 0}.{ext}")


_ALLOC_LOCK = threading.RLock()


class NameAllocator:
    """线程安全的落盘文件名分配器（方案 C：`日期_消息ID.ext` + 磁盘/半成品冲突检测）。"""

    def __init__(self) -> None:
        self._used: dict[str, set[str]] = {}

    def _used_in(self, folder: Path) -> set[str]:
        key = str(folder)
        used = self._used.get(key)
        if used is None:
            used = set()
            try:
                for p in folder.iterdir():
                    if not p.is_file():
                        continue
                    m = _PART_RE.match(p.name)
                    name = m.group("base") if m else p.name
                    used.add(name.rsplit(".", 1)[0].lower())
            except OSError:
                pass
            self._used[key] = used
        return used

    def alloc(self, folder: Path, info: dict) -> str:
        with _ALLOC_LOCK:
            used = self._used_in(folder)
            mid = int(info.get("id") or 0)
            # 同一条消息上次下到一半：沿用原来的名字接着续传，别新开一份
            if mid > 0:
                try:
                    for p in folder.iterdir():
                        m = _PART_RE.match(p.name)
                        if m and int(m.group("id")) == mid:
                            base_name = m.group("base")
                            used.add(base_name.rsplit(".", 1)[0].lower())
                            return base_name
                except OSError:
                    pass
            seq = 1
            while True:
                stem = download_stem(info, seq)
                if stem.lower() not in used:
                    used.add(stem.lower())
                    return f"{stem}.{_media_ext(info)}"
                seq += 1


def assign_save_names(rows: list[dict]) -> None:
    """按方案 C（`日期_消息ID.ext`）为消息列表生成预览 `save_name`。"""
    used_stems: set[str] = set()
    for r in sorted(rows, key=lambda x: int(x.get("id") or 0)):
        if not r.get("filename") or r.get("kind") in (None, "text"):
            continue
        seq = 1
        while True:
            stem = download_stem(r, seq)
            if stem.lower() not in used_stems:
                used_stems.add(stem.lower())
                r["save_name"] = f"{stem}.{_media_ext(r)}"
                break
            seq += 1



def _xml_escape(s: str) -> str:
    return (s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
             .replace('"', "&quot;"))


def _build_docx(title: str, lines: list[str]) -> bytes:
    """不依赖 python-docx，直接拼一个最小可用的 docx（zip + OOXML）。"""
    import io
    import zipfile

    paras = [f'<w:p><w:pPr><w:pStyle w:val="Heading1"/></w:pPr>'
             f'<w:r><w:t xml:space="preserve">{_xml_escape(title)}</w:t></w:r></w:p>']
    for ln in lines[1:]:
        keep = " preserve" if ln[:1].isspace() or ln[-1:].isspace() else ""
        if ln:
            paras.append(f'<w:p><w:r><w:t xml:space="{keep.strip() or "default"}">{_xml_escape(ln)}</w:t></w:r></w:p>')
        else:
            paras.append("<w:p/>")

    document = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<w:document xmlns:w="http://schemas.openxmlformats.org/wordprocessingml/2006/main">'
        "<w:body>" + "".join(paras) + "<w:sectPr/></w:body></w:document>"
    )
    styles = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<w:styles xmlns:w="http://schemas.openxmlformats.org/wordprocessingml/2006/main">'
        '<w:style w:type="paragraph" w:styleId="Heading1"><w:name w:val="heading 1"/>'
        '<w:pPr><w:spacing w:before="240" w:after="120"/></w:pPr>'
        '<w:rPr><w:b/><w:sz w:val="32"/></w:rPr></w:style></w:styles>'
    )
    content_types = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">'
        '<Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>'
        '<Default Extension="xml" ContentType="application/xml"/>'
        '<Override PartName="/word/document.xml" ContentType="application/vnd.openxmlformats-officedocument.wordprocessingml.document.main+xml"/>'
        '<Override PartName="/word/styles.xml" ContentType="application/vnd.openxmlformats-officedocument.wordprocessingml.styles+xml"/>'
        "</Types>"
    )
    rels = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">'
        '<Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="word/document.xml"/>'
        "</Relationships>"
    )
    doc_rels = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">'
        '<Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/styles" Target="styles.xml"/>'
        "</Relationships>"
    )

    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        z.writestr("[Content_Types].xml", content_types)
        z.writestr("_rels/.rels", rels)
        z.writestr("word/document.xml", document)
        z.writestr("word/_rels/document.xml.rels", doc_rels)
        z.writestr("word/styles.xml", styles)
    return buf.getvalue()


def parse_link(text: str) -> tuple[str, Optional[int]]:
    """把用户输入解析成 (peer 引用, 消息 id)。

    支持：@name / t.me/name / t.me/name/123 / t.me/name/topic/123 /
          t.me/c/123456/789 / t.me/c/123456/topic/789 /
          t.me/+invite / 纯数字 id / tg://openmessage?...
    """
    s = (text or "").strip()
    if not s:
        raise ValueError("空的对话标识")

    # 私有频道/群（含话题群二级路径 t.me/c/chat_id/topic_id/msg_id，取最后一段数字作为消息 id）
    m = re.search(r"t\.me/c/(\d+)(?:/(\d+))?(?:/(\d+))?", s)
    if m:
        peer = int("-100" + m.group(1))
        mid_str = m.group(3) or m.group(2)
        return str(peer), (int(mid_str) if mid_str else None)

    # 公开频道/群（含话题群二级路径 t.me/name/topic_id/msg_id）
    m = re.search(r"t\.me/(?:s/)?([A-Za-z0-9_]{3,})/(\d+)(?:/(\d+))?", s)
    if m:
        mid_str = m.group(3) or m.group(2)
        return m.group(1), int(mid_str)

    m = re.search(r"t\.me/(?:s/)?(\+[A-Za-z0-9_-]+|[A-Za-z0-9_]{3,})", s)
    if m:
        return m.group(1), None

    m = re.search(r"tg://openmessage\?.*?(?:chat_id|channel_id)=(-?\d+)", s)
    if m:
        return m.group(1), None

    return s.lstrip("@"), None





def message_info(m) -> Optional[dict]:
    """把消息归一化成前端要的字典；非媒体返回 None。

    demo 模式下消息本身就是字典，直接透传。
    """
    if isinstance(m, dict):
        if not m.get("filename"):
            return None
        row = dict(m)
        row.setdefault("save_name", download_name(row))
        row.setdefault("file_id", "")
        return row

    media = m.media or m.document
    if media is None:
        return None

    filename: Optional[str] = None
    size: Optional[int] = None
    mime: Optional[str] = None
    kind = "file"

    doc = getattr(media, "document", None) or getattr(media, "photo", None)
    if isinstance(media, types.MessageMediaDocument) and media.document:
        doc = media.document
        for attr in doc.attributes:
            if isinstance(attr, types.DocumentAttributeFilename):
                filename = attr.file_name
            elif isinstance(attr, types.DocumentAttributeVideo):
                kind = "video"
            elif isinstance(attr, types.DocumentAttributeAudio):
                kind = "voice" if getattr(attr, "voice", False) else "audio"
            elif isinstance(attr, types.DocumentAttributeSticker):
                kind = "sticker"
        size = doc.size
        mime = doc.mime_type
    elif isinstance(media, types.MessageMediaPhoto):
        kind = "photo"
        size = _photo_size(media)
        mime = "image/jpeg"
    elif isinstance(media, types.MessageMediaWebPage):
        return None

    if not filename:
        ext = (mimetypes.guess_extension(mime or "") or "").lstrip(".")
        if not ext:
            ext = {"photo": "jpg", "video": "mp4", "audio": "mp3", "voice": "ogg"}.get(kind, "bin")
        filename = f"{m.id}_{kind}.{ext}"

    chat = getattr(m, "chat", None)
    title = getattr(chat, "title", None) or getattr(chat, "username", None) or str(m.chat_id or "")
    photo_w, photo_h = _dims(media)
    info = {
        "id": m.id,
        "chat_id": m.chat_id,
        "chat_title": title,
        "filename": filename,
        "size": size,
        "mime": mime,
        "kind": kind,
        "date": m.date.isoformat() if m.date else None,
        "caption": (m.message or "")[:300],
        "grouped_id": getattr(m, "grouped_id", None),
        "duration": _duration(m),
        # 文件指纹：同一个文件被转发到别的对话时 msg_id 变了，这个不变。
        # 用来做「内容级去重」—— 见过就不再从网络上下第二遍。
        "file_id": str(getattr(doc, "id", "") or ""),
        # 原始像素宽高：前端按 Telegram 的相册算法（grouped_layout）排布要用
        "width": photo_w,
        "height": photo_h,
    }
    # 界面显示用原名（用户认的是 Telegram 上的那个名字），落盘用带日期前缀的名字
    info["save_name"] = download_name(info)
    return info


def _photo_size(media) -> Optional[int]:
    """从照片的尺寸档里取最大档字节数；拿不到则返回 None。"""
    photo = getattr(media, "photo", None)
    if photo is None or not getattr(photo, "sizes", None):
        return None
    best = 0
    for sz in photo.sizes:
        s = getattr(sz, "size", None)
        if isinstance(s, int) and s > best:
            best = s
        arr = getattr(sz, "sizes", None)
        if arr:
            m = max((int(x) for x in arr if isinstance(x, int)), default=0)
            if m > best:
                best = m
    return best or None


def _dims(media) -> tuple[Optional[int], Optional[int]]:
    """媒体原始像素宽高；拿不到就 (None, None)。

    照片取最大那个尺寸档，视频/图片文件取文档属性里的 w/h。
    前端要用它算宽高比来选相册排布模板。
    """
    photo = getattr(media, "photo", None)
    if photo is not None and getattr(photo, "sizes", None):
        best: Optional[tuple[int, int]] = None
        for sz in photo.sizes:
            w = getattr(sz, "w", None)
            h = getattr(sz, "h", None)
            if w and h and (best is None or w * h > best[0] * best[1]):
                best = (int(w), int(h))
        if best:
            return best
    doc = getattr(media, "document", None)
    for attr in getattr(doc, "attributes", None) or []:
        w = getattr(attr, "w", None)
        h = getattr(attr, "h", None)
        if w and h:
            return int(w), int(h)
    return None, None


def _duration(m) -> Optional[int]:
    """视频/语音时长（秒），没有就 None。用于卡片角标。"""
    media = getattr(m, "media", None) or m
    doc = getattr(media, "document", None)
    for attr in getattr(doc, "attributes", None) or []:
        secs = getattr(attr, "duration", None)
        if secs:
            return int(secs)
    return None


def _sniff_mime(data: bytes) -> str:
    if data[:8] == b"\x89PNG\r\n\x1a\n":
        return "image/png"
    if data[:4] == b"RIFF" and data[8:12] == b"WEBP":
        return "image/webp"
    if data[:3] == b"GIF":
        return "image/gif"
    if data[4:8] == b"ftyp":
        return "video/mp4"
    if data[:4] == b"\x1aE\xdf\xa3":
        return "video/webm"
    if data[:2] == b"\xff\xd8":
        return "image/jpeg"
    return "application/octet-stream"


def message_row(m) -> Optional[dict]:
    """一条消息的统一结构（含纯文本），列表视图「消息」模式用。"""
    if isinstance(m, dict):  # 演示模式的数据本身就是字典
        row = dict(m)
        row.setdefault("text", row.get("caption") or "")
        row.setdefault("has_media", bool(row.get("filename")))
        row.setdefault("kind", row.get("kind") or "text")
        row.setdefault("grouped_id", None)
        row.setdefault("out", False)
        row.setdefault("sender", "")
        row.setdefault("filename", "")
        return row

    media = m.media or m.document
    info = message_info(m) if media else None
    sender = getattr(m, "sender", None)
    name = entity_title(sender) if sender is not None else ""
    if not name:
        name = entity_title(getattr(m, "chat", None))
    return {
        "id": m.id,
        "date": m.date.isoformat() if m.date else None,
        "text": (m.message or "")[:4000],
        "out": bool(getattr(m, "out", False)),
        "sender": name,
        "grouped_id": getattr(m, "grouped_id", None),
        "has_media": bool(info),
        "kind": (info or {}).get("kind", "text"),
        "filename": (info or {}).get("filename", ""),
        "save_name": (info or {}).get("save_name", ""),
        "size": (info or {}).get("size"),
        "mime": (info or {}).get("mime"),
        "caption": (info or {}).get("caption", ""),
        "duration": (info or {}).get("duration"),
        "width": (info or {}).get("width"),
        "height": (info or {}).get("height"),
        # 文件指纹：前端用它提示「这几个是同一份文件」，下载时也靠它做内容级去重
        "file_id": (info or {}).get("file_id", ""),
        "chat_id": m.chat_id,
    }


from telethon.sessions import StringSession  # noqa: E402  (放在末尾避免循环引用困惑)
