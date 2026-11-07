"""Telegram 对话列表、消息列表（含云端对比校准）、缩略图/头像缓存与 512KB 对齐边下边播流模块。"""

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

from .tg_utils import (
    _media_label,
    _sniff_mime,
    assign_save_names,
    message_info,
    message_row,
    parse_link,
)


class MediaMixin:
    def dialogs(self, limit: int = 200, query: str = "", source: str = "auto") -> list[dict]:
        """对话列表。`source=local` 只读本地库（断网时用），默认拉 Telegram 并顺手落库。"""
        if source == "local":
            try:
                return _rt().DB.list_chats(limit, query)
            except Exception:
                return []
        rows = self.loop.call(self._retry_disconnected(lambda: self._dialogs(limit, query)), timeout=25)
        self._last_rx_ts = time.time()
        try:
            _rt().DB.upsert_chats(rows)  # 顺手存一份，下次断网也能看到列表
        except Exception:
            pass
        return rows

    async def _dialogs(self, limit: int, query: str) -> list[dict]:
        client = await self._ensure()
        out: list[dict] = []
        q = query.strip().lower()
        try:
            me = await client.get_me()
        except Exception:
            me = None
        me_id = getattr(me, "id", None)
        has_saved = False
        async for d in client.iter_dialogs(limit=None if q else limit):
            title = d.name or ""
            if q and q not in title.lower() and not (d.entity and getattr(d.entity, "username", "")):
                continue
            kind = "user"
            if d.is_group:
                kind = "group"
            if d.is_channel:
                kind = "channel"
            # 「我的收藏」在 Telegram 里就是自己跟自己的对话，官方客户端才叫 Saved Messages，
            # Telethon 这里返回的是用户自己的名字，必须显式识别并改名，否则用户根本认不出来。
            is_self = me_id is not None and (
                getattr(d.entity, "id", None) == me_id or d.id == me_id
            )
            if is_self:
                has_saved = True
                kind = "saved"
            preview = ""
            try:
                if d.message is not None:
                    preview = (d.message.message or "").strip().replace("\n", " ")[:60]
                    if not preview and d.message.media:
                        preview = _media_label(d.message.media)
            except Exception:
                preview = ""
            out.append(
                {
                    "id": d.id,
                    "peer": utils.get_peer_id(d.entity) if d.entity else d.id,
                    "title": "我的收藏" if is_self else title,
                    "username": getattr(d.entity, "username", None),
                    "kind": kind,
                    "unread": d.unread_count,
                    "pinned": True if is_self else d.pinned,
                    "date": int(d.date.timestamp()) if d.date else 0,
                    "preview": preview or ("保存给自己的消息" if is_self else ""),
                    # 有没有头像：前端据此决定要不要去 /api/avatar 拉图，
                    # 没头像的对话就不发请求，省掉一堆 404
                    "has_photo": bool(getattr(getattr(d.entity, "photo", None), "photo_id", None)),
                }
            )
            if len(out) >= limit:
                break
        # 有的账号（比如从未用过收藏夹）不会在对话列表里出现这一项，补一条进去，
        # 否则用户永远找不到自己的收藏。
        if me_id is not None and not has_saved:
            out.insert(0, {
                "id": me_id, "peer": me_id, "title": "我的收藏", "username": None,
                "kind": "saved", "unread": 0, "pinned": True, "date": 0,
                "preview": "保存给自己的消息", "has_photo": True,
            })
        return out

    def messages(self, peer: str, limit: int = 50, offset_id: int = 0, media_only: bool = True,
                 source: str = "auto") -> dict:
        """消息列表。`source=local` 只读本地库（断网 / 秒开），默认拉 Telegram 并落库。"""
        if source == "local":
            return self._messages_local(peer, limit, offset_id, media_only)
        res = self.loop.call(
            self._retry_disconnected(lambda: self._messages(peer, limit, offset_id, media_only)),
            timeout=25,
        )
        self._last_rx_ts = time.time()
        return res

    def _messages_local(self, peer: str, limit: int, offset_id: int,
                        media_only: bool) -> dict:
        chat_id = str(peer or "").strip()
        if not chat_id.lstrip("-").isdigit():
            try:
                chat_id = _rt().DB.resolve_chat_id(chat_id)
            except Exception:
                chat_id = ""
        if not chat_id:
            return {"rows": [], "next_offset": 0, "has_more": False, "source": "local"}
        try:
            rows, more = _rt().DB.list_messages(chat_id, limit, offset_id, media_only)
        except Exception:
            rows, more = [], False
        for r in rows:
            r["local"] = bool(_rt().INDEX.get(chat_id, r["id"]))
        assign_save_names(rows)
        return {
            "rows": rows,
            "next_offset": rows[-1]["id"] if rows else 0,
            "has_more": more,
            "scanned": len(rows),
            "source": "local",
        }

    async def _messages(self, peer: str, limit: int, offset_id: int, media_only: bool) -> dict:
        client = await self._call_rpc(self._ensure(), timeout=12.0)
        entity = await self._call_rpc(self._entity(client, peer), timeout=15.0)
        rows: list[dict] = []
        seen = 0          # 实际翻过的消息条数（含被 media_only 过滤掉的），用来判断还有没有下一页
        last_id = 0
        # 落库用的是**全部**消息（含纯文本）：「消息」视图要能离线看聊天内容，
        # 不能因为当前在「媒体」视图就只存媒体。
        fetched: list[dict] = []
        last_gid = None
        extra_album_guard = 0
        scan_cap = (limit + 12) if (limit and limit > 0) else None

        # 迭代拉取消息。裸的 iter_messages 在网络半开时会无限挂起、阻塞整个事件循环，
        # 导致连本应毫秒级的本地库读取也排队卡住（前端表现为「正在读取…」一直转圈）。
        # 这里用 wait_for 套一个 20s 超时（远小于外层 loop.call 的 25s 硬超时），
        # 超时抛 TimeoutError 交给 _retry_disconnected 重连重试，绝不无限阻塞。
        async def _iter():
            nonlocal seen, last_id, last_gid, extra_album_guard
            async for m in client.iter_messages(entity, limit=scan_cap, offset_id=offset_id or 0):
                if limit and limit > 0 and seen >= limit:
                    # 若分页边界恰好切在一个相册（grouped_id）中间，顺延把同相册剩余几条一并读完，避免相册气泡被劈成两半
                    mgid = getattr(m, "grouped_id", None)
                    if not last_gid or mgid != last_gid or extra_album_guard >= 12:
                        break
                    extra_album_guard += 1
                seen += 1
                last_id = m.id
                last_gid = getattr(m, "grouped_id", None)
                row = message_row(m)
                row["local"] = bool(_rt().INDEX.get(str(row.get("chat_id") or peer), m.id))
                fetched.append(row)
                if media_only and not row.get("has_media"):
                    continue
                rows.append(row)

        await asyncio.wait_for(_iter(), timeout=20.0)
        # 顺手存一份到本地库，并与云端窗口比对：云端已删除的消息自动在本地标为 deleted=1
        chat_id = str((fetched[0].get("chat_id") if fetched else getattr(entity, "id", None)) or peer)
        try:
            chat_id = str(utils.get_peer_id(entity))
        except Exception:
            pass
        gone_ids: list[int] = []
        try:
            if fetched:
                _rt().DB.upsert_messages(chat_id, fetched)
                _rt().DB.touch_sync(chat_id, [int(r["id"]) for r in fetched])
            reached_end = (seen < limit) if limit > 0 else True
            gone_ids = _rt().DB.reconcile_window(
                chat_id,
                [int(r["id"]) for r in fetched],
                offset_id=int(offset_id or 0),
                reached_end=reached_end,
                media_only=False,
            )
            if gone_ids:
                self.emit("log", {
                    "level": "info", "category": "sync", "action": "message.reconcile",
                    "peer": chat_id,
                    "msg": f"云端对比同步：发现 {len(gone_ids)} 条消息已在 Telegram 云端删除，已同步从本地缓存移除",
                })
        except Exception:
            pass
        # 同一条相册里的文件共用同一个日期，给它们排上 `_2`、`_3` 的号，
        # 免得界面上 8 个文件的「保存为」全写着同一个名字
        assign_save_names(rows)
        return {
            "rows": rows,
            "next_offset": last_id,
            "has_more": seen >= limit > 0,
            "scanned": seen,
            "removed_ids": gone_ids,
            "source": "remote",
        }

    # -- 缩略图 ------------------------------------------------------------
    def _thumb_from_disk(self, peer: str, msg_id: int) -> tuple[bytes, Path]:
        cache_dir = _rt().HOME / "thumbs"
        cand_peers = [str(peer).strip()]
        try:
            resolved = _rt().DB.resolve_chat_id(peer)
            if resolved and resolved not in cand_peers:
                cand_peers.append(resolved)
        except Exception:
            pass
        for cp in cand_peers:
            cf = cache_dir / f"{hashlib.sha1(cp.encode()).hexdigest()[:12]}_{msg_id}.jpg"
            try:
                if cf.is_file() and cf.stat().st_size > 0:
                    return cf.read_bytes(), cf
            except OSError:
                pass
        cache_file = cache_dir / f"{hashlib.sha1(cand_peers[0].encode()).hexdigest()[:12]}_{msg_id}.jpg"
        for cp in cand_peers:
            loc = _rt().INDEX.get(cp, msg_id)
            if loc is not None and loc.suffix.lower() in (".jpg", ".jpeg", ".png", ".webp", ".gif"):
                try:
                    if loc.stat().st_size <= 8 * 1024 * 1024:
                        return loc.read_bytes(), cache_file
                except OSError:
                    pass
        return b"", cache_file

    def thumb(self, peer: str, msg_id: int) -> bytes:
        """取一条消息的内嵌缩略图：本地已缓存时直接在 HTTP 线程 0ms 返回，绝不排队进 asyncio 事件循环。"""
        hit, _ = self._thumb_from_disk(peer, msg_id)
        if hit:
            return hit
        try:
            return self.loop.call(self._retry_disconnected(lambda: self._thumb(peer, msg_id)), timeout=12)
        except Exception:
            return b""

    async def _thumb(self, peer: str, msg_id: int) -> bytes:
        import io

        hit, cache_file = self._thumb_from_disk(peer, msg_id)
        if hit:
            return hit
        try:
            client = await asyncio.wait_for(self._ensure(), timeout=8.0)
            ref, _ = parse_link(peer)
            entity = await asyncio.wait_for(
                client.get_entity(int(ref) if re.fullmatch(r"-?\d+", ref) else ref),
                timeout=8.0,
            )
            m = await asyncio.wait_for(client.get_messages(entity, ids=msg_id), timeout=8.0)
        except Exception:
            return b""
        if m is None or not (m.media or m.document):
            return b""

        buf = io.BytesIO()
        try:
            await asyncio.wait_for(client.download_media(m, file=buf, thumb=-1), timeout=10.0)
            self._last_rx_ts = time.time()
        except Exception:
            return b""
        data = buf.getvalue()
        if not data:
            return b""
        try:
            cache_file.parent.mkdir(parents=True, exist_ok=True)
            cache_file.write_bytes(data)
        except Exception:
            pass
        return data

    # -- 对话头像 ----------------------------------------------------------
    def avatar(self, peer: str) -> bytes:
        """群 / 频道 / 用户的头像图片：命中 24h 本地缓存时直接在 HTTP 线程 0ms 返回，不占事件循环。"""
        cache_dir = _rt().HOME / "avatars"
        key = hashlib.sha1(str(peer).encode()).hexdigest()[:12]
        if cache_dir.is_dir():
            existing = sorted(cache_dir.glob(f"{key}_*.jpg"), key=lambda p: p.stat().st_mtime, reverse=True)
            if existing:
                try:
                    st = existing[0].stat()
                    if st.st_size > 0 and (time.time() - st.st_mtime) < 86400:
                        return existing[0].read_bytes()
                except OSError:
                    pass
        try:
            return self.loop.call(self._retry_disconnected(lambda: self._avatar(peer)), timeout=12)
        except Exception:
            return b""

    async def _avatar(self, peer: str) -> bytes:
        """下载对话头像，按「peer 哈希 + photo_id」落盘缓存。

        先查本地缓存：24 小时内命中直接返回，不发网络请求；离线或请求失败时
        也退回本地已有缓存，保证断网时群/频道头像依然正常显示。
        """
        cache_dir = _rt().HOME / "avatars"
        key = hashlib.sha1(str(peer).encode()).hexdigest()[:12]
        existing = sorted(cache_dir.glob(f"{key}_*.jpg"), key=lambda p: p.stat().st_mtime, reverse=True) if cache_dir.is_dir() else []
        if existing:
            try:
                st = existing[0].stat()
                if st.st_size > 0 and (time.time() - st.st_mtime) < 86400:
                    return existing[0].read_bytes()
            except OSError:
                pass

        try:
            client = await self._ensure()
            ref, _ = parse_link(peer)
            entity = await client.get_entity(int(ref) if re.fullmatch(r"-?\d+", ref) else ref)
        except Exception:
            if existing:
                try:
                    return existing[0].read_bytes()
                except OSError:
                    pass
            return b""

        pid = getattr(getattr(entity, "photo", None), "photo_id", None)
        if not pid:  # 没设置头像（或者被隐私设置挡住）
            return b""

        cache_file = cache_dir / f"{key}_{pid}.jpg"
        try:
            if cache_file.is_file() and cache_file.stat().st_size > 0:
                return cache_file.read_bytes()
        except OSError:
            pass

        try:
            data = await client.download_profile_photo(entity, file=bytes, download_big=False)
        except Exception:
            return b""
        if not data:
            return b""

        try:
            cache_dir.mkdir(parents=True, exist_ok=True)
            cache_file.write_bytes(data)
            for old in cache_dir.glob(f"{key}_*.jpg"):
                if old != cache_file:
                    try:
                        old.unlink()
                    except OSError:
                        pass
        except OSError:
            pass
        return data

    # -- 在线播放与原图缓存 ------------------------------------------------
    async def _get_cached_msg(self, peer: str, msg_id: int, force_refresh: bool = False):
        """获取并缓存 (Message, info)，避免播放器每次拖动进度条（Range 请求）都重复请求 get_entity + get_messages。"""
        key = (str(peer), int(msg_id))
        now = time.time()
        if not force_refresh and key in self._msg_cache:
            ts, m, info = self._msg_cache[key]
            if now - ts < 900 and m is not None:
                return m, info
        client = await self._ensure()
        ent = await self._entity(client, peer)
        m = await client.get_messages(ent, ids=int(msg_id))
        if m is None:
            raise ValueError("消息不存在或已删除")
        info = message_info(m)
        if not info:
            raise ValueError("这条消息没有可播放的媒体")
        if len(self._msg_cache) >= 500:
            self._msg_cache.clear()
        self._msg_cache[key] = (now, m, info)
        self._meta_cache[key] = info
        return m, info

    def media_meta(self, peer: str, msg_id: int) -> dict:
        """一条消息的媒体元信息（大小 / MIME / 文件名），在线播放前先问一次。"""
        return self.loop.call(self._retry_disconnected(lambda: self._media_meta(peer, msg_id)))

    async def _media_meta(self, peer: str, msg_id: int) -> dict:
        key = (str(peer), int(msg_id))
        cached = self._meta_cache.get(key)
        if not cached:
            # 断网时若本地 SQLite 库已有该消息元信息，直接用本地库兜底
            try:
                _, cached = await self._get_cached_msg(peer, msg_id)
            except Exception:
                cid = _rt().DB.resolve_chat_id(peer) or str(peer)
                row = _rt().DB.get_message(cid, int(msg_id))
                if not row or not row.get("has_media"):
                    raise
                cached = row
        return {**cached, "local": bool(_rt().INDEX.get(str(cached.get("chat_id") or peer), msg_id))}

    def media_bytes(self, peer: str, msg_id: int) -> tuple[bytes, str]:
        """把整条图片/贴纸读回并落盘缓存：二次查看和前后翻图零延迟，断网也能看已浏览过的原图。"""
        return self.loop.call(self._retry_disconnected(lambda: self._media_bytes(peer, msg_id)))

    async def _media_bytes(self, peer: str, msg_id: int) -> tuple[bytes, str]:
        import io

        key = (str(peer), int(msg_id))
        if key in self._blob_cache:
            return self._blob_cache[key]

        # 1) 已下载到本地目录的图片，直接读本地文件
        cand_peers = [str(peer).strip()]
        try:
            resolved = _rt().DB.resolve_chat_id(peer)
            if resolved and resolved not in cand_peers:
                cand_peers.append(resolved)
        except Exception:
            pass
        for cp in cand_peers:
            loc = _rt().INDEX.get(cp, int(msg_id))
            if loc is not None and loc.is_file():
                try:
                    data = loc.read_bytes()
                    if data:
                        mime = mimetypes.guess_type(loc.name)[0] or _sniff_mime(data)
                        return data, mime
                except OSError:
                    pass

        # 2) 检查原图磁盘缓存（~/.tgget/photos/）
        photo_dir = _rt().HOME / "photos"
        pkey = hashlib.sha1(cand_peers[-1].encode()).hexdigest()[:12]
        pfile = photo_dir / f"{pkey}_{int(msg_id)}.bin"
        if pfile.is_file() and pfile.stat().st_size > 0:
            try:
                data = pfile.read_bytes()
                mime = _sniff_mime(data)
                if len(self._blob_cache) >= 24:
                    self._blob_cache.pop(next(iter(self._blob_cache)))
                self._blob_cache[key] = (data, mime)
                return data, mime
            except OSError:
                pass

        # 3) 从 Telegram 下载原图并写入内存 + 磁盘缓存
        m, info = await self._get_cached_msg(peer, msg_id)
        buf = io.BytesIO()
        try:
            await self.client.download_media(m, file=buf)
        except FileReferenceExpiredError:
            m, info = await self._get_cached_msg(peer, msg_id, force_refresh=True)
            buf = io.BytesIO()
            await self.client.download_media(m, file=buf)
        data = buf.getvalue()
        if not data:
            raise ValueError("没有读到内容")
        mime = (info or {}).get("mime") or _sniff_mime(data)
        if len(self._blob_cache) >= 24:
            self._blob_cache.pop(next(iter(self._blob_cache)))
        self._blob_cache[key] = (data, mime)
        try:
            photo_dir.mkdir(parents=True, exist_ok=True)
            pfile.write_bytes(data)
            # 控制原图缓存上限（最多保留最近 200 张），不无限制占 NAS 空间
            all_p = sorted(photo_dir.glob("*.bin"), key=lambda p: p.stat().st_mtime, reverse=True)
            for old_p in all_p[200:]:
                try:
                    old_p.unlink()
                except OSError:
                    pass
        except OSError:
            pass
        return data, mime

    def open_stream(self, peer: str, msg_id: int, start: int = 0, length: int = -1,
                    chunk: int = 512 * 1024):
        """按字节区间从 Telegram 拉流。

        复用 `_get_cached_msg` 缓存的消息对象，拖动视频进度条时无需重复查询消息实体，
        直接从目标偏移启动 `iter_download`，并用 32 块预读队列平滑网络抖动。
        """
        q: queue.Queue = queue.Queue(maxsize=32)
        stop = threading.Event()

        async def producer() -> None:
            try:
                client = await self._ensure()
                m, info = await self._get_cached_msg(peer, msg_id)
                size = int((info or {}).get("size") or 0)
                begin = max(0, int(start))
                if size:
                    begin = min(begin, size)
                end = size if (length is None or length < 0 or not size) else min(size, begin + int(length))
                remaining = (end - begin) if size else -1
                if size and remaining <= 0:
                    return
                req = 512 * 1024
                refreshed = False
                while True:
                    # MTProto 规定 getFile(limit=512KB) 的 offset 必须对齐 512KB 边界（不可跨越 1MB 边界），
                    # 否则浏览器拖动进度条发起非对齐 Range 请求时会触发 400 OFFSET_INVALID 导致一直转圈。
                    aligned_begin = (begin // req) * req
                    skip_first = begin - aligned_begin
                    n_chunks = math.ceil((remaining + skip_first) / req) + 1 if remaining > 0 else None
                    try:
                        async for piece in client.iter_download(
                            m, offset=aligned_begin, limit=n_chunks, request_size=req
                        ):
                            if stop.is_set():
                                return
                            if skip_first > 0:
                                if len(piece) <= skip_first:
                                    skip_first -= len(piece)
                                    continue
                                piece = piece[skip_first:]
                                skip_first = 0
                            if remaining > 0:
                                piece = piece[:remaining]
                                remaining -= len(piece)
                            if piece:
                                begin += len(piece)
                                while not stop.is_set():
                                    try:
                                        q.put(piece, timeout=0.5)
                                        break
                                    except queue.Full:
                                        continue
                            if remaining == 0:
                                return
                        return
                    except asyncio.CancelledError:
                        async with lock:
                            state["inflight"].pop(wid, None)
                        return
                    except FileReferenceExpiredError:
                        if not refreshed:
                            refreshed = True
                            m, info = await self._get_cached_msg(peer, msg_id, force_refresh=True)
                            continue
                        raise
                    except Exception as exc:
                        wait_s = getattr(exc, "seconds", None)
                        if wait_s and 0 < int(wait_s) <= 30 and not stop.is_set():
                            await asyncio.sleep(int(wait_s) + 1)
                            continue
                        raise
            except Exception as exc:  # 交给消费端抛出
                try:
                    q.put(exc, timeout=5)
                except queue.Full:
                    pass
            finally:
                try:
                    q.put(None, timeout=5)
                except queue.Full:
                    pass

        asyncio.run_coroutine_threadsafe(producer(), self.loop.loop)

        def gen():
            while True:
                item = q.get()
                if item is None:
                    return
                if isinstance(item, Exception):
                    raise item
                yield item

        return gen(), stop

