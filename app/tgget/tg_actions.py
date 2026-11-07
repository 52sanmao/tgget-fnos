"""Telegram 消息动作模块：「我的收藏」一键转存归档、普通转发、发送文件、编辑、云端同步删除与导出。"""

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
    NameAllocator,
    _build_docx,
    entity_title,
    human_size,
    message_info,
    message_row,
    reuse_local,
    safe_name,
)


class ActionsMixin:
    # -- 转发 / 发送 / 编辑 -------------------------------------------------
    def transfer_saved(self, peer: str, ids: list[int], target: str,
                       on_step: Optional[Callable] = None,
                       on_progress: Optional[Callable] = None,
                       check_cancel: Optional[Callable[[], bool]] = None) -> dict:
        """把「我的收藏」里的一条消息（或整组相册）以「下载+重新上传」方式原样式发到目标群，
        随后将本地文件按方案 C（源消息原始日期_目标群新消息ID.ext）移动到目标群文件夹、
        更新数据库索引到新消息 id，最后删除我的收藏里的原消息。
        """
        return self.loop.call(
            self._retry_disconnected(lambda: self._transfer_saved(peer, ids, target, on_step, on_progress, check_cancel)),
            timeout=1800,
        )

    async def _transfer_saved(self, peer: str, ids: list[int], target: str,
                              on_step: Optional[Callable] = None,
                              on_progress: Optional[Callable] = None,
                              check_cancel: Optional[Callable[[], bool]] = None) -> dict:
        def _is_cancelled() -> bool:
            return bool(check_cancel and check_cancel())

        def _step(key: str, label: str, status: str = "done", detail: str = "") -> None:
            if on_step:
                try:
                    on_step(key, label, status, detail)
                except Exception:
                    pass

        clean_ids = sorted({int(i) for i in ids if int(i or 0) > 0})
        if not clean_ids:
            raise ValueError("没有选中要转发的消息")
        if not str(target or "").strip():
            raise ValueError("请先指定目标群组 ID")

        client = await self._call_rpc(self._ensure(), timeout=15.0, check_cancel=_is_cancelled)
        src_ent = await self._call_rpc(self._entity(client, peer), timeout=15.0, check_cancel=_is_cancelled)
        dst_ent = await self._call_rpc(self._entity(client, target), timeout=15.0, check_cancel=_is_cancelled)
        src_chat_id = str(utils.get_peer_id(src_ent))
        dst_chat_id = str(utils.get_peer_id(dst_ent))
        src_title = entity_title(src_ent) or src_chat_id
        dst_title = entity_title(dst_ent) or dst_chat_id

        fetched = await self._call_rpc(
            client.get_messages(src_ent, ids=clean_ids), timeout=15.0, check_cancel=_is_cancelled
        )
        if not isinstance(fetched, list):
            fetched = [fetched]
        orig_msgs = sorted([m for m in fetched if m is not None], key=lambda x: int(x.id))
        if not orig_msgs:
            raise ValueError("源消息不存在或已被删除")

        # 若选中的单条消息属于某个相册（grouped_id），自动检查前后相邻消息，避免分页边界把相册切漏
        gids = {getattr(m, "grouped_id", None) for m in orig_msgs if getattr(m, "grouped_id", None)}
        if gids and len(orig_msgs) == 1:
            known_ids = {int(m.id) for m in orig_msgs}
            lo_id, hi_id = min(known_ids), max(known_ids)
            probe_ids = [x for x in range(max(1, lo_id - 9), hi_id + 10) if x not in known_ids]
            if probe_ids:
                try:
                    near = await self._call_rpc(
                        client.get_messages(src_ent, ids=probe_ids), timeout=8.0, check_cancel=_is_cancelled
                    )
                    if not isinstance(near, list):
                        near = [near]
                    for nm in (near or []):
                        if nm is not None and getattr(nm, "grouped_id", None) in gids:
                            orig_msgs.append(nm)
                    orig_msgs.sort(key=lambda x: int(x.id))
                except Exception:
                    pass

        if _is_cancelled():
            return {"cancelled": True}

        _step("check", "校验源消息", "done", f"已读取 {len(orig_msgs)} 条源消息，目标群：{dst_title} ({dst_chat_id})")

        # 第 1 步：确保所有含媒体的消息都已经落盘到本地（使用方案 C `源日期_源消息ID.ext` 独立命名，绝不互相覆盖）
        outdir = self.cfg.download_path
        src_folder = outdir / safe_name(src_title, "unknown")
        src_folder.mkdir(parents=True, exist_ok=True)
        src_alloc = NameAllocator()
        src_files: dict[int, Path] = {}
        orig_infos: dict[int, dict] = {}
        reused_local_cnt = 0
        downloaded_cnt = 0
        need_dl: list[tuple[Any, dict, str]] = []

        for m in orig_msgs:
            info = message_info(m)
            if not info:
                continue
            info["chat_id"] = int(src_chat_id) if src_chat_id.lstrip("-").isdigit() else src_chat_id
            info["chat_title"] = src_title
            orig_infos[m.id] = info
            loc = _rt().INDEX.get(src_chat_id, m.id) or _rt().INDEX.get(str(peer), m.id)
            if loc is None and self.cfg.dedupe_content and info.get("file_id"):
                loc = _rt().INDEX.find_fid(info.get("file_id") or "")
            if loc is not None and loc.is_file() and loc.stat().st_size > 0:
                reused_local_cnt += 1
                src_files[m.id] = loc
            else:
                pre_name = src_alloc.alloc(src_folder, info) if self.cfg.rename_by_date else None
                need_dl.append((m, info, pre_name or ""))

        if reused_local_cnt > 0:
            _step("local", "本地已存在（跳过下载）", "done", f"直接复用 {reused_local_cnt} 个已下载本地文件")

        if need_dl:
            total_dl_bytes = sum(int(inf.get("size") or 0) for _, inf, _ in need_dl)
            dl_prog_map: dict[int, int] = {int(m.id): 0 for m, _, _ in need_dl}
            dl_done_files = [reused_local_cnt]
            dl_t0 = time.time()
            dl_sem = asyncio.Semaphore(max(2, int(getattr(self.cfg, "concurrency", 5) or 5)))
            _step("download", "正在下载源文件", "running",
                  f"并行下载 {len(need_dl)} 个源文件中…")

            def _make_prog(mid_key: int):
                def _cb(inf: dict, d: int, t: int) -> None:
                    dl_prog_map[mid_key] = int(d or 0)
                    if on_progress:
                        cur_sum = sum(dl_prog_map.values())
                        tot_sum = max(total_dl_bytes, cur_sum, int(t or 0))
                        el = max(0.1, time.time() - dl_t0)
                        try:
                            on_progress("download", cur_sum, tot_sum, inf.get("filename") or f"#{mid_key}",
                                        cur_sum / el, dl_done_files[0])
                        except TypeError:
                            on_progress("download", cur_sum, tot_sum, inf.get("filename") or f"#{mid_key}", cur_sum / el)
                        except Exception:
                            pass
                return _cb

            async def _dl_one_tr(m_obj, inf_obj, fname_hint: str):
                res = await self.download_message(
                    m_obj, outdir, dl_sem, _is_cancelled,
                    peer=src_chat_id, info=inf_obj,
                    name=fname_hint or None,
                    progress=_make_prog(int(m_obj.id)),
                )
                if res.get("cancelled") or _is_cancelled():
                    raise asyncio.CancelledError("用户取消了转存任务")
                if not res.get("ok") or not res.get("path"):
                    raise RuntimeError(f"下载源媒体 #{m_obj.id} 失败：{res.get('error') or '未知错误'}")
                p_out = Path(str(res["path"]))
                if not p_out.is_file() or p_out.stat().st_size <= 0:
                    raise RuntimeError(f"源媒体 #{m_obj.id} 落盘校验失败（文件缺失或大小为 0）")
                dl_done_files[0] += 1
                return int(m_obj.id), p_out

            try:
                pairs = await asyncio.gather(*(_dl_one_tr(m_o, i_o, fn_o) for m_o, i_o, fn_o in need_dl))
            except asyncio.CancelledError:
                # 只有用户主动点停止（_is_cancelled() 为真）才返回 cancelled。
                # 网络断连可能让 Telethon 抛 CancelledError，但此时 _is_cancelled() 为假，
                # 应归为失败让上层重试，而不是误判成「用户取消」。
                if _is_cancelled():
                    return {"cancelled": True}
                raise
            except Exception as exc:
                _step("download", "下载源文件失败", "error", str(exc))
                raise
            for mid_k, loc_p in pairs:
                src_files[mid_k] = loc_p
                downloaded_cnt += 1
            _step("download", "源文件下载成功", "done",
                  f"已完成 {downloaded_cnt} 个源媒体下载（{[src_files[m.id].name for m, _, _ in need_dl][:3]}）")

        if _is_cancelled():
            return {"cancelled": True}

        if not orig_infos:
            _step("local", "纯文本消息（无需下载媒体）", "done", "直接进入原样式发送")

        # 第 2 步：以「重新上传」方式发送到目标群（保留原始文字与格式实体 entities，彻底脱离原频道引用）
        media_msgs = [m for m in orig_msgs if m.id in src_files]
        text_msgs = [m for m in orig_msgs if m.id not in src_files]
        sent_media_msgs = []
        sent_text_msgs = []

        up_start = time.time()
        last_up_emit = [0.0]

        def _up_prog(cur: int, tot: int) -> None:
            if _is_cancelled():
                raise asyncio.CancelledError("用户取消了转存上传")
            self._last_rx_ts = time.time()
            now = time.time()
            if now - last_up_emit[0] < 0.25 and cur < tot:
                return
            last_up_emit[0] = now
            el = max(0.1, now - up_start)
            sp = cur / el
            self.emit("upload", {
                "state": "running", "done": cur, "total": tot, "speed": sp,
            })
            if on_progress:
                try:
                    on_progress("upload", cur, tot, f"正在上传至 {dst_title}", sp)
                except Exception:
                    pass

        _step("upload", f"正在上传至「{dst_title}」", "running", "保持原文字与格式样式上传中…")
        try:
            if len(media_msgs) == 1:
                m0 = media_msgs[0]
                p0 = src_files[m0.id]
                sent = await client.send_file(
                    dst_ent,
                    str(p0),
                    caption=m0.message or "",
                    formatting_entities=getattr(m0, "entities", None),
                    supports_streaming=True,
                    progress_callback=_up_prog,
                )
                sent_media_msgs = sent if isinstance(sent, list) else [sent]
            elif len(media_msgs) > 1:
                paths = [str(src_files[m.id]) for m in media_msgs]
                captions = [m.message or "" for m in media_msgs]
                non_empty_msgs = [m for m in media_msgs if (m.message or "").strip()]
                cap_msg = non_empty_msgs[0] if non_empty_msgs else media_msgs[0]
                ents = getattr(cap_msg, "entities", None)
                # Telethon 的 _send_album 会把同一个 formatting_entities 赋给列表里的每一项；
                # 若说明文字原本在相册的第 2~N 项，移到第 1 项可避免第 1 项空文本带 entities 触发 ENTITY_BOUNDS_INVALID
                if len(non_empty_msgs) == 1 and not (captions[0] or "").strip():
                    captions = [cap_msg.message or ""] + ["" for _ in media_msgs[1:]]
                sent = await client.send_file(
                    dst_ent,
                    paths,
                    caption=captions,
                    formatting_entities=ents,
                    supports_streaming=True,
                    progress_callback=_up_prog,
                )
                sent_media_msgs = sent if isinstance(sent, list) else [sent]

            for tm in text_msgs:
                if (tm.message or "").strip():
                    sm = await client.send_message(
                        dst_ent,
                        tm.message,
                        formatting_entities=getattr(tm, "entities", None),
                        link_preview=False,
                    )
                    if sm is not None:
                        sent_text_msgs.append(sm)
        except Exception as exc:
            _step("upload", "上传失败", "error", f"{type(exc).__name__}: {exc}")
            raise
        finally:
            self.emit("upload", {"state": "done", "speed": 0.0})

        new_ids_preview = [int(getattr(m, "id", 0)) for m in (sent_media_msgs + sent_text_msgs) if m is not None]
        _step("upload", "上传成功", "done", f"已发送至「{dst_title}」，新消息 ID：{new_ids_preview}")

        # 第 3 步：按方案 C（`源消息原始日期_目标群新消息ID.ext`，如 `2026-08-30_6787.mp4`）
        #         将本地文件迁移/链接至目标群目录，并做同源引用计数 + 落盘非空硬校验！
        dst_folder = outdir / safe_name(dst_title, "unknown")
        dst_folder.mkdir(parents=True, exist_ok=True)
        alloc = NameAllocator()
        moved_files = []
        new_db_rows = []

        # 统计本次转存批次中每个物理源路径被引用了几次：
        # 如果两条消息指向同一个物理文件（例如相册里前两条是同一份 64MB 视频），
        # 前面的消息必须走硬链接/复制（reuse_local），只有最后一条才允许走 shutil.move！
        pairs_to_move = [(old_m, new_m) for old_m, new_m in zip(media_msgs, sent_media_msgs) if new_m is not None]
        src_ref_remaining: dict[str, int] = {}
        for old_m, _ in pairs_to_move:
            sp_key = str(src_files[old_m.id].resolve())
            src_ref_remaining[sp_key] = src_ref_remaining.get(sp_key, 0) + 1

        old_id_set = {int(m.id) for m in orig_msgs}

        for old_m, new_m in pairs_to_move:
            src_path = src_files[old_m.id]
            sp_key = str(src_path.resolve())
            src_ref_remaining[sp_key] = max(0, src_ref_remaining.get(sp_key, 1) - 1)

            orig_info = dict(orig_infos.get(old_m.id) or {})
            new_info = message_info(new_m) or dict(orig_info)
            # 方案 C 核心：保留源消息的原始发布日期 + 绑定目标群的新消息 ID（如 `2026-08-30_6787.mp4`）
            new_info["date"] = orig_info.get("date") or new_info.get("date")
            new_info["id"] = int(new_m.id)
            new_info["chat_id"] = int(dst_chat_id) if dst_chat_id.lstrip("-").isdigit() else dst_chat_id
            new_info["chat_title"] = dst_title

            if self.cfg.rename_by_date:
                fname = alloc.alloc(dst_folder, new_info)
                dest_path = dst_folder / safe_name(fname, f"{new_m.id}")
                if dest_path.exists() and src_path.resolve() != dest_path.resolve():
                    dest_path = self._dedupe(dest_path)
            else:
                dest_path = self._dedupe(dst_folder / safe_name(new_info.get("filename") or src_path.name, f"{new_m.id}"))

            if src_path.resolve() != dest_path.resolve():
                still_needed_in_batch = src_ref_remaining.get(sp_key, 0) > 0
                other_ref = _rt().INDEX.find_fid(orig_info.get("file_id") or "", exclude=f"{src_chat_id}|{old_m.id}")
                still_needed_elsewhere = (
                    other_ref is not None
                    and other_ref.is_file()
                    and other_ref.resolve() == src_path.resolve()
                )
                if still_needed_in_batch or still_needed_elsewhere:
                    reuse_local(src_path, dest_path)
                else:
                    try:
                        shutil.move(str(src_path), str(dest_path))
                    except OSError:
                        reuse_local(src_path, dest_path)

            # 落盘存在性与非空硬校验：若目标文件未生成，立即抛错中止，绝不写幽灵记录、也绝不删除云端收藏原消息！
            if not dest_path.is_file() or dest_path.stat().st_size <= 0:
                raise RuntimeError(f"目标文件落盘校验失败：{dest_path} 不存在或大小为 0，已中止清理原收藏以保护数据")

            fsize = dest_path.stat().st_size
            new_fid = str(new_info.get("file_id") or orig_info.get("file_id") or "")

            # 写入目标群的新文件索引，并移除「我的收藏」里的旧文件索引记录
            _rt().INDEX.put(dst_chat_id, new_m.id, dest_path, fsize, new_fid)
            for k in {src_chat_id, str(peer).strip()}:
                try:
                    _rt().DB.delete_file(k, old_m.id)
                except Exception:
                    pass

            # 顺手把缩略图缓存也迁移过去，切到目标群时缩略图直接秒出
            thumb_dir = _rt().HOME / "thumbs"
            src_tag = hashlib.sha1(src_chat_id.encode()).hexdigest()[:12]
            dst_tag = hashlib.sha1(dst_chat_id.encode()).hexdigest()[:12]
            old_thumb = thumb_dir / f"{src_tag}_{old_m.id}.jpg"
            new_thumb = thumb_dir / f"{dst_tag}_{new_m.id}.jpg"
            if old_thumb.is_file() and not new_thumb.exists():
                try:
                    shutil.move(str(old_thumb), str(new_thumb))
                except OSError:
                    pass

            moved_files.append(str(dest_path))
            row = message_row(new_m)
            if row:
                row["chat_id"] = dst_chat_id
                row["local"] = True
                new_db_rows.append(row)

        for sm in sent_text_msgs:
            row = message_row(sm)
            if row:
                row["chat_id"] = dst_chat_id
                new_db_rows.append(row)

        if new_db_rows:
            try:
                _rt().DB.upsert_messages(dst_chat_id, new_db_rows)
                _rt().DB.touch_sync(dst_chat_id, [int(r["id"]) for r in new_db_rows])
            except Exception:
                pass

        if moved_files:
            _step("move", "本地文件已迁移并重绑数据库", "done",
                  f"已移动 {len(moved_files)} 个文件至「{dst_title}」目录并绑定新消息 ID")

        # 第 4 步：确认上传与本地迁移全部成功后，删除「我的收藏」里的云端消息与本地消息表记录
        _step("cleanup", "正在清理「我的收藏」源消息", "running", f"删除消息 ID：{ [int(m.id) for m in orig_msgs] }")
        old_msg_ids = [int(m.id) for m in orig_msgs]
        await client.delete_messages(src_ent, old_msg_ids, revoke=True)
        for k in {src_chat_id, str(peer).strip()}:
            try:
                _rt().DB.mark_deleted(k, old_msg_ids)
            except Exception:
                pass
            for mid in old_msg_ids:
                self._meta_cache.pop((k, mid), None)
                self._blob_cache.pop((k, mid), None)
                getattr(self, "_msg_cache", {}).pop((k, mid), None)

        _step("cleanup", "已清理「我的收藏」云端与本地原消息", "done", f"共清理 {len(old_msg_ids)} 条原消息")
        return {
            "transferred": len(sent_media_msgs) + len(sent_text_msgs),
            "deleted_saved": len(old_msg_ids),
            "moved_files": len(moved_files),
            "moved_paths": moved_files,
            "reused_local": reused_local_cnt,
            "downloaded": downloaded_cnt,
            "target_peer": dst_chat_id,
            "target_title": dst_title,
            "new_ids": [int(getattr(m, "id", 0)) for m in (sent_media_msgs + sent_text_msgs) if m is not None],
        }

    def forward(self, peer: str, ids: list[int], target: str, drop_author: bool = False) -> dict:
        """把源对话的若干条消息转发到目标对话。"""
        return self.loop.call(self._retry_disconnected(lambda: self._forward(peer, ids, target, drop_author)))

    async def _forward(self, peer: str, ids: list[int], target: str, drop_author: bool) -> dict:
        client = await self._ensure()
        src = await self._entity(client, peer)
        dst = await self._entity(client, target)
        ok, failed = [], []
        # 分批转发；若源频道开启了「禁止复制和转发内容」(ChatForwardsRestrictedError)，自动降级为下载再原样重传
        for i in range(0, len(ids), 100):
            batch = ids[i:i + 100]
            try:
                res = await client.forward_messages(dst, batch, src, drop_author=drop_author)
                got = res if isinstance(res, list) else [res]
                ok.extend(m.id for m in got if m is not None)
            except Exception as exc:
                ename = f"{type(exc).__name__}: {exc}".lower()
                if "restricted" in ename or "protected" in ename or "noforwards" in ename or "forward" in ename:
                    try:
                        fetched = await client.get_messages(src, ids=batch)
                        if not isinstance(fetched, list):
                            fetched = [fetched]
                        for m in [x for x in (fetched or []) if x is not None]:
                            info = message_info(m)
                            if info:
                                dl_res = await self.download_message(
                                    m, self.cfg.download_path, asyncio.Semaphore(4), lambda: False,
                                    peer=str(utils.get_peer_id(src)), info=info,
                                )
                                if dl_res.get("ok") and dl_res.get("path"):
                                    sm = await client.send_file(
                                        dst, str(dl_res["path"]),
                                        caption=m.message or "",
                                        formatting_entities=getattr(m, "entities", None),
                                        supports_streaming=True,
                                    )
                                    ok.append(getattr(sm, "id", m.id))
                                else:
                                    failed.append({"id": m.id, "error": dl_res.get("error") or "下载受限媒体失败"})
                            elif (m.message or "").strip():
                                sm = await client.send_message(
                                    dst, m.message,
                                    formatting_entities=getattr(m, "entities", None),
                                )
                                ok.append(getattr(sm, "id", m.id))
                        continue
                    except Exception as exc2:
                        failed.extend({"id": x, "error": f"{type(exc2).__name__}: {exc2}"} for x in batch)
                        continue
                failed.extend({"id": x, "error": f"{type(exc).__name__}: {exc}"} for x in batch)
        return {
            "forwarded": len(ok),
            "failed": len(failed),
            "target": entity_title(dst),
            "errors": failed[:10],
        }

    def send_files(self, peer: str, paths: list[str], caption: str = "") -> dict:
        """把本机文件发到对话。路径必须在下载目录内。"""
        return self.loop.call(self._retry_disconnected(lambda: self._send_files(peer, paths, caption)))

    async def _send_files(self, peer: str, paths: list[str], caption: str) -> dict:
        root = self.cfg.download_path.resolve()
        client = await self._ensure()
        dst = await self._entity(client, peer)
        sent, failed = 0, []
        for raw in paths:
            p = Path(raw)
            if not p.is_absolute():
                p = root / p
            try:
                p = p.resolve()
                p.relative_to(root)
            except (ValueError, OSError):
                failed.append({"path": raw, "error": "不在下载目录内"})
                continue
            if not p.is_file():
                failed.append({"path": raw, "error": "文件不存在"})
                continue
            try:
                fsize = p.stat().st_size
                up_start = time.time()
                last_up_emit = [0.0]

                def _up_prog(cur: int, tot: int) -> None:
                    now = time.time()
                    if now - last_up_emit[0] < 0.25 and cur < tot:
                        return
                    last_up_emit[0] = now
                    el = max(0.1, now - up_start)
                    self.emit("upload", {
                        "state": "running", "name": p.name,
                        "done": cur, "total": tot or fsize, "speed": cur / el,
                    })

                await client.send_file(dst, str(p), caption=caption or None, progress_callback=_up_prog)
                self.emit("upload", {"state": "done", "name": p.name, "done": fsize, "total": fsize, "speed": 0.0})
                sent += 1
            except Exception as exc:
                self.emit("upload", {"state": "done", "speed": 0.0})
                failed.append({"path": raw, "error": f"{type(exc).__name__}: {exc}"})
        return {"sent": sent, "failed": len(failed), "errors": failed[:10]}

    def edit_message(self, peer: str, msg_id: int, text: str) -> dict:
        return self.loop.call(self._retry_disconnected(lambda: self._edit_message(peer, msg_id, text)))

    async def _edit_message(self, peer: str, msg_id: int, text: str) -> dict:
        client = await self._ensure()
        ent = await self._entity(client, peer)
        new = await client.edit_message(ent, msg_id, text)
        return {"id": getattr(new, "id", msg_id), "text": getattr(new, "message", text)}

    # -- 删除 --------------------------------------------------------------
    def delete_messages(self, peer: str, ids: list[int], cloud: bool = True) -> dict:
        """删除消息：云端（对所有设备生效）+ 本地已下载文件。

        `cloud=False` 只删本地文件、保留 Telegram 上的消息。
        """
        return self.loop.call(self._retry_disconnected(lambda: self._delete_messages(peer, ids, cloud)))

    async def _delete_messages(self, peer: str, ids: list[int], cloud: bool) -> dict:
        ids = [int(i) for i in ids if int(i or 0) > 0]
        if not ids:
            raise ValueError("没有要删除的消息")

        deleted, failed = 0, []
        ent = None
        if cloud:
            client = await self._ensure()
            ent = await self._entity(client, peer)
            for i in range(0, len(ids), 100):
                batch = ids[i:i + 100]
                try:
                    await client.delete_messages(ent, batch, revoke=True)
                    deleted += len(batch)
                except Exception as exc:
                    failed.extend({"id": x, "error": f"{type(exc).__name__}: {exc}"} for x in batch)

        # 归一 chat_id：用户可能传 @username 或链接，而下载索引和本地库按数字 chat_id 存
        keys = {str(peer).strip()}
        if ent is not None:
            try:
                keys.add(str(utils.get_peer_id(ent)))
            except Exception:
                pass
        try:
            resolved = _rt().DB.resolve_chat_id(peer)
            if resolved:
                keys.add(str(resolved))
        except Exception:
            pass
        keys.discard("")

        # 本地文件、本地 SQLite 消息表、缩略图缓存一起清，防止刷新或离线时“诈尸”
        local_set: set[str] = set()
        for k in keys:
            local_set.update(_rt().INDEX.drop_many(k, ids))
            try:
                _rt().DB.mark_deleted(k, ids)
            except Exception:
                pass
        local = sorted(local_set)
        thumb_dir = _rt().HOME / "thumbs"
        tags = {hashlib.sha1(k.encode()).hexdigest()[:12] for k in keys}
        for mid in ids:
            for tag in tags:
                try:
                    f = thumb_dir / f"{tag}_{mid}.jpg"
                    if f.is_file():
                        f.unlink()
                except OSError:
                    pass
            for k in keys:
                self._meta_cache.pop((k, mid), None)
                self._blob_cache.pop((k, mid), None)
                getattr(self, "_msg_cache", {}).pop((k, mid), None)

        return {
            "deleted": deleted,
            "failed": len(failed),
            "local_files": len(local),
            "errors": failed[:5],
        }

    # -- 导出 --------------------------------------------------------------
    def export_messages(self, peer: str, limit: int = 2000, fmt: str = "txt",
                        outdir: str = "") -> dict:
        """把聊天记录导出成本机文件（txt / docx）。"""
        return self.loop.call(self._retry_disconnected(lambda: self._export_messages(peer, limit, fmt, outdir)))

    async def _export_messages(self, peer: str, limit: int, fmt: str, outdir: str) -> dict:
        client = await self._ensure()
        ent = await self._entity(client, peer)
        title = entity_title(ent) or "chat"
        lines: list[str] = [title, ""]
        count = 0

        async def _iter():
            nonlocal count
            async for m in client.iter_messages(ent, limit=limit):
                count += 1
                who = entity_title(getattr(m, "sender", None)) if getattr(m, "sender", None) else ""
                when = m.date.strftime("%Y-%m-%d %H:%M:%S") if m.date else ""
                body = (m.message or "").replace("\r\n", "\n")
                lines.append(f"[{when}] {who or '—'}")
                if body:
                    lines.append(body)
                info = message_info(m)
                if info:
                    lines.append(f"<附件 {info['filename']} · {human_size(info['size'])}>")
                lines.append("")

        await asyncio.wait_for(_iter(), timeout=60.0)

        root = Path(outdir).expanduser() if outdir else self.cfg.download_path
        try:
            root = root.resolve()
            root.relative_to(self.cfg.download_path.resolve())
        except (ValueError, OSError):
            root = self.cfg.download_path
        root.mkdir(parents=True, exist_ok=True)
        stem = safe_name(f"{title}_导出", "chat")
        if fmt == "docx":
            target = root / f"{stem}.docx"
            target.write_bytes(_build_docx(title, lines))
        else:
            target = root / f"{stem}.txt"
            target.write_text("\n".join(lines), encoding="utf-8")
        return {"path": str(target), "messages": count, "bytes": target.stat().st_size, "format": fmt}

