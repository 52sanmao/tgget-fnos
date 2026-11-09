"""Telegram 核心下载引擎：单文件下载、串行断点续传、多路分片并发流与 0.2s 取消轮询。"""

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
    _dl_lock,
    _unlink,
    download_name,
    message_info,
    reuse_local,
    safe_name,
)


class DownloaderMixin:
    # -- 下载 --------------------------------------------------------------
    async def download_message(self, message, target_dir: Path, sem: asyncio.Semaphore,
                               check_cancel: Callable[[], bool], peer: str = "",
                               progress: Optional[Callable[[dict, int, int], None]] = None,
                               info: Optional[dict] = None,
                               name: Optional[str] = None,
                               force: bool = False) -> dict:
        """单条消息下载，支持 .part 断点续传。返回结果字典。

        progress(info, done, total) 会在下载过程中被高频调用（约每 0.25 秒），
        由上层决定怎么聚合，用于任务卡片显示实时进度。
        `info` / `name` 由下载器预先算好（按消息顺序排号），不传就现场推导。
        `force=True` 时忽略索引和本地副本，老老实实从 Telegram 重下一遍
        （界面上叫「重新下载」，平常不这么用）。
        """
        info = info or message_info(message)
        if not info:
            return {"ok": False, "error": "该消息不含可下载的媒体"}

        # 索引键统一用数字 chat_id。用户可能从链接、@username、数字 id 三个入口
        # 进来，拿调用方给的字符串当键，同一个对话会对不上号 —— 这是「下过了
        # 又下一遍」最主要的原因。
        idx_key = str(info.get("chat_id") or peer or "")

        # 同一条消息被两个任务同时选中时，两边都会查不到索引、同时写同一个
        # .part，最后必有一份是坏的。按 (对话, 消息) 加锁，后来者等前一个写完，
        # 再命中索引直接跳过。
        async with _dl_lock(f"{idx_key}|{info['id']}"):
            return await self._download_one(
                message, target_dir, sem, check_cancel, idx_key, progress, info, name, force
            )

    async def _download_one(self, message, target_dir: Path, sem: asyncio.Semaphore,
                            check_cancel: Callable[[], bool], idx_key: str,
                            progress, info: dict, name: Optional[str],
                            force: bool = False) -> dict:
        """真正干活的单文件下载（已经被 (对话, 消息 id) 锁保护）。"""
        async with sem:
            if check_cancel():
                return {"ok": False, "cancelled": True, **info}

            folder = target_dir / safe_name(info["chat_title"] or "unknown", "unknown")
            folder.mkdir(parents=True, exist_ok=True)

            # 1) 这条消息本身已经下过，而且就在这个目录 → 直接用，不再存一份
            prev = None if force else _rt().INDEX.get(idx_key, info["id"])
            if prev is not None and prev.parent == folder:
                return {"ok": True, "skipped": True, **info,
                        "path": str(prev), "size": prev.stat().st_size}

            # 落盘名（方案 C）：默认 `2026-08-30_6787.mp4`（日期 + 消息 ID）；
            # 未显式传 name 时统一过 NameAllocator，确保磁盘上已有同名文件时自动顺延序号，绝不覆盖
            if self.cfg.rename_by_date:
                fname = name or info.get("save_name") or NameAllocator().alloc(folder, info)
                dest = folder / safe_name(fname, f"{info['id']}")
                if dest.exists() and not force:
                    owner = _rt().DB.find_file_by_path(dest)
                    if not owner or int(owner.get("msg_id") or 0) != int(info.get("id") or 0):
                        dest = self._dedupe(dest)
            else:
                dest = self._dedupe(folder / safe_name(info["filename"], f"{info['id']}"))

            total = info["size"] or 0

            # 2) 本地已有同一份内容，就别再走网络：
            #    - 换了目标目录（索引里记着它下到哪儿了）
            #    - 同一个文件被转发到别的对话/别的消息（file_id 相同）
            # 同盘建硬链接（零字节、瞬时），跨盘退回复制，都比重新下载快得多。
            src = prev
            if src is None and self.cfg.dedupe_content and not force:
                src = _rt().INDEX.find_fid(info.get("file_id") or "", exclude=f"{idx_key}|{int(info['id'])}")
            if src is not None and src.is_file():
                how = reuse_local(src, dest)
                if how:
                    size = dest.stat().st_size
                    _rt().INDEX.put(idx_key, info["id"], dest, size, info.get("file_id") or "")
                    self.emit("task", {**info, "id": info["id"], "state": "done",
                                       "done": size, "total": size, "percent": 100,
                                       "path": str(dest)})
                    return {"ok": True, "reused": how, "from": str(src), **info,
                            "path": str(dest), "size": size}

            # 临时文件带上消息 id：同一天不同消息的 .part 不会互相踩
            part = dest.with_name(f"{dest.name}.{info['id']}.part")
            marker = part.with_name(f"{part.name}.seg")
            can_segment = (
                self.cfg.segments > 1
                and total >= 1 * 1024 * 1024
                and info.get("kind") not in ("photo", "image")
            )

            if force:
                _unlink(marker, part)

            # 若存在 .seg 标记，说明上次走的是多路分片预分配模式：
            # - 继续走多路分片时，交给 _download_segmented 读 .seg 断点续传已完成的块；
            # - 若当前改成了单路串行（can_segment=False），预分配的空洞文件不能按 st_size 续传，才清掉重来。
            if marker.exists():
                if can_segment:
                    return await self._download_segmented(
                        message, part, dest, info, total, idx_key, check_cancel, progress
                    )
                _unlink(marker, part)

            done = part.stat().st_size if part.exists() else 0
            if total and done >= total:
                # 没有索引记录却已经满大小（例如异常残留），清理后重下
                _unlink(part)
                done = 0

            # 全新的大文件走多连接分块；若已有串行留下的半截 .part（done > 0 且无 .seg），继续走串行续传
            if can_segment and done == 0:
                return await self._download_segmented(
                    message, part, dest, info, total, idx_key, check_cancel, progress
                )

            # 串行断点续传：将已下载偏移向下对齐到 4KB 边界（MTProto GetFile 偏移要求），保证续传不报错
            if done > 4096 and (done % 4096) != 0:
                done = (done // 4096) * 4096
                try:
                    with open(part, "r+b") as fh:
                        fh.truncate(done)
                except OSError:
                    _unlink(part)
                    done = 0

            chunk_timeout = float(getattr(self.cfg, "chunk_timeout", 20.0) or 20.0)
            self.emit("task", {"id": info["id"], "state": "running", **info, "done": done, "total": total})
            if progress:
                progress(info, done, total)
            last_emit = 0.0
            refreshed = False
            net_retries = 0
            while True:
                try:
                    client = await self._pick_dl_client(int(info.get("id") or 0) if net_retries == 0 and not refreshed else 0)
                    with open(part, "ab", buffering=2 * 1024 * 1024) as fh:
                        async for chunk in self._iter_chunks(
                            client, message, offset=done, request_size=512 * 1024,
                            timeout=chunk_timeout, check_cancel=check_cancel
                        ):
                            if check_cancel():
                                fh.close()
                                _unlink(part, marker)
                                self.emit("task", {**info, "id": info["id"], "state": "cancelled", "done": 0})
                                return {"ok": False, "cancelled": True, **info}
                            fh.write(chunk)
                            done += len(chunk)
                            net_retries = 0  # 只要收到数据就重置断线重试计数
                            if progress:
                                progress(info, done, total)
                            now = time.time()
                            if now - last_emit > 0.25 or (total and done >= total):
                                last_emit = now
                                self.emit(
                                    "task",
                                    {
                                        **info,
                                        "id": info["id"],
                                        "state": "running",
                                        "done": done,
                                        "total": total,
                                        "percent": round(done * 100 / total, 1) if total else None,
                                        "speed": None,
                                    },
                                )
                    # 流结束但没下满：Telethon 收到短块误判文件末尾提前 close，属于可恢复的
                    # 截断，必须重连重试，绝不能把不完整的文件当成功替换。
                    if total and done < total:
                        raise ConnectionError(f"下载流提前结束（{done}/{total}），连接被截断")
                    if dest.exists() and not force:
                        owner = _rt().DB.find_file_by_path(dest)
                        if not owner or int(owner.get("msg_id") or 0) != int(info.get("id") or 0):
                            dest = self._dedupe(dest)
                    part.replace(dest)
                    _rt().INDEX.put(idx_key, info["id"], dest, done, info.get("file_id") or "")
                    break
                except asyncio.CancelledError:
                    # 只有用户主动点「停止」（check_cancel() 为真）才算取消。
                    # 网络断连时 Telethon 重连会对 pending 请求 future.cancel()，
                    # 让 iter_download 抛 CancelledError —— 这不是取消，是断网，
                    # 应走下面的重连重试，绝不能直接标 cancelled。
                    if check_cancel():
                        _unlink(part, marker)
                        self.emit("task", {**info, "id": info["id"], "state": "cancelled", "done": 0})
                        return {"ok": False, "cancelled": True, **info}
                    await self._force_reconnect(preserve_main=True)
                    continue
                except FileReferenceExpiredError:
                    if not refreshed:
                        refreshed = True
                        fresh_m = await self._refresh_message(message, idx_key, info["id"])
                        if fresh_m is not None:
                            message = fresh_m
                            continue
                    return {"ok": False, "error": "文件引用过期，请点重试继续下载", **info}
                except Exception as exc:
                    if check_cancel():
                        _unlink(part, marker)
                        self.emit("task", {**info, "id": info["id"], "state": "cancelled", "done": 0})
                        return {"ok": False, "cancelled": True, **info}
                    wait_s = getattr(exc, "seconds", None)
                    if wait_s and 0 < int(wait_s) <= 60:
                        await asyncio.sleep(int(wait_s) + 1)
                        continue
                    if self._is_recoverable_net_err(exc) and net_retries < 3 and not check_cancel():
                        net_retries += 1
                        await asyncio.sleep(min(2.0, 0.5 * net_retries))
                        if self._is_session_storm_err(exc) and self._record_storm(exc) >= 3:
                            await self._hard_reset()
                        else:
                            await self._force_reconnect(preserve_main=True)
                        fresh_m = await self._refresh_message(message, idx_key, info["id"])
                        if fresh_m is not None:
                            message = fresh_m
                        continue
                    return {"ok": False, "error": f"{type(exc).__name__}: {exc}", **info}

            self.emit("task", {**info, "id": info["id"], "state": "done", "done": done, "total": done,
                               "percent": 100, "path": str(dest)})
            return {"ok": True, **info, "path": str(dest), "size": done}

    async def _iter_chunks(self, client, message, *, offset: int, request_size: int,
                           limit: Optional[int] = None, timeout: float = 20.0,
                           check_cancel: Optional[Callable[[], bool]] = None):
        """带单块超时保护 + 0.2s 取消轮询的 iter_download 包装。
        - 首块超时收紧至 min(timeout, 15s)，快速识别半开假死连接；
        - 每 0.2s 轮询一次 check_cancel()，即使底层网络完全卡死，点「停止」也能在 0.2s 内立即打断退出。
        """
        it = client.iter_download(
            message, offset=offset, limit=limit, request_size=request_size
        ).__aiter__()
        first_chunk = True
        while True:
            if check_cancel and check_cancel():
                raise asyncio.CancelledError("任务已取消")
            cur_timeout = min(float(timeout or 20.0), 15.0) if first_chunk else float(timeout or 20.0)
            next_task = asyncio.ensure_future(it.__anext__())
            deadline = time.time() + cur_timeout
            try:
                while not next_task.done():
                    if check_cancel and check_cancel():
                        next_task.cancel()
                        raise asyncio.CancelledError("任务已取消")
                    rem = deadline - time.time()
                    if rem <= 0:
                        next_task.cancel()
                        raise TimeoutError(f"下载数据流读取超时（>{cur_timeout:.1f}s 无响应）")
                    await asyncio.wait([next_task], timeout=min(0.2, max(0.01, rem)))
                chunk = await next_task
            except StopAsyncIteration:
                break
            except asyncio.CancelledError:
                if not next_task.done():
                    next_task.cancel()
                raise
            except asyncio.TimeoutError as exc:
                if not next_task.done():
                    next_task.cancel()
                raise TimeoutError(f"下载数据流读取超时（>{cur_timeout:.1f}s 无响应）") from exc
            first_chunk = False
            self._last_rx_ts = time.time()
            yield chunk


    async def _download_segmented(self, message, part: Path, dest: Path, info: dict,
                                  total: int, idx_key: str, check_cancel, progress) -> dict:
        """多路连续流并行下载：将未完成的分片合并为连续区间，每个协程复用单条长连接流式拉取 512KB 块。

        为什么比旧版快得多：旧版每 512KB 就调一次 `iter_download(limit=1)`，导致 Telethon
        每 512KB 就要初始化/销毁一次下载迭代器（跨 DC 时反复借用连接）。现在每个 worker
        拿一段连续大区间，一条 `iter_download` 流到底，只建连一次，吞吐提升数倍。
        同时：用户主动取消（check_cancel）时立即清理 .part 和 .part.seg，不留磁盘垃圾；
        仅在网络异常中断时保留 .part + .part.seg 供「重试」断点续传。
        """
        import json as _json

        seg = 512 * 1024
        workers = max(1, min(16, int(self.cfg.segments)))
        all_offsets = list(range(0, total, seg))
        marker = part.with_name(f"{part.name}.seg")
        chunk_timeout = float(getattr(self.cfg, "chunk_timeout", 60.0) or 60.0)

        # 读取上次异常中断留下的分片记录
        done_offsets: set[int] = set()
        if part.exists() and marker.exists():
            try:
                if part.stat().st_size == total:
                    meta = _json.loads(marker.read_text(encoding="utf-8"))
                    if isinstance(meta, dict) and meta.get("total") == total and meta.get("seg") == seg:
                        done_offsets = {int(x) for x in (meta.get("done") or []) if 0 <= int(x) < total}
            except Exception:
                done_offsets = set()

        todo_offsets = [off for off in all_offsets if off not in done_offsets]
        committed_bytes = sum(min(seg, total - off) for off in done_offsets)
        state = {"committed": committed_bytes, "inflight": {}, "last_emit": 0.0}
        lock = asyncio.Lock()
        io_lock = threading.Lock()
        loop = asyncio.get_running_loop()

        self.emit("task", {"id": info["id"], "state": "running", **info, "done": committed_bytes,
                           "total": total, "segments": workers,
                           "percent": round(committed_bytes * 100 / total, 1) if total else 0})
        if progress:
            progress(info, committed_bytes, total)

        last_marker_ts = [0.0]

        def _save_marker(force_save: bool = False) -> None:
            now_m = time.time()
            if not force_save and (now_m - last_marker_ts[0] < 1.8):
                return
            last_marker_ts[0] = now_m
            tmp_m = marker.with_suffix(".seg.tmp")
            tmp_m.write_text(
                _json.dumps({"total": total, "seg": seg, "done": sorted(done_offsets)}),
                encoding="utf-8",
            )
            tmp_m.replace(marker)

        def _init_part():
            with io_lock:
                if not (part.exists() and part.stat().st_size == total and done_offsets):
                    done_offsets.clear()
                    with open(part, "wb") as fh:
                        fh.truncate(total)
                _save_marker(True)
                return open(part, "r+b", buffering=4 * 1024 * 1024)

        def _write_seg(off: int, data: bytes) -> None:
            with io_lock:
                handle.seek(off)
                handle.write(data)
                done_offsets.add(off)
                _save_marker(False)

        handle = None
        try:
            handle = await loop.run_in_executor(None, _init_part)
        except Exception as exc:
            _unlink(part, marker, marker.with_suffix(".seg.tmp"))
            return {"ok": False, "error": f"无法创建临时文件：{exc}", **info}

        def _report_progress(force_emit: bool = False) -> None:
            cur_done = min(total, state["committed"] + sum(state["inflight"].values()))
            if progress:
                progress(info, cur_done, total)
            now = time.time()
            if force_emit or (now - state["last_emit"] >= 0.25) or cur_done >= total:
                state["last_emit"] = now
                self.emit("task", {
                    **info,
                    "id": info["id"],
                    "state": "running",
                    "done": cur_done,
                    "total": total,
                    "percent": round(cur_done * 100 / total, 1) if total else None,
                    "segments": workers,
                })

        # 将待下载的 512KB 偏移合并为连续区间，再按 workers 均匀切分成若干条连续长流（stripes）
        contiguous_runs: list[list[int]] = []
        for off in todo_offsets:
            if contiguous_runs and contiguous_runs[-1][-1] + seg == off:
                contiguous_runs[-1].append(off)
            else:
                contiguous_runs.append([off])

        stripes: list[list[int]] = []
        target_len = max(1, math.ceil(len(todo_offsets) / max(1, workers)))
        for run in contiguous_runs:
            for i in range(0, len(run), target_len):
                stripes.append(run[i:i + target_len])

        stripe_idx = [0]
        msg_ref = [message]
        ref_lock = asyncio.Lock()

        async def worker(wid: int) -> None:
            while True:
                if check_cancel():
                    return
                async with lock:
                    if stripe_idx[0] >= len(stripes):
                        return
                    my_stripe = list(stripes[stripe_idx[0]])
                    stripe_idx[0] += 1

                pos = 0
                refreshed = False
                net_retries = 0
                while pos < len(my_stripe):
                    if check_cancel():
                        return
                    start_off = my_stripe[pos]
                    rem_blocks = len(my_stripe) - pos
                    buf = bytearray()
                    cur_off = start_off
                    try:
                        client = await self._pick_dl_client(wid if net_retries == 0 else 0)
                        # 单次 iter_download 连续拉取整个区间（每块 512KB），避免每块重建迭代器
                        async for chunk in self._iter_chunks(
                            client, msg_ref[0], offset=start_off, limit=rem_blocks,
                            request_size=seg, timeout=chunk_timeout, check_cancel=check_cancel
                        ):
                            if check_cancel():
                                return
                            buf.extend(chunk)
                            async with lock:
                                state["inflight"][wid] = len(buf)
                                _report_progress(False)

                            while pos < len(my_stripe):
                                cur_off = my_stripe[pos]
                                want = min(seg, total - cur_off)
                                if len(buf) < want:
                                    break
                                block = bytes(buf[:want])
                                del buf[:want]
                                await loop.run_in_executor(None, _write_seg, cur_off, block)
                                pos += 1
                                net_retries = 0
                                async with lock:
                                    state["inflight"][wid] = len(buf)
                                    state["committed"] += want
                                    _report_progress(True)
                        # 如果流提前结束但还没拿够本区间的块，说明 Telethon 在文件未下完时
                        # 收到了短块（< request_size）误判为「文件末尾」提前 close 了流。
                        # 这是可恢复的：抛 ConnectionError 让它走下面的重连重试，绝不能让这个
                        # 分片块永久丢失（否则 done_offsets 缺块，最终校验失败 → 任务反复卡住）。
                        if pos < len(my_stripe):
                            raise ConnectionError(f"分片流提前结束（offset={my_stripe[pos]}），连接被截断")
                    except FileReferenceExpiredError:
                        async with lock:
                            state["inflight"].pop(wid, None)
                        if not refreshed:
                            refreshed = True
                            async with ref_lock:
                                fresh_m = await self._refresh_message(msg_ref[0], idx_key, info["id"])
                                if fresh_m is not None:
                                    msg_ref[0] = fresh_m
                                    continue
                        raise
                    except asyncio.CancelledError:
                        # 用户点停止 → 传播出去；网络断连（Telethon 重连 cancel pending 请求）
                        # → 当作可恢复网络错误，重连后重试，而不是误判成取消。
                        if check_cancel():
                            raise
                        async with lock:
                            state["inflight"].pop(wid, None)
                        if net_retries < 3:
                            net_retries += 1
                            await asyncio.sleep(min(2.0, 0.5 * net_retries))
                            async with ref_lock:
                                await self._force_reconnect(preserve_main=True)
                                fresh_m = await self._refresh_message(msg_ref[0], idx_key, info["id"])
                                if fresh_m is not None:
                                    msg_ref[0] = fresh_m
                            continue
                        raise
                    except Exception as exc:
                        async with lock:
                            state["inflight"].pop(wid, None)
                        wait_s = getattr(exc, "seconds", None)
                        if wait_s and 0 < int(wait_s) <= 60 and not check_cancel():
                            await asyncio.sleep(int(wait_s) + 1)
                            continue
                        if self._is_recoverable_net_err(exc) and net_retries < 3 and not check_cancel():
                            net_retries += 1
                            await asyncio.sleep(min(2.0, 0.5 * net_retries))
                            async with ref_lock:
                                if self._is_session_storm_err(exc) and self._record_storm(exc) >= 3:
                                    await self._hard_reset()
                                else:
                                    await self._force_reconnect(preserve_main=True)
                                fresh_m = await self._refresh_message(msg_ref[0], idx_key, info["id"])
                                if fresh_m is not None:
                                    msg_ref[0] = fresh_m
                            continue
                        raise
                    finally:
                        async with lock:
                            state["inflight"].pop(wid, None)

        try:
            results = await asyncio.gather(*(worker(w) for w in range(min(workers, max(1, len(stripes))))),
                                           return_exceptions=True)
            handle.close()
            if check_cancel():
                # 用户主动点击「停止/取消」：彻底删除未完成的 .part 和 .part.seg，不留垃圾文件
                _unlink(part, marker, marker.with_suffix(".seg.tmp"))
                return {"ok": False, "cancelled": True, **info}
            errors = [r for r in results if isinstance(r, BaseException)]
            if errors:
                raise errors[0]
            if part.stat().st_size != total or len(done_offsets) < len(all_offsets):
                raise RuntimeError("文件分片校验未通过")
            if dest.exists():
                owner = _rt().DB.find_file_by_path(dest)
                if not owner or int(owner.get("msg_id") or 0) != int(info.get("id") or 0):
                    dest = self._dedupe(dest)
            part.replace(dest)
            _unlink(marker, marker.with_suffix(".seg.tmp"))
            _rt().INDEX.put(idx_key, info["id"], dest, dest.stat().st_size, info.get("file_id") or "")
        except FileReferenceExpiredError:
            try:
                if done_offsets:
                    with io_lock:
                        _save_marker(True)
                handle.close()
            except Exception:
                pass
            if not done_offsets:
                _unlink(part, marker)
            return {"ok": False, "error": "文件引用过期，请点重试继续下载", **info}
        except asyncio.CancelledError:
            # 网络断连导致 Telethon 对 pending 请求 future.cancel() 抛出的 CancelledError，
            # 若用户并未点停止（check_cancel 为假），这里是可恢复网络错误，返回失败结果让上层
            # 触发自动重试，绝不能当作「已取消」穿透到任务层。
            try:
                if done_offsets:
                    with io_lock:
                        _save_marker(True)
                handle.close()
            except Exception:
                pass
            return {"ok": False, "error": "连接被服务器中断，可重试续传", **info}
        except Exception as exc:
            try:
                if done_offsets:
                    with io_lock:
                        _save_marker(True)
                handle.close()
            except Exception:
                pass
            if not done_offsets:
                _unlink(part, marker)
            return {"ok": False, "error": f"{type(exc).__name__}: {exc}", **info}

        self.emit("task", {**info, "id": info["id"], "state": "done", "done": total,
                           "total": total, "percent": 100, "path": str(dest)})
        return {"ok": True, **info, "path": str(dest), "size": total}

    def _dedupe(self, path: Path) -> Path:
        if not path.exists() or self.cfg.on_conflict == "overwrite":
            return path
        if self.cfg.on_conflict == "skip":
            return path
        stem, suffix = path.stem, path.suffix
        i = 1
        while True:
            cand = path.with_name(f"{stem} ({i}){suffix}")
            if not cand.exists():
                return cand
            i += 1

