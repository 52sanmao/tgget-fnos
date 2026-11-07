"""事件总线 + 下载任务调度 + 任务落盘。"""

from __future__ import annotations

import asyncio
import json
import threading
import time
import uuid
from collections import deque
from pathlib import Path
from queue import Queue
from typing import Any, Optional

from .config import HOME, Config
from .db import DB
from .store import INDEX
from .tg import NameAllocator, Telegram, message_info, parse_link, safe_name

JOBS_FILE = HOME / "jobs.json"
# 这些状态代表「还没跑完」。程序被人 Ctrl-C / 断电退出后，磁盘上留下的就是它们，
# 下次启动一律改判成 interrupted，由用户决定要不要重试。
ACTIVE = ("queued", "running", "cancelling")
# 历史任务最多留这么多条，再多就把最老的挤掉（每个任务的文件清单也会截断）
KEEP_JOBS = 200
KEEP_FILES = 200


class EventBus:
    """极简 pub/sub，供 SSE 推送使用。"""

    def __init__(self, history: int = 300) -> None:
        self._subs: set[Queue] = set()
        self._lock = threading.Lock()
        self._history: deque = deque(maxlen=history)

    def emit(self, kind: str, payload: dict) -> None:
        evt = {"type": kind, "ts": time.time(), **(payload or {})}
        if kind == "task":  # 进度事件太多，不进历史
            self._broadcast(evt)
            return
        if kind == "log":
            try:
                DB.add_log(
                    level=str((payload or {}).get("level") or "info"),
                    category=str((payload or {}).get("category") or "system"),
                    action=str((payload or {}).get("action") or ""),
                    detail=str((payload or {}).get("msg") or ""),
                    peer=str((payload or {}).get("peer") or ""),
                    meta=(payload or {}).get("meta"),
                )
            except Exception:
                pass
        with self._lock:
            self._history.append(evt)
        self._broadcast(evt)

    def _broadcast(self, evt: dict) -> None:
        with self._lock:
            subs = list(self._subs)
        for q in subs:
            try:
                q.put_nowait(evt)
            except Exception:
                pass

    def subscribe(self) -> Queue:
        q: Queue = Queue(maxsize=2000)
        with self._lock:
            self._subs.add(q)
        for evt in list(self._history)[-50:]:
            try:
                q.put_nowait(evt)
            except Exception:
                pass
        return q

    def unsubscribe(self, q: Queue) -> None:
        with self._lock:
            self._subs.discard(q)


class JobStore:
    """任务表落盘：`~/.tgget/jobs.json`。

    和 files.json 一样是最朴素的 JSON —— 几百条任务、几十 KB，读写都在毫秒级，
    比引一个 SQLite 更省心，用户还能直接打开看。
    """

    def __init__(self, path: Optional[Path] = None) -> None:
        self._path = Path(path) if path else JOBS_FILE
        self.last_error = ""

    def load(self) -> list[dict]:
        self.last_error = ""
        if not self._path.exists():
            return []
        try:
            raw = json.loads(self._path.read_text(encoding="utf-8"))
        except Exception as exc:
            broken = self._path.with_suffix(".broken.json")
            try:
                self._path.replace(broken)
            except OSError:
                pass
            self.last_error = f"任务记录文件无法解析，已备份为 {broken.name}：{exc}"
            print(f"[warn] {self.last_error}")
            return []
        if isinstance(raw, dict):  # 兼容以后可能加的 {"jobs": [...]} 结构
            raw = raw.get("jobs") or []
        return [r for r in raw if isinstance(r, dict) and r.get("job_id")]

    def save(self, rows: list[dict]) -> None:
        try:
            self._path.parent.mkdir(parents=True, exist_ok=True)
            tmp = self._path.with_suffix(".tmp")
            tmp.write_text(json.dumps(rows, ensure_ascii=False, indent=0), encoding="utf-8")
            tmp.replace(self._path)  # 原子替换，写一半断电也不会留下坏文件
        except Exception:
            pass  # 落盘失败不该影响下载本身


class Downloader:
    """任务表 + 并发控制。所有方法都在事件循环线程内被调用。"""

    def __init__(self, tg: Telegram, cfg: Config, bus: EventBus,
                 store_path: Optional[Path] = None) -> None:
        self.tg = tg
        self.cfg = cfg
        self.bus = bus
        self.jobs: dict[str, dict] = {}
        self._cancel: set[str] = set()
        self._tasks: dict[str, asyncio.Task] = {}
        self._futures: dict[str, Any] = {}
        self._sem: Optional[asyncio.Semaphore] = None
        self._loop = tg.loop.loop
        self._store = JobStore(store_path)
        self._persist_lock = threading.Lock()
        self.on_downloaded = None  # 下载完成回调：用于与 AutoRules 同步最新已下载消息 ID
        self._restore()

    def _semaphore(self) -> asyncio.Semaphore:
        if self._sem is None:
            self._sem = asyncio.Semaphore(self.cfg.concurrency)
        return self._sem

    def set_concurrency(self, n: int) -> None:
        """设置页修改并发数后立即替换信号量，无需重启服务。"""
        n = max(1, min(12, int(n)))
        self.cfg.concurrency = n
        if self._loop and self._loop.is_running():
            self._loop.call_soon_threadsafe(self._reset_sem, n)
        else:
            self._sem = None

    def _reset_sem(self, n: int) -> None:
        self._sem = asyncio.Semaphore(n)

    # -- 落盘 --------------------------------------------------------------
    def _restore(self) -> None:
        """把上次退出时的任务读回来。没跑完的标成「已中断」，可一键重试。"""
        rows = self._store.load()
        if self._store.last_error and self.bus:
            self.bus.emit("log", {"level": "error", "msg": self._store.last_error})
        for raw in rows:
            try:
                job = hydrate(raw, str(self.cfg.download_path))
                self.jobs[job["job_id"]] = job
            except Exception as exc:
                msg = f"恢复任务记录 {raw.get('job_id', '?')} 失败：{type(exc).__name__}: {exc}"
                print(f"[warn] {msg}")
                if self.bus:
                    self.bus.emit("log", {"level": "error", "msg": msg})

    def _persist(self) -> None:
        with self._persist_lock:
            rows = []
            jobs = sorted(self.jobs.values(), key=lambda x: -float(x.get("created") or 0))
            for j in jobs[:KEEP_JOBS]:
                row = {k: v for k, v in j.items() if not k.startswith("_")}
                row["files"] = list(j.get("files") or [])[-KEEP_FILES:]
                rows.append(row)
            self._store.save(rows)

    def _set_step(self, job: dict, key: str, label: str, status: str = "done", detail: str = "") -> None:
        job["stage"] = label
        steps = job.setdefault("steps", [])
        now = time.time()
        for st in steps:
            if st.get("key") == key:
                st["label"] = label
                st["status"] = status
                if detail:
                    st["detail"] = detail
                st["ts"] = now
                break
        else:
            steps.append({"key": key, "label": label, "status": status, "detail": detail, "ts": now})
        self.bus.emit("job", public(job))

    # -- 对外 --------------------------------------------------------------
    def submit(self, peer: str, ids: Optional[list[int]] = None, limit: int = 20,
               media_only: bool = True, outdir: Optional[str] = None,
               retry_of: str = "", force: bool = False, mode: str = "limit") -> dict:
        job_id = uuid.uuid4().hex[:8]
        mode_norm = str(mode or ("all" if int(limit or 0) <= 0 and not ids else "limit"))
        job = {
            "job_id": job_id,
            "kind": "download",
            "mode": mode_norm,
            "peer": peer,
            "title": "",
            "sub": "",
            "stage": "排队等待执行",
            "steps": [{"key": "init", "label": "任务已创建，等待执行", "status": "done", "detail": "", "ts": time.time()}],
            "ids": ids or [],
            # 真正要下的消息 id：ids 为空时是「最近 N 条 / 全部 / 新增」，要翻完消息才知道
            "queue": [],
            "limit": int(limit or 0),
            "media_only": media_only,
            # 默认不开强制：已经下过、还在目标目录里的文件直接跳过，不重复下载
            "force": bool(force),
            "outdir": outdir or str(self.cfg.download_path),
            "state": "queued",
            "total": 0,
            "done": 0,
            "failed": 0,
            # 命中本地已有文件（同目录跳过 / 换目录链接过来）的数量
            "reused": 0,
            "size": 0,
            "bytes": 0,
            "speed": 0.0,
            "current": "",
            "error": "",
            "retry_of": retry_of,
            "created": time.time(),
            "files": [],
            # 运行时内部状态（public 会剔除）：_base 已完成字节，_cur 进行中文件字节
            "_base": 0,
            "_cur": {},
            "_samples": deque(maxlen=60),
            "_last_emit": 0.0,
            "_last_persist": 0.0,
        }
        self.jobs[job_id] = job
        self.bus.emit("job", public(job))
        self.bus.emit("log", {
            "level": "info", "category": "download", "action": "download.start", "peer": str(peer),
            "msg": f"创建下载任务 #{job_id}（对话：{peer}，模式：{mode_norm}）",
        })
        self._persist()
        self._futures[job_id] = asyncio.run_coroutine_threadsafe(self._run(job), self._loop)
        return job

    def submit_transfer(self, peer: str, ids: list[int], target: str) -> dict:
        """创建「我的收藏 → 目标群」转存归档任务，在任务台中实时展示每个步骤（本地复用/下载/上传/迁移/清理）。"""
        job_id = uuid.uuid4().hex[:8]
        clean_ids = sorted({int(i) for i in (ids or []) if int(i or 0) > 0})
        job = {
            "job_id": job_id,
            "kind": "transfer",
            "mode": "transfer",
            "peer": str(peer),
            "target": str(target),
            "title": f"我的收藏 → 转存至 {target}",
            "sub": f"共 {len(clean_ids)} 条消息",
            "stage": "准备转存",
            "steps": [{"key": "init", "label": "转存任务已创建", "status": "done",
                       "detail": f"源消息 ID：{clean_ids} → 目标群：{target}", "ts": time.time()}],
            "ids": clean_ids,
            "queue": clean_ids,
            "limit": len(clean_ids),
            "media_only": False,
            "force": False,
            "outdir": str(self.cfg.download_path),
            "state": "running",
            "total": len(clean_ids),
            "done": 0,
            "failed": 0,
            "reused": 0,
            "size": 0,
            "bytes": 0,
            "speed": 0.0,
            "current": "",
            "error": "",
            "retry_of": "",
            "created": time.time(),
            "files": [],
            "_base": 0,
            "_cur": {},
            "_samples": deque(maxlen=60),
            "_last_emit": 0.0,
            "_last_persist": 0.0,
        }
        self.jobs[job_id] = job
        self.bus.emit("job", public(job))
        self.bus.emit("log", {
            "level": "info", "category": "transfer", "action": "transfer.start", "peer": str(target),
            "msg": f"开始转存任务 #{job_id}：从「我的收藏」转存 {len(clean_ids)} 条消息至 {target}",
        })
        self._persist()
        self._futures[job_id] = asyncio.run_coroutine_threadsafe(self._run_transfer(job), self._loop)
        return job

    async def _run_transfer(self, job: dict) -> None:
        job_id = job["job_id"]
        cur_t = asyncio.current_task()
        if cur_t is not None:
            self._tasks[job_id] = cur_t
        ids = list(job.get("ids") or [])
        peer = str(job.get("peer") or "")
        target = str(job.get("target") or "")

        def on_step(key: str, label: str, status: str = "done", detail: str = "") -> None:
            self._set_step(job, key, label, status, detail)
            self.bus.emit("transfer", {
                "job_id": job_id, "ids": ids, "state": "running",
                "stage": label, "detail": detail,
            })
            self.bus.emit("log", {
                "level": "error" if status == "error" else "info",
                "category": "transfer",
                "action": f"transfer.{key}",
                "peer": target,
                "msg": f"转存 #{job_id} · {label}" + (f" — {detail}" if detail else ""),
            })
            self._persist()

        def check_cancel() -> bool:
            return job_id in self._cancel

        def on_progress(phase: str, cur: int, tot: int, label: str = "", speed: float = 0.0,
                        done_files: Optional[int] = None) -> None:
            job["bytes"] = int(cur or 0)
            if tot and int(tot) > 0:
                job["size"] = int(tot)
            if speed > 0:
                job["speed"] = float(speed)
            if done_files is not None:
                job["done"] = int(done_files)
            job["current"] = label or ("正在上传…" if phase == "upload" else "正在下载源文件…")
            pct_str = f" {round(cur * 100 / tot)}%" if tot else ""
            job["stage"] = ("正在上传" if phase == "upload" else "正在下载源文件") + pct_str
            now = time.time()
            if now - job["_last_emit"] >= 0.35:
                job["_last_emit"] = now
                self.bus.emit("job", public(job))
                self.bus.emit("transfer", {
                    "job_id": job_id, "ids": ids, "state": "running",
                    "stage": job["stage"],
                })

        try:
            if hasattr(self.tg, "_transfer_saved"):
                if hasattr(self.tg, "_retry_disconnected"):
                    res = await self.tg._retry_disconnected(
                        lambda: self.tg._transfer_saved(
                            peer, ids, target, on_step=on_step, on_progress=on_progress, check_cancel=check_cancel
                        )
                    )
                else:
                    res = await self.tg._transfer_saved(
                        peer, ids, target, on_step=on_step, on_progress=on_progress, check_cancel=check_cancel
                    )
            else:
                res = self.tg.transfer_saved(peer, ids, target)

            if res.get("cancelled") or check_cancel():
                job["state"] = "cancelled"
                job["speed"] = 0.0
                job["current"] = ""
                job["finished"] = time.time()
                self._set_step(job, "cancel", "转存已取消（我的收藏原消息未删除）", "error")
                self.bus.emit("transfer", {
                    "job_id": job_id, "ids": ids, "state": "cancelled",
                    "stage": "已取消",
                })
                return

            t_title = res.get("target_title") or target
            job["title"] = f"我的收藏 → {t_title}"
            job["done"] = int(res.get("transferred") or len(ids))
            job["total"] = max(job["total"], job["done"])
            job["reused"] = int(res.get("reused_local") or 0)
            if job["size"]:
                job["bytes"] = job["size"]
            job["speed"] = 0.0
            job["current"] = ""
            job["state"] = "done"
            job["finished"] = time.time()
            moved_paths = res.get("moved_paths") or []
            for idx_p, mp in enumerate(moved_paths):
                p_obj = Path(str(mp))
                job["files"].append({
                    "id": (res.get("new_ids") or [0])[idx_p] if idx_p < len(res.get("new_ids") or []) else idx_p + 1,
                    "name": p_obj.name,
                    "path": str(p_obj),
                    "size": p_obj.stat().st_size if p_obj.is_file() else 0,
                    "kind": "file",
                    "reused": "move",
                })
            summary = f"成功转存 {job['done']} 条至「{t_title}」，迁移本地文件 {res.get('moved_files', 0)} 个，已清理我的收藏原消息"
            job["note"] = summary
            self._set_step(job, "finish", "转存归档全部完成", "done", summary)
            self.bus.emit("log", {
                "level": "ok", "category": "transfer", "action": "transfer.done", "peer": str(res.get("target_peer") or target),
                "msg": f"转存 #{job_id} 完成：{summary}",
            })
            self.bus.emit("transfer", {
                "job_id": job_id, "ids": ids, "state": "done",
                "stage": "转存完成", "result": res,
            })
            if self.on_downloaded and res.get("new_ids"):
                try:
                    self.on_downloaded(str(res.get("target_peer") or target), max(int(x) for x in res["new_ids"]))
                except Exception:
                    pass
        except (Exception, asyncio.CancelledError) as exc:
            # 只有用户主动点了「停止」（job_id 在 _cancel 集合里，或状态已被 cancel() 置为 cancelled）
            # 才算「取消」。网络断连/超时导致的 asyncio.CancelledError（Telethon 重连时会对
            # 未收到响应的 pending 请求执行 future.cancel()）绝不能误判为「用户取消」，
            # 否则下载到一半断连会被静默标成「已取消」，用户看不到真实失败原因。
            if job_id in self._cancel or job.get("state") == "cancelled":
                job["state"] = "cancelled"
                job["speed"] = 0.0
                job["current"] = ""
                job["finished"] = job.get("finished") or time.time()
                self._clean_job_parts(job)
                self._set_step(job, "cancel", "转存已取消（我的收藏原消息未删除）", "error")
                self.bus.emit("transfer", {
                    "job_id": job_id, "ids": ids, "state": "cancelled",
                    "stage": "已取消",
                })
            else:
                err_msg = f"{type(exc).__name__}: {exc}"
                job["state"] = "failed"
                job["failed"] = len(ids)
                job["error"] = err_msg
                job["speed"] = 0.0
                job["current"] = ""
                job["finished"] = time.time()
                self._set_step(job, "error", "转存中断失败", "error", err_msg)
                self.bus.emit("log", {
                    "level": "error", "category": "transfer", "action": "transfer.error", "peer": target,
                    "msg": f"转存任务 #{job_id} 失败：{err_msg}（我的收藏原消息已安全保留，未删除）",
                })
                self.bus.emit("transfer", {
                    "job_id": job_id, "ids": ids, "state": "failed",
                    "stage": "转存失败", "error": err_msg,
                })
        finally:
            self._tasks.pop(job_id, None)
            self._futures.pop(job_id, None)
            self.bus.emit("job", public(job))
            self._persist()

    def _abort_task(self, job_id: str) -> None:
        t = self._tasks.pop(job_id, None)
        if t is not None and not t.done():
            try:
                t.cancel()
            except Exception:
                pass
        f = self._futures.pop(job_id, None)
        if f is not None and not f.done():
            try:
                f.cancel()
            except Exception:
                pass

    def cancel(self, job_id: str) -> dict:
        job = self.jobs.get(job_id)
        if job is None:
            return {"ok": False, "error": "任务不存在"}
        if job["state"] not in ("queued", "running", "cancelling"):
            return {"ok": False, "error": f"任务已结束（{job['state']}）"}
        self._cancel.add(job_id)
        job["state"] = "cancelled"
        job["speed"] = 0.0
        job["current"] = ""
        job["finished"] = time.time()
        if job.get("kind") == "transfer":
            self._set_step(job, "cancel", "转存已取消（我的收藏原消息未删除）", "error")
            self.bus.emit("transfer", {
                "job_id": job_id, "ids": list(job.get("ids") or []),
                "state": "cancelled", "stage": "已取消",
            })
        else:
            self._set_step(job, "download", "任务已取消，正在清理临时文件", "error")
        # 立即向事件循环注入 Task.cancel()，强行打断正在等待的 get_messages / iter_download 等长耗时网络 await
        if self._loop and self._loop.is_running():
            try:
                self._loop.call_soon_threadsafe(self._abort_task, job_id)
            except Exception:
                pass
        self._clean_job_parts(job)
        self.bus.emit("log", {
            "level": "warn",
            "category": "transfer" if job.get("kind") == "transfer" else "download",
            "action": "job.cancel",
            "peer": str(job.get("peer") or ""),
            "msg": f"用户手动停止了任务 #{job_id}（{job.get('title') or job.get('peer')}）",
        })
        self.bus.emit("job", public(job))
        self._persist()
        return {"ok": True, "state": "cancelled"}

    def retry(self, job_id: str) -> dict:
        """重试：只补没成功的那些消息，成功的不重复下。

        新建一个任务（而不是复活旧的）有两个好处：历史记录保留下来能对照，
        旧任务的状态也不会被悄悄改掉。
        """
        old = self.jobs.get(job_id)
        if old is None:
            return {"ok": False, "error": "任务不存在"}
        if old["state"] in ("queued", "running", "cancelling"):
            return {"ok": False, "error": "任务还在跑，先停止再重试"}
        if old.get("kind") == "transfer":
            job = self.submit_transfer(old["peer"], list(old.get("ids") or []), str(old.get("target") or ""))
            return {"ok": True, **public(job)}

        ids = retry_ids(old)
        if not ids and not old.get("limit") and old.get("mode") not in ("all", "new"):
            return {"ok": False, "error": "这个任务没有记录到具体消息，没法重试"}

        job = self.submit(
            old["peer"],
            ids=ids,
            limit=int(old.get("limit") or 20),
            media_only=bool(old.get("media_only", True)),
            outdir=old.get("outdir"),
            retry_of=job_id,
        )
        return {"ok": True, **public(job)}

    def _maybe_auto_retry(self, job: dict) -> None:
        """失败 / 部分失败后，若设置里开了「自动重试」且不是用户手动取消，就退避几秒后
        自动补下没成功的文件（新建一个任务），直到全部成功或用户手动停止。

        只对下载任务生效；转存任务失败涉及「我的收藏」原消息删除等副作用，不自动重试。
        """
        if not getattr(self.cfg, "auto_retry", False):
            return
        if job.get("kind") != "download":
            return
        if job.get("job_id") in self._cancel:
            return
        ids = retry_ids(job)
        if not ids:
            return

        async def _do():
            await asyncio.sleep(3.0)  # 退避几秒，避免瞬时打爆 / 死循环
            if job.get("job_id") in self._cancel:
                return
            # 复用 submit：只补失败的文件（retry_ids 已剔除成功的），新建任务记录重试历史
            self.submit(
                job.get("peer", ""),
                ids=ids,
                limit=int(job.get("limit") or 20),
                media_only=bool(job.get("media_only", True)),
                outdir=job.get("outdir"),
                retry_of=job.get("job_id", ""),
            )
            self.bus.emit("log", {
                "level": "info", "category": "download", "action": "download.auto_retry",
                "peer": str(job.get("peer") or ""),
                "msg": f"自动重试：任务 #{job['job_id']} 失败，补下 {len(ids)} 个未成功的文件",
            })

        try:
            self._futures[f"retry:{job['job_id']}"] = asyncio.run_coroutine_threadsafe(_do(), self._loop)
        except Exception:
            pass

    def redo(self, job_id: str) -> dict:
        """强制重下：忽略本地已有的同名文件，整批从 Telegram 再拉一遍。

        给「文件下坏了 / 手动改过想还原」用。平常不需要 —— 默认就是防重复的。
        """
        old = self.jobs.get(job_id)
        if old is None:
            return {"ok": False, "error": "任务不存在"}
        if old["state"] in ACTIVE:
            return {"ok": False, "error": "任务还在跑，先停止再重新下载"}
        ids = [int(i) for i in (old.get("queue") or old.get("ids") or [])]
        if not ids and not old.get("limit"):
            return {"ok": False, "error": "这个任务没有记录到具体消息，没法重新下载"}
        job = self.submit(
            old["peer"],
            ids=ids,
            limit=int(old.get("limit") or 20),
            media_only=bool(old.get("media_only", True)),
            outdir=old.get("outdir"),
            retry_of=job_id,
            force=True,
        )
        return {"ok": True, **public(job)}

    def list(self) -> list[dict]:
        return [public(j) for j in sorted(self.jobs.values(), key=lambda x: -x["created"])]
    def clear(self, only_finished: bool = True) -> dict:
        keep = set(ACTIVE) if only_finished else set()
        removed = [k for k, j in self.jobs.items() if j["state"] not in keep]
        for k in removed:
            j = self.jobs.pop(k, None)
            # 失败 / 部分失败 / 取消 / 中断的任务：清空时连同已下载的成品文件一起删，
            # 避免坏文件堆积。成功（done）任务的文件保留。
            if j and j.get("state") in ("cancelled", "failed", "interrupted", "partial"):
                self._clean_job_parts(j, del_finished=True)
        self._persist()
        return {"removed": len(removed), "jobs": self.list()}

    @staticmethod
    def _clean_job_parts(job: dict, del_finished: bool = False) -> None:
        """清理已取消 / 失败 / 被清空任务遗留的临时文件，防止占满 NAS 磁盘。

        - `.part` / `.part.seg`：断点续传的临时分片，永远清。
        - 当 `del_finished=True`（清空失败任务时）才删除该任务已下载成功的成品文件，
          成功任务的文件绝不动。
        """
        try:
            outdir = Path(str(job.get("outdir") or "")).expanduser()
            ids = {int(i) for i in (job.get("queue") or job.get("ids") or []) if str(i).lstrip("-").isdigit()}
            files = job.get("files") or []
            for f in files:
                if f.get("id"):
                    ids.add(int(f["id"]))
            if outdir.is_dir():
                for mid in ids:
                    for p in outdir.rglob(f"*.{mid}.part*"):
                        try:
                            if p.is_file():
                                p.unlink()
                        except OSError:
                            pass
            # 删除失败/取消任务已下到磁盘的成品文件（清空任务时用）
            if del_finished:
                for f in files:
                    p = Path(str(f.get("path") or "")).expanduser()
                    if not p.is_file():
                        continue
                    # 只删位于下载目录内的文件，绝不越界误删用户其它目录
                    try:
                        p.resolve().relative_to(outdir.resolve())
                    except ValueError:
                        continue
                    try:
                        p.unlink()
                    except OSError:
                        pass
        except Exception:
            pass

    # -- 进度 --------------------------------------------------------------
    def _progress(self, job: dict, info: dict, done: int) -> None:
        """单文件实时进度：累加到任务总字节并限流推给前端（约 2.5 次/秒）。"""
        job["_cur"][info.get("id")] = done
        total_bytes = job["_base"] + sum(job["_cur"].values())
        job["bytes"] = total_bytes
        job["current"] = info.get("filename") or ""

        now = time.time()
        samples = job["_samples"]
        samples.append((now, total_bytes))
        while len(samples) > 2 and now - samples[0][0] > 3.0:
            samples.popleft()
        if len(samples) >= 2:
            span = samples[-1][0] - samples[0][0]
            if span > 0.2:
                job["speed"] = max(0.0, (samples[-1][1] - samples[0][1]) / span)

        if now - job["_last_emit"] >= 0.4:
            job["_last_emit"] = now
            self.bus.emit("job", public(job))
        # 进度也隔几秒落一次盘：不然大文件下到 90% 时进程被杀，重启后
        # 界面会显示 0 B（虽然 .part 还在，续传其实能接上）
        if now - job["_last_persist"] >= 5.0:
            job["_last_persist"] = now
            self._persist()

    def _finish_one(self, job: dict, info: dict, size: int) -> None:
        job["_cur"].pop(info.get("id"), None)
        job["_base"] += size or 0
        job["bytes"] = job["_base"] + sum(job["_cur"].values())
        job["_samples"].clear()
        job["_samples"].append((time.time(), job["bytes"]))

    # -- 执行 --------------------------------------------------------------
    async def _run(self, job: dict) -> None:
        job_id = job["job_id"]
        cur_t = asyncio.current_task()
        if cur_t is not None:
            self._tasks[job_id] = cur_t
        try:
            await self._run_inner(job)
        except (Exception, asyncio.CancelledError) as exc:
            # 同 _run_transfer：仅当用户主动停止（job_id 在 _cancel / 状态已被置 cancelled）才算取消；
            # 网络断连导致的 CancelledError 归为失败，记录真实错误，避免「下载自动被取消」的假象。
            if job_id in self._cancel or job.get("state") == "cancelled":
                job["state"] = "cancelled"
                job["speed"] = 0.0
                job["current"] = ""
                job["finished"] = job.get("finished") or time.time()
                self._clean_job_parts(job)
                self._set_step(job, "download", "任务已取消，已清理未完成临时文件", "error")
            else:
                job["state"] = "failed"
                job["error"] = f"{type(exc).__name__}: {exc}"
                job["finished"] = time.time()
                job["speed"] = 0.0
                self._maybe_auto_retry(job)
            self.bus.emit("job", public(job))
            self._persist()
        finally:
            self._tasks.pop(job_id, None)
            self._futures.pop(job_id, None)

    async def _run_inner(self, job: dict) -> None:
        job_id = job["job_id"]
        is_cancelled = lambda: (job_id in self._cancel or job.get("state") == "cancelled")
        if is_cancelled():
            raise asyncio.CancelledError("任务已取消")
        ref, link_id = parse_link(job["peer"])
        target_ref = int(ref) if ref.lstrip("-").isdigit() else ref

        async def _resolve():
            c = await self.tg._ensure()
            if hasattr(self.tg, "_entity"):
                coro = self.tg._entity(c, job["peer"])
            else:
                coro = c.get_entity(target_ref)
            if hasattr(self.tg, "_call_rpc"):
                ent = await self.tg._call_rpc(coro, timeout=15.0, check_cancel=is_cancelled)
            else:
                ent = await coro
            return c, ent

        try:
            if hasattr(self.tg, "_retry_disconnected"):
                client, entity = await self.tg._retry_disconnected(_resolve)
            else:
                client, entity = await _resolve()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            if is_cancelled():
                raise asyncio.CancelledError("任务已取消")
            job["state"] = "failed"
            job["error"] = f"无法解析对话：{type(exc).__name__}: {exc}"
            self.bus.emit("job", public(job))
            self._persist()
            return
        job["title"] = await self._job_title(client, entity, job, [])

        ids = list(job["ids"])
        if not ids and link_id:
            ids = [link_id]

        mode = str(job.get("mode") or "limit")
        min_id = 0
        if not ids and mode == "new":
            # 「新增下载」：自动查询该对话上次已下载到的最大消息 ID，只拉比它更新的消息
            ent_id = str(getattr(entity, "id", "") or "")
            min_id = max(DB.max_downloaded_id(job["peer"]), DB.max_downloaded_id(f"-100{ent_id}"), DB.max_downloaded_id(ent_id))
            self._set_step(job, "resolve", "解析对话与检查增量基线", "done",
                           f"上次已下载至消息 #{min_id}，本次仅拉取 #{min_id} 之后的新增内容" if min_id else "该对话尚无下载记录，将拉取全部内容")
        else:
            self._set_step(job, "resolve", "解析对话成功", "done", f"对话：{job['title'] or job['peer']}")

        self._set_step(job, "fetch", "正在读取消息列表", "running")

        async def _fetch_msgs():
            c = await self.tg._ensure()
            out = []
            if ids:
                if hasattr(self.tg, "_call_rpc"):
                    fetched = await self.tg._call_rpc(c.get_messages(entity, ids=ids), timeout=15.0, check_cancel=is_cancelled)
                else:
                    fetched = await c.get_messages(entity, ids=ids)
                if not isinstance(fetched, list):
                    fetched = [fetched]
                out = [m for m in (fetched or []) if m is not None]
                # 链接下载相册自动扩选：当用户粘贴单条消息链接（如受限频道的多视频/多图相册链接），
                # 且未带 ?single 参数时，自动把同一 grouped_id 的整组相册媒体一并拉下来。
                if link_id and len(ids) == 1 and "?single" not in str(job.get("peer") or "").lower() and out:
                    gid = getattr(out[0], "grouped_id", None)
                    if gid:
                        mid0 = int(out[0].id)
                        around_ids = [x for x in range(max(1, mid0 - 10), mid0 + 11) if x != mid0]
                        try:
                            if hasattr(self.tg, "_call_rpc"):
                                near = await self.tg._call_rpc(c.get_messages(entity, ids=around_ids), timeout=8.0, check_cancel=is_cancelled)
                            else:
                                near = await c.get_messages(entity, ids=around_ids)
                            if not isinstance(near, list):
                                near = [near]
                            same_group = [m for m in (near or []) if m is not None and getattr(m, "grouped_id", None) == gid]
                            if same_group:
                                by_id = {int(m.id): m for m in (out + same_group)}
                                out = [by_id[k] for k in sorted(by_id.keys())]
                        except Exception:
                            pass
            elif mode == "all" or int(job.get("limit") or 0) <= 0:
                async for m in c.iter_messages(entity, limit=None):
                    if is_cancelled():
                        raise asyncio.CancelledError("任务已取消")
                    out.append(m)
            elif mode == "new":
                if min_id > 0:
                    async for m in c.iter_messages(entity, limit=None, min_id=min_id):
                        if is_cancelled():
                            raise asyncio.CancelledError("任务已取消")
                        out.append(m)
                else:
                    async for m in c.iter_messages(entity, limit=None):
                        if is_cancelled():
                            raise asyncio.CancelledError("任务已取消")
                        out.append(m)
            else:
                async for m in c.iter_messages(entity, limit=int(job["limit"])):
                    if is_cancelled():
                        raise asyncio.CancelledError("任务已取消")
                    out.append(m)
            return c, out

        messages = []
        try:
            if hasattr(self.tg, "_retry_disconnected"):
                client, messages = await self.tg._retry_disconnected(_fetch_msgs)
            else:
                client, messages = await _fetch_msgs()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            if is_cancelled():
                raise asyncio.CancelledError("任务已取消")
            job["state"] = "failed"
            job["error"] = f"读取消息失败：{type(exc).__name__}: {exc}"
            self.bus.emit("job", public(job))
            self._persist()
            return

        infos = [message_info(m) for m in messages]
        targets = [(m, i) for m, i in zip(messages, infos) if i]
        if not targets:
            if mode == "new":
                job["state"] = "done"
                job["note"] = f"自上次下载（#{min_id}）以来暂无新增媒体文件"
                job["finished"] = time.time()
                self._set_step(job, "fetch", "暂无新增媒体（已是最新）", "done", job["note"])
                self.bus.emit("log", {"level": "info", "category": "download", "action": "download.nonew",
                                      "peer": job["peer"], "msg": f"{job['title']}：{job['note']}"})
                self.bus.emit("job", public(job))
                self._persist()
                return
            job["state"] = "failed"
            job["error"] = "没有可下载的媒体（这些消息里没有图片、视频或文件）"
            self._set_step(job, "fetch", "未找到可下载的媒体", "error", job["error"])
            self.bus.emit("job", public(job))
            self._persist()
            return
        self._set_step(job, "fetch", "读取消息列表完成", "done", f"共扫描到 {len(targets)} 个媒体文件")

        outdir = Path(job["outdir"]).expanduser()
        outdir.mkdir(parents=True, exist_ok=True)

        # 下载前先在本地筛一遍：这条消息如果已经在同一个目标目录里落了成品，
        # 就没必要再建协程去下第二遍。这条路径比「下完再比对」重要得多 ——
        # 用户重复点下载时，任务总数、进度条、耗时都应该是「0 个要做」，
        # 而不是假装在下载然后又全部跳过。
        # 换了目标目录、或同一个文件被转发到别的对话的，交给 download_message
        # 做硬链接复用（同样不走网络）。
        reuse_rows: list[dict] = []
        todo: list[tuple] = []
        for m, info in targets:
            key = str(info.get("chat_id") or job["peer"])
            folder = outdir / safe_name(info.get("chat_title") or "unknown", "unknown")
            prev = None if job.get("force") else INDEX.get(key, info["id"])
            if prev is not None and prev.parent == folder:
                reuse_rows.append({
                    "id": info.get("id"), "name": prev.name, "path": str(prev),
                    "size": prev.stat().st_size, "kind": info.get("kind"),
                    "skipped": True,
                })
            else:
                todo.append((m, info))

        # 标题补上「消息内容」：从我的收藏下载时，对话名就是自己的账号名，
        # 光看名字完全分不出任务；有文字/说明就用它当副标题
        job["title"] = await self._job_title(client, entity, job, [i for _, i in targets])
        job["queue"] = sorted(int(i.get("id") or 0) for _, i in targets)
        job["reused"] = len(reuse_rows)
        job["done"] = len(reuse_rows)
        job["total"] = len(targets)
        job["size"] = (sum(int(i["size"] or 0) for _, i in todo)
                       + sum(int(r.get("size") or 0) for r in reuse_rows))
        job["files"] = list(reuse_rows)
        job["_base"] = sum(int(r.get("size") or 0) for r in reuse_rows)
        job["bytes"] = job["_base"]
        job["state"] = "running"
        if reuse_rows:
            job["note"] = f"{len(reuse_rows)} 个文件已经在目标目录里，没有重复下载"

        if not todo:
            job["state"] = "done"
            job["note"] = f"这 {len(reuse_rows)} 个文件都已在目标目录里，没有重复从 Telegram 下载"
            job["finished"] = time.time()
            self._set_step(job, "precheck", "本地已全部存在（跳过下载）", "done", job["note"])
            self.bus.emit("log", {"level": "info", "category": "download", "action": "download.reused",
                                  "peer": job["peer"], "msg": f"{job['title']}：{job['note']}"})
            self.bus.emit("job", public(job))
            self._persist()
            return

        if reuse_rows:
            self._set_step(job, "precheck", "本地防重预检完成", "done",
                           f"本地已存在 {len(reuse_rows)} 个（直接跳过），需下载 {len(todo)} 个")
        self._set_step(job, "download", f"正在下载媒体（共 {len(todo)} 个）", "running")
        self.bus.emit("job", public(job))
        self._persist()

        # 落盘名必须在并发之前定好：同一天的文件按消息 id 升序（= 发送顺序）
        # 依次排号 `日期.ext`、`日期_2.ext`…… 否则 4 个协程谁先跑到谁拿 1 号，
        # 相册里的顺序每次下载都不一样。
        names: dict[Any, str] = {}
        if self.cfg.rename_by_date:
            alloc = NameAllocator()
            for _, info in sorted(todo, key=lambda t: int(t[1].get("id") or 0)):
                folder = outdir / safe_name(info.get("chat_title") or "unknown", "unknown")
                folder.mkdir(parents=True, exist_ok=True)
                if job.get("force"):
                    old = INDEX.info(str(info.get("chat_id") or job["peer"]), info["id"])
                    old_path = Path(str((old or {}).get("path") or ""))
                    if old and old_path.parent == folder:
                        # 重新下载：沿用原来那份的名字直接覆盖，别再生出一份 `_2`
                        names[info.get("id")] = old_path.name
                        continue
                names[info.get("id")] = alloc.alloc(folder, info)

        async def one(m, info):
            try:
                res = await self.tg.download_message(
                    m, outdir, self._semaphore(), lambda: job["job_id"] in self._cancel,
                    peer=job["peer"],
                    progress=lambda i, done, total: self._progress(job, i, done),
                    info=info,
                    name=names.get(info.get("id")),
                    force=bool(job.get("force")),
                )
            except (Exception, asyncio.CancelledError) as exc:  # 单条炸了不能带塌整个任务（含连接被打断引发的 CancelledError）
                res = {"ok": False, "error": f"{type(exc).__name__}: {exc}", **info}
            saved = Path(res["path"]).name if res.get("path") else info["filename"]
            if res.get("cancelled"):
                job["state"] = "cancelled"
                job["_cur"].pop(info.get("id"), None)
                job["files"].append({"id": info.get("id"), "name": saved,
                                     "kind": info.get("kind"), "error": "已取消"})
            elif res.get("ok"):
                job["done"] += 1
                if res.get("reused"):
                    # 本地已有同一份内容，只做了硬链接/复制，不算「重新下载」
                    job["reused"] = int(job.get("reused") or 0) + 1
                self._finish_one(job, info, res.get("size") or 0)
                job["files"].append({"id": info.get("id"), "name": saved,
                                     "path": res.get("path"), "size": res.get("size"),
                                     "kind": info.get("kind"), "skipped": bool(res.get("skipped")),
                                     "reused": res.get("reused") or ""})
            else:
                job["failed"] += 1
                job["_cur"].pop(info.get("id"), None)
                job["files"].append({"id": info.get("id"), "name": saved,
                                     "kind": info.get("kind"),
                                     "error": res.get("error") or "未知错误"})
                self.bus.emit("log", {"level": "error",
                                      "msg": f"下载失败 · {saved} · {res.get('error') or '未知错误'}"})
            job["_last_emit"] = 0.0
            self.bus.emit("job", public(job))
            self._persist()
            return res

        await asyncio.gather(*(one(m, i) for m, i in todo), return_exceptions=True)

        if job["state"] == "cancelled" or job["job_id"] in self._cancel:
            job["state"] = "cancelled"
            self._clean_job_parts(job)
            self._set_step(job, "download", "任务已取消，已清理未完成临时文件", "error")
            self.bus.emit("log", {"level": "warn", "category": "download", "action": "download.cancel",
                                  "peer": job["peer"], "msg": f"任务 #{job['job_id']} 已取消并清理临时文件"})
        elif job["state"] != "cancelled":
            job["state"] = "done" if job["failed"] == 0 else "partial"
            if job["failed"]:
                job["error"] = f"{job['failed']} 个文件下载失败，展开下方文件列表看原因，或点「重试失败项」"
                self._set_step(job, "download", f"下载结束（成功 {job['done']}，失败 {job['failed']}）", "error", job["error"])
                self._maybe_auto_retry(job)
            else:
                self._set_step(job, "download", f"全部下载完成（共 {job['done']} 个文件）", "done")
                self.bus.emit("log", {"level": "ok", "category": "download", "action": "download.done",
                                      "peer": job["peer"], "msg": f"任务 #{job['job_id']}（{job['title']}）全部完成，共 {job['done']} 个文件"})
            # 与自动化规则联动：手动/批量下载完成后，同步推进该对话自动下载规则的基线 ID，避免重复扫描
            if self.on_downloaded and job.get("queue"):
                try:
                    chat_k = str(targets[0][1].get("chat_id") or job["peer"])
                    self.on_downloaded(chat_k, max(job["queue"]))
                    if str(job["peer"]) != chat_k:
                        self.on_downloaded(str(job["peer"]), max(job["queue"]))
                except Exception:
                    pass
        job["speed"] = 0.0
        job["current"] = ""
        job["finished"] = time.time()
        self.bus.emit("job", public(job))
        self._persist()

    async def _job_title(self, client, entity, job: dict, infos: list[dict]) -> str:
        """任务标题：优先用对话名（群/频道名），收藏夹用自己的消息内容。

        Telegram 里「我的收藏」本质是自己跟自己的对话，`entity.title` 拿不到，
        只能拿到自己的姓名 —— 那是所有任务都一样的「bug big」，等于没标题。
        """
        try:
            me = await client.get_me()
            me_id = getattr(me, "id", None)
        except Exception:
            me_id = None
        is_self = me_id is not None and getattr(entity, "id", None) == me_id

        name = "" if is_self else entity_title(entity)
        if is_self:
            name = "我的收藏"
            snippet = next((_snippet(i) for i in infos if _snippet(i)), "")
            if snippet:
                name = f"{name} · {snippet}"
        elif not name:
            snippet = next((_snippet(i) for i in infos if _snippet(i)), "")
            name = snippet or job.get("peer") or "未命名任务"
        return name[:80]


def hydrate(raw: dict, default_outdir: str = "") -> dict:
    """把磁盘上的一条任务记录补全成运行时可用的结构。

    重点是「上次没跑完」的判定：磁盘里留着 queued/running/cancelling 说明
    进程是被人硬杀的，这次启动不可能再有人接着跑，一律改判 interrupted，
    并把还没下完的文件数写在 error 里，用户点「重试」就能继续。
    """
    job = dict(raw)
    job.setdefault("files", [])
    job.setdefault("peer", "")
    job.setdefault("title", "")
    job.setdefault("outdir", default_outdir)
    job.setdefault("created", 0)
    job.setdefault("total", 0)
    job.setdefault("done", 0)
    job.setdefault("failed", 0)
    job.setdefault("size", 0)
    job.setdefault("ids", [])
    job.setdefault("queue", [])
    job.setdefault("media_only", True)
    job.setdefault("limit", 0)
    job.setdefault("force", False)
    job.setdefault("kind", "download")
    job.setdefault("mode", "limit")
    job.setdefault("stage", "")
    job.setdefault("steps", [])
    job.setdefault("reused", 0)
    job.setdefault("note", "")

    done_ids = {f.get("id") for f in job["files"] if not f.get("error")}
    job["_base"] = sum(int(f.get("size") or 0) for f in job["files"] if not f.get("error"))
    job["bytes"] = job["_base"]
    job["speed"] = 0.0
    job["current"] = ""
    job["_cur"] = {}
    job["_samples"] = deque(maxlen=60)
    job["_last_emit"] = 0.0
    job["_last_persist"] = 0.0
    if job.get("state") in ACTIVE:
        job["state"] = "interrupted"
        left = max(0, int(job.get("total") or 0) - len(done_ids))
        job["error"] = f"程序退出，任务被中断（还有 {left} 个文件没下完），可以点「重试」继续"
    return job


def retry_ids(job: dict) -> list[int]:
    """重试时要补的消息 id：成功的跳过，一个都不剩就整批重来。"""
    done = {f.get("id") for f in job.get("files") or [] if not f.get("error")}
    queue = [int(i) for i in (job.get("queue") or job.get("ids") or [])]
    return [i for i in queue if i not in done] or queue


def _snippet(info: dict) -> str:
    """一条消息的文字/说明/文件名，用来给任务起个有信息量的名字。"""
    for key in ("caption", "text", "filename"):
        v = (info.get(key) or "").strip().replace("\n", " ")
        if v:
            return v[:40]
    return ""


def entity_title(entity) -> str:
    """对话显示名：频道/群组用 title，用户用姓名或用户名。"""
    for attr in ("title", "name"):
        v = getattr(entity, attr, None)
        if v:
            return str(v)
    name = " ".join(filter(None, [getattr(entity, "first_name", None), getattr(entity, "last_name", None)]))
    return name or getattr(entity, "username", "") or ""


def public(job: dict) -> dict:
    out = {k: v for k, v in job.items() if k not in ("ids", "queue") and not k.startswith("_")}
    out["files"] = job["files"][-50:]
    return out
