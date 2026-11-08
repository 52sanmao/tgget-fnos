"""动作分发：`POST /api/actions/execute {"action": "...", "payload": {...}}`

把命令行、Web UI、脚本统一到同一套动作名上，方便以后加客户端。
"""

from __future__ import annotations

import asyncio
import os
import platform
import socket
import subprocess
import time
import urllib.parse
from pathlib import Path

from . import __version__
from .auto import AutoRules
from .config import HOME, Config
from .db import DB
from .files import Files
from .jobs import Downloader, EventBus
from .store import INDEX
from .tg import human_size


class Api:
    def __init__(self, cfg: Config, tg, bus: EventBus, dl: Downloader, started: float,
                 demo: bool = False) -> None:
        self.cfg = cfg
        self.tg = tg
        self.bus = bus
        self.dl = dl
        self.started = started
        self.demo = demo
        self.files = Files(cfg)
        self.auto = AutoRules(
            cfg, tg, dl, bus,
            rule_file=((HOME / "auto-demo.json") if demo else None),
        )
        self.dl.on_downloaded = self.auto.sync_baseline
        if not demo:
            self.auto.start()
        self._routes = {
            "system.status": self.system_status,
            "settings.get": self.settings_get,
            "settings.set": self.settings_set,
            "net.test": self.net_test,
            # 登录
            "login.qr.start": lambda p: self.tg.qr_start(),
            "login.qr.poll": lambda p: self.tg.qr_poll(),
            "login.password": lambda p: self.tg.qr_password(str(p.get("password", ""))),
            "login.demo.authorize": lambda p: self.tg.demo_authorize(),
            "login.code.send": self.login_send_code,
            "login.code.verify": self.login_verify,
            "login.logout": lambda p: self.tg.logout(),
            # 对话与消息
            "dialog.list": self.dialog_list,
            "message.list": self.message_list,
            # 下载与任务
            "download.start": self.download_start,
            "download.cancel": lambda p: self.dl.cancel(str(p.get("job_id", ""))),
            "download.retry": lambda p: self.dl.retry(str(p.get("job_id", ""))),
            "download.redo": lambda p: self.dl.redo(str(p.get("job_id", ""))),
            "task.list": lambda p: {"jobs": self.dl.list()},
            "task.clear": self.task_clear,
            # 转发 / 发送 / 编辑 / 删除
            "forward.start": self.forward_start,
            "transfer.saved": self.transfer_saved,
            "send.files": self.send_files,
            "message.edit": self.message_edit,
            "message.delete": self.message_delete,
            "export.messages": self.export_messages,
            # 自动化
            "auto.list": lambda p: self.auto.list(),
            "auto.create": lambda p: self.auto.create(p),
            "auto.update": lambda p: self.auto.update(p),
            "auto.delete": lambda p: self.auto.delete(p),
            "auto.all": lambda p: self.auto.set_all(bool(p.get("enabled", True))),
            "auto.run": lambda p: self.auto.run_now(str(p.get("id", ""))),
            # 磁盘信息与持久化日志
            "files.disk": lambda p: self.files.disk(),
            "files.browse": lambda p: self.files.browse(str(p.get("path", "") or "")),
            "files.roots": lambda p: {"roots": self.files.browse_roots()},
            "dialog.progress": self.dialog_progress,
            "log.list": self.log_list,
            "log.clear": lambda p: {"removed": DB.clear_logs()},
            # 兼容旧动作
            "config.get": lambda p: self.cfg.mask(),
            "config.set": self.config_set,
        }

    # -- 分发 --------------------------------------------------------------
    def execute(self, action: str, payload: dict | None = None) -> dict:
        fn = self._routes.get(action)
        if fn is None:
            return {"ok": False, "error": f"未知动作：{action}", "actions": sorted(self._routes)}
        p = payload or {}
        try:
            data = fn(p)
            # 对关键写操作统一落库记录操作审计日志
            if action in ("settings.set", "message.delete", "forward.start", "export.messages",
                          "auto.delete", "auto.all", "task.clear", "login.logout"):
                self.bus.emit("log", {
                    "level": "info",
                    "category": action.split(".")[0],
                    "action": action,
                    "peer": str(p.get("peer") or p.get("target") or ""),
                    "msg": f"执行操作 {action} 成功",
                })
            return {"ok": True, "data": data}
        except (Exception, asyncio.CancelledError) as exc:
            self.bus.emit("log", {
                "level": "error",
                "category": action.split(".")[0],
                "action": action,
                "peer": str(p.get("peer") or p.get("target") or ""),
                "msg": f"{action} 执行异常：{type(exc).__name__}: {exc}",
            })
            return {"ok": False, "error": f"{type(exc).__name__}: {exc}"}

    # -- 状态 --------------------------------------------------------------
    def system_status(self, _p: dict) -> dict:
        state = self.tg.state()
        try:
            disk = self.files.disk()
        except Exception:
            disk = {}
        try:
            import cryptg  # noqa: F401
            fast_crypto = True
        except ImportError:
            fast_crypto = False
        return {
            "version": __version__,
            "cryptg": fast_crypto,
            "uptime": round(time.time() - self.started, 1),
            "python": platform.python_version(),
            "os": f"{platform.system()} {platform.release()}",
            "demo": self.demo,
            "config": self.cfg.mask(),
            "telegram": state,
            "disk": disk,
            "proxy": self.cfg.proxy,
            "queue": {
                "active": sum(1 for j in self.dl.jobs.values() if j["state"] in ("running", "queued")),
                "total": len(self.dl.jobs),
            },
            "auto": self.auto.stats(),
            "library": INDEX.stats(),
            "db": DB.stats(),
        }

    def settings_get(self, _p: dict) -> dict:
        return {
            "config": self.cfg.mask(),
            "disk": self.files.disk(),
            "proxy": self.cfg.proxy,
            "download_dir": str(self.cfg.download_path),
            "concurrency": self.cfg.concurrency,
            "on_conflict": self.cfg.on_conflict,
            "rename_by_date": self.cfg.rename_by_date,
        }

    def settings_set(self, p: dict) -> dict:
        changed = {}
        need_reconnect = False

        if "api_id" in p and p["api_id"] not in (None, ""):
            raw_id = str(p["api_id"]).strip()
            if raw_id.isdigit() and int(raw_id) > 0:
                new_id = int(raw_id)
                if new_id != self.cfg.api_id:
                    self.cfg.api_id = new_id
                    changed["api_id"] = self.cfg.api_id
                    need_reconnect = True
        if "api_hash" in p and p["api_hash"] not in (None, ""):
            raw_hash = str(p["api_hash"]).strip()
            # 忽略前端传回的脱敏占位符（如 ********abcd）
            if raw_hash and not raw_hash.startswith("*"):
                if raw_hash != self.cfg.api_hash:
                    self.cfg.api_hash = raw_hash
                    changed["api_hash"] = "***"
                    need_reconnect = True

        if "download_dir" in p and p["download_dir"]:
            new_dir = str(Path(str(p["download_dir"]).strip()).expanduser())
            self.cfg.download_dir = new_dir
            self.cfg.custom_dir = True
            changed["download_dir"] = self.cfg.download_dir
        if "concurrency" in p and p["concurrency"]:
            self.cfg.concurrency = max(1, min(12, int(p["concurrency"])))
            if hasattr(self.dl, "set_concurrency"):
                self.dl.set_concurrency(self.cfg.concurrency)
            changed["concurrency"] = self.cfg.concurrency
        if "segments" in p and p["segments"]:
            self.cfg.segments = max(1, min(16, int(p["segments"])))
            changed["segments"] = self.cfg.segments
        if "on_conflict" in p and p["on_conflict"] in ("rename", "skip", "overwrite"):
            self.cfg.on_conflict = p["on_conflict"]
            changed["on_conflict"] = self.cfg.on_conflict
        if "rename_by_date" in p:
            self.cfg.rename_by_date = bool(p["rename_by_date"])
            changed["rename_by_date"] = self.cfg.rename_by_date
        if "dedupe_content" in p:
            self.cfg.dedupe_content = bool(p["dedupe_content"])
            changed["dedupe_content"] = self.cfg.dedupe_content
        if "auto_retry" in p:
            self.cfg.auto_retry = bool(p["auto_retry"])
            changed["auto_retry"] = self.cfg.auto_retry
        if "archive_peer" in p:
            self.cfg.archive_peer = str(p.get("archive_peer") or "").strip()
            changed["archive_peer"] = self.cfg.archive_peer
        if "archive_title" in p:
            self.cfg.archive_title = str(p.get("archive_title") or "").strip()
            changed["archive_title"] = self.cfg.archive_title
        if "proxy" in p:
            new_proxy = str(p["proxy"] or "").strip()
            if new_proxy != self.cfg.proxy:
                need_reconnect = True
            self.cfg.proxy = new_proxy
            changed["proxy"] = self.cfg.proxy

        self.cfg.save()
        reconnected = None
        if need_reconnect and hasattr(self.tg, "reconnect") and self.cfg.api_id and self.cfg.api_hash:
            try:
                reconnected = self.tg.reconnect()
            except Exception as exc:
                reconnected = {"error": f"{type(exc).__name__}: {exc}"}
        return {"changed": changed, "config": self.cfg.mask(), "telegram": reconnected}

    def net_test(self, p: dict) -> dict:
        """探一下网络：有代理就测代理端口，没有就测 Telegram 入口。"""
        proxy = str(p.get("proxy", self.cfg.proxy) or "").strip()
        host, port, label = "149.154.167.51", 443, "Telegram DC4"
        if proxy:
            parsed = urllib.parse.urlparse(proxy if "://" in proxy else f"socks5://{proxy}")
            host = parsed.hostname or host
            port = parsed.port or 1080
            label = f"代理 {host}:{port}"
        started = time.time()
        try:
            with socket.create_connection((host, port), timeout=6):
                ms = round((time.time() - started) * 1000)
                return {"ok": True, "target": label, "latency_ms": ms,
                        "message": f"{label} 连通，耗时 {ms} ms"}
        except Exception as exc:
            return {"ok": False, "target": label,
                    "message": f"{label} 连不上：{type(exc).__name__}: {exc}"}

    # -- 登录 --------------------------------------------------------------
    def login_send_code(self, p: dict) -> dict:
        phone = str(p.get("phone", "")).strip()
        if not phone:
            raise ValueError("请填写手机号（含国际区号，如 +8613800000000）")
        return self.tg.send_code(phone)

    def login_verify(self, p: dict) -> dict:
        return self.tg.sign_in(
            str(p.get("phone", "")).strip(),
            str(p.get("code", "")).strip(),
            str(p.get("password", "")),
        )

    # -- 对话与消息 --------------------------------------------------------
    def dialog_list(self, p: dict) -> dict:
        source = str(p.get("source") or "auto")
        rows = self.tg.dialogs(int(p.get("limit", 200)), str(p.get("query", "")), source=source)
        return {"rows": rows, "count": len(rows), "source": source if source == "local" else "remote"}

    def message_list(self, p: dict) -> dict:
        peer = str(p.get("peer", "")).strip()
        if not peer:
            raise ValueError("缺少 peer")
        return self.tg.messages(
            peer,
            limit=int(p.get("limit", 50)),
            offset_id=int(p.get("offset_id", 0) or 0),
            media_only=bool(p.get("media_only", True)),
            source=str(p.get("source") or "auto"),
        )

    # -- 下载 --------------------------------------------------------------
    def dialog_progress(self, p: dict) -> dict:
        peer = str(p.get("peer") or "").strip()
        max_id = DB.max_downloaded_id(peer) if peer else 0
        return {"peer": peer, "max_downloaded_id": max_id}

    def log_list(self, p: dict) -> dict:
        rows = DB.list_logs(
            limit=int(p.get("limit") or 200),
            level=str(p.get("level") or ""),
            category=str(p.get("category") or ""),
            query=str(p.get("query") or ""),
        )
        return {"rows": rows, "count": len(rows)}

    def download_start(self, p: dict) -> dict:
        peer = str(p.get("peer", "")).strip()
        if not peer:
            raise ValueError("缺少 peer")
        ids = p.get("ids") or []
        if isinstance(ids, (int, str)):
            ids = [int(ids)]
        ids = [int(i) for i in ids]
        raw_limit = p.get("limit", 20)
        limit_val = int(raw_limit) if raw_limit not in (None, "") else 0
        mode = str(p.get("mode") or ("all" if limit_val <= 0 and not ids else "limit"))
        job = self.dl.submit(
            peer,
            ids=ids,
            limit=limit_val,
            media_only=bool(p.get("media_only", True)),
            outdir=p.get("outdir") or None,
            force=bool(p.get("force", False)),
            mode=mode,
        )
        return {"job_id": job["job_id"], "state": job["state"], "mode": mode}

    def task_clear(self, p: dict) -> dict:
        """清空已结束的任务（会同步清掉磁盘上的任务记录）。"""
        return self.dl.clear(only_finished=True)

    # -- 转发 / 发送 / 编辑 ------------------------------------------------
    def transfer_saved(self, p: dict) -> dict:
        peer = str(p.get("peer", "")).strip()
        target = str(p.get("target") or self.cfg.archive_peer or "").strip()
        if not peer:
            raise ValueError("缺少源对话 peer")
        if not target:
            raise ValueError("尚未设置目标群组 ID")
        ids = p.get("ids") or []
        if isinstance(ids, (int, str)):
            ids = [int(ids)]
        ids = [int(i) for i in ids if str(i).strip()]
        if not ids:
            raise ValueError("没有选中要转存的消息")
        if p.get("async_job"):
            job = self.dl.submit_transfer(peer, ids, target)
            return {"job_id": job["job_id"], "state": job["state"], "target_peer": target}
        res = self.tg.transfer_saved(peer, ids, target)
        # 顺手记住目标群的真实数字 chat_id 和当前名称
        if res.get("target_peer") and self.cfg.archive_peer != str(res["target_peer"]):
            self.cfg.archive_peer = str(res["target_peer"])
            self.cfg.archive_title = str(res.get("target_title") or self.cfg.archive_title)
            self.cfg.save()
        elif res.get("target_title") and self.cfg.archive_title != str(res["target_title"]):
            self.cfg.archive_title = str(res["target_title"])
            self.cfg.save()
        return res

    def forward_start(self, p: dict) -> dict:
        peer = str(p.get("peer", "")).strip()
        target = str(p.get("target", "")).strip()
        if not peer or not target:
            raise ValueError("需要源对话和目标对话")
        ids = p.get("ids") or []
        if isinstance(ids, (int, str)):
            ids = [int(ids)]
        ids = [int(i) for i in ids]
        if not ids:
            raise ValueError("没有选中要转发的消息")
        return self.tg.forward(peer, ids, target, drop_author=bool(p.get("drop_author", False)))

    def send_files(self, p: dict) -> dict:
        peer = str(p.get("peer", "")).strip()
        paths = p.get("paths") or []
        if isinstance(paths, str):
            paths = [paths]
        if not peer:
            raise ValueError("请选择目标对话")
        if not paths:
            raise ValueError("没有要发送的文件")
        return self.tg.send_files(peer, [str(x) for x in paths], str(p.get("caption", "")))

    def message_edit(self, p: dict) -> dict:
        peer = str(p.get("peer", "")).strip()
        mid = int(p.get("id", 0) or 0)
        text = str(p.get("text", ""))
        if not peer or not mid:
            raise ValueError("需要 peer 和消息 id")
        if not text.strip():
            raise ValueError("新内容不能为空")
        return self.tg.edit_message(peer, mid, text)

    def message_delete(self, p: dict) -> dict:
        """删除消息：默认连 Telegram 云端一起删（所有设备同步消失）。"""
        peer = str(p.get("peer", "")).strip()
        if not peer:
            raise ValueError("缺少 peer")
        ids = p.get("ids") or []
        if isinstance(ids, (int, str)):
            ids = [int(ids)]
        ids = [int(i) for i in ids if str(i).strip()]
        if not ids:
            raise ValueError("没有选中要删除的消息")
        return self.tg.delete_messages(peer, ids, cloud=bool(p.get("cloud", True)))

    # -- 导出 --------------------------------------------------------------
    def export_messages(self, p: dict) -> dict:
        peer = str(p.get("peer", "")).strip()
        if not peer:
            raise ValueError("请选择要导出的对话")
        fmt = str(p.get("format", "txt")).lower()
        if fmt not in ("txt", "docx"):
            raise ValueError("只支持 txt 或 docx")
        return self.tg.export_messages(
            peer, int(p.get("limit", 2000)), fmt, str(p.get("outdir", ""))
        )

    # -- 兼容 --------------------------------------------------------------
    def config_set(self, p: dict) -> dict:
        return self.settings_set(p)


def format_bytes(n) -> str:
    return human_size(n)
