"""自动化规则：盯住一个对话，把新出现的媒体自动下载到指定目录。

规则持久化在 `~/.tgget/auto.json`；由后台线程按各规则自己的间隔轮询。
新建规则时会把"当前最新消息 id"记为基线，避免一建规则就把整个历史拉下来。
"""

from __future__ import annotations

import json
import threading
import time
import uuid
from pathlib import Path
from typing import Optional

from .config import HOME
from .db import DB

RULE_FILE = HOME / "auto.json"
KINDS = ("video", "image", "audio", "file")
# 消息里的 kind 和规则里的类型名不是一套写法：照片落地是 "photo"，
# 规则里叫 "image"，语音是 "voice"。不归一化的话「图片」条件永远匹配不到。
KIND_ALIAS = {"photo": "image", "image": "image", "voice": "audio", "sticker": "image"}


def norm_kind(kind: Optional[str]) -> str:
    k = (kind or "file").lower()
    return KIND_ALIAS.get(k, k)


class AutoRules:
    def __init__(self, cfg, tg, dl, bus=None, rule_file: Optional[Path] = None) -> None:
        self.cfg = cfg
        self.tg = tg
        self.dl = dl
        self.bus = bus
        # 演示模式给一份独立的规则文件，免得假规则被真实实例读到
        self._file = Path(rule_file) if rule_file else RULE_FILE
        self.rules: list[dict] = self._load()
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._busy: set[str] = set()
        self._lock = threading.Lock()
        self._err = ""

    # -- 持久化 ------------------------------------------------------------
    def _load(self) -> list[dict]:
        if not self._file.exists():
            return []
        try:
            data = json.loads(self._file.read_text(encoding="utf-8"))
        except Exception as exc:
            # 文件被写坏（比如进程在写盘时被杀）时不要静默丢掉规则：
            # 先留一份现场，方便事后人工恢复，再从空列表开始
            try:
                self._file.replace(self._file.with_suffix(".broken.json"))
            except OSError:
                pass
            print(f"[warn] 自动规则文件无法解析，已备份为 auto.broken.json：{exc}")
            return []
        if isinstance(data, dict):
            data = data.get("rules") or []
        return [r for r in data if isinstance(r, dict)]

    def _save(self) -> None:
        self._file.parent.mkdir(parents=True, exist_ok=True)
        # 先写临时文件再原子替换：中途被 kill 也不会留下半截 JSON
        tmp = self._file.with_suffix(".tmp")
        tmp.write_text(json.dumps(self.rules, indent=2, ensure_ascii=False), encoding="utf-8")
        tmp.replace(self._file)

    def _emit(self, rule: dict) -> None:
        if self.bus:
            self.bus.emit("auto", {"rule": _public(rule), "stats": self.stats()})

    # -- 查询 --------------------------------------------------------------
    def stats(self) -> dict:
        active = sum(1 for r in self.rules if r.get("enabled") and r["id"] not in self._busy)
        return {
            "total": len(self.rules),
            "enabled": sum(1 for r in self.rules if r.get("enabled")),
            "running": len(self._busy),
            "idle": active,
            "ok": sum(int(r.get("ok") or 0) for r in self.rules),
            "fail": sum(int(r.get("fail") or 0) for r in self.rules),
            "error": self._err,
        }

    def list(self) -> dict:
        return {"rules": [_public(r) for r in self.rules], "stats": self.stats(),
                "kinds": list(KINDS)}

    # -- 增删改 ------------------------------------------------------------
    def create(self, p: dict) -> dict:
        peer = str(p.get("peer") or "").strip()
        if not peer:
            raise ValueError("请选择一个对话")
        title = str(p.get("peer_title") or peer)
        kinds = [k for k in (p.get("kinds") or []) if k in KINDS] or list(KINDS)
        now_i = int(time.time())

        # 防连点重复创建：5 秒内对同一个对话建的相同规则直接复用返回，避免弹出两个任务
        with self._lock:
            for existing in self.rules:
                if existing.get("peer") == peer and abs(now_i - int(existing.get("created") or 0)) <= 5:
                    return _public(existing)

        # 基线策略（与前面的手动/批量下载打通）：
        # - smart（默认「智能续接」）：如果该对话以前下载过文件（DB.max_downloaded_id > 0），
        #   就从「上次已下载的最大消息 ID」接着往后下（立即补齐上次下载之后新增的内容）；
        #   如果从未下载过，则取当前最新消息 ID 为基线（只下今后新发的）。
        # - latest：强制以当前最新消息为基线（只下今后新发的）
        # - all：从 0 开始（把历史所有符合条件的媒体也全部下载）
        bmode = str(p.get("baseline_mode") or "smart").lower()
        dl_max = 0
        try:
            dl_max = DB.max_downloaded_id(peer)
        except Exception:
            pass

        baseline = 0
        immediate_catchup = False
        if bmode == "all":
            baseline = 0
            immediate_catchup = True
        elif bmode == "smart" and dl_max > 0:
            baseline = dl_max
            immediate_catchup = True
        else:
            try:
                local_rows = self.tg.messages(peer, limit=1, media_only=False, source="local").get("rows") or []
                if local_rows:
                    baseline = int(local_rows[0]["id"])
            except Exception:
                pass
            if not baseline:
                rows = self.tg.messages(peer, limit=1, media_only=False).get("rows") or []
                baseline = int(rows[0]["id"]) if rows else 0

        with self._lock:
            for existing in self.rules:
                if existing.get("peer") == peer and abs(int(time.time()) - int(existing.get("created") or 0)) <= 5:
                    return _public(existing)
            rule = {
                "id": uuid.uuid4().hex[:12],
                "name": str(p.get("name") or f"{title} 自动下载")[:60],
                "peer": peer,
                "peer_title": title,
                "kinds": kinds,
                "min_bytes": int(p.get("min_bytes") or 0),
                "interval": max(30, int(p.get("interval") or 300)),
                "outdir": str(p.get("outdir") or ""),
                "enabled": bool(p.get("enabled", True)),
                "created": int(time.time()),
                # 若接力历史已下载进度（immediate_catchup=True），在无活跃手工参数覆盖时允许后台轮询立即补齐差量
                "last_run": 0 if (immediate_catchup and "baseline_mode" in p) else int(time.time()),
                "last_id": baseline,
                "baseline_mode": bmode,
                "ok": 0,
                "fail": 0,
                "last_error": "",
                "last_job": "",
            }
            self.rules.append(rule)
            self._save()
            init_snap = _public(rule)
        if self.bus:
            self.bus.emit("log", {
                "level": "info", "category": "auto", "action": "auto.create", "peer": peer,
                "msg": f"创建自动下载规则「{rule['name']}」（基线消息 ID：#{baseline}，已下载最大 ID：#{dl_max}）",
            })
        if rule.get("enabled"):
            self.start()
        self._emit(rule)
        return init_snap

    def sync_baseline(self, peer: str, max_id: int) -> None:
        """当手动下载/批量下载/收藏转存完成时，自动推进同对话自动规则的 last_id（兼容 @用户名 与数字 ID）。"""
        if not peer or int(max_id or 0) <= 0:
            return
        cands = {str(peer).strip()}
        try:
            resolved = DB.resolve_chat_id(peer)
            if resolved:
                cands.add(str(resolved))
        except Exception:
            pass
        changed = False
        with self._lock:
            for r in self.rules:
                rp = str(r.get("peer") or "").strip()
                r_cands = {rp}
                try:
                    rr = DB.resolve_chat_id(rp)
                    if rr:
                        r_cands.add(str(rr))
                except Exception:
                    pass
                if (cands & r_cands) and int(max_id) > int(r.get("last_id") or 0):
                    r["last_id"] = int(max_id)
                    changed = True
            if changed:
                self._save()

    def update(self, p: dict) -> dict:
        rule = self._find(p.get("id"))
        for key in ("name", "peer_title", "outdir"):
            if key in p and p[key] is not None:
                rule[key] = str(p[key])
        if "interval" in p and p["interval"]:
            rule["interval"] = max(30, int(p["interval"]))
        if "min_bytes" in p and p["min_bytes"] is not None:
            rule["min_bytes"] = int(p["min_bytes"])
        if "kinds" in p and isinstance(p["kinds"], list):
            rule["kinds"] = [k for k in p["kinds"] if k in KINDS] or list(KINDS)
        if "enabled" in p:
            rule["enabled"] = bool(p["enabled"])
            if rule["enabled"]:
                self.start()
        self._save()
        self._emit(rule)
        return _public(rule)

    def delete(self, p: dict) -> dict:
        rule = self._find(p.get("id"))
        self.rules.remove(rule)
        self._save()
        if self.bus:
            self.bus.emit("auto", {"rule": None, "stats": self.stats()})
        return {"id": rule["id"]}

    def set_all(self, enabled: bool) -> dict:
        for rule in self.rules:
            rule["enabled"] = enabled
        self._save()
        if enabled:
            self.start()
        return self.stats()

    # -- 调度 --------------------------------------------------------------
    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = threading.Thread(target=self._loop, name="tgget-auto", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()

    def _loop(self) -> None:
        while not self._stop.is_set():
            try:
                self.tick()
            except Exception as exc:  # 单轮失败不应该打死线程
                self._err = f"{type(exc).__name__}: {exc}"
            self._stop.wait(10)

    def tick(self, force: bool = False) -> None:
        now = time.time()
        for rule in list(self.rules):
            if not rule.get("enabled") and not force:
                continue
            if rule["id"] in self._busy:
                continue
            if not force and now - float(rule.get("last_run") or 0) < rule["interval"]:
                continue
            self._busy.add(rule["id"])
            try:
                self._run(rule)
            except Exception as exc:
                rule["fail"] = int(rule.get("fail") or 0) + 1
                rule["last_error"] = f"{type(exc).__name__}: {exc}"
                if self.bus:
                    self.bus.emit("log", {"level": "error",
                                          "msg": f"自动化「{rule.get('name')}」执行失败：{rule['last_error']}"})
                self._emit(rule)
            finally:
                rule["last_run"] = int(time.time())
                self._busy.discard(rule["id"])
                self._save()
                self._emit(rule)

    def run_now(self, rule_id: str) -> dict:
        rule = self._find(rule_id)
        rule["last_id"] = 0  # 手动触发时重新扫一遍最近一批
        self.tick_one(rule)
        return _public(rule)

    def tick_one(self, rule: dict) -> None:
        self._busy.add(rule["id"])
        try:
            self._run(rule)
        except Exception as exc:
            # 手动「立即执行」失败时，把原因同时写进规则和日志，接口再把异常抛回去
            rule["fail"] = int(rule.get("fail") or 0) + 1
            rule["last_error"] = f"{type(exc).__name__}: {exc}"
            if self.bus:
                self.bus.emit("log", {"level": "error",
                                      "msg": f"自动化「{rule.get('name')}」执行失败：{rule['last_error']}"})
            raise
        finally:
            rule["last_run"] = int(time.time())
            self._busy.discard(rule["id"])
            self._save()
            self._emit(rule)

    def _run(self, rule: dict) -> None:
        last_id = int(rule.get("last_id") or 0)
        # 若刚创建时选了「包含历史全部媒体」（last_id == 0 且 baseline_mode == 'all'），走更大窗口扫描
        scan_limit = 200 if (last_id == 0 and rule.get("baseline_mode") == "all") else 80
        data = self.tg.messages(rule["peer"], limit=scan_limit, media_only=True)
        rows = data.get("rows") or []
        if not rows:
            rule["last_error"] = ""
            return
        top = max(int(r["id"]) for r in rows)
        last_id = int(rule.get("last_id") or 0)
        fresh = [r for r in rows if int(r["id"]) > last_id]
        fresh = [r for r in fresh if self._match(rule, r)]
        # 已经通过「我的收藏 → 转发归档」移入本地目录并登记索引的消息（local=True），
        # 直接推进游标跳过，不再新建一个空转的下载任务
        if not rule.get("outdir"):
            fresh = [r for r in fresh if not r.get("local")]
        if not fresh:
            if top > last_id:
                rule["last_id"] = top
            rule["last_error"] = ""
            return

        ids = [int(r["id"]) for r in fresh]
        job = self.dl.submit(rule["peer"], ids=ids, outdir=rule.get("outdir") or None)
        rule["last_job"] = job["job_id"]
        rule["ok"] = int(rule.get("ok") or 0) + len(ids)
        rule["last_error"] = ""
        top = max(int(r["id"]) for r in rows)
        rule["last_id"] = max(top, max(ids))
        if self.bus:
            self.bus.emit("log", {"level": "info",
                                  "msg": f"自动化「{rule['name']}」抓到 {len(ids)} 个新媒体，已排队"})

    @staticmethod
    def _match(rule: dict, row: dict) -> bool:
        kinds = {norm_kind(k) for k in (rule.get("kinds") or KINDS)}
        k = norm_kind(row.get("kind"))
        if k not in kinds:
            return False
        min_b = int(rule.get("min_bytes") or 0)
        if min_b > 0:
            sz = row.get("size")
            # 旧缓存照片若未拿到精确字节数（size=None），只要阈值不超过 20MB 不误杀
            if sz is None and k == "image":
                if min_b > 20 * 1024 * 1024:
                    return False
            elif int(sz or 0) < min_b:
                return False
        return True

    def _find(self, rule_id) -> dict:
        for rule in self.rules:
            if rule["id"] == str(rule_id):
                return rule
        raise KeyError(f"规则不存在：{rule_id}")


def _public(rule: dict) -> dict:
    out = dict(rule)
    out["busy"] = False
    return out
