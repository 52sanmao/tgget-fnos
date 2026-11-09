"""Telegram 连接管理、事件循环线程、内存级多连接池、智能重连与登录鉴权模块。"""

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

from .tg_utils import entity_title, parse_link


class LoopThread:
    """常驻后台事件循环。"""

    def __init__(self) -> None:
        self.loop = asyncio.new_event_loop()
        self._t = threading.Thread(target=self._run, name="tgget-loop", daemon=True)
        self._started = threading.Event()

    def _run(self) -> None:
        asyncio.set_event_loop(self.loop)
        self._started.set()
        self.loop.run_forever()

    def start(self) -> "LoopThread":
        self._t.start()
        self._started.wait(5)
        return self

    def call(self, coro, timeout: float = 120):
        return asyncio.run_coroutine_threadsafe(coro, self.loop).result(timeout)

    def submit(self, coro):
        return asyncio.run_coroutine_threadsafe(coro, self.loop)

    def spawn(self, coro) -> None:
        asyncio.run_coroutine_threadsafe(coro, self.loop)




class ConnMixin:
    def __init__(self, cfg: Config, emit: Callable[[str, dict], None]) -> None:
        self.cfg = cfg
        self.emit = emit
        self.loop = LoopThread().start()
        self.client: Optional[TelegramClient] = None
        self._qr = None
        self._qr_cache: dict = {}
        self._qr_need_pwd = False
        self._qr_task = None
        self._lock = threading.Lock()
        self._conn_lock: Optional[asyncio.Lock] = None
        self._dl_clients: list[TelegramClient] = []
        self._dl_pool_lock: Optional[asyncio.Lock] = None
        self._session_sql_lock = threading.RLock()
        self._last_rx_ts: float = time.time()
        self._state_cache: Optional[dict] = None
        self._state_ts: float = 0.0
        self._state_refreshing: bool = False
        self._meta_cache: dict[tuple, dict] = {}
        self._blob_cache: dict[tuple, tuple] = {}
        self._msg_cache: dict[tuple, tuple[float, Any, dict]] = {}
        # 连接风暴检测与硬重置熔断状态
        self._storm_count: int = 0          # 连续命中「会话层错乱」特征的次数
        self._storm_last_ts: float = 0.0    # 最近一次命中时间戳
        self._hard_reset_at: float = 0.0    # 最近一次硬重置时间戳（用于冷却）
        self._hard_reset_lock: Optional[asyncio.Lock] = None

    async def _call_rpc(self, coro, timeout: float = 15.0,
                        check_cancel: Optional[Callable[[], bool]] = None):
        """带超时保护 + 0.2s 协作/抢占双保险取消检查的 Telegram RPC 包装器。

        解决痛点：
        1. 代理半开或跨 DC 抖动时，get_entity / get_messages 等请求不再无限挂起 2 分钟以上，
           超过 timeout（默认 15s）立即取消并抛 TimeoutError 触发重连；
        2. 用户点击「停止」时，最多 0.2s 内立即打断正在等待的 await 网络请求，瞬间退出。
        """
        if check_cancel and check_cancel():
            try:
                coro.close()
            except Exception:
                pass
            raise asyncio.CancelledError("任务已取消")
        task = asyncio.ensure_future(coro)
        deadline = time.time() + float(timeout or 15.0)
        try:
            while not task.done():
                if check_cancel and check_cancel():
                    task.cancel()
                    raise asyncio.CancelledError("任务已取消")
                rem = deadline - time.time()
                if rem <= 0:
                    task.cancel()
                    raise TimeoutError(f"Telegram 请求响应超时（>{int(timeout)}s 无响应）")
                await asyncio.wait([task], timeout=min(0.2, rem))
            res = await task
            self._last_rx_ts = time.time()
            return res
        except asyncio.CancelledError:
            if not task.done():
                task.cancel()
            raise

    def _get_conn_lock(self) -> asyncio.Lock:
        lk = getattr(self, "_conn_lock", None)
        if lk is None:
            lk = asyncio.Lock()
            self._conn_lock = lk
        return lk

    def _harden_session(self, client: Any) -> None:
        """为 Telethon 的 SQLiteSession 开启 WAL + 30s busy_timeout + 线程锁包装，
        彻底解决转存任务与下载任务并发执行时 session.session 抛出 OperationalError: database is locked。
        """
        sess = getattr(client, "session", None)
        if sess is None:
            return
        conn = getattr(sess, "_conn", None)
        if conn is not None:
            try:
                conn.execute("PRAGMA journal_mode=WAL")
                conn.execute("PRAGMA synchronous=NORMAL")
                conn.execute("PRAGMA busy_timeout=30000")
            except Exception:
                pass
        if getattr(sess, "_tgnas_hardened", False):
            return
        sql_lock = getattr(self, "_session_sql_lock", None) or threading.RLock()
        self._session_sql_lock = sql_lock
        for mname in ("save", "process_entities", "get_input_entity", "get_entity_rows_by_id"):
            orig_fn = getattr(sess, mname, None)
            if callable(orig_fn):
                def _wrap(fn):
                    def _safe(*a, **kw):
                        for attempt in range(5):
                            try:
                                with sql_lock:
                                    return fn(*a, **kw)
                            except Exception as exc:
                                if "locked" in str(exc).lower() or "busy" in str(exc).lower():
                                    time.sleep(0.05 * (attempt + 1))
                                    continue
                                if fn.__name__ in ("save", "process_entities"):
                                    return None
                                raise
                        return None
                    return _safe
                try:
                    setattr(sess, mname, _wrap(orig_fn))
                except Exception:
                    pass
        try:
            setattr(sess, "_tgnas_hardened", True)
        except Exception:
            pass

    # -- 连接 --------------------------------------------------------------
    async def _ensure(self) -> TelegramClient:
        if self.client is not None and self.client.is_connected():
            return self.client
        async with self._get_conn_lock():
            if self.client is not None:
                if self.client.is_connected():
                    return self.client
                # 优先在原客户端实例上重连，避免丢弃实例后重复打开同一个 session.session 引发锁冲突
                try:
                    self._harden_session(self.client)
                    await asyncio.wait_for(self.client.connect(), timeout=15.0)
                    if self.client.is_connected():
                        return self.client
                except Exception:
                    try:
                        await self.client.disconnect()
                    except Exception:
                        pass
                self.client = None
            if not self.cfg.api_id or not self.cfg.api_hash:
                raise RuntimeError(
                    "缺少 api_id / api_hash。请到 https://my.telegram.org 创建应用后填入配置。"
                )
            kwargs: dict[str, Any] = {}
            if self.cfg.proxy:
                try:
                    kwargs["proxy"] = self._parse_proxy(self.cfg.proxy)
                except Exception as exc:
                    raise RuntimeError(f"代理地址无效：{exc}") from exc
            if self.cfg.session_string:
                client = TelegramClient(
                    StringSession(self.cfg.session_string), self.cfg.api_id, self.cfg.api_hash, **kwargs
                )
            else:
                Path(self.cfg.session).parent.mkdir(parents=True, exist_ok=True)
                client = TelegramClient(self.cfg.session, self.cfg.api_id, self.cfg.api_hash, **kwargs)
            self._harden_session(client)
            await asyncio.wait_for(client.connect(), timeout=15.0)
            self._harden_session(client)
            self.client = client
        self.emit("state", await self._state())
        return client

    async def _pick_dl_client(self, slot: int = 0) -> TelegramClient:
        """多连接并发下载池：主连接之外，使用内存级 StringSession 派生独立 TCP 下载连接（receive_updates=False）。

        优势：
        1. 突破单条 MTProto TCP 连接的窗口与限速瓶颈，多文件/多分片真正走多条独立套接字并行拉取；
        2. 辅助连接全部使用纯内存 StringSession，绝不读写磁盘上的 session.session，零锁冲突。
        """
        main_client = await self._ensure()
        if slot <= 0 or not getattr(self.cfg, "api_id", 0) or not getattr(self.cfg, "api_hash", ""):
            return main_client
        max_aux = max(0, min(6, int(getattr(self.cfg, "dl_connections", 3) or 3)))
        if max_aux <= 0:
            return main_client

        pool_lock = getattr(self, "_dl_pool_lock", None)
        if pool_lock is None:
            pool_lock = asyncio.Lock()
            self._dl_pool_lock = pool_lock
        dl_clients = getattr(self, "_dl_clients", None)
        if dl_clients is None:
            dl_clients = []
            self._dl_clients = dl_clients

        alive = [c for c in dl_clients if c is not None and c.is_connected()]
        dl_clients[:] = alive
        if len(dl_clients) < max_aux and not getattr(self, "_dl_warming", False):
            self._dl_warming = True

            async def _warm_one() -> None:
                try:
                    async with pool_lock:
                        if len(dl_clients) >= max_aux:
                            return
                        try:
                            s_str = self.cfg.session_string or StringSession.save(getattr(main_client, "session", None))
                        except Exception:
                            s_str = ""
                        if not (s_str and isinstance(s_str, str) and len(s_str) > 16):
                            return
                        kwargs: dict[str, Any] = {"receive_updates": False}
                        if self.cfg.proxy:
                            try:
                                kwargs["proxy"] = self._parse_proxy(self.cfg.proxy)
                            except Exception:
                                kwargs = {"receive_updates": False}
                        aux = TelegramClient(
                            StringSession(s_str), self.cfg.api_id, self.cfg.api_hash, **kwargs
                        )
                        await asyncio.wait_for(aux.connect(), timeout=8.0)
                        if aux.is_connected():
                            dl_clients.append(aux)
                except Exception:
                    pass
                finally:
                    self._dl_warming = False

            try:
                asyncio.ensure_future(_warm_one())
            except Exception:
                self._dl_warming = False

        pool = [main_client] + [c for c in dl_clients if c is not None and c.is_connected()]
        return pool[slot % len(pool)]

    @staticmethod
    def _parse_proxy(url: str) -> dict:
        """把 socks5://user:pass@host:port 解析成 Telethon 需要的 dict。

        Telethon 只认 tuple/dict，不接受 URL 字符串，传错会抛
        `Proxy of unknown format`。
        """
        from urllib.parse import unquote, urlparse

        u = url.strip()
        if not u:
            return {}
        if "://" not in u:
            u = "socks5://" + u
        p = urlparse(u)
        scheme = (p.scheme or "socks5").lower()
        if scheme not in ("socks5", "socks4", "http"):
            raise RuntimeError(f"不支持的代理协议：{scheme}（可用 socks5 / socks4 / http）")
        if not p.hostname or not p.port:
            raise RuntimeError("代理地址缺少主机或端口")
        if scheme.startswith("socks"):
            try:
                import python_socks  # noqa: F401
            except ImportError as exc:
                raise RuntimeError("使用 socks 代理需要先安装 python-socks") from exc
        return {
            "proxy_type": scheme,
            "addr": p.hostname,
            "port": int(p.port),
            "username": unquote(p.username) if p.username else None,
            "password": unquote(p.password) if p.password else None,
            "rdns": True,
        }

    async def _state(self) -> dict:
        if self.client is None:
            try:
                await asyncio.wait_for(self._ensure(), timeout=6.0)
            except Exception as exc:
                st_err = {"connected": False, "authorized": False, "error": f"{type(exc).__name__}: {exc}"}
                self._state_cache = st_err
                self._state_ts = time.time()
                return st_err
        try:
            authorized = await asyncio.wait_for(self.client.is_user_authorized(), timeout=5.0)
            me = await asyncio.wait_for(self.client.get_me(), timeout=5.0) if authorized else None
            self._last_rx_ts = time.time()
        except Exception as exc:
            # 若已有登录缓存但当前网络短暂拥塞超时，保留已登录用户信息并仅更新连接状态，不阻塞前端
            prev = getattr(self, "_state_cache", None)
            # 连接风暴的「静默」表现：主连接 TCP 看似正常，但 mtproto 会话层错乱，
            # 表现为 is_user_authorized / get_me 连续超时（TimeoutError）。此时软重连无效，
            # 需累计风暴并在达到阈值后硬重置重建实例。
            if isinstance(exc, (TimeoutError, asyncio.TimeoutError)):
                if self._record_storm(exc) >= 3:
                    try:
                        await asyncio.wait_for(self._hard_reset(), timeout=20.0)
                        return await self._state()
                    except Exception:
                        pass
            if prev and prev.get("authorized"):
                return {**prev, "connected": bool(self.client and self.client.is_connected())}
            if not self.client.is_connected():
                try:
                    await asyncio.wait_for(self.client.connect(), timeout=5.0)
                    authorized = await asyncio.wait_for(self.client.is_user_authorized(), timeout=5.0)
                    me = await asyncio.wait_for(self.client.get_me(), timeout=5.0) if authorized else None
                    self._last_rx_ts = time.time()
                except Exception as exc2:
                    return {"connected": self.client.is_connected(), "authorized": False,
                            "error": f"{type(exc2).__name__}: {exc2}"}
            else:
                return {"connected": self.client.is_connected(), "authorized": False,
                        "error": f"{type(exc).__name__}: {exc}"}
        res = {
            "connected": self.client.is_connected(),
            "authorized": authorized,
            "user": (
                {
                    "id": me.id,
                    "name": " ".join(filter(None, [me.first_name, me.last_name])) or me.username,
                    "username": me.username,
                    "phone": me.phone,
                }
                if me
                else None
            ),
        }
        self._state_cache = res
        self._state_ts = time.time()
        return res

    async def _refresh_state_bg(self) -> None:
        if getattr(self, "_state_refreshing", False):
            return
        self._state_refreshing = True
        try:
            await self._state()
        except Exception:
            pass
        finally:
            self._state_refreshing = False

    def state(self, fast: bool = True) -> dict:
        """获取 Telegram 连接与登录状态。
        fast=True（供 /api/system/status 高频轮询使用）时：若内存已有缓存，0ms 直接返回，
        绝不阻塞 HTTP 线程或等待被慢任务占用的事件循环；后台异步刷新状态缓存。
        """
        cached = getattr(self, "_state_cache", None)
        if fast and cached is not None:
            now = time.time()
            if now - float(getattr(self, "_state_ts", 0.0) or 0.0) > 20.0:
                try:
                    self.loop.spawn(self._refresh_state_bg())
                except Exception:
                    pass
            return {
                **cached,
                "connected": bool(self.client is not None and self.client.is_connected()),
            }
        try:
            return self.loop.call(self._state(), timeout=6.0)
        except Exception as exc:
            if cached is not None:
                return {**cached, "connected": bool(self.client is not None and self.client.is_connected())}
            return {"connected": False, "authorized": False, "error": f"状态查询超时：{exc}"}

    # -- 登录：二维码 ------------------------------------------------------
    def qr_start(self) -> dict:
        return self.loop.call(self._qr_start())

    async def _qr_start(self) -> dict:
        client = await self._ensure()
        if await client.is_user_authorized():
            return {"authorized": True, **(await self._state())}
        self._qr_need_pwd = False
        self._qr = await client.qr_login()
        self._qr_cache = await self._qr_payload()
        self._qr_restart()
        return dict(self._qr_cache)

    def _qr_restart(self) -> None:
        """后台常驻监听扫码事件。

        关键：`QRLogin.wait()` 内部是「注册 UpdateLoginToken 事件处理器 → 等事件」。
        如果只在 HTTP 轮询里调用，两次轮询之间就有一段没人监听的窗口，
        用户恰好在这段时间扫码，事件会丢，页面就一直卡在二维码上。
        所以这里起一个常驻任务，全程挂着监听，HTTP 只读缓存。
        """
        task = getattr(self, "_qr_task", None)
        if task and not task.done():
            task.cancel()
        self._qr_task = asyncio.ensure_future(self._qr_watch())

    async def _qr_watch(self) -> None:
        while True:
            qr = self._qr
            if qr is None:
                return
            try:
                await qr.wait()  # 不设超时：一直守到扫码或 token 过期
            except SessionPasswordNeededError:
                self._qr_need_pwd = True
                self.emit("log", {"level": "info", "msg": "账号开启了两步验证，等待密码"})
                self.emit("qr", {"need_password": True})
                return
            except asyncio.TimeoutError:
                pass  # token 过期，下面重建
            except Exception as exc:
                self.emit("log", {"level": "error", "msg": f"扫码等待中断：{exc}"})

            client = self.client
            if client is not None:
                try:
                    if await client.is_user_authorized():
                        self.emit("state", await self._state())
                        self.emit("qr", {"authorized": True})
                        return
                except Exception:
                    return

            try:
                await qr.recreate()
                self._qr_cache = await self._qr_payload()
                self.emit("qr", {**self._qr_cache, "refreshed": True})
                self.emit("log", {"level": "info", "msg": "二维码已刷新"})
            except Exception as exc:
                self.emit("log", {"level": "error", "msg": f"二维码刷新失败：{exc}"})
                return

    async def _qr_payload(self) -> dict:
        qr = self._qr
        if qr is None:
            return {"authorized": False, "error": "尚未开始二维码登录"}
        svg = ""
        try:
            import io

            import qrcode
            import qrcode.image.svg as svgimg

            img = qrcode.make(qr.url, image_factory=svgimg.SvgPathImage, box_size=10)
            b = io.BytesIO()
            img.save(b)
            svg = b.getvalue().decode("utf-8")
        except Exception:
            svg = ""
        expires = getattr(qr, "expires", None)
        return {
            "authorized": False,
            "url": qr.url,
            "expires": expires.isoformat() if expires else None,
            "svg": svg,
            "hint": "手机 Telegram → 设置 → 设备 → 扫描二维码登录",
        }

    def qr_poll(self) -> dict:
        return self.loop.call(self._qr_poll())

    async def _qr_poll(self) -> dict:
        """只读缓存，不阻塞 —— 真正的等待在 _qr_watch 里。"""
        client = self.client
        if client is None:
            return {"authorized": False, "need_restart": True}
        try:
            if await client.is_user_authorized():
                st = await self._state()
                st["authorized"] = True
                return st
        except Exception:
            pass
        if self._qr_need_pwd:
            return {"authorized": False, "need_password": True}
        if not self._qr_cache:
            return {"authorized": False, "need_restart": True}
        return dict(self._qr_cache)

    def qr_password(self, password: str) -> dict:
        """扫码后账号开了两步验证：用密码补齐登录。"""
        return self.loop.call(self._qr_password(password))

    async def _qr_password(self, password: str) -> dict:
        client = await self._ensure()
        try:
            await client.sign_in(password=password)
        except PasswordHashInvalidError:
            return {"ok": False, "error": "两步验证密码不正确"}
        except Exception as exc:
            return {"ok": False, "error": f"{type(exc).__name__}: {exc}"}
        st = await self._state()
        st["ok"] = bool(st.get("authorized"))
        return st

    # -- 登录：手机号 ------------------------------------------------------
    def send_code(self, phone: str) -> dict:
        return self.loop.call(self._send_code(phone))

    async def _send_code(self, phone: str) -> dict:
        client = await self._ensure()
        sent = await client.send_code_request(phone)
        return {"ok": True, "type": type(sent.type).__name__, "hint": "验证码已发送"}

    def sign_in(self, phone: str, code: str, password: str = "") -> dict:
        return self.loop.call(self._sign_in(phone, code, password))

    async def _sign_in(self, phone: str, code: str, password: str) -> dict:
        client = await self._ensure()
        try:
            await client.sign_in(phone=phone, code=code.strip())
        except SessionPasswordNeededError:
            if not password:
                return {"ok": False, "need_password": True}
            await client.sign_in(password=password)
        except PhoneCodeInvalidError:
            return {"ok": False, "error": "验证码不正确"}
        except PasswordHashInvalidError:
            return {"ok": False, "error": "两步验证密码不正确"}
        return {"ok": True, **(await self._state())}

    def logout(self) -> dict:
        return self.loop.call(self._logout())

    async def _logout(self) -> dict:
        if self.client is not None:
            try:
                await self.client.log_out()
            except Exception:
                pass
            await self.client.disconnect()
            self.client = None
        return {"ok": True}

    # -- 数据 --------------------------------------------------------------
    def reconnect(self) -> dict:
        """改了代理之后重建连接（会话文件不变，不需要重新登录）。"""
        return self.loop.call(self._reconnect())

    async def _reconnect(self) -> dict:
        for aux in list(getattr(self, "_dl_clients", []) or []):
            try:
                await aux.disconnect()
            except Exception:
                pass
        if hasattr(self, "_dl_clients") and self._dl_clients is not None:
            self._dl_clients.clear()
        if self.client is not None:
            try:
                await self.client.disconnect()
            except Exception:
                pass
            self.client = None
        self._qr = None
        return await self._state()

    async def _retry_disconnected(self, fn):
        """执行操作协程；若因连接断开或 TCP 半开超时失败，强制重连后重试一次。"""
        try:
            return await fn()
        except asyncio.CancelledError:
            raise
        except (ConnectionError, TimeoutError, asyncio.TimeoutError) as exc:
            if isinstance(exc, ConnectionError) and "disconnect" not in str(exc).lower() and "reset" not in str(exc).lower():
                raise
            # 命中会话层错乱特征则累计风暴，达到阈值升级为硬重置
            if self._is_session_storm_err(exc):
                if self._record_storm(exc) >= 3:
                    await self._hard_reset()
                else:
                    await self._force_reconnect(preserve_main=False)
            else:
                await self._force_reconnect(preserve_main=False)
            return await fn()


    # -- 内部 --------------------------------------------------------------
    async def _entity(self, client, ref: str):
        """把 @用户名 / 数字 id / t.me 链接统一解析成 Telethon 实体。
        针对从未在当前会话打开过的私有受限频道链接（https://t.me/c/...），若直接 get_entity
        找不到 access_hash，则自动遍历一次对话列表预热实体缓存后再解析。
        """
        ref, _ = parse_link(ref)
        target = int(ref) if re.fullmatch(r"-?\d+", ref) else ref
        try:
            return await asyncio.wait_for(client.get_entity(target), timeout=12.0)
        except ValueError:
            if hasattr(client, "iter_dialogs"):
                try:
                    # 裸 iter_dialogs 网络半开时会挂起；用 wait_for 套 10s 超时（同 jobs.py/tg_media.py 策略），
                    # 超时抛 TimeoutError，交给外层 _retry_disconnected 重连重试，绝不无限阻塞。
                    async def _scan():
                        async for d in client.iter_dialogs(limit=200):
                            ent = getattr(d, "entity", None)
                            if ent is not None and isinstance(target, int):
                                try:
                                    if utils.get_peer_id(ent) == target or getattr(ent, "id", None) == abs(target):
                                        return ent
                                except Exception:
                                    pass
                    found = await asyncio.wait_for(_scan(), timeout=10.0)
                    if found is not None:
                        return found
                except Exception:
                    pass
            return await client.get_entity(target)

    async def _refresh_message(self, message, peer: str, msg_id: int):
        """大批量下载排队久了 file_reference 会过期，重新取一次消息即可刷新引用。"""
        try:
            client = await self._ensure()
            target = getattr(message, "peer_id", None) or (await self._entity(client, peer))
            return await client.get_messages(target, ids=int(msg_id))
        except Exception:
            return None


    @staticmethod
    def _is_recoverable_net_err(exc: BaseException) -> bool:
        msg = str(exc).lower()
        if "database is locked" in msg or "database is busy" in msg:
            return True
        if isinstance(exc, (ConnectionError, TimeoutError, asyncio.TimeoutError, OSError)):
            # 排除本地磁盘满等文件系统错误
            if isinstance(exc, OSError) and getattr(exc, "errno", None) in (28, 13):  # ENOSPC, EACCES
                return False
            return True
        return "disconnect" in msg or "timeout" in msg or "connection" in msg or "reset" in msg

    async def _force_reconnect(self, preserve_main: bool = False) -> None:
        """智能重连：
        1. 先清理内存级辅助下载连接池 (_dl_clients)；
        2. 判断主连接是否真的处于活跃收包状态：
           - 若 preserve_main=True 且主连接在最近 8 秒内收到过数据（说明其它任务/分片正在正常走流），则不动主连接；
           - 若 preserve_main=False，或主连接已超过 8 秒没有任何数据进账（TCP 半开假死，哪怕 is_connected() 仍返回 True），
             也必须强制执行 disconnect() + connect() 重建底层套接字，彻底破除 0% 假死！
        """
        pool_lock = getattr(self, "_dl_pool_lock", None)
        if pool_lock is not None:
            async with pool_lock:
                dl_clients = getattr(self, "_dl_clients", None)
                if dl_clients:
                    for c in dl_clients:
                        try:
                            await asyncio.wait_for(c.disconnect(), timeout=2.0)
                        except Exception:
                            pass
                    dl_clients.clear()

        now = time.time()
        last_rx = float(getattr(self, "_last_rx_ts", 0.0) or 0.0)
        if preserve_main and (now - last_rx) < 8.0 and self.client is not None and self.client.is_connected():
            return

        async with self._get_conn_lock():
            # 若刚被另一个并发 worker 重连成功过（3 秒内），直接复用，不重复断连
            if preserve_main and (time.time() - float(getattr(self, "_last_rx_ts", 0.0) or 0.0)) < 3.0:
                if self.client is not None and self.client.is_connected():
                    return
            if self.client is not None:
                try:
                    self._harden_session(self.client)
                    await asyncio.wait_for(self.client.disconnect(), timeout=4.0)
                except Exception:
                    pass
                try:
                    await asyncio.wait_for(self.client.connect(), timeout=10.0)
                    if self.client.is_connected():
                        self._last_rx_ts = time.time()
                        return
                except Exception:
                    pass
                self.client = None
        await self._ensure()
        self._last_rx_ts = time.time()

    # -- 连接风暴检测与硬重置 ----------------------------------------------
    def _is_session_storm_err(self, exc: BaseException) -> bool:
        """判断异常是否属于「mtproto 会话层错乱」特征（软重连治不好的那种）。

        典型：wrong session ID / very old message / Too many messages had to be ignored。
        这类错误来自 _sender 与 _message_box 的内存态错乱，单纯的 disconnect()+connect()
        无法清除，必须重建整个 TelegramClient 实例才能归零。
        """
        msg = str(exc).lower()
        return any(k in msg for k in (
            "wrong session id",
            "very old message",
            "too many messages",
            "security error",
            "ignored consecutively",
        ))

    def _record_storm(self, exc: Optional[BaseException] = None) -> int:
        """记录一次连接风暴命中，返回当前连续命中次数。

        策略：在短时间内（60s 窗口内）连续命中会话层错乱特征，计数递增；
        超过阈值则触发硬重置。命中风暴后进入冷却，避免风暴期间反复硬重置。
        """
        now = time.time()
        # 冷却窗口内（硬重置后 30s）不再累计，避免刚重置又立刻误判风暴
        if now - float(getattr(self, "_hard_reset_at", 0.0) or 0.0) < 30.0:
            return 0
        last = float(getattr(self, "_storm_last_ts", 0.0) or 0.0)
        if exc is not None and self._is_session_storm_err(exc):
            # 60s 窗口内的连续命中才累加，超过窗口则从头计
            if now - last <= 60.0:
                self._storm_count += 1
            else:
                self._storm_count = 1
        elif now - last > 60.0:
            # 长时间无风暴，计数自然衰减
            self._storm_count = 0
        self._storm_last_ts = now
        return self._storm_count

    async def _hard_reset(self) -> bool:
        """硬重置：彻底重建主 TelegramClient 实例，清掉 _sender/_message_box 会话层错乱状态。

        与 _force_reconnect（同一实例上 disconnect+connect）不同，这里：
        1. 断开并丢弃旧实例（含错乱的 _sender / _message_box / _connection）；
        2. 用 session 文件（或 session_string）new 一个全新实例，内部状态归零；
        3. 重新 connect()。

        返回是否成功。session 文件本身不动，登录态（auth_key）保留，无需重新扫码。
        """
        lock = getattr(self, "_hard_reset_lock", None)
        if lock is None:
            lock = asyncio.Lock()
            self._hard_reset_lock = lock
        async with lock:
            # 1. 清理辅助下载连接池（都是内存 StringSession，直接丢弃）
            pool_lock = getattr(self, "_dl_pool_lock", None)
            if pool_lock is not None:
                async with pool_lock:
                    dl_clients = getattr(self, "_dl_clients", None)
                    if dl_clients:
                        for c in dl_clients:
                            try:
                                await asyncio.wait_for(c.disconnect(), timeout=2.0)
                            except Exception:
                                pass
                        dl_clients.clear()

            # 2. 丢弃旧主实例
            async with self._get_conn_lock():
                old = self.client
                self.client = None
                if old is not None:
                    try:
                        await asyncio.wait_for(old.disconnect(), timeout=4.0)
                    except Exception:
                        pass
                    # 显式释放内部引用，帮助 GC 尽快回收错乱状态
                    for attr in ("_sender", "_connection", "_message_box"):
                        try:
                            setattr(old, attr, None)
                        except Exception:
                            pass

            # 3. 重建全新实例（复用 _ensure 的构建逻辑，但强制跳过旧实例缓存）
            try:
                await self._ensure()
            except Exception:
                self._hard_reset_at = time.time()
                return False
            self._hard_reset_at = time.time()
            self._storm_count = 0
            self._storm_last_ts = 0.0
            self._last_rx_ts = time.time()
            self.emit("log", {
                "level": "info", "category": "conn",
                "msg": "检测到连接风暴，已硬重置 Telegram 连接（重建实例）",
            })
            return True

    async def _force_reconnect_with_storm_guard(self, preserve_main: bool = False) -> None:
        """带风暴熔断的重连：优先走轻量软重连；连续命中会话层错乱时升级为硬重置。

        调用方（下载/转存等长连接任务）在捕获到网络错误后调用本方法替代裸 _force_reconnect，
        即可在「正常抖动」与「连接风暴」之间自动切换，避免无限软重试的死循环。
        """
        if self._record_storm() >= 3:
            await self._hard_reset()
            return
        await self._force_reconnect(preserve_main=preserve_main)

    # -- 工具 --------------------------------------------------------------
    def close(self) -> None:
        try:
            if self.client is not None:
                self.loop.call(self.client.disconnect(), timeout=10)
        except Exception:
            pass

