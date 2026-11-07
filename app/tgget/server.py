"""本地控制中心：标准库 http.server，零额外依赖。

路由：
    GET  /                                控制台页面
    GET  /health                          健康检查
    GET  /api/system/status               状态
    GET  /api/events                      SSE 事件流
    GET  /api/media/thumb                 缩略图
    GET  /api/media/stream                在线播放（边下边播，支持 Range）
    POST /api/actions/execute             动作分发
"""

from __future__ import annotations

import json
import mimetypes
import os
import queue
import re
import threading
import time
import urllib.parse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from .api import Api
from .config import Config
from .jobs import EventBus
from .store import INDEX

WEB_DIR = Path(__file__).parent / "web"

# 缩略图会一屏并发几十个请求，全部打到 Telegram 容易触发 FLOOD_WAIT，限制同时 4 个
_THUMB_GATE = threading.Semaphore(4)


class Handler(BaseHTTPRequestHandler):
    server_version = "tgnas/0.9"
    protocol_version = "HTTP/1.1"

    # 由 create_server 注入
    api: Api
    bus: EventBus
    cfg: Config

    # -- 基础 --------------------------------------------------------------
    def log_message(self, fmt: str, *args) -> None:  # 静音访问日志
        pass

    def finish(self) -> None:
        try:
            super().finish()
        finally:
            # ThreadingHTTPServer 每个请求起一个临时线程，请求结束时主动关掉该线程的 SQLite 连接，防止句柄累积
            try:
                from .db import DB
                DB.close()
            except Exception:
                pass

    def _send(self, code: int, body: bytes = b"", ctype: str = "application/json; charset=utf-8",
              extra: dict | None = None) -> None:
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        extra = extra or {}
        # extra 里给了 Cache-Control 就用它，别同时发两个同名头（浏览器会当成两个值）
        if not any(k.lower() == "cache-control" for k in extra):
            self.send_header("Cache-Control", "no-store")
        for k, v in extra.items():
            self.send_header(k, v)
        self.end_headers()
        if body and self.command != "HEAD":
            self.wfile.write(body)

    def _json(self, obj, code: int = 200) -> None:
        self._send(code, json.dumps(obj, ensure_ascii=False, default=str).encode("utf-8"))

    def _auth_ok(self) -> bool:
        if not self.cfg.token:
            return True
        supplied = self.headers.get("X-TGGet-Token") or ""
        if not supplied:
            auth = self.headers.get("Authorization", "")
            if auth.lower().startswith("bearer "):
                supplied = auth[7:].strip()
        if not supplied:
            q = urllib.parse.parse_qs(urllib.parse.urlparse(self.path).query)
            supplied = (q.get("token") or [""])[0]
        if not supplied:
            cookie = self.headers.get("Cookie", "")
            for part in cookie.split(";"):
                k, _, v = part.strip().partition("=")
                if k == "tgget_token":
                    supplied = v
        return supplied == self.cfg.token

    # -- 方法 --------------------------------------------------------------
    def do_GET(self) -> None:
        parsed = urllib.parse.urlparse(self.path)
        path = urllib.parse.unquote(parsed.path)

        if path in ("/", "/index.html"):
            return self._serve_index()
        if path == "/health":
            return self._json({"ok": True, "ts": time.time()})

        if path.startswith("/api/") and not self._auth_ok():
            return self._json({"ok": False, "error": "需要访问令牌"}, 401)

        if path == "/api/system/status":
            return self._json({"ok": True, "data": self.api.system_status({})})
        if path == "/api/events":
            return self._serve_events()
        if path == "/api/task.list":
            return self._json({"ok": True, "data": {"jobs": self.api.dl.list()}})
        if path == "/api/media/thumb":
            return self._serve_thumb(parsed.query)
        if path == "/api/avatar":
            return self._serve_avatar(parsed.query)
        if path == "/api/media/stream":
            return self._serve_stream(parsed.query)
        return self._json({"ok": False, "error": "not found", "path": path}, 404)

    def do_POST(self) -> None:
        parsed = urllib.parse.urlparse(self.path)
        path = urllib.parse.unquote(parsed.path)
        length = int(self.headers.get("Content-Length") or 0)
        raw = self.rfile.read(length) if length else b"{}"

        if path == "/api/auth/login":
            try:
                payload = json.loads(raw or b"{}")
            except Exception:
                payload = {}
            if str(payload.get("token", "")) == self.cfg.token or not self.cfg.token:
                return self._send(200, b'{"ok":true}', extra={
                    "Set-Cookie": f"tgget_token={urllib.parse.quote(self.cfg.token)}; Path=/; HttpOnly; SameSite=Lax"
                })
            return self._json({"ok": False, "error": "令牌不正确"}, 401)

        if path.startswith("/api/") and not self._auth_ok():
            return self._json({"ok": False, "error": "需要访问令牌"}, 401)

        if path == "/api/actions/execute":
            try:
                payload = json.loads(raw or b"{}")
            except Exception as exc:
                return self._json({"ok": False, "error": f"请求体不是合法 JSON：{exc}"}, 400)
            action = str(payload.get("action", ""))
            return self._json(self.api.execute(action, payload.get("payload") or {}))
        return self._json({"ok": False, "error": "not found"}, 404)

    def do_HEAD(self) -> None:
        return self.do_GET()

    # -- 静态与媒体 --------------------------------------------------------
    def _serve_index(self) -> None:
        f = WEB_DIR / "index.html"
        if not f.exists():
            return self._send(500, b"web/index.html missing", "text/plain; charset=utf-8")
        body = f.read_bytes().replace(b"{{TOKEN_NEEDED}}", b"1" if self.cfg.token else b"0")
        self._send(200, body, "text/html; charset=utf-8")

    def _serve_thumb(self, query: str) -> None:
        """按 peer + msg_id 取 Telegram 内嵌缩略图，前端 <img> 直接引用。"""
        q = urllib.parse.parse_qs(query)
        peer = (q.get("peer") or [""])[0].strip()
        try:
            msg_id = int((q.get("id") or ["0"])[0])
        except (TypeError, ValueError):
            msg_id = 0
        if not peer or msg_id <= 0 or len(peer) > 80:
            return self._json({"ok": False, "error": "参数不完整"}, 400)
        # 只允许 t.me 链接、@用户名、纯数字 id 三种形态，避免把任意串塞给 get_entity
        if not re.fullmatch(r"@?[A-Za-z0-9_]{1,32}|-?\d{1,20}|https?://t\.me/\S{1,60}", peer):
            return self._json({"ok": False, "error": "peer 不合法"}, 400)

        try:
            with _THUMB_GATE:
                data = self.api.tg.thumb(peer, msg_id)
        except Exception as exc:
            return self._json({"ok": False, "error": str(exc)[:160]}, 404)
        if not data:
            return self._json({"ok": False, "error": "没有缩略图"}, 404)
        # Telegram 缩略图多为 JPEG，但贴纸可能是 WEBP，演示模式是 PNG —— 按真实字节给类型
        ctype = "image/jpeg"
        if data[:8] == b"\x89PNG\r\n\x1a\n":
            ctype = "image/png"
        elif data[:4] == b"RIFF" and data[8:12] == b"WEBP":
            ctype = "image/webp"
        elif data[:3] == b"GIF":
            ctype = "image/gif"
        self._send(200, data, ctype, extra={"Cache-Control": "public, max-age=604800"})

    def _serve_avatar(self, query: str) -> None:
        """对话头像：`<img src="/api/avatar?peer=...">` 直接引用。

        列表里可能有上百个对话，前端只在 `has_photo` 为真时才发请求，并且
        `loading="lazy"`：真正可见的才拉。头像体积很小，一次拉完就进磁盘缓存。
        """
        q = urllib.parse.parse_qs(query)
        peer = (q.get("peer") or [""])[0].strip()
        if not peer or len(peer) > 80:
            return self._json({"ok": False, "error": "参数不完整"}, 400)
        if not re.fullmatch(r"@?[A-Za-z0-9_]{1,32}|-?\d{1,20}|https?://t\.me/\S{1,60}", peer):
            return self._json({"ok": False, "error": "peer 不合法"}, 400)

        try:
            with _THUMB_GATE:
                data = self.api.tg.avatar(peer)
        except Exception as exc:
            return self._json({"ok": False, "error": str(exc)[:160]}, 404)
        if not data:
            # 没头像别让浏览器反复来问：短缓存一会儿
            return self._send(404, b'{"ok":false,"error":"no avatar"}',
                              extra={"Cache-Control": "public, max-age=600"})
        ctype = "image/jpeg"
        if data[:8] == b"\x89PNG\r\n\x1a\n":
            ctype = "image/png"
        self._send(200, data, ctype, extra={"Cache-Control": "public, max-age=86400"})

    def _serve_local_file(self, path: Path, download: bool = False) -> None:
        """把已经下载到磁盘的媒体直接发出去，支持 Range（播放器拖进度条）。"""
        try:
            size = path.stat().st_size
            fh = open(path, "rb")
        except OSError as exc:
            return self._json({"ok": False, "error": f"本地文件不可读：{exc}"}, 404)

        ctype = mimetypes.guess_type(path.name)[0] or "application/octet-stream"
        disp = "attachment" if download else "inline"
        cd = f"{disp}; filename*=UTF-8''{urllib.parse.quote(path.name)}"

        try:
            start, end, partial = 0, max(0, size - 1), False
            rng = self.headers.get("Range")
            if rng and rng.startswith("bytes="):
                try:
                    spec = rng[6:].split(",")[0]
                    s, _, e = spec.partition("-")
                    start = int(s) if s else 0
                    end = int(e) if e else size - 1
                    end = min(end, size - 1)
                    start = max(0, start)
                    if start > end or start >= size:
                        raise ValueError
                    partial = True
                except Exception:
                    self.send_response(416)
                    self.send_header("Content-Range", f"bytes */{size}")
                    self.send_header("Content-Length", "0")
                    self.end_headers()
                    return

            length = end - start + 1
            self.send_response(206 if partial else 200)
            self.send_header("Content-Type", ctype)
            self.send_header("Content-Length", str(length))
            self.send_header("Accept-Ranges", "bytes")
            if partial:
                self.send_header("Content-Range", f"bytes {start}-{end}/{size}")
            self.send_header("Content-Disposition", cd)
            self.send_header("Cache-Control", "private, max-age=60")
            self.end_headers()
            if self.command == "HEAD":
                return

            fh.seek(start)
            left = length
            while left > 0:
                chunk = fh.read(min(512 * 1024, left))
                if not chunk:
                    break
                self.wfile.write(chunk)
                left -= len(chunk)
        except (BrokenPipeError, ConnectionResetError, OSError):
            pass
        finally:
            try:
                fh.close()
            except Exception:
                pass
            self.close_connection = True

    def _serve_stream(self, query: str) -> None:
        """把 Telegram 上的媒体边下边播给浏览器（支持 Range，能拖进度条）。

        飞牛 NAS 上没法「用播放器打开文件」，所以播放必须走这里：
        `<video src="/api/media/stream?peer=..&id=..">` 直接流式转发，
        浏览器先缓冲一点就能开播，不用等整个文件落盘。
        """
        q = urllib.parse.parse_qs(query)
        peer = (q.get("peer") or [""])[0].strip()
        try:
            msg_id = int((q.get("id") or ["0"])[0])
        except (TypeError, ValueError):
            msg_id = 0
        if not peer or msg_id <= 0 or len(peer) > 80:
            return self._json({"ok": False, "error": "参数不完整"}, 400)
        if not re.fullmatch(r"@?[A-Za-z0-9_]{1,32}|-?\d{1,20}|https?://t\.me/\S{1,60}", peer):
            return self._json({"ok": False, "error": "peer 不合法"}, 400)
        want_download = (q.get("dl") or ["0"])[0] == "1"

        # 已经下载到本地的，直接读磁盘：秒开、不占带宽，拖进度条也不回源。
        # 先按调用方给的 peer 查（前端传的就是 chat_id，通常一次命中）；没命中
        # 再问一次媒体元信息，用其中的数字 chat_id 兜底 —— 从 t.me 链接或
        # @username 进来的 peer 字符串和索引键不一样，否则会白白重新拉一遍流。
        local = INDEX.get(peer, msg_id)
        meta0 = None
        if local is None:
            try:
                meta0 = self.api.tg.media_meta(peer, msg_id)
            except Exception:
                meta0 = None
            if meta0 and meta0.get("chat_id"):
                local = INDEX.get(str(meta0["chat_id"]), msg_id)
            # 若按 (chat_id, msg_id) 没查到，但本地库中已有同一 file_id 的其它消息文件，直接复用并自愈修复索引
            if local is None and meta0 and meta0.get("file_id"):
                fid_hit = INDEX.find_fid(str(meta0["file_id"]))
                if fid_hit is not None:
                    local = fid_hit
                    try:
                        INDEX.put(str(meta0.get("chat_id") or peer), msg_id, fid_hit, fid_hit.stat().st_size, str(meta0["file_id"]))
                    except Exception:
                        pass
        if local is not None:
            return self._serve_local_file(local, want_download)

        meta = meta0
        if meta is None:
            try:
                meta = self.api.tg.media_meta(peer, msg_id)
            except Exception as exc:
                return self._json({"ok": False, "error": str(exc)[:200]}, 404)

        ctype = (meta.get("mime") or mimetypes.guess_type(meta.get("filename") or "")[0]
                 or "application/octet-stream")
        size = int(meta.get("size") or 0)
        fname = meta.get("filename") or f"{msg_id}.bin"
        disp = "attachment" if want_download else "inline"
        cd = f"{disp}; filename*=UTF-8''{urllib.parse.quote(fname)}"

        # 照片体积小（或拿不到精确流式字节数），直接整体读进内存缓存返回
        if not size or meta.get("kind") in ("photo", "image"):
            try:
                data, mime = self.api.tg.media_bytes(peer, msg_id)
            except Exception as exc:
                return self._json({"ok": False, "error": str(exc)[:200]}, 404)
            return self._send(200, data, mime or ctype,
                              extra={"Content-Disposition": cd, "Cache-Control": "private, max-age=600"})

        start, end, partial = 0, size - 1, False
        rng = self.headers.get("Range")
        if rng and rng.startswith("bytes="):
            try:
                spec = rng[6:].split(",")[0]
                s, _, e = spec.partition("-")
                start = int(s) if s else 0
                end = int(e) if e else size - 1
                end = min(end, size - 1)
                start = max(0, start)
                if start > end or start >= size:
                    raise ValueError
                partial = True
            except Exception:
                self.send_response(416)
                self.send_header("Content-Range", f"bytes */{size}")
                self.send_header("Content-Length", "0")
                self.end_headers()
                return

        length = end - start + 1
        self.send_response(206 if partial else 200)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(length))
        self.send_header("Accept-Ranges", "bytes")
        if partial:
            self.send_header("Content-Range", f"bytes {start}-{end}/{size}")
        self.send_header("Content-Disposition", cd)
        self.send_header("Cache-Control", "private, max-age=3600")
        self.end_headers()
        if self.command == "HEAD":
            return

        stop = None
        try:
            gen, stop = self.api.tg.open_stream(peer, msg_id, start, length)
            for chunk in gen:
                self.wfile.write(chunk)
        except (BrokenPipeError, ConnectionResetError, OSError):
            pass  # 播放器拖进度条会主动断开，属于正常现象
        except Exception:
            pass
        finally:
            if stop is not None:
                stop.set()
            self.close_connection = True

    # -- SSE ---------------------------------------------------------------
    def _serve_events(self) -> None:
        q = self.bus.subscribe()
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream; charset=utf-8")
        self.send_header("Cache-Control", "no-cache")
        self.send_header("Connection", "keep-alive")
        self.send_header("X-Accel-Buffering", "no")
        self.end_headers()
        try:
            self.wfile.write(b"retry: 2000\n\n")
            self.wfile.flush()
            last_ping = time.time()
            while True:
                try:
                    evt = q.get(timeout=1.0)
                    data = json.dumps(evt, ensure_ascii=False, default=str)
                    self.wfile.write(f"data: {data}\n\n".encode("utf-8"))
                    self.wfile.flush()
                except queue.Empty:
                    if time.time() - last_ping > 15:
                        self.wfile.write(b": ping\n\n")
                        self.wfile.flush()
                        last_ping = time.time()
        except (BrokenPipeError, ConnectionResetError, OSError):
            pass
        finally:
            self.bus.unsubscribe(q)


def create_server(cfg: Config, api: Api, bus: EventBus) -> ThreadingHTTPServer:
    handler = type("BoundHandler", (Handler,), {"api": api, "bus": bus, "cfg": cfg})
    httpd = ThreadingHTTPServer((cfg.host, cfg.port), handler)
    httpd.daemon_threads = True
    return httpd


def serve(cfg: Config, api: Api, bus: EventBus, open_browser: bool = True) -> None:
    httpd = create_server(cfg, api, bus)
    url = f"http://{'127.0.0.1' if cfg.host in ('0.0.0.0', '') else cfg.host}:{cfg.port}/"
    print(f"[tgnas] 控制中心已启动：{url}")
    if cfg.host in ("0.0.0.0", ""):
        lan = _lan_ip()
        if lan and lan != "127.0.0.1":
            print(f"[tgnas] 局域网 / 手机访问：http://{lan}:{cfg.port}/")
    print(f"[tgnas] 下载目录：{cfg.download_path}")
    if cfg.token:
        print(f"[tgnas] 已启用访问令牌（打开页面后输入）")
    if open_browser and _has_display():
        threading.Timer(0.8, lambda: _open(url)).start()
    else:
        print("[tgnas] 无图形界面（NAS / 服务器），请用浏览器访问上面的地址")
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print("\n[tgnas] 正在退出…")
    finally:
        httpd.shutdown()


def _has_display() -> bool:
    """判断当前环境有没有图形界面。

    飞牛 NAS 上跑 `xdg-open` 只会失败并往日志里刷错误，所以先探测再决定要不要开浏览器。
    """
    import platform
    import sys

    system = platform.system()
    if system in ("Windows", "Darwin"):
        return True
    if not sys.platform.startswith("linux"):
        return False
    return bool(os.environ.get("DISPLAY") or os.environ.get("WAYLAND_DISPLAY"))


def _open(url: str) -> None:
    try:
        import webbrowser

        webbrowser.open(url)
    except Exception:
        pass


def _lan_ip() -> str:
    """探测本机局域网 IP，方便 NAS / 手机直接访问。"""
    import socket

    try:
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
            s.connect(("223.5.5.5", 80))
            return s.getsockname()[0]
    except Exception:
        return ""
