"""命令行入口。

    python -m tgget serve            # 启动控制台（默认）
    python -m tgget login            # 终端里扫码登录
    python -m tgget dialogs [关键词]
    python -m tgget ls <对话> [--limit 30]
    python -m tgget dl <链接或对话> [--ids 1,2,3] [--limit N] [-o 目录]
    python -m tgget doctor           # 自检
"""

from __future__ import annotations

import argparse
import asyncio
import sys
import time
from pathlib import Path

from . import __version__
from .api import Api
from .config import HOME, Config
from .jobs import Downloader, EventBus
from .tg import Telegram, human_size, parse_link


def _utf8_stdout() -> None:
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass


def build(cfg: Config, demo: bool = False):
    bus = EventBus()
    if demo:
        from .demo import DemoTelegram

        tg = DemoTelegram(bus.emit)
        print("[tgnas] 演示模式：不会连接 Telegram")
    else:
        tg = Telegram(cfg, bus.emit)
    # 演示模式的任务记录单独放一个文件：否则 --demo 建的假任务会混进
    # 真实账号的任务台（两个实例用的是同一份 jobs.json）
    dl = Downloader(tg, cfg, bus,
                    store_path=(HOME / "jobs-demo.json") if demo else None)
    api = Api(cfg, tg, bus, dl, time.time(), demo=demo)
    return tg, bus, dl, api


# ---------------------------------------------------------------- serve
def cmd_serve(args) -> int:
    from .server import serve

    cfg = Config.load(host=args.host, port=args.port, download_dir=args.dir, token=args.token)
    tg, bus, dl, api = build(cfg, demo=args.demo)
    if not args.demo and not (cfg.api_id and cfg.api_hash):
        print("[!] 尚未配置 api_id / api_hash，控制台能打开但无法登录。")
        print("    到 https://my.telegram.org → API development tools 创建应用，然后：")
        print("    python -m tgget config --set api_id=123456 --set api_hash=abcdef0123456789")
        print("    或者加 --demo 先看界面。\n")
    try:
        serve(cfg, api, bus, open_browser=not args.no_browser)
    finally:
        tg.close()
    return 0


# ---------------------------------------------------------------- login
def cmd_login(args) -> int:
    cfg = Config.load()
    tg, bus, dl, api = build(cfg)
    try:
        st = tg.state()
        if st.get("authorized"):
            print(f"已登录：{st['user']['name']}")
            return 0
        if args.phone:
            phone = args.phone
            tg.send_code(phone)
            code = input("验证码：").strip()
            r = tg.sign_in(phone, code)
            if r.get("need_password"):
                pwd = input("两步验证密码：").strip()
                r = tg.sign_in(phone, code, pwd)
            print("登录成功" if r.get("ok") else f"失败：{r.get('error')}")
            return 0 if r.get("ok") else 1

        print("正在获取二维码…")
        payload = tg.qr_start()
        url = payload.get("url", "")
        _print_qr_in_terminal(url)
        print(f"\n如果终端二维码显示不正常，直接用手机打开：{url}\n")
        deadline = time.time() + 120
        while time.time() < deadline:
            time.sleep(3)
            st = tg.qr_poll()
            if st.get("authorized"):
                print("登录成功")
                return 0
            print(".", end="", flush=True)
        print("\n超时，请重试")
        return 1
    finally:
        tg.close()


def _print_qr_in_terminal(text: str) -> None:
    """用半角块字符在终端画二维码。"""
    try:
        import qrcode

        qr = qrcode.QRCode(border=1)
        qr.add_data(text)
        qr.make()
        m = qr.get_matrix()
        # 两个竖排半块 → 一个字符，行数减半
        for y in range(0, len(m), 2):
            line = []
            for x in range(len(m[0])):
                top = m[y][x]
                bot = m[y + 1][x] if y + 1 < len(m) else False
                line.append("█" if top and bot else "▀" if top else "▄" if bot else " ")
            print("".join(line))
    except Exception as exc:
        print(f"(终端二维码生成失败：{exc})")


# ---------------------------------------------------------------- 数据
def cmd_dialogs(args) -> int:
    cfg = Config.load()
    tg, *_ = build(cfg)
    try:
        rows = tg.dialogs(limit=args.limit, query=args.query or "")
        for r in rows:
            print(f"{r['peer']:>18}  {r['kind']:<7}  {r['title']}"
                  + (f"  @{r['username']}" if r.get("username") else ""))
        print(f"\n共 {len(rows)} 个")
        return 0
    finally:
        tg.close()


def cmd_ls(args) -> int:
    cfg = Config.load()
    tg, *_ = build(cfg)
    try:
        d = tg.messages(args.peer, limit=args.limit, media_only=not args.all)
        for r in d["rows"]:
            print(f"{r['id']:>8}  {r['kind']:<7}  {human_size(r['size']):>10}  {r['filename']}")
        return 0
    finally:
        tg.close()


def cmd_dl(args) -> int:
    cfg = Config.load()
    if args.dir:
        cfg.download_dir = str(Path(args.dir).expanduser())
    tg, bus, dl, api = build(cfg)

    def on_evt(kind: str, payload: dict) -> None:
        if kind == "task":
            pct = payload.get("percent")
            bar = f"{pct:5.1f}%" if pct is not None else "  ?  "
            print(f"\r  {bar}  {human_size(payload.get('done')):>10}  {payload.get('filename','')[:48]}",
                  end="", flush=True)
        elif kind == "job" and payload.get("state") in ("done", "partial", "failed", "cancelled"):
            print(f"\n任务 {payload['job_id']}：{payload['state']} "
                  f"({payload.get('done',0)}/{payload.get('total',0)})")
            for f in payload.get("files", [])[-5:]:
                print(f"   · {f.get('name')} → {f.get('path') or f.get('error')}")

    bus._subs.add(_QueueShim(on_evt))
    try:
        peer, link_id = parse_link(args.target)
        ids = [int(x) for x in args.ids.split(",")] if args.ids else ([link_id] if link_id else [])
        job = dl.submit(peer, ids=ids, limit=args.limit, media_only=not args.all)
        print(f"任务 {job['job_id']} 已提交，下载到 {cfg.download_path}")
        # 等它跑完
        deadline = time.time() + args.timeout
        while time.time() < deadline:
            cur = dl.jobs.get(job["job_id"], {})
            if cur.get("state") in ("done", "partial", "failed", "cancelled"):
                return 0 if cur.get("state") == "done" else 2
            time.sleep(0.5)
        print("\n超时未完成，任务仍在后台继续（进程退出会中断）")
        return 3
    finally:
        tg.close()


class _QueueShim:
    """把回调包装成 EventBus 认识的 subscriber。"""

    def __init__(self, fn):
        self.fn = fn

    def put_nowait(self, evt: dict) -> None:
        self.fn(evt.get("type", ""), evt)


# ---------------------------------------------------------------- 其它
def cmd_config(args) -> int:
    cfg = Config.load()
    if not args.set:
        import json

        print(json.dumps(cfg.mask(), indent=2, ensure_ascii=False))
        return 0
    for pair in args.set:
        k, _, v = pair.partition("=")
        k, v = k.strip(), v.strip()
        if not hasattr(cfg, k):
            print(f"未知配置项：{k}")
            return 1
        setattr(cfg, k, int(v) if k in ("api_id", "port", "concurrency", "chunk_size") else v)
        print(f"{k} = {v}")
    cfg.save()
    print(f"已保存到 {Path.home() / '.tgget' / 'config.json'}")
    return 0


def cmd_doctor(args) -> int:
    cfg = Config.load()
    ok = True
    print(f"tgnas {__version__}")
    print(f"python {sys.version.split()[0]}  ({sys.executable})")
    try:
        import telethon

        print(f"telethon {telethon.__version__}")
    except Exception as exc:
        ok = False
        print(f"[x] telethon 导入失败：{exc}")
    try:
        import qrcode  # noqa: F401

        print("qrcode 已安装")
    except Exception as exc:
        print(f"[!] qrcode 未安装（终端二维码不可用）：{exc}")
    print(f"api_id：{'已配置 ' + str(cfg.api_id) if cfg.api_id else '[x] 未配置'}")
    print(f"api_hash：{'已配置' if cfg.api_hash else '[x] 未配置'}")
    if cfg.api_id and cfg.api_hash:
        tg, *_ = build(cfg)
        try:
            st = tg.state()
            print(f"连接：{'正常' if st.get('connected') else '失败'}")
            print(f"登录：{'是 ' + st['user']['name'] if st.get('authorized') else '否'}")
        except Exception as exc:
            ok = False
            print(f"[x] 连接失败：{exc}")
        finally:
            tg.close()
    print(f"下载目录：{cfg.download_path} （{'可写' if _writable(cfg.download_path) else '不可写'}）")
    return 0 if ok else 1


def _writable(p: Path) -> bool:
    try:
        t = p / ".tgget_write_test"
        t.write_text("x")
        t.unlink()
        return True
    except Exception:
        return False


def main(argv: list[str] | None = None) -> int:
    _utf8_stdout()
    parser = argparse.ArgumentParser(prog="tgnas", description="开源的 Telegram 文件下载器")
    sub = parser.add_subparsers(dest="cmd")

    p = sub.add_parser("serve", help="启动本地控制中心")
    p.add_argument("--host", default=None)
    p.add_argument("--port", type=int, default=None)
    p.add_argument("--dir", default=None, help="下载目录")
    p.add_argument("--token", default=None, help="访问令牌")
    p.add_argument("--demo", action="store_true", help="演示模式（假数据）")
    p.add_argument("--no-browser", action="store_true")
    p.set_defaults(fn=cmd_serve)

    p = sub.add_parser("login", help="登录 Telegram")
    p.add_argument("--phone", default=None, help="用手机号+验证码登录（默认扫码）")
    p.set_defaults(fn=cmd_login)

    p = sub.add_parser("dialogs", help="列出对话")
    p.add_argument("query", nargs="?", default="")
    p.add_argument("--limit", type=int, default=200)
    p.set_defaults(fn=cmd_dialogs)

    p = sub.add_parser("ls", help="列出对话里的文件")
    p.add_argument("peer")
    p.add_argument("--limit", type=int, default=30)
    p.add_argument("--all", action="store_true", help="包含非媒体消息")
    p.set_defaults(fn=cmd_ls)

    p = sub.add_parser("dl", help="下载")
    p.add_argument("target", help="链接 / @用户名 / 对话 id")
    p.add_argument("--ids", default="", help="消息 id，逗号分隔")
    p.add_argument("--limit", type=int, default=20, help="未指定 ids 时下载最近 N 个文件")
    p.add_argument("--all", action="store_true")
    p.add_argument("-o", "--dir", default=None)
    p.add_argument("--timeout", type=float, default=3600)
    p.set_defaults(fn=cmd_dl)

    p = sub.add_parser("config", help="查看/修改配置")
    p.add_argument("--set", action="append", metavar="KEY=VALUE")
    p.set_defaults(fn=cmd_config)

    p = sub.add_parser("doctor", help="自检")
    p.set_defaults(fn=cmd_doctor)

    args = parser.parse_args(argv)
    if not getattr(args, "fn", None):
        args = parser.parse_args(["serve"])
    return args.fn(args)


if __name__ == "__main__":
    raise SystemExit(main())
