"""配置加载。

优先级：命令行参数 > 环境变量 > 配置文件 > 默认值
配置文件：~/.tgget/config.json（飞牛 fnOS 下为 $TRIM_PKGVAR/config.json）
"""

from __future__ import annotations

import json
import os
from dataclasses import asdict, dataclass, field
from pathlib import Path


def _default_home() -> Path:
    # 飞牛 fnOS 原生应用：数据根目录由 TG_HOME 环境变量注入（cmd/main 里
    # export TG_HOME="$TRIM_PKGVAR"），跨重启保留；否则回落到 ~/.tgget。
    env = os.environ.get("TG_HOME")
    if env:
        return Path(env)
    return Path.home() / ".tgget"


def _default_download_dir() -> Path:
    # 飞牛 fnOS：最优先落到「用户在应用设置里授权的目录」（TRIM_DATA_ACCESSIBLE_PATHS，
    # 用 : 分隔，取第一个）——用户通过系统文件选择器浏览选中的文件夹，下载内容
    # 直接落在用户指定位置，而不是困在应用内部。
    accessible = os.environ.get("TRIM_DATA_ACCESSIBLE_PATHS")
    if accessible:
        first = accessible.split(":")[0].strip()
        if first:
            return Path(first)
    # 其次落到 config/resource 声明的共享目录（TRIM_DATA_SHARE_PATHS，: 分隔取第一个），
    # 这样下载结果在文件管理器里直接可见。
    share = os.environ.get("TRIM_DATA_SHARE_PATHS")
    if share:
        first = share.split(":")[0].strip()
        if first:
            return Path(first) / "downloads"
    old = Path.home() / "Downloads" / "tgget"
    return old if old.exists() else (Path.home() / "Downloads" / "tgnas")


HOME = _default_home()
CONFIG_FILE = HOME / "config.json"


@dataclass
class Config:
    # --- Telegram 应用凭据 -------------------------------------------------
    # 必须自己到 https://my.telegram.org 申请（免费，1 分钟）。任何第三方
    # 客户端都绕不过这一关，官方 API 就是这么设计的。
    api_id: int = 0
    api_hash: str = ""
    # 会话文件（登录一次后可重复使用）
    session: str = str(HOME / "session")
    # 已有 session string（可在另一台机器登录后导出）
    session_string: str = ""
    # 可选：socks5://user:pass@127.0.0.1:7890
    proxy: str = ""

    # --- 下载 --------------------------------------------------------------
    download_dir: str = field(default_factory=lambda: str(_default_download_dir()))
    # 用户是否在网页「设置」中显式指定过保存目录（为 True 时跨重启严格保留，不被默认共享目录覆盖）
    custom_dir: bool = False
    # 同时下载几个文件（配合多连接池与多路分片）
    concurrency: int = 5
    # 单文件分块大小（断点续传的最小粒度）
    chunk_size: int = 512 * 1024
    # 单个大文件用几路并行分片流下载（默认 8 路，最高支持 16 路）
    segments: int = 8
    # 额外创建的内存级并发下载连接数（使用 StringSession 独立建连，不碰 SQLite 会话文件）
    dl_connections: int = 3
    # 是否已完成 v0.9.1 并发参数自动提速迁移
    turbo_v091: bool = False
    # 重名文件处理：rename / skip / overwrite
    on_conflict: str = "rename"
    # 落盘时按方案 C（原始发布日期_消息ID.ext）命名，关掉则沿用 Telegram 上的原始文件名。
    rename_by_date: bool = True
    # 内容级去重：同一个文件被转发到别的对话/别的消息（file_id 相同）时，
    # 不再从 Telegram 下一遍，直接在目标目录建硬链接（跨盘退回复制）。
    dedupe_content: bool = True
    # 失败自动重试：任务失败 / 部分失败时，自动重试没下成功的文件，直到全部成功或用户手动停止。
    auto_retry: bool = False
    # 「我的收藏」一键转发归档的目标群组 ID（用数字 chat_id，群改名也不受影响）
    archive_peer: str = ""
    archive_title: str = ""

    # --- 控制中心 ----------------------------------------------------------
    # 默认监听 0.0.0.0，方便飞牛 NAS 部署或局域网手机直接访问
    host: str = "0.0.0.0"
    port: int = 64780
    # 访问令牌；为空表示本机免验证。绑定 0.0.0.0 时强烈建议设置。
    token: str = ""

    @property
    def download_path(self) -> Path:
        p = Path(self.download_dir).expanduser()
        p.mkdir(parents=True, exist_ok=True)
        return p

    # ----------------------------------------------------------------------
    @classmethod
    def load(cls, **overrides) -> "Config":
        data: dict = {}
        from_disk = False
        if CONFIG_FILE.exists():
            try:
                data = json.loads(CONFIG_FILE.read_text(encoding="utf-8"))
                from_disk = True
            except Exception as exc:  # 配置损坏不应该阻止启动
                print(f"[warn] 配置文件无法解析，忽略：{exc}")

        # 旧版默认 concurrency=3/4、segments=4 在高延迟代理下跑不满带宽；
        # 首次升级到 v0.9.1 时自动提升到 concurrency=5, segments=8 并落盘记录。
        if from_disk and not data.get("turbo_v091"):
            if int(data.get("concurrency") or 0) <= 4 and "concurrency" not in overrides:
                data["concurrency"] = 5
            if int(data.get("segments") or 0) <= 4 and "segments" not in overrides:
                data["segments"] = 8
            data["turbo_v091"] = True

        # 若用户尚未在网页端手动锁定保存目录（custom_dir=False），且飞牛注入了授权目录，
        # 自动跟随最新的默认授权目录；若用户已在网页端手动保存过目录（custom_dir=True），则严格保留。
        if not data.get("custom_dir") and (
            os.environ.get("TRIM_DATA_ACCESSIBLE_PATHS") or os.environ.get("TRIM_DATA_SHARE_PATHS")
        ):
            data["download_dir"] = str(_default_download_dir())

        env = {
            "api_id": os.environ.get("TG_API_ID"),
            "api_hash": os.environ.get("TG_API_HASH"),
            "session_string": os.environ.get("TG_SESSION_STRING"),
            "proxy": os.environ.get("TG_PROXY"),
            "download_dir": os.environ.get("TGGET_DIR"),
            "port": os.environ.get("TGGET_PORT") or os.environ.get("TRIM_SERVICE_PORT"),
            "token": os.environ.get("TGGET_TOKEN"),
        }
        for k, v in env.items():
            if v not in (None, ""):
                data[k] = v

        data.update({k: v for k, v in overrides.items() if v is not None})

        cfg = cls()
        for k, v in data.items():
            if not hasattr(cfg, k):
                continue
            if k == "api_id":
                v = int(v) if str(v).strip().isdigit() else 0
            elif k == "port":
                v = int(v)
            elif k == "concurrency":
                v = max(1, int(v))
            elif k == "segments":
                v = max(1, min(16, int(v)))
            elif k == "dl_connections":
                v = max(1, min(6, int(v)))
            elif k == "custom_dir":
                v = bool(v)
            setattr(cfg, k, v)
        return cfg

    def save(self) -> None:
        self.turbo_v091 = True
        HOME.mkdir(parents=True, exist_ok=True)
        CONFIG_FILE.write_text(
            json.dumps(asdict(self), indent=2, ensure_ascii=False), encoding="utf-8"
        )
        try:  # 配置里有 api_hash，尽量收紧权限（Windows 上会退化为普通写）
            os.chmod(CONFIG_FILE, 0o600)
        except Exception:
            pass

    def mask(self) -> dict:
        """给前端看的脱敏视图。"""
        return {
            "api_id": self.api_id,
            "api_hash": ("*" * 8 + self.api_hash[-4:]) if self.api_hash else "",
            "has_creds": bool(self.api_id and self.api_hash),
            "download_dir": str(self.download_path),
            "custom_dir": self.custom_dir,
            "concurrency": self.concurrency,
            "segments": self.segments,
            "dl_connections": self.dl_connections,
            "on_conflict": self.on_conflict,
            "host": self.host,
            "port": self.port,
            "proxy": self.proxy,
            "auth_required": bool(self.token),
            "rename_by_date": self.rename_by_date,
            "dedupe_content": self.dedupe_content,
            "auto_retry": self.auto_retry,
            "archive_peer": self.archive_peer,
            "archive_title": self.archive_title,
        }
