"""Telethon 客户端聚合入口模块。

为保持代码结构清晰、职责单一，原先约 3000 行的实现已按功能拆分为 5 个独立子模块：
- `tg_utils.py`      ：路径净化、链接解析、消息元信息提取、方案 C 命名分配器 `NameAllocator`、DOCX 导出
- `tg_conn.py`       ：`LoopThread` 后台事件循环、连接管理、内存级多连接池、智能重连、登录鉴权
- `tg_media.py`      ：对话列表、消息列表与云端对比校准、缩略图/头像缓存、512KB 对齐边下边播流
- `tg_actions.py`    ：「我的收藏」转存归档、普通转发（含受限频道自动降级重传）、发送、编辑、同步删除、导出
- `tg_downloader.py` ：单文件下载、串行断点续传、多路分片并发流、0.2s 取消轮询

本模块聚合组装 `Telegram` 类并完整导出原有符号，保证外部调用与离线测试 100% 向后兼容。
"""

from __future__ import annotations

from .config import HOME, Config
from .db import DB
from .store import INDEX
from .tg_actions import ActionsMixin
from .tg_conn import ConnMixin, LoopThread
from .tg_downloader import DownloaderMixin
from .tg_media import MediaMixin
from .tg_utils import (
     NameAllocator,
    _ALLOC_LOCK,
    _DL_LOCKS,
    _KIND_CN,
    _PART_RE,
    _BAD_CHARS,
    _SAFE_MAX,
    _build_docx,
    _dims,
    _dl_lock,
    _duration,
    _media_ext,
    _media_label,
    _msg_datetime,
    _photo_size,
    _sniff_mime,
    _unlink,
    _xml_escape,
    assign_save_names,
    date_tag,
    download_name,
    download_stem,
    entity_title,
    human_size,
    message_info,
    message_row,
    parse_link,
    reuse_local,
    safe_name,
)


class Telegram(ConnMixin, MediaMixin, ActionsMixin, DownloaderMixin):
    """Telethon 客户端门面类（由连接、媒体、操作、下载四个功能 Mixin 组合而成）。"""


__all__ = [
    "Config",
    "DB",
    "HOME",
    "INDEX",
    "LoopThread",
    "NameAllocator",
    "Telegram",
    "_DL_LOCKS",
    "_build_docx",
    "_dims",
    "_dl_lock",
    "_duration",
    "_media_ext",
    "_media_label",
    "_msg_datetime",
    "_photo_size",
    "_sniff_mime",
    "_unlink",
    "assign_save_names",
    "date_tag",
    "download_name",
    "download_stem",
    "entity_title",
    "human_size",
    "message_info",
    "message_row",
    "parse_link",
    "reuse_local",
    "safe_name",
]
