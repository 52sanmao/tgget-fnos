# tgget — 飞牛 fnOS 原生应用

Telegram 文件下载器，封装为飞牛 fnOS 原生应用（`.fpk`）。扫码登录 Telegram，批量下载 / 转发 / 归档文件到 NAS 本地，支持断点续传、多路分片提速、失败自动重试，数据全在本机。

- 应用包名：`tgget`
- 版本：`0.9.4`
- 运行时依赖：`python312`（飞牛运行时包）
- 访问方式：端口服务（默认 `64780`），桌面入口在飞牛桌面窗口打开

## 功能

- 扫码 / 手机号登录 Telegram，会话持久化
- 批量下载（串行断点续传 + 多路分片并发，最高 16 路）
- 「我的收藏」一键归档、跨群转发（受限频道自动降级重传）
- 相册自动成组、按日期重命名、内容级去重（硬链接）
- 失败自动重试、取消即清理残留 `.part/.seg`
- 下载结果落到共享目录，文件管理器直接可见

## 目录结构

```text
tgget-fnos/
├── manifest              # 应用清单（appname、依赖、端口等）
├── ICON.PNG              # 64×64 包图标
├── ICON_256.PNG          # 256×256 包图标
├── app/
│   ├── run.py            # 入口
│   ├── requirements.txt  # Python 依赖
│   ├── tgget/            # 应用代码（模块化拆分：tg_conn/tg_media/tg_actions/tg_downloader/tg_utils）
│   └── ui/
│       ├── config        # 桌面入口定义
│       └── images/       # 入口图标
├── cmd/                  # 生命周期脚本（main/install/upgrade/uninstall/config）
├── config/
│   ├── privilege         # 以专用包用户运行
│   └── resource          # 声明共享下载目录
└── wizard/
    └── install           # 安装向导（可选填 Telegram 凭据 / 访问令牌）
```

## 飞牛环境适配

应用通过环境变量适配飞牛 NAS 的目录体系，不硬编码路径：

| 环境变量 | 作用 | 注入位置 |
| --- | --- | --- |
| `TG_HOME` | 数据根目录（config.json / session / tgget.db） | `cmd/main` → `$TRIM_PKGVAR` |
| `TGGET_DIR` | 下载目录 | `cmd/main` → `$TRIM_DATA_SHARE_PATHS` 首个共享目录 |
| `TRIM_SERVICE_PORT` | 监听端口 | `config.py` 读取 |
| `TG_API_ID` / `TG_API_HASH` / `TGGET_TOKEN` | 可选凭据 | 安装向导 `wizard_*` → `cmd/main` |

## 本地开发

```bash
cd tgget-fnos/app
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
python run.py serve            # 启动控制台
python run.py serve --demo     # 演示模式（假数据）
python run.py serve --no-browser
```

## 打包 `.fpk`

需要飞牛官方打包工具 `fnpack`（见 [fnpack 文档](https://developer.fnnas.com/docs/cli/fnpack/)）：

```bash
cd tgget-fnos
fnpack build
# 生成 tgget.fpk
```

本项目已配置 GitHub Actions，推送后自动用 `fnpack-1.2.3-linux-amd64` 编译 `.fpk` 并上传为 artifact。

## 安装

1. 下载 release 中的 `tgget.fpk`
2. 飞牛 fnOS 应用中心 → 手动安装 → 选择该文件
3. 安装向导可跳过（凭据可稍后在网页里填）
4. 应用中心点开「Telegram 下载器」→ 网页控制台扫码登录

下载文件落盘到共享目录 `tgget/downloads`，可在文件管理器中直接访问。
