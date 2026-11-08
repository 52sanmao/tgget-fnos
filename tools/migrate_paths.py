#!/usr/bin/env python3
"""把 tgget 数据库/任务记录里的「旧下载目录」批量替换成「新下载目录」。

用途：用户重装后更换了下载保存地址，但历史任务记录（jobs.json 的 outdir /
files[].path）与下载记录（tgget.db 的 files.path）仍指向旧路径，导致旧文件
变成死链。跑这个脚本把这些绝对路径里的旧目录前缀替换成新目录前缀。

用法（在 NAS 上，应用数据目录是 $TRIM_PKGVAR，例如 /vol1/@appdata/tgget）：

    # 先停服务，避免写入冲突
    python3 tools/migrate_paths.py \
        --home /vol1/@appdata/tgget \
        --old  /旧下载目录 \
        --new  /新下载目录 \
        [--dry-run] [--move]

参数：
    --home   应用数据目录（含 jobs.json / tgget.db / config.json），必填
    --old    旧下载目录（绝对路径），必填
    --new    新下载目录（绝对路径），必填
    --dry-run   只预览要改哪些，不真正写入
    --move      同时把旧目录里仍然存在的媒体文件 mv 到新目录（默认只改数据库记录、不动文件）

说明：
    - 默认只改「记录里的路径字符串」，不搬真实文件。若你想连文件一起搬，加 --move。
    - 替换只作用于「以旧目录开头」的路径，其它路径不动。
    - 自动备份：改动前把 jobs.json / tgget.db 复制成 *.bak_migrate。
"""

from __future__ import annotations

import argparse
import json
import shutil
import sqlite3
import sys
from pathlib import Path


def backup(path: Path) -> Path:
    bak = path.with_name(path.name + ".bak_migrate")
    shutil.copy2(path, bak)
    return bak


def norm(p: str) -> str:
    # 统一成正斜杠，避免 Windows 本地 Path() 转出反斜杠与记录里的正斜杠对不上。
    # NAS（Linux）上本身就是正斜杠，无影响。
    return str(Path(p).expanduser()).replace("\\", "/").rstrip("/") or "/"


def replace_prefix(value: str, old: str, new: str) -> str:
    if not value:
        return value
    v = str(value)
    # 精确前缀匹配（含子目录），避免把 /old2 误当 /old 处理
    if v == old or v.startswith(old + "/") or v.startswith(old + "\\"):
        return new + v[len(old):]
    return v


def migrate_jobs(home: Path, old: str, new: str, dry: bool) -> int:
    jf = home / "jobs.json"
    if not jf.exists():
        print(f"[skip] 未找到 {jf}")
        return 0
    data = json.loads(jf.read_text(encoding="utf-8"))
    rows = data if isinstance(data, list) else data.get("jobs", data)
    if not isinstance(rows, list):
        print("[warn] jobs.json 结构未知，跳过")
        return 0

    changed = 0
    for job in rows:
        if not isinstance(job, dict):
            continue
        if "outdir" in job:
            nv = replace_prefix(job["outdir"], old, new)
            if nv != job["outdir"]:
                print(f"  job[{job.get('job_id')}] outdir: {job['outdir']!r} -> {nv!r}")
                job["outdir"] = nv
                changed += 1
        for f in job.get("files") or []:
            if isinstance(f, dict) and "path" in f:
                nv = replace_prefix(f["path"], old, new)
                if nv != f["path"]:
                    f["path"] = nv
                    changed += 1
                    print(f"  job[{job.get('job_id')}] files[].path -> {nv!r}")

    if changed and not dry:
        backup(jf)
        jf.write_text(json.dumps(data, ensure_ascii=False, indent=0), encoding="utf-8")
        print(f"[ok] jobs.json 已更新（{changed} 处），备份 {jf.name}.bak_migrate")
    elif changed:
        print(f"[dry-run] jobs.json 将更新 {changed} 处")
    else:
        print("[ok] jobs.json 无需改动")
    return changed


def migrate_auto(home: Path, old: str, new: str, dry: bool) -> int:
    """自动下载规则 auto.json：每条规则的 outdir 字段也存绝对路径。"""
    af = home / "auto.json"
    if not af.exists():
        print(f"[skip] 未找到 {af}")
        return 0
    try:
        data = json.loads(af.read_text(encoding="utf-8"))
    except Exception as exc:
        print(f"[warn] auto.json 无法解析，跳过：{exc}")
        return 0
    rows = data.get("rules") if isinstance(data, dict) else data
    if not isinstance(rows, list):
        print("[warn] auto.json 结构未知，跳过")
        return 0

    changed = 0
    for rule in rows:
        if not isinstance(rule, dict):
            continue
        if "outdir" in rule and rule["outdir"]:
            nv = replace_prefix(rule["outdir"], old, new)
            if nv != rule["outdir"]:
                print(f"  auto[{rule.get('id')}] outdir: {rule['outdir']!r} -> {nv!r}")
                rule["outdir"] = nv
                changed += 1

    if changed and not dry:
        backup(af)
        af.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"[ok] auto.json 已更新（{changed} 处），备份 {af.name}.bak_migrate")
    elif changed:
        print(f"[dry-run] auto.json 将更新 {changed} 处")
    else:
        print("[ok] auto.json 无需改动")
    return changed


def migrate_db(home: Path, old: str, new: str, dry: bool) -> int:
    dbf = home / "tgget.db"
    if not dbf.exists():
        print(f"[skip] 未找到 {dbf}")
        return 0
    if not dry:
        backup(dbf)
    conn = sqlite3.connect(str(dbf), timeout=30.0)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    rows = cur.execute("SELECT chat_id, msg_id, path FROM files").fetchall()
    changed = 0
    updates = []
    for r in rows:
        p = str(r["path"] or "")
        nv = replace_prefix(p, old, new)
        if nv != p:
            updates.append((nv, r["chat_id"], r["msg_id"]))
            changed += 1
            print(f"  files[{r['chat_id']}|{r['msg_id']}]: {p!r} -> {nv!r}")
    if changed and not dry:
        cur.executemany(
            "UPDATE files SET path=? WHERE chat_id=? AND msg_id=?", updates
        )
        conn.commit()
        print(f"[ok] tgget.db files 表已更新（{changed} 处），备份 tgget.db.bak_migrate")
    elif changed:
        print(f"[dry-run] tgget.db files 表将更新 {changed} 处")
    else:
        print("[ok] tgget.db 无需改动")
    conn.close()
    return changed


def move_files(old: Path, new: Path) -> int:
    if not old.is_dir():
        print(f"[skip] 旧目录不存在，不搬文件：{old}")
        return 0
    new.mkdir(parents=True, exist_ok=True)
    moved = 0
    for p in sorted(old.rglob("*")):
        if p.is_dir():
            continue
        rel = p.relative_to(old)
        dst = new / rel
        dst.parent.mkdir(parents=True, exist_ok=True)
        if dst.exists():
            print(f"[skip-move] 目标已存在：{dst}")
            continue
        shutil.move(str(p), str(dst))
        moved += 1
    print(f"[ok] 移动 {moved} 个文件：{old} -> {new}")
    return moved


def main() -> int:
    ap = argparse.ArgumentParser(description="迁移 tgget 下载目录路径")
    ap.add_argument("--home", required=True, help="应用数据目录（含 jobs.json/tgget.db/auto.json）")
    ap.add_argument("--old", required=True, help="旧下载目录（绝对路径）")
    ap.add_argument("--new", required=True, help="新下载目录（绝对路径）")
    ap.add_argument("--dry-run", action="store_true", help="只预览不写入")
    ap.add_argument("--move", action="store_true", help="同时把旧目录里仍存在的媒体文件搬到新目录")
    args = ap.parse_args()

    home = Path(args.home).expanduser()
    old = norm(args.old)
    new = norm(args.new)

    if old == new:
        print("[error] 新旧目录相同，无需迁移")
        return 1

    print(f"==> 迁移路径：{old!r} -> {new!r}")
    print(f"==> 应用数据目录：{home}")
    if args.dry_run:
        print("==> DRY-RUN 模式，不写任何文件")

    c1 = migrate_jobs(home, old, new, args.dry_run)
    c2 = migrate_db(home, old, new, args.dry_run)
    c3 = migrate_auto(home, old, new, args.dry_run)
    total = c1 + c2 + c3

    if total == 0:
        print("\n没有需要迁移的记录。可能原因：旧目录写错了，或历史记录里没有以旧目录开头的路径。")

    if args.move and not args.dry_run:
        move_files(Path(old), Path(new))

    print(f"\n完成：共迁移 {total} 处路径。")
    return 0


if __name__ == "__main__":
    sys.exit(main())
