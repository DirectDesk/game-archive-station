"""刷新回归工具（在容器 /app 内以 `python tools/refresh_check.py` 运行）。

设计原则
--------
1. **默认不跑全量**：必须显式给出要检查的游戏 id，或显式加 `--all`。
   这是为了避免无意间的全量回归 —— 全量会消耗 RAWG 配额，并且可能
   通过术语表反向改写已手工修改好的译名。
2. **保护用户译名**：运行前后对 title 做快照 diff，任何 title 变化都会
   被高亮列出；配合 SystemConfig.keep_user_title（默认 True）防止误改。
3. **幂等校验不靠跑两遍**：用前后 source_ids 快照对比即可判断是否稳定，
   无需重复请求外部 API。

用法
----
    python tools/refresh_check.py 1 5 25 28     # 只检查这几条（推荐）
    python tools/refresh_check.py --all           # 全量（明确确认后才用）
    python tools/refresh_check.py --all --yes     # 跳过交互确认
    python tools/refresh_check.py 1 --dry-run     # 只快照不刷新（看当前状态）
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
import time

sys.path.insert(0, "/app")


async def _snapshot(gid: int) -> dict:
    from sqlalchemy import select

    from app.database import SessionLocal
    from app.models import Game

    async with SessionLocal() as s:
        g = (await s.execute(select(Game).where(Game.id == gid))).scalar_one_or_none()
        if g is None:
            return {}
        try:
            sids = json.loads(g.source_ids or "{}")
        except Exception:
            sids = {}
        try:
            sd = json.loads(g.source_data or "{}")
        except Exception:
            sd = {}
        titles = {
            k: (v.get("title") or v.get("name") or "")[:44]
            for k, v in sd.items()
            if isinstance(v, dict)
        }
        return {
            "title": g.title,
            "source_type": g.source_type,
            "source_id": g.source_id,
            "source_ids": sids,
            "source_titles": titles,
        }


async def _all_ids() -> list[int]:
    from sqlalchemy import select

    from app.database import SessionLocal
    from app.models import Game

    async with SessionLocal() as s:
        return list((await s.execute(select(Game.id).order_by(Game.id))).scalars().all())


def _fmt(snap: dict) -> str:
    return "pri=%s/%s ids=%s" % (
        snap.get("source_type"), snap.get("source_id"),
        json.dumps(snap.get("source_ids", {}), ensure_ascii=False),
    )


async def main() -> int:
    ap = argparse.ArgumentParser(description="刷新回归工具（默认只跑指定 id）")
    ap.add_argument("ids", nargs="*", type=int, help="要检查的游戏 id 列表")
    ap.add_argument("--all", action="store_true", help="全量回归（消耗 RAWG 配额，慎用）")
    ap.add_argument("--yes", action="store_true", help="跳过全量确认")
    ap.add_argument("--dry-run", action="store_true", help="只打印当前快照，不刷新")
    args = ap.parse_args()

    if args.all:
        targets = await _all_ids()
        if not targets:
            print("库中没有游戏")
            return 0
        print("⚠ 全量回归将刷新 %d 条记录" % len(targets))
        print("  会消耗 RAWG 配额（预计 %d~%d 次请求）" % (len(targets) * 6, len(targets) * 12))
        print("  并且可能通过术语表改写已手工修改的译名")
        if not args.yes:
            try:
                ans = input("确认继续？输入 yes 继续，其它任意键取消: ").strip().lower()
            except EOFError:
                ans = ""
            if ans != "yes":
                print("已取消。")
                return 0
    elif args.ids:
        targets = args.ids
    else:
        print("错误：请指定游戏 id，例如：python tools/refresh_check.py 1 5 25")
        print("      或显式全量：python tools/refresh_check.py --all")
        return 2

    from app.services import refresh_game_metadata

    print("目标 id：%s" % targets)
    if args.dry_run:
        for gid in targets:
            snap = await _snapshot(gid)
            if not snap:
                print("  [%d] 不存在" % gid)
                continue
            print("  [%d] %s" % (gid, _fmt(snap)))
        return 0

    changed_ids: list[int] = []
    title_changed: list[tuple[int, str, str]] = []

    for gid in targets:
        before = await _snapshot(gid)
        if not before:
            print("  [%d] 不存在，跳过" % gid)
            continue
        t0 = time.time()
        try:
            await refresh_game_metadata(gid)
        except Exception as exc:  # noqa: BLE001
            print("  !! [%d] ERROR %s: %s" % (gid, type(exc).__name__, exc))
            continue
        dt = time.time() - t0
        after = await _snapshot(gid)
        ids_changed = before["source_ids"] != after["source_ids"]
        if ids_changed:
            changed_ids.append(gid)
        if before["title"] != after["title"]:
            title_changed.append((gid, before["title"], after["title"]))
        mark = "**" if ids_changed else "  "
        print("%s[%d] %-30s (%.1fs)" % (mark, gid, (after["title"] or "")[:30], dt))
        print("     before: %s" % _fmt(before))
        print("     after : %s" % _fmt(after))

    print("=" * 70)
    print("CHANGED IDS:", changed_ids)
    if title_changed:
        print("⚠ TITLE 被改动（请确认是否符合预期）：")
        for gid, old, new in title_changed:
            print("  [%d] %r -> %r" % (gid, old, new))
    else:
        print("TITLE 无改动 ✓（用户译名受保护）")
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
