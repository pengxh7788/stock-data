#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""判断某天是否 A 股交易日（用仓库自己的 calendar.json，认节假日）。

用法: python3 scripts/trading_day.py [YYYY-MM-DD]
退出码: 0 交易日 / 1 非交易日 / 2 无法判断（调用方按"不采"处理）

日历来源优先级：
  1. 本地 ./calendar.json（history 分支里的那份）
  2. GitHub API 读 history 分支的 calendar.json（需要 GH_TOKEN）
  3. 兜底：只按周末判断
"""
import json, os, sys, urllib.request
from datetime import datetime, timedelta, timezone

CST = timezone(timedelta(hours=8))
GH = os.environ.get("GH_TOKEN") or os.environ.get("GH") or ""
REPO = os.environ.get("OWNER_REPO") or "pengxh7788/stock-data"


def from_api():
    req = urllib.request.Request(
        f"https://api.github.com/repos/{REPO}/contents/calendar.json?ref=history",
        headers={"Authorization": "Bearer " + GH, "Accept": "application/vnd.github.raw",
                 "User-Agent": "cal"})
    with urllib.request.urlopen(req, timeout=60) as r:
        return json.loads(r.read().decode("utf-8"))


def load_cal():
    for p in ("./calendar.json", "../calendar.json"):
        if os.path.exists(p):
            try:
                return json.load(open(p, encoding="utf-8")), "本地"
            except Exception:
                pass
    if GH:
        try:
            return from_api(), "API"
        except Exception as e:
            print(f"[cal] API 读取失败: {e}", file=sys.stderr)
    return None, None


def main():
    day = sys.argv[1] if len(sys.argv) > 1 else datetime.now(CST).strftime("%Y-%m-%d")
    cal, src = load_cal()
    if cal:
        days = set(cal.get("tradingDays") or [])
        if day in days:
            print(f"[cal] {day} 是交易日（{src}，共 {len(days)} 个交易日）")
            return 0
        print(f"[cal] {day} 不是交易日（{src}）→ 今日不采集")
        return 1
    # 兜底：只判周末
    dt = datetime.strptime(day, "%Y-%m-%d")
    if dt.weekday() >= 5:
        print(f"[cal] {day} 周末（无日历，按周末判）→ 今日不采集")
        return 1
    print(f"[cal] {day} 无法确认日历，按工作日放行")
    return 0


if __name__ == "__main__":
    sys.exit(main())
