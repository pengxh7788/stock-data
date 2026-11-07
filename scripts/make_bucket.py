#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""把 live.json 落成「带时间点的价格桶」，只保留最近 2 个（供前端兜底）。

为什么：本地缓存的桶可能失效（冷启动/换设备/跨日/被清理）。
这时前端要能按「对应时间点」从仓库取回同一个桶，而不是拿最新价糊弄。

产物: data/buckets/<date>/<HHMM>.json
      {"date":"2026-11-06","hhmm":"1030","ts":"...","n":5219,"q":{"sz000001":11.81}}
保留: 全局只留最新 2 个桶（更早的连同空目录一起删）

用法: python3 scripts/make_bucket.py
"""
import json, os, shutil, sys, time
from datetime import datetime, timezone, timedelta

BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA = os.path.join(BASE, "data")
LIVE = os.path.join(DATA, "live.json")
BUCKETS = os.path.join(DATA, "buckets")
KEEP = 2                     # 只保留最近 2 个桶
CST = timezone(timedelta(hours=8))


def floor_half_hour(dt):
    return dt.replace(minute=(0 if dt.minute < 30 else 30), second=0, microsecond=0)


def main():
    if not os.path.exists(LIVE):
        print("[bucket] 没有 live.json，跳过")
        return 0
    live = json.load(open(LIVE, encoding="utf-8"))
    q = live.get("q") or {}
    if not q:
        print("[bucket] live.json 里没有数据，跳过")
        return 0

    # 用 live.json 的时间戳对齐到整半点（没有就用现在）
    ts = live.get("ts") or ""
    try:
        dt = datetime.fromisoformat(ts.replace("Z", "+00:00")).astimezone(CST)
    except Exception:
        dt = datetime.now(CST)
    b = floor_half_hour(dt)
    date_s = b.strftime("%Y-%m-%d")
    hhmm = b.strftime("%H%M")

    # 只存价格（策略只需要价格；chg 可由价格与昨收算）
    prices = {code: v[0] for code, v in q.items() if v and v[0]}
    payload = {"date": date_s, "hhmm": hhmm,
               "ts": b.isoformat(timespec="seconds"),
               "n": len(prices), "q": prices}

    out_dir = os.path.join(BUCKETS, date_s)
    os.makedirs(out_dir, exist_ok=True)
    path = os.path.join(out_dir, f"{hhmm}.json")
    json.dump(payload, open(path, "w", encoding="utf-8"), ensure_ascii=False, separators=(",", ":"))
    print(f"[bucket] 写入 {date_s}/{hhmm}.json · {len(prices)} 只 · {os.path.getsize(path)/1024:.0f} KB")

    # 清理：全局只保留最新 KEEP 个文件
    files = []
    for d in os.listdir(BUCKETS):
        dp = os.path.join(BUCKETS, d)
        if not os.path.isdir(dp):
            continue
        for f in os.listdir(dp):
            if f.endswith(".json"):
                files.append((d + f.replace(".json", ""), os.path.join(dp, f)))
    files.sort()
    for _, fp in files[:-KEEP]:
        os.remove(fp)
        print(f"[bucket] 删除旧桶 {os.path.relpath(fp, DATA)}")
    for d in os.listdir(BUCKETS):
        dp = os.path.join(BUCKETS, d)
        if os.path.isdir(dp) and not os.listdir(dp):
            os.rmdir(dp)
    keep = [os.path.relpath(fp, DATA) for _, fp in files[-KEEP:]]
    print(f"[bucket] 现存 {len(keep)} 个：{keep}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
