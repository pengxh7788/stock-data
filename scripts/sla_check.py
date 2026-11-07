#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""收盘后 20 分钟 SLA 检查：15:20 前当日数据必须已发布。

判定：
  meta.json 的 date == 今天 → 已发布，看发布时刻是否 ≤ 15:20
  否则 → 未发布；若已过 15:20，判定违约并补触发采集

环境变量：GH_TOKEN / OWNER_REPO（由 workflow 注入）
退出码：0 达标 / 1 违约（Actions 会标红，便于发现）
"""
import json, os, sys, urllib.request
from datetime import datetime, timedelta, timezone

CST = timezone(timedelta(hours=8))
SLA_MIN = 20          # 收盘后多少分钟内必须就绪
CLOSE = (15, 0)
GH = os.environ.get("GH_TOKEN", "")
REPO = os.environ.get("OWNER_REPO", "")


def api(path, data=None, method=None):
    body = json.dumps(data).encode() if data is not None else None
    req = urllib.request.Request("https://api.github.com" + path, data=body,
        method=method or ("POST" if body else "GET"),
        headers={"Authorization": "Bearer " + GH, "Accept": "application/vnd.github+json",
                 "User-Agent": "sla", "Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=60) as r:
        return json.loads(r.read())


def main():
    now = datetime.now(CST)
    if now.weekday() >= 5:
        print(f"[sla] {now:%Y-%m-%d} 周末，跳过")
        return 0

    deadline = now.replace(hour=CLOSE[0], minute=CLOSE[1], second=0, microsecond=0) + timedelta(minutes=SLA_MIN)
    try:
        meta = json.load(open("data/meta.json", encoding="utf-8"))
    except Exception as e:
        print(f"[sla] 读不到 meta.json: {e}")
        meta = {}
    d = meta.get("date") or ""
    today = now.strftime("%Y-%m-%d")

    if d == today:
        # 已发布：用最近一次成功运行的时间算发布时刻
        published = None
        try:
            runs = api(f"/repos/{REPO}/actions/workflows/collect.yml/runs?per_page=10")
            for r in runs.get("workflow_runs", []):
                if r.get("conclusion") == "success" and r.get("updated_at"):
                    t = datetime.fromisoformat(r["updated_at"].replace("Z", "+00:00")).astimezone(CST)
                    if t.strftime("%Y-%m-%d") == today:
                        published = t
                        break
        except Exception as e:
            print(f"[sla] 查询运行记录失败: {e}")
        if published:
            mins = (published - published.replace(hour=CLOSE[0], minute=CLOSE[1], second=0, microsecond=0)).total_seconds() / 60
            ok = mins <= SLA_MIN
            print(f"[sla] 数据日期 {d} · 发布 {published:%H:%M} · 收盘后 {mins:.0f} 分钟 · {'✅ 达标' if ok else '❌ 超时'}")
            return 0 if ok else 1
        print(f"[sla] 数据日期 {d} 已是今天，但查不到发布时刻")
        return 0

    # 未发布
    if now < deadline:
        print(f"[sla] 数据日期 {d}（今天 {today} 还没出），现在 {now:%H:%M} 未到 {deadline:%H:%M}，继续等")
        return 0

    print(f"[sla] ❌ 已过 {deadline:%H:%M} 仍无当日数据（meta.date={d}）→ 补触发采集")
    try:
        api(f"/repos/{REPO}/actions/workflows/collect.yml/dispatches",
            {"ref": "main", "inputs": {"force": "1"}}, method="POST")
        print("[sla] 已 dispatch collect.yml")
    except Exception as e:
        print(f"[sla] dispatch 失败: {e}")
    return 1


if __name__ == "__main__":
    sys.exit(main())
