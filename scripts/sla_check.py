#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""收盘后 20 分钟 SLA 检查：**必须在 15:20 前启动当日数据采集**。

要求原文：收盘后 20 分钟之内必须"启动"保存数据，不要求 20 分钟内跑完。
（不检查的话，有时会拖到几小时后才开始，数据就一直是昨天的。）

判定方式：查 collect.yml 今天的运行记录，看最早一次 run_started_at 是否 ≤ 15:20。
  - 15:20 前已有运行启动 → ✅ 达标（跑没跑完不影响判定）
  - 还没到 15:20 → 继续等
  - 过了 15:20 仍无运行 → ❌ 违约 + 立即 dispatch 补跑

环境变量：GH_TOKEN / OWNER_REPO（workflow 注入）
退出码：0 达标 / 1 违约（Actions 标红，便于发现）
"""
import json, os, sys, urllib.request
from datetime import datetime, timedelta, timezone

CST = timezone(timedelta(hours=8))
SLA_MIN = 20
CLOSE_H, CLOSE_M = 15, 0
GH = os.environ.get("GH_TOKEN", "")
REPO = os.environ.get("OWNER_REPO", "")
WORKFLOW = "collect.yml"


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

    close = now.replace(hour=CLOSE_H, minute=CLOSE_M, second=0, microsecond=0)
    deadline = close + timedelta(minutes=SLA_MIN)
    today = now.strftime("%Y-%m-%d")

    # 今天最早启动的那次运行
    first = None
    try:
        runs = api(f"/repos/{REPO}/actions/workflows/{WORKFLOW}/runs?per_page=50")
        for r in runs.get("workflow_runs", []):
            t = r.get("run_started_at") or r.get("created_at")
            if not t:
                continue
            st = datetime.fromisoformat(t.replace("Z", "+00:00")).astimezone(CST)
            if st.strftime("%Y-%m-%d") != today:
                continue
            if first is None or st < first:
                first = st
    except Exception as e:
        print(f"[sla] 查询运行记录失败: {e}")
        return 0

    if first:
        delay = (first - close).total_seconds() / 60
        tag = "✅ 达标" if delay <= SLA_MIN else "❌ 超时启动"
        print(f"[sla] 今日最早启动 {first:%H:%M}（收盘后 {delay:.0f} 分钟）→ {tag}")
        # 顺带记录数据是否已出，便于看进度，但不影响判定
        try:
            meta = json.load(open("data/meta.json", encoding="utf-8"))
            print(f"[sla] 当前数据日期 {meta.get('date')}（完成与否不参与判定）")
        except Exception:
            pass
        return 0 if delay <= SLA_MIN else 1

    if now < deadline:
        print(f"[sla] 现在 {now:%H:%M}，还没启动；截止 {deadline:%H:%M}，继续等")
        return 0

    print(f"[sla] ❌ 已过 {deadline:%H:%M} 仍未启动采集 → 立即 dispatch")
    try:
        api(f"/repos/{REPO}/actions/workflows/{WORKFLOW}/dispatches",
            {"ref": "main", "inputs": {"force": "1"}}, method="POST")
        print("[sla] 已 dispatch collect.yml（强制）")
    except Exception as e:
        print(f"[sla] dispatch 失败: {e}")
    return 1


if __name__ == "__main__":
    sys.exit(main())
