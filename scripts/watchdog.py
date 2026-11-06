#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""数据自愈守护（watchdog）。

每次运行检查两件事，缺哪个就 wake 对应的采集 workflow：
  1) history 分支 manifest.lastTradingDate 是否已到「应有交易日」
  2) main 分支 data/meta.json 的 date 是否已到「应有交易日」

「应有交易日」：收盘定稿（15:10）之后 = 今天（若今天是交易日）；之前 = 上一个交易日。
所以它同时负责：收盘后当天立刻补、以及次日盘前补昨天漏掉的一天。

为什么需要它：GitHub 的 schedule 触发会被丢弃。实测 2026-11-04 当天
07:00Z / 07:15Z / 07:26Z 三个定时触发全部没执行，仓库因此缺了一整天的日K。
本脚本用仓库自带 GITHUB_TOKEN（permissions: actions: write）调用
workflow_dispatch 叫醒采集任务 —— 这条路径不需要外部账号、不需要 PAT。
"""

import json
import os
import sys
import urllib.error
import urllib.request
from datetime import datetime, timedelta, timezone

OWNER_REPO = os.environ.get("OWNER_REPO", "pengxh7788/stock-data")
TOKEN = os.environ.get("GH_TOKEN", "")
CST = timezone(timedelta(hours=8))
API = "https://api.github.com"
RAW_BASES = (
    f"https://gh-proxy.com/https://raw.githubusercontent.com/{OWNER_REPO}",
    f"https://cdn.jsdelivr.net/gh/{OWNER_REPO}",
    f"https://raw.githubusercontent.com/{OWNER_REPO}",
)
CLOSE_SETTLE_MIN = 15 * 60 + 10   # 15:10 之后当日数据才定稿


def log(msg):
    print(f"[watchdog {datetime.now(CST):%H:%M:%S}] {msg}", flush=True)


def http(url, method="GET", body=None, timeout=25, auth=False):
    data = None if body is None else json.dumps(body).encode()
    headers = {"User-Agent": "stock-data-watchdog", "Accept": "application/vnd.github+json"}
    if body is not None:
        headers["Content-Type"] = "application/json"
    if auth and TOKEN:
        headers["Authorization"] = f"Bearer {TOKEN}"
    req = urllib.request.Request(url, data=data, method=method, headers=headers)
    with urllib.request.urlopen(req, timeout=timeout) as r:
        raw = r.read()
        return r.status, (json.loads(raw.decode()) if raw else None)


def raw_json(path):
    """读 raw 文件（不走 API，避免 token 依赖和限额）。"""
    for base in RAW_BASES:
        try:
            _, data = http(f"{base}/{path}", timeout=30)
            if isinstance(data, dict):
                return data
        except Exception as e:  # noqa: BLE001
            log(f"读取 {base}/{path} 失败：{e}")
    return None


def api(path, method="GET", body=None):
    return http(API + path, method=method, body=body, auth=True)


def workflow_busy(filename):
    try:
        _, data = api(f"/repos/{OWNER_REPO}/actions/workflows/{filename}/runs?per_page=5")
    except Exception as e:  # noqa: BLE001
        log(f"查询 {filename} 运行状态失败：{e}（按“忙”处理）")
        return True
    for run in (data or {}).get("workflow_runs", []):
        if run.get("status") in ("queued", "in_progress", "requested", "waiting", "pending"):
            log(f"{filename} 正在运行（{run.get('status')}，开始于 {run.get('created_at')}），跳过")
            return True
    return False


def wake(filename, label):
    if workflow_busy(filename):
        return False
    try:
        api(f"/repos/{OWNER_REPO}/actions/workflows/{filename}/dispatches", method="POST", body={"ref": "main"})
        log(f"✅ 已 dispatch {filename}（{label} 缺失）")
        return True
    except Exception as e:  # noqa: BLE001
        log(f"❌ dispatch {filename} 失败：{e}")
        return False


def main():
    now = datetime.now(CST)
    today = now.strftime("%Y-%m-%d")
    minutes = now.hour * 60 + now.minute

    if now.weekday() >= 5:
        log("周末，跳过")
        return 0

    cal = raw_json("history/calendar.json")
    if not cal or not cal.get("tradingDays"):
        log("读不到 calendar.json，跳过本次")
        return 0
    days = cal["tradingDays"]

    if minutes >= CLOSE_SETTLE_MIN and today in days:
        expected = today
    else:
        earlier = [d for d in days if d < today]
        if not earlier:
            log("没有更早的交易日，跳过")
            return 0
        expected = earlier[-1]
    log(f"今日 {today}，应有数据日期 {expected}")

    problems = 0

    manifest = raw_json("history/manifest.json")
    have = (manifest or {}).get("lastTradingDate")
    log(f"history/daily 最新 = {have}")
    if have != expected:
        wake("history.yml", f"日K {expected}")
        problems += 1

    meta = raw_json("main/data/meta.json")
    have_meta = (meta or {}).get("date")
    log(f"main/data/meta.json 日期 = {have_meta}")
    if have_meta != expected:
        wake("collect.yml", f"快照包 {expected}")
        problems += 1

    if problems == 0:
        log("数据齐了，无需处理")
    return 0


if __name__ == "__main__":
    sys.exit(main())
