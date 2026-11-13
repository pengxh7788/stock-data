#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
verify_dataset.py —— 采集完成后自动校验 data/*.json

用法：
    python3 verify_dataset.py            # 人类可读
    python3 verify_dataset.py --json     # 机器可读（CI 用）

退出码：0 = 全部通过；1 = 有致命项失败（CI 应中止，不要推送）

检查项分两类：
  FATAL  致命：数据不可用，推送出去会让 App 出错
  WARN   警告：可疑但不阻塞
"""
import json
import os
import sys

DATA = "data"
CAP = {"进攻": 0.8, "中性": 0.5, "防守": 0.3, "空仓": 0.0}

results = []


def add(level, name, ok, detail=""):
    results.append({"level": level, "name": name, "ok": bool(ok), "detail": detail})


def load(name, default=None):
    p = os.path.join(DATA, name)
    if not os.path.exists(p):
        return None
    try:
        with open(p, encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        add("FATAL", f"{name} 可解析", False, str(e))
        return None


def main():
    analysis = load("analysis.json")
    quotes_f = load("quotes.json")
    meta = load("meta.json")
    hist = load("history.json")

    if not analysis:
        add("FATAL", "analysis.json 存在", False, "文件缺失或无法解析")
        return report()
    add("FATAL", "analysis.json 存在", True)

    market = analysis.get("market") or {}
    stocks = analysis.get("stocks") or {}
    quotes = (quotes_f or {}).get("quotes") or quotes_f or {}

    # ---------- 1. 市况 ----------
    regime = market.get("regime")
    add("FATAL", "market.regime 合法",
        regime in CAP, f"regime={regime}")

    # status 必须与 regime 同值（别名），否则客户端会读到矛盾值
    status = market.get("status")
    if status is None:
        add("WARN", "market.status 已移除（可选）", True, "新方案可只保留 regime")
    else:
        add("FATAL", "market.status 与 regime 一致",
            status == regime, f"status={status} regime={regime}")

    # ---------- 2. 仓位 ----------
    cap = market.get("positionCap")
    expect = CAP.get(regime)
    add("FATAL", "positionCap 与 regime 匹配",
        expect is not None and abs((cap or -1) - expect) < 1e-9,
        f"positionCap={cap} 期望={expect}（regime={regime}）")

    # ---------- 3. 市场宽度 ----------
    breadth = market.get("breadth")
    add("FATAL", "market.breadth 存在",
        isinstance(breadth, (int, float)) and 0 <= breadth <= 100,
        f"breadth={breadth}")

    # ---------- 4. modelEval 退化档 ----------
    me = analysis.get("modelEval") or {}
    by = me.get("byRegime") or {}
    bad = []
    for reg, buckets in by.items():
        for bk, v in (buckets or {}).items():
            if not v:
                continue
            degenerate = (v.get("lo") == v.get("hi")) or (
                v.get("mid") == v.get("lo") == v.get("hi"))
            if degenerate and not v.get("invalid"):
                bad.append(f"{reg}/{bk}")
    add("FATAL", "modelEval 无未标记的退化档",
        not bad, f"未标记退化档 {len(bad)} 个: {bad[:6]}")

    # ---------- 5. 连板字段（打板策略依赖） ----------
    total = len(stocks)
    has_board = sum(1 for s in stocks.values() if "board" in s)
    has_lu = sum(1 for s in stocks.values() if "isLimitUp" in s)
    add("FATAL", "stocks[].board 字段存在",
        total > 0 and has_board / total > 0.95,
        f"{has_board}/{total} 只带 board")
    add("FATAL", "stocks[].isLimitUp 字段存在",
        total > 0 and has_lu / total > 0.95,
        f"{has_lu}/{total} 只带 isLimitUp")

    limit_up = [c for c, s in stocks.items() if s.get("isLimitUp")]
    board_ok = [c for c in limit_up if (stocks[c].get("board") or 0) >= 1]
    add("FATAL", "封板股都有 board>=1",
        len(limit_up) == len(board_ok),
        f"封板 {len(limit_up)} 只，board 有效 {len(board_ok)} 只")

    # 交叉校验：analysis 的封板数应接近 quotes 的涨停数
    q_limit = 0
    for c, x in quotes.items():
        px, lu = x.get("p") or 0, x.get("lu") or 0
        if px > 0 and lu > 0 and px >= lu * 0.999:
            q_limit += 1
    diff = abs(len(limit_up) - q_limit)
    # 保险：quotes 明明有一批涨停，analysis 却一个封板都没算出来 → board 逻辑失效
    if q_limit >= 5 and len(limit_up) == 0:
        add("FATAL", "board 计算未失效", False,
            f"quotes 涨停 {q_limit} 只，analysis 封板 0 只（history 末行可能缺当日收盘）")
    else:
        add("WARN", "封板数与 quotes 涨停数接近",
            diff <= max(3, q_limit * 0.1),
            f"analysis {len(limit_up)} vs quotes {q_limit}（差 {diff}）")

    # ---------- 5.5 短线策略字段 ----------
    # 新股/停牌没有 60 天历史，覆盖率不可能 100%：<80% 才算系统性 bug
    for f, name in (("vr5", "5日量比"), ("atrPct", "ATR14%"), ("pos60", "60日位置")):
        n = sum(1 for st in stocks.values() if st.get(f) is not None)
        r = n / total if total else 0
        if r < 0.80:
            add("FATAL", f"stocks[].{f}（{name}）覆盖率 ≥ 80%", False, f"{n}/{total} = {r*100:.1f}%")
        elif r < 0.95:
            add("WARN", f"stocks[].{f}（{name}）覆盖率 ≥ 95%（新股无历史属正常）", False, f"{n}/{total} = {r*100:.1f}%")
        else:
            add("FATAL", f"stocks[].{f}（{name}）覆盖率", True, f"{n}/{total} = {r*100:.1f}%")

    # 值域合理性
    vr = [st.get("vr5") for st in stocks.values() if st.get("vr5") is not None]
    if vr:
        bad = sum(1 for x in vr if x <= 0 or x > 50)
        add("WARN", "vr5 值域合理", bad / len(vr) < 0.01, f"异常 {bad}/{len(vr)}")
    at = [st.get("atrPct") for st in stocks.values() if st.get("atrPct") is not None]
    if at:
        bad = sum(1 for x in at if x <= 0 or x > 60)
        add("WARN", "atrPct 值域合理", bad / len(at) < 0.01, f"异常 {bad}/{len(at)}")
    po = [st.get("pos60") for st in stocks.values() if st.get("pos60") is not None]
    if po:
        bad = sum(1 for x in po if x < 0 or x > 1)
        add("FATAL", "pos60 必须在 0~1", bad == 0, f"越界 {bad}/{len(po)}")

    # ---------- 5.6 ST / 停牌标记 ----------
    for f, name in (("isST", "ST标记"), ("isSuspended", "停牌标记")):
        n = sum(1 for st in stocks.values() if f in st)
        add("FATAL", f"stocks[].{f}（{name}）字段存在",
            total > 0 and n / total > 0.95, f"{n}/{total}")

    # ---------- 6. 基础指标 ----------
    # 新股/停牌天然无历史，覆盖率不可能 100%：
    # < 80% 视为系统性 bug（致命），80~95% 仅提示
    for f in ("chg20", "rsi", "vratio", "ma20"):
        n = sum(1 for s in stocks.values() if s.get(f) is not None)
        r = n / total if total else 0
        if r < 0.80:
            add("FATAL", f"stocks[].{f} 覆盖率 ≥ 80%", False, f"{n}/{total} = {r*100:.1f}%")
        elif r < 0.95:
            add("WARN", f"stocks[].{f} 覆盖率 ≥ 95%（新股无历史属正常）",
                False, f"{n}/{total} = {r*100:.1f}%")
        else:
            add("FATAL", f"stocks[].{f} 覆盖率", True, f"{n}/{total} = {r*100:.1f}%")

    # ---------- 7. 量纲 ----------
    if hist and hist.get("dates"):
        vols = hist.get("volume") or {}
        sample = [v[-1] for v in list(vols.values())[:200] if v and v[-1]]
        if sample:
            mid = sorted(sample)[len(sample) // 2]
            add("WARN", "history.volume 量纲为「万股」",
                0 < mid < 1e6, f"末值中位数 {mid:.1f}（若为股则会是十万级）")
    if meta:
        add("WARN", "meta.date 与 analysis.date 一致",
            meta.get("date") == analysis.get("date"),
            f"meta={meta.get('date')} analysis={analysis.get('date')}")

    # ---------- 8. 覆盖率 ----------
    if quotes:
        cov = len(stocks) / max(1, len(quotes))
        add("WARN", "analysis 覆盖 quotes 股票 > 95%",
            cov > 0.95, f"{len(stocks)}/{len(quotes)} = {cov*100:.1f}%")

    return report()


def report():
    fatal_bad = [r for r in results if r["level"] == "FATAL" and not r["ok"]]
    warn_bad = [r for r in results if r["level"] == "WARN" and not r["ok"]]

    if "--json" in sys.argv:
        print(json.dumps({"ok": not fatal_bad, "fatal": len(fatal_bad),
                          "warn": len(warn_bad), "items": results},
                         ensure_ascii=False, indent=1))
    else:
        print("=== 数据校验 ===")
        for r in results:
            mark = "OK  " if r["ok"] else ("FAIL" if r["level"] == "FATAL" else "WARN")
            print(f"  [{mark}] {r['name']}" + (f"  — {r['detail']}" if r["detail"] else ""))
        print()
        if fatal_bad:
            print(f"致命失败 {len(fatal_bad)} 项，禁止推送。")
            for r in fatal_bad:
                print(f"  - {r['name']}: {r['detail']}")
        else:
            print(f"全部致命项通过（警告 {len(warn_bad)} 项）。")
    return 0 if not fatal_bad else 1


if __name__ == "__main__":
    sys.exit(main())
