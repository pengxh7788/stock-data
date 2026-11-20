#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
服务端规则回测（纯标准库，无第三方依赖）—— 产出 live/data/backtest.json

用法：
    python3 scripts/backtest.py --data daily --out out/backtest.json --days 500

输入：daily/<YYYY-MM-DD>.json
      {"bars": {"sh600000": [open, close, high, low, volume, amount, turnover]}}

输出：{ v, generatedAt, window, benchmark, cost, rules:[{id,name,params,stats,byBreadth,vsBenchmark,gate}] }

固定口径：
  1. 信号日收盘出信号；
  2. 次日开盘价买入；当日已涨停（涨幅≥9.5%）不买；开盘高开 > gap_max 不追；
  3. 止损 = 入场 × (1 − k×ATR%)，止盈 = 入场 × (1 + k×ATR%×R)；
  4. 最长持有 H 个交易日，到期按收盘出；
  5. 扣双边成本 COST=0.15%。

说明：只用标准库，Actions 里不需要 pip install，500 天 × 5000 票约 2~4 分钟。
"""
import argparse
import json
import math
import os
import sys
from collections import deque
from datetime import datetime, timezone

COST = 0.0015
NAN = float("nan")
LIMITS = (("sh688", 19.8), ("sh68", 19.8), ("sz30", 19.8), ("bj", 29.8))


def limit_pct(code):
    for k, v in LIMITS:
        if code.startswith(k):
            return v
    return 9.8


def f(v):
    try:
        x = float(v)
        return x if math.isfinite(x) else NAN
    except (TypeError, ValueError):
        return NAN


def load_kline(data_dir, days):
    """读 history 分支的 kline/<code>.json（每股一个文件，250 根日线）
    格式: {"code": "...", "bars": [["2026-01-05", open, close, high, low, volume, amount, turnover], ...]}
    """
    files = sorted(x for x in os.listdir(data_dir) if x.endswith(".json"))
    raw = {}
    all_dates = set()
    for fn in files:
        try:
            with open(os.path.join(data_dir, fn), "r", encoding="utf-8") as fh:
                obj = json.load(fh)
        except Exception:
            continue
        bars = obj.get("bars") or []
        code = obj.get("code") or fn[:-5]
        m = raw.setdefault(code, {})
        for b in bars:
            if not isinstance(b, list) or len(b) < 8:
                continue
            d = str(b[0])
            try:
                m[d] = [f(b[1]), f(b[2]), f(b[3]), f(b[4]), f(b[5]), f(b[6]), f(b[7])]
            except (TypeError, ValueError):
                continue
            all_dates.add(d)
    dates = sorted(all_dates)[-days:]
    keep = set(dates)
    for code in list(raw):
        raw[code] = {d: v for d, v in raw[code].items() if d in keep}
        if len(raw[code]) < 30:
            del raw[code]
    return dates, raw


def load_daily(data_dir, days):
    files = sorted(x for x in os.listdir(data_dir) if x.endswith(".json"))[-days:]
    dates, raw = [], {}
    for fn in files:
        try:
            with open(os.path.join(data_dir, fn), "r", encoding="utf-8") as fh:
                obj = json.load(fh)
        except Exception as e:                      # 坏文件跳过，不中断
            print("[warn] skip %s: %s" % (fn, e), file=sys.stderr)
            continue
        bars = obj.get("bars") or {}
        if not bars:
            continue
        d = fn[:-5]
        dates.append(d)
        for code, b in bars.items():
            if not isinstance(b, list) or len(b) < 7:
                continue
            raw.setdefault(code, {})[d] = [f(x) for x in b[:7]]
    return dates, raw


def roll_mean(x, w):
    out = [NAN] * len(x)
    s = 0.0
    for i, v in enumerate(x):
        s += v
        if i >= w:
            s -= x[i - w]
        if i >= w - 1:
            out[i] = s / w
    return out


def roll_max(x, w):
    out, dq = [NAN] * len(x), deque()
    for i, v in enumerate(x):
        while dq and x[dq[-1]] <= v:
            dq.pop()
        dq.append(i)
        if dq[0] <= i - w:
            dq.popleft()
        if i >= w - 1:
            out[i] = x[dq[0]]
    return out


def roll_min(x, w):
    out, dq = [NAN] * len(x), deque()
    for i, v in enumerate(x):
        while dq and x[dq[-1]] >= v:
            dq.pop()
        dq.append(i)
        if dq[0] <= i - w:
            dq.popleft()
        if i >= w - 1:
            out[i] = x[dq[0]]
    return out


def ewm_alpha(x, a):
    out, prev = [NAN] * len(x), None
    for i, v in enumerate(x):
        if v != v:
            out[i] = prev if prev is not None else NAN
            continue
        prev = v if prev is None else a * v + (1 - a) * prev
        out[i] = prev
    return out


def build(dates, raw, min_bars=30):
    idx_of = {d: i for i, d in enumerate(dates)}
    n = len(dates)
    up = [0] * n
    tot = [0] * n
    bars = {}

    for code, m in raw.items():
        if len(m) < min_bars:
            continue
        di = sorted(idx_of[d] for d in m)
        rows = [m[dates[i]] for i in di]
        o = [r[0] for r in rows]
        c = [r[1] for r in rows]
        h = [r[2] for r in rows]
        lo = [r[3] for r in rows]
        v = [r[4] for r in rows]
        a = [r[5] for r in rows]
        L = len(c)
        if any(not (x > 0) for x in c):
            continue

        ma5, ma20, ma60 = roll_mean(c, 5), roll_mean(c, 20), roll_mean(c, 60)
        w60 = min(60, L)
        hh60, ll60 = roll_max(h, w60), roll_min(lo, w60)
        pos60 = [((c[i] - ll60[i]) / (hh60[i] - ll60[i])) if (hh60[i] == hh60[i] and hh60[i] > ll60[i]) else 0.5
                 for i in range(L)]

        chg5 = [NAN] * L
        chg20 = [NAN] * L
        for i in range(5, L):
            if c[i - 5] > 0:
                chg5[i] = (c[i] / c[i - 5] - 1) * 100
        for i in range(20, L):
            if c[i - 20] > 0:
                chg20[i] = (c[i] / c[i - 20] - 1) * 100

        vprev = [NAN] * L
        run = 0.0
        for i in range(L):
            if i >= 1:
                run += v[i - 1]
                if i - 6 >= 0:
                    run -= v[i - 6]
                if i >= 5:
                    vprev[i] = run / 5
        vr5 = [(v[i] / vprev[i]) if (vprev[i] == vprev[i] and vprev[i] > 0) else NAN for i in range(L)]

        trs = [NAN] * L
        for i in range(1, L):
            trs[i] = max(h[i] - lo[i], abs(h[i] - c[i - 1]), abs(lo[i] - c[i - 1]))
        atr = roll_mean(trs[1:], 14)
        atrp = [NAN] + [(atr[i] / c[i + 1] * 100) if (atr[i] == atr[i] and c[i + 1] > 0) else NAN
                        for i in range(L - 1)]

        diff = [NAN] + [c[i] - c[i - 1] for i in range(1, L)]
        gain = ewm_alpha([x if x == x and x > 0 else (0.0 if x == x else NAN) for x in diff], 1 / 14)
        loss = ewm_alpha([(-x) if x == x and x < 0 else (0.0 if x == x else NAN) for x in diff], 1 / 14)
        rsi = []
        for i in range(L):
            g, l_ = gain[i], loss[i]
            if g != g or l_ != l_:
                rsi.append(NAN)
            elif l_ == 0:
                rsi.append(100.0)
            else:
                rsi.append(100 - 100 / (1 + g / l_))

        cs = [((c[i] - lo[i]) / (h[i] - lo[i])) if h[i] > lo[i] else 0.7 for i in range(L)]

        lim = limit_pct(code)
        board = [0] * L
        runb = 0
        for i in range(L):
            if i >= 1 and c[i - 1] > 0:
                need = round(c[i - 1] * (1 + lim / 100), 2)
                if c[i] >= need - 0.005 and h[i] >= need - 0.02:
                    runb += 1
                else:
                    runb = 0
            else:
                runb = 0
            board[i] = runb
        is_limit = [board[i] >= 1 and c[i] >= h[i] - 0.011 * c[i] for i in range(L)]

        cp = [NAN] + [(c[i] / c[i - 1] - 1) * 100 if c[i - 1] > 0 else NAN for i in range(1, L)]

        for i in range(L):
            m20 = ma20[i]
            if m20 == m20 and m20 > 0:
                tot[di[i]] += 1
                if c[i] > m20:
                    up[di[i]] += 1

        bars[code] = dict(i=di, o=o, c=c, h=h, l=lo, v=v, amount=a,
                          ma5=ma5, ma20=ma20, ma60=ma60, pos60=pos60, chg5=chg5, chg20=chg20,
                          vr5=vr5, atrp=atrp, cs=cs, board=board, is_limit=is_limit, rsi=rsi, cp=cp)

    breadth = [(up[i] / tot[i] * 100) if tot[i] else NAN for i in range(n)]
    return bars, breadth, dates


# ---------------------------------------------------------------- 规则库

def r_s1_strict(F):
    return [1 if (F["chg20"][i] < -35 and F["rsi"][i] < 30 and F["pos60"][i] < 0.2
                  and 1 < F["vr5"][i] < 2.5 and F["cs"][i] > 0.6
                  and F["c"][i] > F["ma5"][i] and F["amount"][i] > 3e8) else 0
            for i in range(len(F["c"]))]


def r_s1_loose(F):
    return [1 if (F["chg20"][i] < -25 and F["rsi"][i] < 35 and F["pos60"][i] < 0.3
                  and F["cs"][i] > 0.6 and 1 < F["vr5"][i] <= 10) else 0
            for i in range(len(F["c"]))]


def r_oversold_reversal(F):
    return [1 if (F["chg20"][i] < -25 and F["rsi"][i] < 35 and F["cs"][i] > 0.65
                  and F["vr5"][i] > 1.2 and F["amount"][i] > 3e8) else 0
            for i in range(len(F["c"]))]


def r_first_board(F):
    return [1 if (F["board"][i] == 1 and F["is_limit"][i] and F["amount"][i] > 5e8) else 0
            for i in range(len(F["c"]))]


def r_multi_board(F):
    return [1 if (F["board"][i] >= 2 and F["is_limit"][i]) else 0 for i in range(len(F["c"]))]


def r_pullback_ma20(F):
    return [1 if (F["c"][i] > F["ma20"][i] and F["chg5"][i] < -2 and F["vr5"][i] < 0.9
                  and F["amount"][i] > 3e8) else 0 for i in range(len(F["c"]))]


def r_breakout20(F):
    return [1 if (F["pos60"][i] > 0.92 and F["chg5"][i] > 0 and F["amount"][i] > 3e8) else 0
            for i in range(len(F["c"]))]


def r_neutral_tape(F):
    return [1 if (-5 < F["chg20"][i] < 5 and 40 < F["rsi"][i] < 55 and F["pos60"][i] < 0.5
                  and F["amount"][i] > 3e8) else 0 for i in range(len(F["c"]))]


def _s1_score(c, r, p, v, cs):
    if c != c or r != r or p != p or v != v or cs != cs:
        return None
    s = 30 if c <= -35 else 22 if c <= -28 else 14 if c <= -20 else 0
    s += 25 if r < 20 else 18 if r < 30 else 10 if r < 40 else 0
    s += 20 if p < 0.10 else 14 if p < 0.20 else 8 if p < 0.30 else 0
    s += 15 if 1 < v < 2.5 else 8 if 1 < v <= 5 else 0
    s += 10 if cs > 0.6 else 0
    return s


def r_s1_score_a(F):
    out = []
    for i in range(len(F["c"])):
        sc = _s1_score(F["chg20"][i], F["rsi"][i], F["pos60"][i], F["vr5"][i], F["cs"][i])
        out.append(1 if sc is not None and sc >= 75 and F["amount"][i] > 3e8 else 0)
    return out


def r_s1_score_b(F):
    out = []
    for i in range(len(F["c"])):
        sc = _s1_score(F["chg20"][i], F["rsi"][i], F["pos60"][i], F["vr5"][i], F["cs"][i])
        out.append(1 if sc is not None and 55 <= sc < 75 and F["amount"][i] > 3e8 else 0)
    return out


RULES = [
    ("s1_strict", "S1 严格档（线上现配置）", r_s1_strict),
    ("s1_loose", "S1 宽松档（线上观察档）", r_s1_loose),
    ("oversold_reversal", "超跌 + 放量反包", r_oversold_reversal),
    ("first_board", "首板打板", r_first_board, "close"),
    ("multi_board", "连板 ≥2", r_multi_board, "close"),
    ("pullback_ma20", "强势回踩 MA20（缩量）", r_pullback_ma20),
    ("breakout20", "突破 20 日新高", r_breakout20),
    ("neutral_tape", "中性量价（S4 示例）", r_neutral_tape),
    ("s1_score_a", "S1 分级 A 档（评分≥75）", r_s1_score_a),
    ("s1_score_b", "S1 分级 B 档（55~74，观察）", r_s1_score_b),
]

PARAMS = [
    {"stop": 2.0, "target": 1.0, "hold": 2},
    {"stop": 2.0, "target": 2.0, "hold": 2},
    {"stop": 2.0, "target": 1.0, "hold": 3},
]


def simulate(F, mask, stop_mult, target_r, hold, gap_max=0.03, entry_mode="open"):
    c, o, h, l = F["c"], F["o"], F["h"], F["l"]
    out = []
    n = len(c)
    for p in range(n):
        if not mask[p]:
            continue
        price = c[p]
        if not (price > 0):
            continue
        if p + 1 >= n:
            continue
        if entry_mode == "close":
            entry = price                 # 打板：当日涨停价成交
        else:
            if F["cp"][p] >= 9.5:         # 当日已涨停，次日买不进
                continue
            entry = o[p + 1]
            if not (entry > 0) or entry / price - 1 > gap_max:
                continue
        a = F["atrp"][p]
        sp = min(0.30, stop_mult * (a if a == a and a > 0 else 4) / 100)
        stop = entry * (1 - sp)
        target = entry * (1 + sp * target_r)
        ret = None
        end = min(p + 1 + hold, n)
        for j in range(p + 1, end):
            if l[j] <= stop:
                ret = (min(o[j] if o[j] > 0 else stop, stop) / entry - 1) * 100
                break
            if h[j] >= target:
                ret = (target / entry - 1) * 100
                break
            if j == end - 1:
                ret = (c[j] / entry - 1) * 100
        if ret is None:
            continue
        out.append((F["i"][p], ret - COST * 100))
    return out


def summarize(vals):
    if not vals:
        return {"n": 0}
    r = sorted(vals)
    n = len(r)
    win = sum(1 for x in r if x > 0) / n * 100
    avg = sum(r) / n
    pos = sum(x for x in r if x > 0)
    neg = sum(x for x in r if x <= 0)
    pf = (pos / abs(neg)) if neg else 9.99
    med = r[n // 2] if n % 2 else (r[n // 2 - 1] + r[n // 2]) / 2
    wins = [x for x in r if x > 0]
    loss = [x for x in r if x <= 0]
    return {"n": n, "win": round(win, 2), "avg": round(avg, 3), "median": round(med, 3),
            "pf": round(pf, 2),
            "avgWin": round(sum(wins) / len(wins), 2) if wins else 0.0,
            "avgLoss": round(sum(loss) / len(loss), 2) if loss else 0.0}


def benchmark(bars, hold):
    per_day = {}
    for F in bars.values():
        n = len(F["c"])
        for p in range(0, n - hold - 1):
            entry, exit_ = F["o"][p + 1], F["c"][p + 1 + hold]
            if entry > 0 and exit_ > 0:
                per_day.setdefault(F["i"][p], []).append((exit_ / entry - 1) * 100)
    if not per_day:
        return None
    means = [sum(v) / len(v) for v in per_day.values() if v]
    return round(sum(means) / len(means), 3)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--data", default="daily")
    ap.add_argument("--mode", default="daily", choices=["daily", "kline"])
    ap.add_argument("--out", default="out/backtest.json")
    ap.add_argument("--days", type=int, default=500)
    ap.add_argument("--min-samples", type=int, default=100)
    args = ap.parse_args()

    loader = load_kline if args.mode == "kline" else load_daily
    dates, raw = loader(args.data, args.days)
    if not dates:
        print("no data", file=sys.stderr)
        sys.exit(1)
    print("dates=%d %s..%s codes=%d" % (len(dates), dates[0], dates[-1], len(raw)), flush=True)

    bars, breadth, dates = build(dates, raw)
    ok = [b for b in breadth if b == b]
    print("codes ok=%d breadth=%.1f%%" % (len(bars), (sum(ok) / len(ok) if ok else NAN)), flush=True)

    bench = {"hold%d" % h: benchmark(bars, h) for h in (2, 3, 5)}
    print("benchmark", bench, flush=True)

    results = []
    for item in RULES:
        rid, name, fn = item[0], item[1], item[2]
        entry_mode = item[3] if len(item) > 3 else "open"
        for prm in PARAMS:
            trades = []
            for code, F in bars.items():
                mask = fn(F)
                if any(mask):
                    trades.extend(simulate(F, mask, prm["stop"], prm["target"], prm["hold"],
                                           entry_mode=entry_mode))
            if not trades:
                results.append({"id": rid, "name": name, "params": prm, "stats": {"n": 0}})
                print("%-20s %s n=0" % (rid, prm), flush=True)
                continue
            stats = summarize([t[1] for t in trades])
            low = [t[1] for t in trades if breadth[t[0]] == breadth[t[0]] and breadth[t[0]] < 45]
            high = [t[1] for t in trades if breadth[t[0]] == breadth[t[0]] and breadth[t[0]] >= 45]
            b = bench.get("hold%d" % prm["hold"])
            vs = round(stats["avg"] - (b or 0), 3)
            passed = (stats["n"] >= args.min_samples and b is not None and vs > 0.15 and stats["win"] >= 43)
            results.append({
                "id": rid, "name": name, "params": prm, "stats": stats,
                "byBreadth": {"low45": summarize(low), "high45": summarize(high)},
                "vsBenchmark": vs,
                "gate": {"passed": bool(passed),
                         "reason": ("样本不足" if stats["n"] < args.min_samples
                                    else "未跑赢基准 0.15%" if vs <= 0.15
                                    else "胜率低于 43%" if stats["win"] < 43 else "通过")},
            })
            print("%-20s %s n=%6d win=%5.1f%% avg=%+6.2f%% vs=%+5.2f%%"
                  % (rid, prm, stats["n"], stats["win"], stats["avg"], vs), flush=True)

    out = {
        "v": 1,
        "generatedAt": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "window": {"from": dates[0], "to": dates[-1], "days": len(dates), "codes": len(bars)},
        "benchmark": bench, "cost": COST, "rules": results,
    }
    d = os.path.dirname(args.out)
    if d:
        os.makedirs(d, exist_ok=True)
    with open(args.out, "w", encoding="utf-8") as fh:
        json.dump(out, fh, ensure_ascii=False, separators=(",", ":"))
    print("written", args.out, os.path.getsize(args.out), "bytes")


if __name__ == "__main__":
    main()
