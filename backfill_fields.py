#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
backfill_fields.py —— 用现有 data/*.json 一次性补齐所有新字段

用途：不想等下一次采集跑完，立刻让前端拿到 board / isLimitUp / breadth /
      vr5 / atrPct / pos60 / prevHigh / prevChg。

用法：
    python3 backfill_fields.py                  # 就地改 data/analysis.json（会备份）
    python3 backfill_fields.py --out xxx.json   # 另存，不动原文件

补的字段：
    market.status        = market.regime（同值别名，消除矛盾）
    market.positionCap   = 由 regime 推导
    market.breadth       = 收盘>MA20 的占比
    modelEval            退化档（lo==hi）置 null + invalid
    stocks[].board       连板数
    stocks[].isLimitUp   当日是否封板
    stocks[].vr5         今日量 / 前5日均量
    stocks[].atrPct      ATR14 / 现价 × 100
    stocks[].pos60       60 日波段位置
    stocks[].prevHigh    昨日最高
    stocks[].prevChg     昨日涨跌幅
"""
import json
import os
import sys
import shutil

DATA = "data"
CAP = {"进攻": 0.8, "中性": 0.5, "防守": 0.3, "空仓": 0.0}


def load(p, default=None):
    try:
        with open(p, encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        print(f"[warn] 读取 {p} 失败: {e}")
        return default


def limit_pct(code):
    if code.startswith("bj"):
        return 0.30
    if code.startswith(("sh688", "sh689", "sz30")):
        return 0.20
    return 0.10


def main():
    out_path = None
    if "--out" in sys.argv:
        out_path = sys.argv[sys.argv.index("--out") + 1]

    analysis = load(os.path.join(DATA, "analysis.json"))
    quotes_f = load(os.path.join(DATA, "quotes.json"), {})
    hist = load(os.path.join(DATA, "history.json"), {})
    if not analysis or not analysis.get("stocks"):
        print("[fatal] analysis.json 缺失或无 stocks")
        return 1

    quotes = quotes_f.get("quotes") or quotes_f or {}
    stocks = analysis["stocks"]
    market = analysis.setdefault("market", {})

    cser = hist.get("close") or {}
    hser = hist.get("high") or {}
    lser = hist.get("low") or {}
    vser = hist.get("volume") or {}

    # ---------- 1. 市况字段 ----------
    regime = market.get("regime") or "防守"
    market["status"] = regime
    market["positionCap"] = CAP.get(regime, 0.3)

    # ---------- 2. 市场宽度 ----------
    above = tot = 0
    for c, st in stocks.items():
        q = quotes.get(c) or {}
        p = q.get("p") or 0
        m20 = st.get("ma20")
        if not p or not m20:
            continue
        tot += 1
        if p > m20:
            above += 1
    market["breadth"] = round(above / tot * 100, 1) if tot else None
    market["breadthDef"] = "收盘价 > MA20 的股票占比(%)"
    print(f"[市况] status={market['status']} cap={market['positionCap']} breadth={market['breadth']}")

    # ---------- 3. modelEval 退化档 ----------
    me = analysis.get("modelEval") or {}
    bad = 0
    for reg, buckets in (me.get("byRegime") or {}).items():
        for bk, v in (buckets or {}).items():
            if not v:
                continue
            if v.get("lo") == v.get("hi") or (v.get("mid") == v.get("lo") == v.get("hi")):
                v.update({"mid": None, "lo": None, "hi": None, "invalid": True,
                          "reason": "lo=hi，统计退化"})
                bad += 1
    if me.get("byRegime"):
        me["degenerateCount"] = bad
        analysis["modelEval"] = me
    print(f"[modelEval] 置空退化档 {bad} 个")

    # ---------- 4. 逐只补字段 ----------
    ok_board = ok_vr5 = ok_atr = ok_pos = 0
    for code, st in stocks.items():
        try:
            q = quotes.get(code) or {}
            today_p = q.get("p") or 0
            today_h = q.get("h") or 0
            today_l = q.get("l") or 0
            today_v = q.get("v") or 0

            # 历史有效行（收/高/低俱全），剔除末尾占位
            cs = cser.get(code) or []
            hs = hser.get(code) or []
            ls = lser.get(code) or []
            n = min(len(cs), len(hs), len(ls))
            rows = []
            for i in range(max(0, n - 70), n):
                if cs[i] and hs[i] and ls[i]:
                    rows.append((float(cs[i]), float(hs[i]), float(ls[i])))

            # 把今日行情接到最后（若历史末行就是今天，替换而不是追加）
            if today_p > 0:
                if rows and abs(rows[-1][0] - today_p) < 1e-6 and today_h and rows[-1][1] == today_h:
                    rows[-1] = (today_p, today_h or today_p, today_l or today_p)
                else:
                    rows.append((today_p, today_h or today_p, today_l or today_p))

            # ---- board / isLimitUp ----
            st["board"] = 0
            st["isLimitUp"] = False
            if len(rows) >= 2 and today_p > 0:
                lim = limit_pct(code)
                r1 = rows[-1][0] / rows[-2][0] - 1
                if r1 >= lim * 0.98 and rows[-1][0] >= rows[-1][1] * 0.999:
                    b = 1
                    for k in range(3, min(10, len(rows) + 1)):
                        if rows[-k][0] / rows[-k - 1][0] - 1 >= lim * 0.98 and rows[-k][0] >= rows[-k][1] * 0.999:
                            b += 1
                        else:
                            break
                    st["board"] = b
                    st["isLimitUp"] = True
                    ok_board += 1

            # ---- vr5 ----
            # 历史量单位 = 万股；quotes.v 单位：科创板是「股」，其他是「手」
            vs = [x for x in (vser.get(code) or []) if x]
            st["vr5"] = None
            if len(vs) >= 5 and today_v > 0:
                factor = 10000 if code.startswith(("sh688", "sh689")) else 100
                base5 = sum(vs[-5:]) / 5.0 * factor
                if base5 > 0:
                    st["vr5"] = round(today_v / base5, 2)
                    ok_vr5 += 1

            # ---- atrPct ----
            st["atrPct"] = None
            if len(rows) >= 15:
                trs = []
                for k in range(len(rows) - 14, len(rows)):
                    cl, hh, ll = rows[k]
                    pcl = rows[k - 1][0]
                    trs.append(max(hh - ll, abs(hh - pcl), abs(ll - pcl)))
                if rows[-1][0] > 0:
                    st["atrPct"] = round((sum(trs) / 14.0) / rows[-1][0] * 100, 2)
                    ok_atr += 1

            # ---- pos60 ----
            st["pos60"] = None
            if len(rows) >= 60:
                seg = rows[-60:]
                mx = max(x[1] for x in seg)
                mn = min(x[2] for x in seg)
                if mx > mn:
                    st["pos60"] = round((rows[-1][0] - mn) / (mx - mn), 3)
                    ok_pos += 1

            # ---- prevHigh / prevChg（相对今日的"昨天"）----
            st["prevHigh"] = round(rows[-2][1], 2) if len(rows) >= 2 else None
            st["prevChg"] = round((rows[-2][0] / rows[-3][0] - 1) * 100, 2) if len(rows) >= 3 and rows[-3][0] > 0 else None
        except Exception:
            st.setdefault("board", 0)
            st.setdefault("isLimitUp", False)
            st.setdefault("vr5", None)
            st.setdefault("atrPct", None)
            st.setdefault("pos60", None)
            st.setdefault("prevHigh", None)
            st.setdefault("prevChg", None)

    # ---------- 4.4 板块多空（当日口径）----------
    secs = market.get("sectors") or []
    if secs:
        bd = {"up": 0, "flat": 0, "down": 0}
        bm = {"up": 0, "flat": 0, "down": 0}
        for sec in secs:
            chg = sec.get("chg") or 0
            td = "up" if chg > 0.1 else ("down" if chg < -0.1 else "flat")
            sec["trendDay"] = td
            bd[td] += 1
            tr = sec.get("trend")
            if tr in bm:
                bm[tr] += 1
        bd["total"] = len(secs)
        bm["total"] = len(secs)
        market["sectorBreadthDay"] = bd
        market["sectorBreadthMid"] = bm
        print(f"[板块] 当日 涨{bd['up']}/平{bd['flat']}/跌{bd['down']}  中期 涨{bm['up']}/平{bm['flat']}/跌{bm['down']}  共{len(secs)}")

    # ---------- 4.5 ST / 停牌 ----------
    ok_st = 0
    for code, st in stocks.items():
        try:
            q = quotes.get(code) or {}
            nm = q.get("n") or ""
            st["isST"] = bool(("ST" in nm.upper()) or ("退" in nm))
            if st["isST"]:
                ok_st += 1
            vs2 = [x for x in (vser.get(code) or []) if x is not None]
            tail = vs2[-3:] if vs2 else []
            st["isSuspended"] = bool(tail) and all((not v) for v in tail)
        except Exception:
            st.setdefault("isST", False)
            st.setdefault("isSuspended", False)

    total = len(stocks)
    print(f"[字段] 封板 {ok_board} / vr5 {ok_vr5} / atrPct {ok_atr} / pos60 {ok_pos} / ST {ok_st}  (共 {total})")

    # ---------- 5. 写回 ----------
    if out_path:
        dst = out_path
    else:
        dst = os.path.join(DATA, "analysis.json")
        bak = dst + ".bak"
        shutil.copy(dst, bak)
        print(f"[备份] {bak}")
    with open(dst, "w", encoding="utf-8") as f:
        json.dump(analysis, f, ensure_ascii=False, separators=(",", ":"))
    print(f"[完成] 写入 {dst}  {os.path.getsize(dst)/1024/1024:.2f} MB")
    return 0


if __name__ == "__main__":
    sys.exit(main())
