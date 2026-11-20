#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""生成 data/tail.json —— 前端算「含今日即时价」因子所需的最小数据包。

前端只需要三样东西就能算今日的 MA5/MA20/RSI14/ATR14/pos60：
  ① 最近 21 根 [收盘, 最高, 最低]
  ② 60 日区间的高低点（因为要 60 根，全发太大）
  ③ 昨收（在 ① 的最后一根里）

用法: python3 scripts/make_tail.py [--n 21]
输出: data/tail.json {"date":"2026-11-06","n":21,"d":{code:{"b":[[c,h,l],...],"lo60":x,"hi60":y}}}
"""
import json, os, sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
HIST = os.path.join(ROOT, "data", "history.json")
OUT = os.path.join(ROOT, "data", "tail.json")
N = int(sys.argv[sys.argv.index("--n") + 1]) if "--n" in sys.argv else 21
POS_N = 60


def main():
    H = json.load(open(HIST, encoding="utf-8"))
    dates = H.get("dates") or []
    if not dates:
        sys.exit("history.json 没有 dates")
    close = H.get("close") or {}
    high = H.get("high") or {}
    low = H.get("low") or {}
    hl_start = int(H.get("hlStart") or 0)
    m = len(dates)
    n = min(N, m)

    def pick(arr, start_idx, count):
        """从绝对下标 start_idx 起取 count 个；数组可能被裁过，返回等长列表"""
        out = []
        for i in range(start_idx, start_idx + count):
            j = i - hl_start if arr is high or arr is low else i
            out.append(arr[j] if 0 <= j < len(arr) else None)
        return out

    d = {}
    for code, closes in close.items():
        hs = high.get(code) or []
        ls = low.get(code) or []
        cs = closes

        bars = []
        for i in range(m - n, m):
            c = cs[i] if 0 <= i < len(cs) else None
            j = i - hl_start
            h = hs[j] if 0 <= j < len(hs) else None
            l = ls[j] if 0 <= j < len(ls) else None
            bars.append([c, h, l])

        # 60 日区间（用 high/low，与仓库 pos60 同口径）
        seg = range(max(0, m - POS_N), m)
        hi60 = None; lo60 = None
        for i in seg:
            j = i - hl_start
            if 0 <= j < len(hs) and hs[j] is not None:
                hi60 = hs[j] if hi60 is None else max(hi60, hs[j])
            if 0 <= j < len(ls) and ls[j] is not None:
                lo60 = ls[j] if lo60 is None else min(lo60, ls[j])

        d[code] = {"b": bars, "lo60": lo60, "hi60": hi60}

    out = {"date": dates[-1], "n": n, "d": d}
    json.dump(out, open(OUT, "w", encoding="utf-8"), ensure_ascii=False, separators=(",", ":"))
    size = os.path.getsize(OUT)
    print(f"[tail] {len(d)} 只 × {n} 根 + 60日区间 · {size/1048576:.2f} MB → {OUT}")


if __name__ == "__main__":
    main()
