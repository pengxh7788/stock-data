#!/usr/bin/env python3
"""history.json 完整性护栏（校验 → 自动回填 → 复查 → 不合格则报警）。

用法:
    python3 scripts/guard_history.py                    # 校验 + 自动补 + 复查
    python3 scripts/guard_history.py --dry              # 只校验不补
    python3 scripts/guard_history.py --threshold 0.03   # 缺失率阈值（默认 5%）
    python3 scripts/guard_history.py --limit 300        # 只处理 300 只（冒烟测试）

退出码: 0 合格 / 2 仍有缺口（应中止发布）
产物:   data/_quality.json  —— 每次运行的体检报告，可直接喂给前端「链路状态」页
"""
import json, sys, os, time, urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
HIST = os.path.join(ROOT, "data", "history.json")
REPORT = os.path.join(ROOT, "data", "_quality.json")
RECENT = 60          # 只体检最近 60 个交易日
THRESH = 0.05        # 单日缺失率超过 5% 判为异常

def secid(code):
    n = code[-6:]
    if code.startswith("sh"): return "1." + n
    if code.startswith("bj"): return "0." + n
    return ("1." if n[0] == "6" else "0.") + n

def em_kline(code, retry=2):
    u = ("https://push2his.eastmoney.com/api/qt/stock/kline/get?secid=" + secid(code) +
         "&fields1=f1,f2,f3,f4,f5,f6&fields2=f51,f52,f53,f54,f55,f56&klt=101&fqt=1"
         "&beg=20260101&end=20500101")
    for i in range(retry + 1):
        try:
            r = urllib.request.urlopen(urllib.request.Request(
                u, headers={"User-Agent": "Mozilla/5.0", "Referer": "https://quote.eastmoney.com/"}),
                timeout=15)
            kl = (json.loads(r.read()).get("data") or {}).get("klines") or []
            return {s.split(",")[0]: (float(s.split(",")[2]), float(s.split(",")[3]),
                                      float(s.split(",")[4]), float(s.split(",")[5])) for s in kl}
        except Exception:
            if i == retry: return None
            time.sleep(0.4 * (i + 1))

def coverage(H, dates):
    n = len(H["close"]); start = max(0, len(dates) - RECENT)
    return [(i, dates[i], sum(1 for v in H["close"].values() if not v[i])) for i in range(start, len(dates))]

def main():
    thr = THRESH
    if "--threshold" in sys.argv: thr = float(sys.argv[sys.argv.index("--threshold") + 1])
    limit = int(sys.argv[sys.argv.index("--limit") + 1]) if "--limit" in sys.argv else 0
    dry = "--dry" in sys.argv

    H = json.load(open(HIST, encoding="utf-8"))
    dates = H["dates"]; total = len(H["close"])
    for k in ("close", "high", "low", "volume"):
        H.setdefault(k, {})

    cov = coverage(H, dates)
    bad = [(i, d, c) for i, d, c in cov if c / total > thr]
    print(f"[体检] {total} 只 / 最近 {RECENT} 日 / 阈值 {thr:.0%} → 异常日 {len(bad)} 个", flush=True)
    for i, d, c in bad:
        print(f"  ✗ {d}  缺 {c} ({c/total:.1%})", flush=True)

    # 需要修的：异常日的空格 + 最近窗口内单票缺口
    start = max(0, len(dates) - RECENT)
    need = {i for i, _, _ in bad}
    codes = [c for c, v in H["close"].items()
             if any(not v[i] for i in range(start, len(dates)))]
    print(f"[计划] 待修单元格涉及 {len(codes)} 只 / {len(need)} 个整列异常日", flush=True)
    if dry or not codes:
        json.dump({"checkedAt": time.strftime("%Y-%m-%dT%H:%M:%S"), "total": total,
                   "badDates": [{"date": d, "missing": c} for _, d, c in bad],
                   "repaired": 0, "ok": not bad}, open(REPORT, "w"), ensure_ascii=False, indent=1)
        sys.exit(2 if bad else 0)

    if limit: codes = codes[:limit]
    filled = 0; done = 0
    with ThreadPoolExecutor(max_workers=8) as ex:
        futs = {ex.submit(em_kline, c): c for c in codes}
        for f in as_completed(futs):
            code = futs[f]; m = f.result(); done += 1
            if done % 500 == 0: print(f"  [回填] {done}/{len(codes)} · 已填 {filled}", flush=True)
            if not m: continue
            for name, k in (("close", 0), ("high", 1), ("low", 2), ("volume", 3)):
                arr = H[name].get(code)
                if arr is None or len(arr) != len(dates):
                    arr = ([None] * (len(dates) - len(arr)) + list(arr)) if arr and len(arr) < len(dates) else [None] * len(dates)
                for i in range(start, len(dates)):
                    if arr[i]: continue
                    row = m.get(dates[i])
                    if row: arr[i] = row[k]; filled += 1
                H[name][code] = arr

    json.dump(H, open(HIST, "w", encoding="utf-8"), ensure_ascii=False, separators=(",", ":"))
    print(f"[回填] 完成，写入 {filled} 个单元格", flush=True)

    # 复查
    cov2 = coverage(H, dates)
    still = [(d, c) for _, d, c in cov2 if c / total > thr]
    ok = not still
    json.dump({"checkedAt": time.strftime("%Y-%m-%dT%H:%M:%S"), "total": total,
               "badDatesBefore": [{"date": d, "missing": c} for _, d, c in bad],
               "repairedCells": filled,
               "badDatesAfter": [{"date": d, "missing": c} for d, c in still],
               "ok": ok}, open(REPORT, "w"), ensure_ascii=False, indent=1)
    if ok:
        print("[结果] ✅ 历史数据完整，可以发布", flush=True)
        sys.exit(0)
    print("[结果] ❌ 回填后仍有缺口，**中止发布**：", flush=True)
    for d, c in still: print(f"  {d} 缺 {c}", flush=True)
    sys.exit(2)

if __name__ == "__main__":
    main()
