#!/usr/bin/env python3
"""history.json 完整性护栏：体检 → 回填 → 复查 → 不合格中止发布。

回填优先级：
  ① history 分支 daily/<date>.json（自己的每日全市场分片，同源、无外依赖）
  ② 东财前复权日 K（daily 里也没有时兜底）

用法:
    python3 scripts/guard_history.py                  # 体检 + 自动回填 + 复查
    python3 scripts/guard_history.py --dry            # 只体检（CI 可用）
    python3 scripts/guard_history.py --threshold 0.03 # 缺失率阈值（默认 5%）
    python3 scripts/guard_history.py --limit 300      # 只回填 300 只（冒烟）
    python3 scripts/guard_history.py --no-net         # 只用 daily 分片，不调东财

退出码: 0 合格 / 2 仍有缺口（必须中止发布）
产物:   data/_quality.json（体检报告，可喂给前端「链路状态」）
"""
import json, sys, os, time, urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
HIST = os.path.join(ROOT, "data", "history.json")
REPORT = os.path.join(ROOT, "data", "_quality.json")
REPO = os.environ.get("OWNER_REPO", "pengxh7788/stock-data")
RAW = os.environ.get("RAW_BASE", f"https://gh-proxy.com/https://raw.githubusercontent.com/{REPO}/history")
RECENT = 60      # 只体检最近 60 个交易日
THRESH = 0.05    # 单日缺失率 > 5% 判异常

def http_json(url, retry=2, timeout=30):
    for i in range(retry + 1):
        try:
            r = urllib.request.urlopen(urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"}), timeout=timeout)
            return json.loads(r.read())
        except Exception:
            if i == retry: return None
            time.sleep(0.5 * (i + 1))

def fetch_daily(date):
    """history 分支的当日全市场分片 → {code: [o,c,h,l,v,...]}"""
    for base in (RAW, f"https://raw.githubusercontent.com/{REPO}/history"):
        j = http_json(f"{base}/daily/{date}.json")
        if j and j.get("bars"): return j["bars"]
    return None

def secid(code):
    n = code[-6:]
    if code.startswith("sh"): return "1." + n
    if code.startswith("bj"): return "0." + n
    return ("1." if n[0] == "6" else "0.") + n

def em_kline(code, retry=1):
    u = ("https://push2his.eastmoney.com/api/qt/stock/kline/get?secid=" + secid(code) +
         "&fields1=f1,f2&fields2=f51,f52,f53,f54,f55,f56&klt=101&fqt=1&beg=20260101&end=20500101")
    for i in range(retry + 1):
        try:
            j = json.loads(urllib.request.urlopen(urllib.request.Request(
                u, headers={"User-Agent": "Mozilla/5.0", "Referer": "https://quote.eastmoney.com/"}),
                timeout=15).read())
            return {s.split(",")[0]: (float(s.split(",")[2]), float(s.split(",")[3]),
                                      float(s.split(",")[4]), float(s.split(",")[5]))
                    for s in ((j.get("data") or {}).get("klines") or [])}
        except Exception:
            if i == retry: return None
            time.sleep(0.4)

def main():
    thr = float(sys.argv[sys.argv.index("--threshold") + 1]) if "--threshold" in sys.argv else THRESH
    limit = int(sys.argv[sys.argv.index("--limit") + 1]) if "--limit" in sys.argv else 0
    dry = "--dry" in sys.argv
    no_net = "--no-net" in sys.argv

    H = json.load(open(HIST, encoding="utf-8"))
    dates = H["dates"]; total = len(H["close"])
    for k in ("close", "high", "low", "volume"):
        H.setdefault(k, {})

    start = max(0, len(dates) - RECENT)
    cov = [(i, dates[i], sum(1 for v in H["close"].values() if not v[i])) for i in range(start, len(dates))]
    bad = [(i, d, c) for i, d, c in cov if c / total > thr]
    need_codes = [c for c, v in H["close"].items() if any(not v[i] for i in range(start, len(dates)))]

    print(f"[体检] {total} 只 / 最近 {RECENT} 日 / 阈值 {thr:.0%}", flush=True)
    for _, d, c in bad:
        print(f"  ✗ {d}  缺 {c} ({c/total:.1%})", flush=True)
    if not bad:
        print("  ✓ 全部合格")
    print(f"[计划] 待修 {len(need_codes)} 只 / 异常整列 {len(bad)} 天", flush=True)

    filled = 0
    if bad and not dry:
        # ① 用自家 daily 分片回填
        idx = {d: i for i, d in enumerate(dates)}
        for _, d, _c in bad:
            bars = fetch_daily(d)
            if not bars:
                print(f"  [daily] {d} 取不到，留给东财兜底", flush=True); continue
            got = 0
            for code in need_codes:
                b = bars.get(code)
                if not b or len(b) < 5: continue
                for name, vi in (("close", 1), ("high", 2), ("low", 3), ("volume", 4)):
                    arr = H[name].get(code)
                    if arr is None or len(arr) != len(dates):
                        arr = [None] * len(dates) if not arr else ([None] * (len(dates) - len(arr)) + list(arr))
                    if not arr[idx[d]]:
                        arr[idx[d]] = b[vi]; got += 1
                    H[name][code] = arr
            print(f"  [daily] {d} 回填 {got} 格", flush=True)
            filled += got

        # ② 东财兜底（只补还空着的）
        left = [c for c in need_codes
                if any(not (H["close"].get(c) or [None]*len(dates))[i] for i in range(start, len(dates)))]
        if left and not no_net:
            if limit: left = left[:limit]
            print(f"  [东财] 兜底 {len(left)} 只", flush=True)
            done = 0
            with ThreadPoolExecutor(max_workers=8) as ex:
                futs = {ex.submit(em_kline, c): c for c in left}
                for f in as_completed(futs):
                    code = futs[f]; m = f.result(); done += 1
                    if done % 500 == 0: print(f"    {done}/{len(left)}", flush=True)
                    if not m: continue
                    for name, k in (("close", 0), ("high", 1), ("low", 2), ("volume", 3)):
                        arr = H[name].get(code)
                        if arr is None or len(arr) != len(dates):
                            arr = [None] * len(dates) if not arr else ([None] * (len(dates) - len(arr)) + list(arr))
                        for i in range(start, len(dates)):
                            if not arr[i]:
                                row = m.get(dates[i])
                                if row: arr[i] = row[k]; filled += 1
                        H[name][code] = arr

        json.dump(H, open(HIST, "w", encoding="utf-8"), ensure_ascii=False, separators=(",", ":"))

    cov2 = [(d, sum(1 for v in H["close"].values() if not v[i])) for i, d, _ in cov]
    still = [(d, c) for d, c in cov2 if c / total > thr]
    ok = not still
    json.dump({"checkedAt": time.strftime("%Y-%m-%dT%H:%M:%S"), "total": total, "threshold": thr,
               "badDatesBefore": [{"date": d, "missing": c} for _, d, c in bad],
               "repairedCells": filled,
               "badDatesAfter": [{"date": d, "missing": c} for d, c in still],
               "ok": ok}, open(REPORT, "w"), ensure_ascii=False, indent=1)
    if ok:
        print(f"[结果] ✅ 合格（本次回填 {filled} 格）", flush=True); sys.exit(0)
    print("[结果] ❌ 仍有缺口，中止发布：", flush=True)
    for d, c in still: print(f"  {d} 缺 {c}", flush=True)
    sys.exit(2)

if __name__ == "__main__":
    main()
