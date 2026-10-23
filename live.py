#!/usr/bin/env python3
# 盘中实时快照：抓腾讯实时行情 → data/live.json（约 300KB）
import json, os, sys, time, urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed

BASE = os.path.dirname(os.path.abspath(__file__))
DATA = os.path.join(BASE, "data")
UA = {"User-Agent": "Mozilla/5.0", "Referer": "https://finance.qq.com"}
BATCH = 100
WORKERS = 4


def load_codes():
    p = os.path.join(DATA, "quotes.json")
    try:
        q = json.load(open(p, encoding="utf-8")).get("quotes") or {}
        codes = [c for c in q.keys() if q[c].get("p")]
        if codes:
            print(f"[输入] quotes.json {len(codes)} 只")
            return codes
    except Exception as e:
        print(f"[输入] quotes.json 读取失败: {e}")
    p2 = os.path.join(DATA, "analysis.json")
    codes = list((json.load(open(p2, encoding="utf-8")).get("stocks") or {}).keys())
    print(f"[输入] 回退 analysis.json {len(codes)} 只")
    return codes


def fetch(batch):
    url = "https://qt.gtimg.cn/q=" + ",".join(batch)
    for _ in range(2):
        try:
            req = urllib.request.Request(url, headers=UA)
            txt = urllib.request.urlopen(req, timeout=20).read().decode("gb18030", "ignore")
            out = {}
            for seg in txt.split(";"):
                if "=" not in seg or not seg.strip().startswith("v_"):
                    continue
                var, body = seg.split("=", 1)
                code = var.strip()[2:]            # v_sh601991 → sh601991
                body = body.strip().strip('"')
                f = body.split("~")
                if len(f) < 38 or not f[2]:
                    continue
                try:
                    price = float(f[3])
                    chg = float(f[32])
                    amt = float(f[37] or 0)
                    vol = float(f[36] or 0)
                except Exception:
                    continue
                if price <= 0:
                    continue
                out[code] = [round(price, 2), round(chg, 2), round(amt, 0), round(vol, 0)]
            return out
        except Exception:
            time.sleep(0.5)
    return {}


def main():
    t0 = time.time()
    codes = load_codes()
    batches = [codes[i:i + BATCH] for i in range(0, len(codes), BATCH)]
    print(f"[抓取] {len(batches)} 批，并发 {WORKERS}")
    res = {}
    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        futs = {ex.submit(fetch, b): i for i, b in enumerate(batches)}
        done = 0
        for fu in as_completed(futs):
            res.update(fu.result() or {})
            done += 1
            if done % 10 == 0:
                print(f"  {done}/{len(batches)} 批，已取 {len(res)} 只")
    now = time.time()
    payload = {
        "ts": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(now)),
        "date": time.strftime("%Y-%m-%d", time.gmtime(now + 8 * 3600)),
        "count": len(res),
        "q": res,
    }
    os.makedirs(DATA, exist_ok=True)
    with open(os.path.join(DATA, "live.json"), "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, separators=(",", ":"))
    size = os.path.getsize(os.path.join(DATA, "live.json")) / 1024
    print(f"[完成] {len(res)} 只，{size:.0f} KB，用时 {time.time() - t0:.0f}s")
    ratio = len(res) / max(1, len(codes))
    if ratio < 0.6:
        print(f"[警告] 只抓到 {ratio*100:.0f}% 的股票，可能是接口异常")
        sys.exit(1)


if __name__ == "__main__":
    main()
