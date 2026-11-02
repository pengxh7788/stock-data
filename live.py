#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""盘中行情：抓腾讯实时 → data/live.json + data/snap/<date>_<HHMM>.json

- live.json        最新一份（覆盖）
- snap/*.json      半小时桶（幂等，已有则跳过），保留 2 天
只在 A 股交易时段写入（09:25-11:35 / 12:55-15:05，北京时间）
"""
import json, os, sys, time, urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed

BASE = os.path.dirname(os.path.abspath(__file__))
DATA = os.path.join(BASE, "data")
SNAP = os.path.join(DATA, "snap")
UA = {"User-Agent": "Mozilla/5.0", "Referer": "https://finance.qq.com"}
BATCH = 100
WORKERS = 6
KEEP_DAYS = 2
BUCKETS = ["0930", "1000", "1030", "1100", "1130",
           "1300", "1330", "1400", "1430", "1500"]
BUCKET_MIN = {"0930": 570, "1000": 600, "1030": 630, "1100": 660, "1130": 690,
              "1300": 780, "1330": 810, "1400": 840, "1430": 870, "1500": 900}


def cst_now():
    return time.gmtime(time.time() + 8 * 3600)


def in_session(t):
    hm = t.tm_hour * 60 + t.tm_min
    return (565 <= hm <= 695) or (775 <= hm <= 905)   # 09:25-11:35 / 12:55-15:05


def bucket_of(t):
    hm = t.tm_hour * 60 + t.tm_min
    label = "0930"
    for b in BUCKETS:
        if hm >= BUCKET_MIN[b]:
            label = b
    return label


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
    try:
        codes = list((json.load(open(p2, encoding="utf-8")).get("stocks") or {}).keys())
        if codes:
            print(f"[输入] analysis.json {len(codes)} 只")
            return codes
    except Exception:
        pass
    # 兜底：从 history 分支最近一个交易日的 daily 文件里取全市场代码
    try:
        base = "https://raw.githubusercontent.com/pengxh7788/stock-data/history"
        man = json.loads(urllib.request.urlopen(
            urllib.request.Request(base + "/manifest.json", headers=UA), timeout=20
        ).read().decode("utf-8"))
        d = man["lastTradingDate"]
        bars = json.loads(urllib.request.urlopen(
            urllib.request.Request(f"{base}/daily/{d}.json", headers=UA), timeout=30
        ).read().decode("utf-8"))["bars"]
        codes = sorted(bars.keys())
        print(f"[输入] history/daily/{d}.json {len(codes)} 只")
        return codes
    except Exception as e:
        print(f"[输入] 远程名单失败: {e}")
    return []


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
                code = var.strip()[2:]
                f = body.strip().strip('"').split("~")
                if len(f) < 39 or not f[2]:
                    continue
                try:
                    price = float(f[3])
                except Exception:
                    continue
                if price <= 0:
                    continue

                def n(i):
                    try:
                        return float(f[i] or 0)
                    except Exception:
                        return 0.0

                out[code] = [
                    round(price, 2), round(n(32), 2),          # 价, 涨跌幅
                    round(n(33) or price, 2), round(n(34) or price, 2),  # 高, 低
                    round(n(5) or price, 2),                    # 开
                    round(n(6)), round(n(37) * 10000),          # 量(手), 额(元)
                    round(n(38), 2),                            # 换手%
                    round(n(4), 2),                             # 昨收
                ]
            return out
        except Exception:
            time.sleep(0.5)
    return {}


def prune(today):
    cutoff = time.strftime("%Y-%m-%d", time.gmtime(time.time() + 8 * 3600 - KEEP_DAYS * 86400))
    if not os.path.isdir(SNAP):
        return
    for name in os.listdir(SNAP):
        if name[:10] < cutoff:
            try:
                os.remove(os.path.join(SNAP, name))
                print("[清理]", name)
            except Exception:
                pass


def main():
    t = cst_now()
    if not in_session(t):
        print(f"[跳过] {t.tm_hour:02d}:{t.tm_min:02d} 非交易时段")
        return

    date = time.strftime("%Y-%m-%d", t)
    label = bucket_of(t)
    snap_path = os.path.join(SNAP, f"{date}_{label}.json")

    if os.path.exists(snap_path):
        print(f"[跳过] {date}_{label} 桶已存在，本次只更新 live.json")

    t0 = time.time()
    codes = load_codes()
    if not codes:
        print("[失败] 拿不到股票名单")
        sys.exit(1)
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

    if len(res) < max(1000, len(codes) * 0.5):
        print(f"[失败] 只取到 {len(res)} 只，不写文件")
        sys.exit(1)

    payload = {"v": 1, "date": date, "label": label,
               "at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
               "n": len(res), "q": res}

    os.makedirs(SNAP, exist_ok=True)
    if not os.path.exists(snap_path):
        with open(snap_path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, separators=(",", ":"))
        print(f"[写入] {snap_path} {os.path.getsize(snap_path)/1024:.0f}KB")

    with open(os.path.join(DATA, "live.json"), "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, separators=(",", ":"))
    print(f"[完成] {len(res)} 只，用时 {time.time()-t0:.0f}s")

    prune(date)


if __name__ == "__main__":
    main()
