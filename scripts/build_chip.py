#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""生成 chip/（东财股东户数）并写入 out/chip，供 history 分支前端/量化读取。

在 history 工作流里紧跟 build-history.mjs 执行；上游失败不影响主流程。
字段：date / conc / holderNum / holderChange / holderRatio / avgHoldNum
conc 口径：clamp(10 - 户数变化率*0.5, 0, 20)（与 App qzEM_chip 一致）
"""
import json
import os
import re
import time
import urllib.request

OUT = os.environ.get("OUT", "out")
API = ("https://datacenter-web.eastmoney.com/api/data/v1/get?reportName=RPT_HOLDERNUMLATEST"
       "&columns=ALL&source=WEB&client=WEB&pageNumber=%d&pageSize=500"
       "&sortColumns=END_DATE&sortTypes=-1")


def fetch(page, tries=3):
    last = None
    for k in range(tries):
        try:
            req = urllib.request.Request(API % page, headers={
                "User-Agent": "Mozilla/5.0", "Referer": "https://data.eastmoney.com/"})
            with urllib.request.urlopen(req, timeout=30) as r:
                return json.loads(r.read())
        except Exception as e:
            last = e
            time.sleep(1 + k)
    raise last


def num(v):
    if v is None or isinstance(v, bool):
        return None
    if isinstance(v, (int, float)):
        return v
    try:
        return float(v)
    except Exception:
        return None


def clamp(v, lo, hi):
    return max(lo, min(hi, v))


def main():
    first = fetch(1)
    res = (first or {}).get("result") or {}
    pages = int(res.get("pages") or 1)
    rows = list(res.get("data") or [])
    for p in range(2, pages + 1):
        j = fetch(p)
        rows += ((j or {}).get("result") or {}).get("data") or []
        time.sleep(0.2)

    latest = {}
    for r in rows:
        sec = str(r.get("SECUCODE") or "")
        parts = sec.split(".")
        code = parts[0]
        mkt = parts[-1].upper() if len(parts) > 1 else ""
        if not re.match(r"^\d{6}$", code):
            continue
        prefix = {"SH": "sh", "SZ": "sz", "BJ": "bj"}.get(mkt)
        if not prefix:
            continue
        d = str(r.get("END_DATE") or "")[:10]
        c = prefix + code
        if c in latest and (latest[c][0] or "") >= d:
            continue
        latest[c] = (d, r)

    chip_dir = os.path.join(OUT, "chip")
    os.makedirs(chip_dir, exist_ok=True)
    date_max = ""
    for c, (d, r) in latest.items():
        cur = num(r.get("HOLDER_NUM"))
        pre = num(r.get("PRE_HOLDER_NUM"))
        ratio = num(r.get("HOLDER_NUM_RATIO"))
        if ratio is None and cur and pre:
            ratio = (cur / pre - 1) * 100
        ratio = round(ratio, 2) if ratio is not None else None
        chg = num(r.get("HOLDER_NUM_CHANGE"))
        if chg is None and cur is not None and pre is not None:
            chg = cur - pre
        rec = {
            "code": c,
            "date": d,
            "conc": round(clamp(10 - (ratio or 0) * 0.5, 0, 20), 2),
            "holderNum": cur,
            "holderChange": chg,
            "holderRatio": ratio,
            "avgHoldNum": num(r.get("AVG_HOLD_NUM")),
            "source": "eastmoney:holder",
            "_kind": "chip",
        }
        with open(os.path.join(chip_dir, c + ".json"), "w", encoding="utf-8") as f:
            json.dump(rec, f, ensure_ascii=False, separators=(",", ":"))
        if d > date_max:
            date_max = d

    mf = os.path.join(OUT, "manifest.json")
    try:
        with open(mf, encoding="utf-8") as f:
            man = json.load(f)
    except Exception:
        man = {}
    man["chipCount"] = len(latest)
    man["chipDate"] = date_max
    man["chipUpdatedAt"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    with open(mf, "w", encoding="utf-8") as f:
        json.dump(man, f, ensure_ascii=False, separators=(",", ":"))

    print("chip:", len(latest), "date:", date_max)


if __name__ == "__main__":
    main()
