#!/usr/bin/env python3
# 用 Baostock 校正 out/kline/*.json 的成交额与换手率（真实值，替代腾讯估算）。
# 只更新已有文件里出现过的日期；不新增股票、不动 flow。
import json
import os
import sys
import time
from datetime import date, timedelta

import baostock as bs

OUT = "out/kline"
ADJUST = "2"          # 1=后复权 2=前复权 3=不复权
KEEP = 250
LIMIT = int(os.environ.get("BAOSTOCK_LIMIT", "0") or 0)
START = (date.today() - timedelta(days=420)).isoformat()


def to_bs(code: str) -> str:
    if code.startswith("sh"):
        return "sh." + code[2:]
    if code.startswith("sz"):
        return "sz." + code[2:]
    return ""          # 北交所 baostock 不提供


def load(path):
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


def save(path, obj):
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, separators=(",", ":"))
    os.replace(tmp, path)


def main():
    lg = bs.login()
    if lg.error_code != "0":
        print("login failed", lg.error_msg)
        sys.exit(1)

    files = sorted(f for f in os.listdir(OUT) if f.endswith(".json"))
    if LIMIT > 0:
        files = files[:LIMIT]
    print(f"[baostock] files={len(files)} start={START}")

    ok = 0
    fail = 0
    for i, name in enumerate(files, 1):
        code = name[:-5]
        bscode = to_bs(code)
        if not bscode:
            continue
        path = os.path.join(OUT, name)
        data = load(path)
        if not data or not data.get("bars"):
            continue

        try:
            rs = bs.query_history_k_data_plus(
                bscode,
                "date,open,high,low,close,volume,amount,turn",
                start_date=START,
                end_date=date.today().isoformat(),
                frequency="d",
                adjustflag=ADJUST,
            )
            if rs.error_code != "0":
                raise RuntimeError(rs.error_msg)
            rows = {}
            while rs.next():
                r = rs.get_row_data()
                try:
                    d, o, h, l, c = r[0], float(r[1]), float(r[2]), float(r[3]), float(r[4])
                    vol = float(r[5]) / 100.0       # 股 -> 手
                    amt = float(r[6])
                    turn = float(r[7])
                except Exception:
                    continue
                if o > 0 and c > 0:
                    rows[d] = [d, o, c, h, l, vol, amt, turn]
            if not rows:
                fail += 1
                continue

            merged = {}
            for b in data["bars"]:
                merged[b[0]] = b
            for d, b in rows.items():
                merged[d] = b                     # Baostock 覆盖同一天
            bars = sorted(merged.values(), key=lambda x: x[0])[-KEEP:]

            data["bars"] = bars
            data["amountEst"] = False
            data["turnSource"] = "baostock"
            data["lastDate"] = bars[-1][0]
            data["updated"] = bars[-1][0]
            save(path, data)
            ok += 1
        except Exception as e:
            fail += 1
            if fail % 50 == 0:
                print(f"[baostock] fail={fail} last={code} {e}")

        if i % 200 == 0:
            print(f"[baostock] {i}/{len(files)} ok={ok} fail={fail}")

    bs.logout()
    print(f"[baostock] done ok={ok} fail={fail}")


if __name__ == "__main__":
    main()
