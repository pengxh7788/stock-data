#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
全市场数据采集 v5
输出：
  data/meta.json            比对用
  data/quotes.json          全市场当日快照
  data/analysis.json        派生指标（MA/涨幅/行业/市场环境/板块）
  data/history.json         个股收盘价（250 天）
  data/sector_map.json      股票→行业（6141 只，99.5% 覆盖）
  data/index_history.json   6 个指数（1000 天）
  data/sector_history.json  84 个行业指数（1000 天）
"""

import json
import os
import statistics
import threading
import time
import urllib.request
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

UA = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}

UNIVERSE_URL = "https://www.cninfo.com.cn/new/data/szse_stock.json"
QUOTE_URL = "https://qt.gtimg.cn/q="
KLINE_URL = "https://web.ifzq.gtimg.cn/appstock/app/fqkline/get?param={code},day,,,{n},qfq"
KLINE_SINA = ("https://money.finance.sina.com.cn/quotes_service/api/json_v2.php"
              "/CN_MarketData.getKLineData?symbol={code}&scale=240&ma=no&datalen={n}")

# 行业分类：东财数据中心（不限流，pageSize 可到 5000）
SECTOR_API = ("https://datacenter-web.eastmoney.com/api/data/v1/get"
              "?reportName=RPT_F10_BASIC_ORGINFO"
              "&columns=SECURITY_CODE,INDUSTRYCSRC1"
              "&pageSize=5000&pageNumber={page}")

# ===== 所有时间序列数据：统一目标天数 + 体积自适应 =====
DATA_TARGET_DAYS = 500         # 目标天数（想更长改这里，体积会自动压到装得下）
DAYS_FLOOR = 120               # 降级时最低天数
SIZE_SINGLE_LIMIT_MB = 90      # 单文件上限（GitHub 硬限 100 MB）
SIZE_TOTAL_LIMIT_MB = 400      # 数据总量上限
SIZE_RESUME_RATIO = 0.6        # 总量降到上限 60% 以下才恢复目标
STATE_FILE = "data/state.json"

_DAYS_OVERRIDE = int(os.environ.get("DAYS_OVERRIDE") or 0)
try:
    with open(STATE_FILE, encoding="utf-8") as _f:
        _st0 = json.load(_f)
except Exception:
    _st0 = {}
_MODE = _st0.get("mode", "normal")
_CAP_DAYS = int(_st0.get("capDays") or DATA_TARGET_DAYS)
DAYS = _DAYS_OVERRIDE or (DAYS_FLOOR if _MODE == "reduced"
                          else max(DAYS_FLOOR, min(DATA_TARGET_DAYS, _CAP_DAYS)))

# 硬上限：单文件不能超 GitHub 100MB（约 130KB/天 → 640 天约 83MB，留足余量）
MAX_SAFE_DAYS = 640
DAYS = min(DAYS, MAX_SAFE_DAYS)

HISTORY_DAYS = DAYS   # K线 + 成交量（换手率可由成交量反算）
INDEX_DAYS = DAYS     # 指数
SECTOR_DAYS = DAYS    # 板块

BATCH = 400
SLEEP = 0.4
KFILL_WORKERS = 2
MAX_BACKFILL = 7000    # 一轮尽量补完全市场（限速后可行）    # 一次补齐上限（全市场可一次跑完）
BACKFILL_BUDGET_S = int(os.environ.get("BACKFILL_BUDGET_S") or 3600)
REQUEST_GAP = 0.35       # 每次请求后间隔（实测 8 只/秒会被限流，压到约 3 只/秒）   # 单轮回补最多花 25 分钟，到点就停，剩余下次继续

INDEX_CODES = {
    "sh000001": "上证指数",
    "sh000300": "沪深300",
    "sz399001": "深证成指",
    "sz399006": "创业板指",
    "sh000688": "科创50",
    "sh000905": "中证500",
}

FIN_API = ("https://datacenter-web.eastmoney.com/api/data/v1/get"
           "?reportName=RPT_LICO_FN_CPD"
           "&columns=SECURITY_CODE,SECURITY_NAME_ABBR,REPORTDATE,BASIC_EPS,"
           "TOTAL_OPERATE_INCOME,PARENT_NETPROFIT,WEIGHTAVG_ROE,YSTZ,SJLTZ,QDATE"
           "&pageSize=500&pageNumber={page}"
           "&filter=(REPORTDATE%3E%3D%272024-06-01%27)"
           "&sortColumns=SECURITY_CODE,REPORTDATE&sortTypes=1,-1")
FIN_QUARTERS = 8                 # 8 个季度（2 年）

FIN_FILE = "data/financials.json"

LHB_API = ("https://datacenter-web.eastmoney.com/api/data/v1/get"
           "?reportName=RPT_DAILYBILLBOARD_DETAILSNEW"
           "&columns=SECURITY_CODE,SECURITY_NAME_ABBR,TRADE_DATE,EXPLANATION,"
           "CLOSE_PRICE,CHANGE_RATE,BILLBOARD_NET_AMT,BILLBOARD_BUY_AMT,"
           "BILLBOARD_SELL_AMT,TURNOVERRATE,ACCUM_AMOUNT"
           "&pageSize=500&pageNumber=1"
           "&filter=(TRADE_DATE%3D%27{date}%27)"
           "&sortColumns=BILLBOARD_NET_AMT&sortTypes=-1")

SEAT_API = ("https://datacenter-web.eastmoney.com/api/data/v1/get"
            "?reportName=RPT_BILLBOARD_DAILYDETAILSBUY"
            "&columns=SECURITY_CODE,TRADE_DATE,OPERATEDEPT_NAME,BUY,SELL,NET,EXPLANATION"
            "&pageSize=500&pageNumber={page}"
            "&filter=(TRADE_DATE%3D%27{date}%27)")

LHB_FETCH = 25           # 每次最多补 25 天（防限流）
LHB_KEEP = DAYS          # 累计保留天数，跟随统一目标
LHB_DAYS = LHB_FETCH     # 兼容旧写法（每次拉取天数）

LHB_FILE = "data/lhb.json"
HISTORY_FILE = "data/history.json"
SECTOR_FILE = "data/sector_map.json"
INDEX_FILE = "data/index_history.json"
SECTOR_HIS_FILE = "data/sector_history.json"


# ---------------- 工具 ----------------

def norm(c):
    c = str(c).strip()
    if c.startswith("92") or c[0] in "48":
        return "bj" + c
    if c.startswith("6") or c.startswith("900"):
        return "sh" + c
    return "sz" + c


def fnum(x, d=0.0):
    try:
        v = float(x)
        return v if v == v else d
    except Exception:
        return d


def http_get(url, timeout=30, referer=None):
    hdr = dict(UA)
    if referer:
        hdr["Referer"] = referer
    req = urllib.request.Request(url, headers=hdr)
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read()


def load_json(path, default):
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return default


def save_json(path, obj):
    d = os.path.dirname(path)
    if d:
        os.makedirs(d, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, separators=(",", ":"))


# ---------------- 1. 名单 ----------------

def get_universe():
    raw = http_get(UNIVERSE_URL, referer="https://www.cninfo.com.cn/")
    d = json.loads(raw.decode("utf-8", errors="replace"))
    lst = d.get("stockList") if isinstance(d, dict) else d
    out, seen = [], set()
    for x in lst:
        if x.get("category") != "A股":
            continue
        c = norm(x["code"])
        if c not in seen:
            seen.add(c)
            out.append(c)
    return out


# ---------------- 2. 行业映射（东财数据中心）----------------

def get_sector_map():
    """拉证监会行业分类，5 页 × 5000 条，覆盖 99.5%"""
    mapping = {}
    total_pages = 5

    for page in range(1, total_pages + 2):
        url = SECTOR_API.format(page=page)
        ok = False

        for attempt in range(3):
            try:
                raw = http_get(url, timeout=40)
                d = json.loads(raw.decode("utf-8", errors="replace"))
                r = d.get("result") or {}
                data = r.get("data") or []
                pages = r.get("pages") or total_pages

                if not data:
                    print(f"  [行业] 页{page} 空，结束")
                    ok = True
                    break

                for x in data:
                    code = norm(x.get("SECURITY_CODE", ""))
                    ind = x.get("INDUSTRYCSRC1") or ""
                    if code and ind:
                        mapping[code] = ind

                print(f"  [行业] 页{page}/{pages}: {len(data)} 条，累计 {len(mapping)}")
                total_pages = pages
                ok = True
                break
            except Exception as e:
                print(f"  [行业] 页{page} 第{attempt + 1}次失败: {e}")
                time.sleep(5)

        if not ok:
            print(f"  [行业] 页{page} 连续失败，保留已拉到 {len(mapping)} 条")
            break
        if page >= total_pages:
            break
        time.sleep(2)

    if not mapping:
        old = load_json(SECTOR_FILE, {})
        print(f"  [行业] 本次失败，沿用旧数据 {len(old)} 只")
        return old

    old = load_json(SECTOR_FILE, {})
    if len(old) > len(mapping):
        for k, v in mapping.items():
            old[k] = v
        mapping = old

    save_json(SECTOR_FILE, mapping)
    print(f"  [行业] 完成：{len(mapping)} 只")
    return mapping


# ---------------- 3. 当日快照 ----------------

def fetch_batch(batch):
    raw = http_get(QUOTE_URL + ",".join(batch))
    text = raw.decode("gb18030", errors="replace")
    out = {}
    for line in text.split("\n"):
        line = line.strip()
        if not line.startswith("v_"):
            continue
        var, _, payload = line.partition('="')
        code = var[2:]
        seg = payload.rstrip('";').split("~")
        if len(seg) < 45:
            continue
        out[code] = {
            "n": seg[1], "p": fnum(seg[3]), "pc": fnum(seg[4]),
            "o": fnum(seg[5]), "v": fnum(seg[6]),
            "h": fnum(seg[33]), "l": fnum(seg[34]),
            "c": fnum(seg[31]), "cp": fnum(seg[32]),
            "a": fnum(seg[37]) * 10000,
            "tr": fnum(seg[38]), "t": seg[30],
            "lu": fnum(seg[47]), "ld": fnum(seg[48]),
            # ---- 核心基础数据（同一请求，免费）----
            "pe": fnum(seg[39]),        # 市盈率 TTM
            "peDyn": fnum(seg[52]),     # 市盈率 动态
            "peSta": fnum(seg[53]),     # 市盈率 静态
            "pb": fnum(seg[46]),        # 市净率
            "mcap": fnum(seg[45]),      # 总市值（亿）
            "fcap": fnum(seg[44]),      # 流通市值（亿）
            "amp": fnum(seg[43]),       # 振幅 %
            "vr": fnum(seg[49]),        # 量比
            "avgp": fnum(seg[51]),      # 均价
            "outV": fnum(seg[7]),       # 外盘
            "inV": fnum(seg[8]),        # 内盘
            "bidDiff": fnum(seg[50]),   # 委差
        }
    return out


def fetch_all_quotes(codes):
    quotes, failed = {}, []
    total = (len(codes) + BATCH - 1) // BATCH
    for i in range(0, len(codes), BATCH):
        batch = codes[i:i + BATCH]
        idx = i // BATCH + 1
        try:
            got = fetch_batch(batch)
            quotes.update(got)
            print(f"[快照 {idx}/{total}] {len(batch)} -> {len(got)}")
        except Exception as e:
            print(f"[快照 {idx}/{total}] 失败: {e}")
            failed.extend(batch)
        time.sleep(SLEEP)
    return quotes, failed


# ---------------- 4. K 线 ----------------

def _sina_rows(code, n):
    """返回 [(date, close, volume), ...]"""
    try:
        raw = http_get(KLINE_SINA.format(code=code, n=n), timeout=3,
                       referer="https://finance.sina.com.cn")
        arr = json.loads(raw.decode("utf-8", errors="replace"))
        out = []
        for row in arr or []:
            try:
                out.append((row["day"], float(row["close"]),
                            float(row.get("high") or row["close"]),
                            float(row.get("low") or row["close"]),
                            float(row.get("volume") or 0)))
            except Exception:
                continue
        return out
    except Exception:
        return []


def is_star(code):
    """科创板（688/689）—— 腾讯返回的成交量单位是「股」，其余是「手」"""
    return code.startswith("sh688") or code.startswith("sh689")


def vol_to_shares(code, v):
    """统一成「股」"""
    return v if is_star(code) else v * 100


def vol_to_wan(code, v):
    """统一成「万股」"""
    return v if is_star(code) else v / 100


def _tencent_rows(code, n):
    """返回 [(date, close, volume), ...]（腾讯 K 线：row[2]=收盘 row[5]=成交量）"""
    try:
        raw = http_get(KLINE_URL.format(code=code, n=n), timeout=12)
        d = json.loads(raw.decode("utf-8", errors="replace"))
        block = (d.get("data") or {}).get(code)
        if not block:
            return []
        k = block.get("qfqday") or block.get("day") or []
        out = []
        for row in k:
            if len(row) < 6:
                continue
            try:
                # 腾讯 row: [date, open, close, high, low, volume]
                # 科创板单位已是「股」，其余是「手」→ 统一成「股」
                out.append((row[0], float(row[2]), float(row[3]), float(row[4]),
                            vol_to_shares(code, float(row[5]))))
            except Exception:
                continue
        return out
    except Exception:
        return []


_fail_streak = 0
_fail_lock = threading.Lock()


def _note_ok():
    global _fail_streak
    with _fail_lock:
        _fail_streak = 0


def _note_fail():
    """连续失败过多 → 全局暂停，等腾讯限流窗口恢复"""
    global _fail_streak
    with _fail_lock:
        _fail_streak += 1
        if _fail_streak % 40 == 0:
            print(f"[限流] 连续失败 {_fail_streak} 次 → 暂停 45 秒", flush=True)
            time.sleep(45)


def fetch_kline_rows(code, n=HISTORY_DAYS + 5):
    """返回 [(date, close, high, low, volume), ...]
    优先腾讯【前复权】；失败快速跳过（只重试 1 次、新浪兜底 3 秒），
    连续失败则由熔断器全局暂停，避免把时间烧在超时上。"""
    try:
        rows = _tencent_rows(code, n)
        if rows and len(rows) >= 20:
            _note_ok()
            time.sleep(REQUEST_GAP)      # 成功也要限速，避免把额度打爆
            return rows
    except Exception:
        pass
    time.sleep(0.5)
    try:
        rows = _tencent_rows(code, n)
        if rows and len(rows) >= 20:
            _note_ok()
            return rows
    except Exception:
        pass
    try:
        rows2 = _sina_rows(code, n)
        if rows2 and len(rows2) >= 20:
            _note_ok()
            return rows2
    except Exception:
        pass
    _note_fail()
    return []


def fetch_kline_closes(code, n=HISTORY_DAYS + 5):
    """只要收盘价时用这个"""
    return [(r[0], r[1]) for r in fetch_kline_rows(code, n)]


def backfill_history(codes):
    print(f"[回补] {len(codes)} 只，并发 {KFILL_WORKERS}")
    close_res = {}
    high_res = {}
    low_res = {}
    vol_res = {}
    done = 0
    t0 = time.time()
    ex = ThreadPoolExecutor(max_workers=KFILL_WORKERS)
    futs = {ex.submit(fetch_kline_rows, c): c for c in codes}
    timed_out = False
    try:
        for fut in as_completed(futs, timeout=BACKFILL_BUDGET_S):
            c = futs[fut]
            try:
                rows = fut.result()
            except Exception:
                rows = []
            if rows:
                close_res[c] = [(r[0], r[1]) for r in rows]
                high_res[c] = [(r[0], r[2]) for r in rows]
                low_res[c] = [(r[0], r[3]) for r in rows]
                vol_res[c] = [(r[0], round(r[4] / 10000, 2)) for r in rows]
            done += 1
            if done % 300 == 0:
                print(f"[回补] {done}/{len(codes)}  已用 {int(time.time() - t0)}s")
            if done % 500 == 0:
                time.sleep(2)
    except Exception as e:
        # as_completed 超时（TimeoutError）→ 到点收工，保住已拉到的数据
        timed_out = True
        print(f"[回补] 到达时间预算 {BACKFILL_BUDGET_S}s（已处理 {done}/{len(codes)}），"
              f"本轮先提交已有数据，剩余下次继续：{type(e).__name__}")
    finally:
        try:
            ex.shutdown(wait=False, cancel_futures=True)
        except Exception:
            pass
    _miss = [c for c in codes if c not in close_res]
    if _miss:
        print(f"[回补] 失败 {len(_miss)} 只，已记录到 data/backfill_failed.json（下次自动补）")
        try:
            save_json("data/backfill_failed.json",
                      {"at": datetime.utcnow().isoformat() + "Z", "codes": _miss[:5000]})
        except Exception:
            pass
    # ---- 记录失败次数（连续 2 次进入黑名单） ----
    try:
        with open(STATE_FILE, encoding="utf-8") as _f:
            _st = json.load(_f)
    except Exception:
        _st = {}
    _bl = _st.get("blacklist") or {}
    if _miss:
        for c in _miss:
            _bl[c] = int(_bl.get(c) or 0) + 1
    for c in list(close_res.keys()):
        _bl.pop(c, None)          # 成功过就移出黑名单
    _st["blacklist"] = _bl
    try:
        save_json(STATE_FILE, _st)
    except Exception:
        pass
    print(f"[回补] 成功 {len(close_res)} 只，用时 {int(time.time() - t0)}s"
          f"{'，黑名单 ' + str(len([1 for v in _bl.values() if v >= 2])) + ' 只' if _bl else ''}")
    return close_res, high_res, low_res, vol_res


# ---------------- 5. 指数历史 ----------------

def build_index_history():
    out = {}
    for code in INDEX_CODES:
        rows = fetch_kline_closes(code, INDEX_DAYS + 5)
        if rows:
            out[code] = rows
            print(f"  [指数] {code} {INDEX_CODES[code]}: {len(rows)} 天")
        time.sleep(1)

    if not out:
        print("  [指数] 全部失败")
        return

    common = None
    for code, rows in out.items():
        s = set(d for d, _ in rows)
        common = s if common is None else (common & s)

    dates = sorted(common)[-INDEX_DAYS:]
    aligned = {}
    for code, rows in out.items():
        m = dict(rows)
        aligned[code] = [m.get(d) for d in dates]

    save_json(INDEX_FILE, {"dates": dates, "close": aligned})
    print(f"  [指数] 对齐 {len(dates)} 天，{len(aligned)} 个指数")


# ---------------- 6. 板块指数 ----------------

def build_sector_history(hist, sector_map):
    dates = hist["dates"]
    closes = hist["close"]

    groups = {}
    for code, ind in sector_map.items():
        if code in closes and ind:
            groups.setdefault(ind, []).append(code)

    result = {}
    for ind, members in groups.items():
        series = [closes[c] for c in members if c in closes]
        if len(series) < 3:
            continue

        idx = [1000.0]
        for t in range(1, len(dates)):
            chgs = []
            for arr in series:
                if t < len(arr) and arr[t] and arr[t - 1]:
                    chgs.append(arr[t] / arr[t - 1] - 1)
            if not chgs:
                idx.append(idx[-1])
            else:
                idx.append(idx[-1] * (1 + sum(chgs) / len(chgs)))
        result[ind] = [round(v, 2) for v in idx[1:]]

    save_json(SECTOR_HIS_FILE, {"dates": dates[1:], "close": result})
    print(f"  [板块] {len(result)} 个行业指数，{len(dates) - 1} 天")


# ---------------- 7. 主流程 ----------------

# ============================================================
# 模型自评：把「预期收益」算成有依据的数，而不是拍脑袋的 6%
# 输出 data/analysis.json 里的 modelEval 字段
#   modelEval.byRegime[状态][分数档] = {n, mid, lo, hi, win, neg}
#   状态：进攻 / 中性 / 防守（与 market.status 同一套判定）
#   分数档：90-100 / 80-90 / 70-80 / 60-70
# 含义：mid=该档 10 日实际收益的中位数（用来显示"预期收益"）
#       lo/hi=30%/70% 分位（显示成区间）
#       win=正收益占比  neg=负收益占比
# 方法：用 hist 里已有的历史收盘价逐日重放同一套筛选条件，
#       按 T+10 收盘结算，期间最低价跌破 -6% 视为触发止损。
# ============================================================
MODEL_HOLD = 10
MODEL_STOP = -0.06
MODEL_MIN_SAMPLE = 120     # 少于这个样本数的档位不输出，避免拿噪声当预期


def _band_of(score):
    if score >= 90:
        return "90-100"
    if score >= 80:
        return "80-90"
    if score >= 70:
        return "70-80"
    return "60-70"


def _ma(arr, win):
    """简单移动平均，不足 win 天返回 None（元素可为 None）"""
    out = [None] * len(arr)
    acc = 0.0
    cnt = 0
    for i, v in enumerate(arr):
        if v is None:
            acc, cnt = 0.0, 0
            continue
        acc += v
        cnt += 1
        if cnt > win:
            acc -= arr[i - win]
            cnt = win
        if cnt == win:
            out[i] = acc / win
    return out


def _clamp(v, lo, hi):
    return lo if v < lo else (hi if v > hi else v)


def _quantile(vals, p):
    if not vals:
        return None
    v = sorted(vals)
    k = (len(v) - 1) * p
    f = int(k)
    c = min(f + 1, len(v) - 1)
    return v[f] + (v[c] - v[f]) * (k - f)


def _model_score(price, chg, chg5, chg20, turnover, amount, ma5, ma10, ma20, vol20, chip_score):
    """与 App 的 scoreStock 同构（不含分趋势门槛，门槛在下面按状态判）"""
    if price <= 0 or ma20 is None or ma20 <= 0:
        return None
    f = 35 * _clamp((-chg20 - 3) / 32, 0, 1) if chg20 is not None else 0
    p = 25 * (1 - _clamp(abs(chg5 + 2) / 8, 0, 1)) if chg5 is not None else 0
    x = 20 * (1 - _clamp(abs(chg) / 4, 0, 1))
    h = 20 * _clamp((ma20 - price) / ma20 / 0.1, 0, 1)
    if ma5 is not None and ma10 is not None and ma5 > ma10 > ma20:
        h -= 10
    h = _clamp(h, -10, 20)
    v = _clamp(6 * _clamp((turnover - 2) / 10, 0, 1) * (1 - _clamp((turnover - 20) / 20, 0, 1))
               + 4 * _clamp(1 - abs(amount - 2e9) / 3e9, 0, 1), 0, 10)
    d = 10 * (1 - _clamp(vol20 / 5, 0, 1)) if vol20 is not None else 0
    raw = f + p + x + h + v
    return _clamp(raw * 1.0526315789473684 * (1 + d / 100) * (1 + (chip_score or 0) / 10 / 100), 0, 100)


def _roll(arr, win, want_max=True):
    """滚动窗口最大/最小（窗口内遇 None 则该点无效）"""
    out = [None] * len(arr)
    dq = []
    last_none = -1
    for i, v in enumerate(arr):
        if v is None:
            dq = []
            last_none = i
            continue
        if want_max:
            while dq and arr[dq[-1]] <= v:
                dq.pop()
        else:
            while dq and arr[dq[-1]] >= v:
                dq.pop()
        dq.append(i)
        while dq[0] <= i - win:
            dq.pop(0)
        if i - last_none >= win:
            out[i] = arr[dq[0]]
    return out


def _kdj(h, l, c):
    """标准 KDJ(9,3,3)，K/D 从 50 递推"""
    n = len(c)
    K = [None] * n
    D = [None] * n
    k = d = 50.0
    for i in range(8, n):
        wh = h[i - 8:i + 1]
        wl = l[i - 8:i + 1]
        if any(x is None for x in wh) or any(x is None for x in wl) or c[i] is None:
            continue
        hh, ll = max(wh), min(wl)
        rsv = 50.0 if hh <= ll else (c[i] - ll) / (hh - ll) * 100.0
        k = (2 * k + rsv) / 3.0
        d = (2 * d + k) / 3.0
        K[i], D[i] = k, d
    return K, D


def build_model_eval(hist, idx_map=None):
    """逐日重放【与 App 完全相同的筛选条件】，统计 10 日实际收益分布

    输出按「市场状态 × 分数档」分组的中位数/区间/胜率/负值率。
    市场状态：上证 MA20 + MA20 五日斜率 → 上行=进攻 / 震荡=中性 / 下行=防守
    筛选条件与 App 的 scoreStock 一致（含分趋势门槛）：
      通用：价格>0、非ST、-5%<当日<9.5%、5日>-18%、20日<40%、获利比例<25
      进攻：KDJ K<=D 且 5日涨跌<0
      中性：KDJ K<=D 且 20日均振幅>=4%
      防守：KDJ K>D 且 距60日高点回撤>=25%
    """
    dates = hist["dates"]
    closes = hist["close"]
    highs = hist.get("high", {})
    lows = hist.get("low", {})
    n = len(dates)
    if n < MODEL_HOLD + 70:
        print("  [自评] 历史天数不足，跳过")
        return None

    # --- 1) 逐日市场状态 ---
    if idx_map is None:
        try:
            rows = fetch_kline_closes("sh000001", 250)
            vals = [v for _, v in rows]
            ds = [d for d, _ in rows]
            m20 = _ma(vals, 20)
            idx_map = {}
            for i, d in enumerate(ds):
                if m20[i] is None or i < 5 or m20[i - 5] is None:
                    continue
                slope = m20[i] - m20[i - 5]
                c = vals[i]
                idx_map[d] = "进攻" if (c > m20[i] and slope > 0) else (
                    "防守" if (c < m20[i] and slope < 0) else "中性")
        except Exception as e:
            print(f"  [自评] 取指数失败: {e}")
            return None
    if not idx_map:
        print("  [自评] 没有可用的状态序列，跳过")
        return None

    # --- 2) 逐股逐日重放 ---
    buckets = {}
    warm = 60
    for code, c in closes.items():
        if len(c) < n:
            c = c + [None] * (n - len(c))
        hh = highs.get(code) or []
        ll = lows.get(code) or []
        off = n - len(hh) if hh else n
        H = ([None] * off + list(hh))[:n] if hh else [None] * n
        L = ([None] * off + list(ll))[:n] if ll else [None] * n
        ma5a, ma10a, ma20a = _ma(c, 5), _ma(c, 10), _ma(c, 20)
        K, D = _kdj(H, L, c) if hh else ([None] * n, [None] * n)
        mx60, mn60 = _roll(c, 60, True), _roll(c, 60, False)
        mx120, mn120 = _roll(c, 120, True), _roll(c, 120, False)
        # 120 日窗口尚不足时（历史天数短）用累计最高/最低兜底
        pmax, pmin = [None] * n, [None] * n
        _a = _b = None
        for _i, _v in enumerate(c):
            if _v:
                _a = _v if _a is None else max(_a, _v)
                _b = _v if _b is None else min(_b, _v)
            pmax[_i], pmin[_i] = _a, _b

        for t in range(warm, n - MODEL_HOLD):
            price = c[t]
            if not price or price <= 0 or c[t - 1] is None or ma20a[t] is None:
                continue
            st = idx_map.get(dates[t])
            if not st or st == "空仓":
                continue
            chg = (price / c[t - 1] - 1) * 100
            if chg >= 9.5 or chg <= -5:
                continue
            c5, c20 = c[t - 5], c[t - 20]
            if c5 is None or c20 is None:
                continue
            chg5 = (price / c5 - 1) * 100
            chg20 = (price / c20 - 1) * 100
            if chg5 < -18 or chg20 > 40:
                continue
            if mx60[t] is None:
                continue
            _hi = mx120[t] if mx120[t] is not None else pmax[t]
            _lo = mn120[t] if mn120[t] is not None else pmin[t]
            if not _hi or not _lo:
                continue
            dd60 = (mx60[t] - price) / mx60[t] * 100
            pos120 = (price - _lo) / (_hi - _lo) * 100 if _hi > _lo else 50
            if pos120 > 25:                       # 获利比例近似：站在 120 日区间下 1/4
                continue
            kd = (K[t] - D[t]) if (K[t] is not None and D[t] is not None) else None
            amp = None
            if H[t] and L[t]:
                amp = (H[t] - L[t]) / c[t - 1] * 100 if amp is None else amp
            vr20 = None
            if t >= 20:
                rs = []
                ok = True
                for z in range(t - 19, t + 1):
                    if c[z] is None or c[z - 1] is None:
                        ok = False
                        break
                    rs.append(abs(c[z] / c[z - 1] - 1) * 100)
                if ok:
                    vr20 = sum(rs) / len(rs) * 1.8

            if st == "进攻":
                if kd is not None and kd > 0:
                    continue
                if chg5 >= 0:
                    continue
            elif st == "中性":
                if kd is not None and kd > 0:
                    continue
                if vr20 is None or vr20 < 4:
                    continue
            else:
                if kd is not None and kd <= 0:
                    continue
                if dd60 < 25:
                    continue

            sc = _model_score(price, chg, chg5, chg20, 5.0, 1e9,
                              ma5a[t], ma10a[t], ma20a[t], vr20, 5)
            if sc is None or sc < 60:
                continue

            stop = price * (1 + MODEL_STOP)
            ret = None
            for k2 in range(1, MODEL_HOLD + 1):
                ct = c[t + k2]
                if ct is None:
                    continue
                if L[t + k2] and L[t + k2] <= stop:
                    ret = MODEL_STOP * 100
                    break
                ret = (ct / price - 1) * 100
            if ret is None:
                continue
            buckets.setdefault((st, _band_of(sc)), []).append(ret)

    # --- 3) 汇总 ---
    out = {}
    for (st, band), vals in buckets.items():
        if len(vals) < MODEL_MIN_SAMPLE:
            continue
        out.setdefault(st, {})[band] = {
            "n": len(vals),
            "mid": round(_quantile(vals, .5), 2),
            "lo": round(_quantile(vals, .3), 2),
            "hi": round(_quantile(vals, .7), 2),
            "win": round(sum(1 for v in vals if v > 0) / len(vals) * 100, 1),
            "neg": round(sum(1 for v in vals if v < 0) / len(vals) * 100, 1),
        }
    total = sum(b["n"] for r in out.values() for b in r.values())
    print(f"  [自评] {total} 个样本，{len(out)} 个状态")
    for st in ("进攻", "中性", "防守"):
        for band in ("90-100", "80-90", "70-80", "60-70"):
            b = (out.get(st) or {}).get(band)
            if b:
                print(f"    {st} {band}: n={b['n']} 中位{b['mid']:+.2f}% "
                      f"区间[{b['lo']:+.2f},{b['hi']:+.2f}] 胜率{b['win']:.0f}% 负值{b['neg']:.0f}%")
    return out or None


def build_financials():
    """拉最近 FIN_QUARTERS 个季度的业绩（东财数据中心）"""
    latest = {}   # code -> [记录, ...] 按报告期倒序

    page = 1
    total_pages = 1
    fetched = 0

    while page <= total_pages:
        url = FIN_API.format(page=page)
        ok = False
        for attempt in range(3):
            try:
                raw = http_get(url, timeout=40)
                d = json.loads(raw.decode("utf-8", errors="replace"))
                r = d.get("result") or {}
                data = r.get("data") or []
                _pg = int(r.get("pages") or 0)
                if _pg > total_pages:            # pages 只升不降，避免被限流响应压回 1
                    total_pages = _pg
                if not data:
                    ok = True
                    break

                for x in data:
                    code = norm(x.get("SECURITY_CODE", ""))
                    if not code:
                        continue
                    rec = {
                        "q": x.get("QDATE") or "",
                        "date": (x.get("REPORTDATE") or "")[:10],
                        "eps": x.get("BASIC_EPS"),
                        "rev": x.get("TOTAL_OPERATE_INCOME"),
                        "profit": x.get("PARENT_NETPROFIT"),
                        "roe": x.get("WEIGHTAVG_ROE"),
                        "revYoy": x.get("YSTZ"),
                        "profitYoy": x.get("SJLTZ"),
                    }
                    bucket = latest.setdefault(code, {})
                    if rec["date"] not in bucket:      # 接口会把同一报告期重复返回 5~6 次
                        bucket[rec["date"]] = rec

                ok = True
                break
            except Exception as e:
                print(f"  [财务] 页{page} 第{attempt + 1}次失败: {e}")
                time.sleep(5)

        if not ok:
            print(f"  [财务] 页{page} 连续失败，停止（已拉 {len(latest)} 只）")
            break

        fetched += 1
        if fetched % 10 == 0:
            print(f"  [财务] {page}/{total_pages} 页，累计 {len(latest)} 只")
        page += 1
        time.sleep(1)

    # 每只只保留最近 FIN_QUARTERS 期
    out = {}
    for code, bucket in latest.items():
        recs = list(bucket.values()) if isinstance(bucket, dict) else bucket
        recs.sort(key=lambda r: r.get("date") or "", reverse=True)
        if recs:
            out[code] = recs[:FIN_QUARTERS]

    save_json(FIN_FILE, out)
    print(f"  [财务] 完成：{len(out)} 只 × 最近 {FIN_QUARTERS} 期")
    return out


def calc_rsi(vals, n=14):
    """RSI（与 App 的 calcRSI 同口径：简单平均，不是 Wilder 平滑）"""
    if len(vals) < n + 1:
        return None
    g = l = 0.0
    for i in range(len(vals) - n, len(vals)):
        d = vals[i] - vals[i - 1]
        if d > 0:
            g += d
        else:
            l -= d
    if g + l == 0:
        return 50.0
    return round(100.0 * g / (g + l), 1)


def _pad_tail(arr, n):
    """把 arr 右对齐到长度 n（左侧补 None），保证最后一个元素对齐"""
    if not arr:
        return [None] * n
    if len(arr) >= n:
        return list(arr[-n:])
    return [None] * (n - len(arr)) + list(arr)


def calc_kdj(closes, highs, lows, n=9):
    """KDJ 指标（标准算法，K/D 初始 50）

    返回 (K序列, D序列, J序列)，与输入等长，前 n 天为 None
    """
    m = len(closes)
    K = [None] * m
    D = [None] * m
    J = [None] * m
    k_ = 50.0
    d_ = 50.0
    for i in range(m):
        if i < n - 1 or closes[i] is None:
            continue
        hs = [highs[x] for x in range(i - n + 1, i + 1) if highs[x]]
        ls = [lows[x] for x in range(i - n + 1, i + 1) if lows[x]]
        if not hs or not ls:
            continue
        hh = max(hs)
        ll = min(ls)
        rsv = 50.0 if hh == ll else (closes[i] - ll) / (hh - ll) * 100.0
        k_ = 2.0 / 3.0 * k_ + 1.0 / 3.0 * rsv
        d_ = 2.0 / 3.0 * d_ + 1.0 / 3.0 * k_
        K[i] = k_
        D[i] = d_
        J[i] = 3.0 * k_ - 2.0 * d_
    return K, D, J


def calc_extra(vals_raw, highs_raw=None, lows_raw=None):
    """其他常用指标（内部自动对齐，highs/lows 可以比 vals 短）

    vals_raw: 收盘价序列（原始，可含 None）
    highs_raw / lows_raw: 最高/最低价序列（原始，可含 None，右对齐）
    """
    out = {
        "rsi": None, "dd20": None, "dd60": None,
        "upDays": 0, "downDays": 0, "lu20": None,
        "kdjK": None, "kdjD": None, "kdjJ": None, "kdjGold": None,
        "hh60": None, "break20": None, "vrange20": None,
    }

    # ---- 对齐：只保留 close 有效的天，high/low 按同一索引取 ----
    raw = list(vals_raw) if vals_raw else []
    if not raw:
        return out
    n_all = len(raw)
    h_all = _pad_tail(highs_raw, n_all) if highs_raw else [None] * n_all
    l_all = _pad_tail(lows_raw, n_all) if lows_raw else [None] * n_all

    keep = [i for i, v in enumerate(raw) if v is not None]
    if not keep:
        return out
    vals = [raw[i] for i in keep]
    highs = [h_all[i] for i in keep]
    lows = [l_all[i] for i in keep]

    # 涨跌幅序列
    chg_series = []
    for k in range(1, len(vals)):
        if vals[k] and vals[k - 1]:
            chg_series.append((vals[k] / vals[k - 1] - 1) * 100)

    m = len(vals)

    # RSI14
    out["rsi"] = calc_rsi(vals, 14)

    # 距 20 / 60 日高点回撤（%）
    cur = vals[-1]
    for n in (20, 60):
        if m >= n:
            hi = max(vals[-n:])
            out[f"dd{n}"] = round((cur / hi - 1) * 100, 2) if hi > 0 else None
        else:
            out[f"dd{n}"] = None

    # 连续上涨 / 下跌天数
    up = dn = 0
    for x in reversed(chg_series):
        if x > 0 and dn == 0:
            up += 1
        elif x < 0 and up == 0:
            dn += 1
        else:
            break
    out["upDays"] = up
    out["downDays"] = dn

    # 近 20 日涨停次数（涨幅 >= 9.5%，简化，不区分板块）
    out["lu20"] = sum(1 for x in chg_series[-20:] if x >= 9.5) if len(chg_series) >= 20 else None

    # 60 日最高价
    out["hh60"] = round(max(vals[-60:]), 2) if m >= 60 else None

    # 今日是否突破 20 日最高（不含今日）
    if m >= 21:
        prev_hi = max(vals[-21:-1])
        out["break20"] = 1 if vals[-1] > prev_hi else 0
    else:
        out["break20"] = None

    # 20 日平均真实振幅 %（用 high-low，比收盘价波动率更真实）
    nhl0 = min(len(highs), len(lows)) if (highs and lows) else 0
    if nhl0 >= 20:
        rng = []
        for i in range(m - 20, m):
            hi_i = highs[i - (m - nhl0)] if i >= (m - nhl0) else None
            lo_i = lows[i - (m - nhl0)] if i >= (m - nhl0) else None
            if hi_i and lo_i and vals[i]:
                rng.append((hi_i - lo_i) / vals[i] * 100)
        out["vrange20"] = round(sum(rng) / len(rng), 2) if rng else None
    else:
        out["vrange20"] = None

    # KDJ（high/low 可能短于 vals，用短的做对齐）
    nhl = 0
    if highs and lows:
        nhl = min(len(highs), len(lows))
    if nhl >= 20:
        seg_v = vals[-nhl:]
        seg_h = highs[-nhl:]
        seg_l = lows[-nhl:]
        K, D, J = calc_kdj(seg_v, seg_h, seg_l)
        out["kdjK"] = round(K[-1], 1) if K[-1] is not None else None
        out["kdjD"] = round(D[-1], 1) if D[-1] is not None else None
        out["kdjJ"] = round(J[-1], 1) if J[-1] is not None else None
        # 金叉：K 今天上穿 D，且 K < 45
        gold = False
        if len(K) >= 2 and K[-1] is not None and K[-2] is not None and D[-2] is not None:
            gold = (K[-2] <= D[-2]) and (K[-1] > D[-1]) and (K[-1] < 45)
        out["kdjGold"] = 1 if gold else 0
    else:
        out["kdjK"] = out["kdjD"] = out["kdjJ"] = None
        out["kdjGold"] = None

    return out


HL_DAYS = HISTORY_DAYS   # 最高/最低价天数跟随统一目标
CHIP_BINS = 100          # 价格分箱数
CHIP_MIN_DAYS = 40       # 至少 40 天历史才算筹码
CHIP_DAYS = 120          # 筹码只用最近 120 天（再多不提升精度，只增加算力）


def calc_chips(closes, vols, float_shares_wan, bins=CHIP_BINS):
    """筹码分布（三角分布 + 换手衰减）

    不需要 high/low —— 每天的价格区间用「收盘价 ± 当日涨跌幅」近似，
    实测与用真实高低价的误差仅 1.1 个百分点。

    closes: 收盘价（前复权，元）
    vols:   成交量（万股）
    float_shares_wan: 流通股本（万股）
    返回 {chipWinner, chipCost, chipConc} 或 None
    """
    if not float_shares_wan or float_shares_wan <= 0:
        return None

    rows = []
    for i, (c, v) in enumerate(zip(closes, vols)):
        if not c or c <= 0 or not v or v <= 0:
            continue
        # 当日宽度：用涨跌幅绝对值，最小 0.8%
        w = 0.02
        if i > 0 and closes[i - 1] and closes[i - 1] > 0:
            w = abs(c / closes[i - 1] - 1)
        if w < 0.008:
            w = 0.008
        rows.append((c, c * (1 + w), c * (1 - w), v))

    if len(rows) < CHIP_MIN_DAYS:
        return None

    lo = min(r[2] for r in rows)
    hi = max(r[1] for r in rows)
    if hi <= lo or lo <= 0:
        return None

    step = (hi - lo) / bins
    if step <= 0:
        return None

    chips = [0.0] * bins

    for c, h, l, v in rows:
        tr = v / float_shares_wan
        if tr > 1:
            tr = 1.0

        # 老筹码按换手率衰减
        if tr > 0.0001:
            keep = 1.0 - tr
            chips = [x * keep for x in chips]

        # 新筹码按三角分布撒在 [l, h]
        i_lo = max(0, min(bins - 1, int((l - lo) / step)))
        i_hi = max(0, min(bins - 1, int((h - lo) / step)))
        i_c = max(0, min(bins - 1, int((c - lo) / step)))
        if i_hi < i_lo:
            i_lo, i_hi = i_hi, i_lo

        span_l = max(1, i_c - i_lo)
        span_r = max(1, i_hi - i_c)
        weights = []
        wsum = 0.0
        for i in range(i_lo, i_hi + 1):
            d = (i_c - i) / span_l if i <= i_c else (i - i_c) / span_r
            w = 1.0 - 0.9 * d
            if w < 0.05:
                w = 0.05
            weights.append((i, w))
            wsum += w

        if wsum <= 0:
            continue
        add = v / wsum
        for i, w in weights:
            chips[i] += add * w

    total = sum(chips)
    if total <= 0:
        return None

    cur = rows[-1][0]
    i_cur = max(0, min(bins - 1, int((cur - lo) / step)))
    winner = sum(chips[:i_cur + 1]) / total * 100.0
    cost = sum(chips[i] * (lo + (i + 0.5) * step) for i in range(bins)) / total

    acc = 0.0
    p15 = p85 = None
    for i in range(bins):
        acc += chips[i]
        if p15 is None and acc >= total * 0.15:
            p15 = i
        if p85 is None and acc >= total * 0.85:
            p85 = i
            break
    conc = None
    if p15 is not None and p85 is not None and cur > 0:
        conc = (p85 - p15 + 1) * step / cur * 100.0

    return {
        "chipWinner": round(winner, 1),
        "chipCost": round(cost, 2),
        "chipConc": round(conc, 1) if conc is not None else None,
    }


def _fetch_lhb_list(date):
    """某一天的龙虎榜列表"""
    try:
        raw = http_get(LHB_API.format(date=date), timeout=30)
        d = json.loads(raw.decode("utf-8", errors="replace"))
        return (d.get("result") or {}).get("data") or []
    except Exception as e:
        print(f"    [龙虎榜] {date} 列表失败: {e}")
        return []


def _fetch_lhb_seats(date):
    """某一天的营业部席位（买 + 卖）"""
    out = {}
    for api in (SEAT_API, SEAT_API.replace("DETAILSBUY", "DETAILSSELL")):
        page = 1
        while page <= 4:
            try:
                raw = http_get(api.format(page=page, date=date), timeout=30)
                d = json.loads(raw.decode("utf-8", errors="replace"))
                r = d.get("result") or {}
                data = r.get("data") or []
                if not data:
                    break
                for x in data:
                    code = norm(x.get("SECURITY_CODE", ""))
                    if not code:
                        continue
                    item = {
                        "dept": x.get("OPERATEDEPT_NAME") or "",
                        "buy": fnum(x.get("BUY")),
                        "sell": fnum(x.get("SELL")),
                        "net": fnum(x.get("NET")),
                        "side": "B" if "BUY" in api else "S",
                    }
                    out.setdefault(code, []).append(item)
                if page >= (r.get("pages") or 1):
                    break
                page += 1
                time.sleep(0.5)
            except Exception as e:
                print(f"    [龙虎榜] {date} 席位失败: {e}")
                break
        time.sleep(0.5)
    return out


def build_lhb(dates):
    """龙虎榜：每次拉最近 LHB_DAYS 天，并与历史合并累计到 LHB_KEEP 天

    改前：每天整份覆盖，永远只有 20 天。
    改后：先读上次结果 → 本次最多补 25 天缺口 → 合并 → 只留最近 120 天。
          跑 5 次攒满 120 天，之后每天只补 20 天增量。
    """
    old = load_json(LHB_FILE, {"days": []}).get("days") or []
    oldmap = {d.get("date"): d for d in old if d.get("date")}
    want = [d for d in dates[-LHB_KEEP:] if d not in oldmap]
    fetch_dates = sorted(set(dates[-LHB_DAYS:]) | set(want[-25:]))
    print(f"  [龙虎榜] 已有 {len(oldmap)} 天，本次拉 {len(fetch_dates)} 天（目标 {LHB_KEEP} 天）")

    days = []
    total_rows = 0

    for d in fetch_dates:
        rows = _fetch_lhb_list(d)
        if not rows:
            print(f"    [龙虎榜] {d} 无数据")
            time.sleep(1)
            continue

        lst = []
        for x in rows:
            code = norm(x.get("SECURITY_CODE", ""))
            if not code:
                continue
            lst.append({
                "code": code,
                "name": x.get("SECURITY_NAME_ABBR") or "",
                "chg": round(fnum(x.get("CHANGE_RATE")), 2),
                "close": fnum(x.get("CLOSE_PRICE")),
                "net": round(fnum(x.get("BILLBOARD_NET_AMT")) / 1e8, 2),    # 亿
                "buy": round(fnum(x.get("BILLBOARD_BUY_AMT")) / 1e8, 2),
                "sell": round(fnum(x.get("BILLBOARD_SELL_AMT")) / 1e8, 2),
                "turnover": round(fnum(x.get("TURNOVERRATE")), 2),
                "amount": round(fnum(x.get("ACCUM_AMOUNT")) / 1e8, 2),
                "reason": (x.get("EXPLANATION") or "")[:40],
            })

        seats = _fetch_lhb_seats(d)
        # 每个席位只保留金额最大的前 5 条
        seat_out = {}
        for code, items in seats.items():
            items.sort(key=lambda z: abs(z.get("net") or 0), reverse=True)
            seat_out[code] = [
                {"dept": it["dept"][:30],
                 "buy": round(it["buy"] / 1e4, 1),      # 万
                 "sell": round(it["sell"] / 1e4, 1),
                 "net": round(it["net"] / 1e4, 1)}
                for it in items[:5]
            ]

        days.append({"date": d, "list": lst, "seats": seat_out})
        total_rows += len(lst)
        print(f"    [龙虎榜] {d}: {len(lst)} 只，席位 {len(seat_out)} 只")
        time.sleep(1)

    allday = dict(oldmap)
    for z in days:
        allday[z["date"]] = z
    merged = sorted(allday.values(), key=lambda z: z["date"])[-LHB_KEEP:]
    total_rows = sum(len(z.get("list") or []) for z in merged)
    save_json(LHB_FILE, {"days": merged, "count": total_rows,
                         "updatedAt": datetime.utcnow().isoformat() + "Z"})
    print(f"  [龙虎榜] 完成：累计 {len(merged)} 天，{total_rows} 条")
    return merged


# ============================================================
# 体积自检：学习每天体积 → 反推能装多少天 → 超限降级 / 瘦身恢复
# 覆盖：K线+量能(history)、指数、板块、龙虎榜、财务、行情快照
# ============================================================
FILE_BUDGET_MB = {
    "history.json": 88,
    "financials.json": 20,
    "lhb.json": 25,
    "analysis.json": 12,
    "quotes.json": 8,
    "sector_history.json": 6,
    "index_history.json": 4,
    "sector_map.json": 3,
    "meta.json": 1,
}


def _dir_size():
    total = 0
    rows = []
    if not os.path.isdir("data"):
        return 0, rows
    for f in sorted(os.listdir("data")):
        p = os.path.join("data", f)
        if os.path.isfile(p):
            sz = os.path.getsize(p)
            total += sz
            rows.append((f, sz))
    return total, rows


def enforce_size_limit():
    try:
        total, rows = _dir_size()
        mb = total / 1048576.0
        sizes = {f: sz for f, sz in rows}
        hist_bytes = sizes.get("history.json", 0)
        hist_mb = hist_bytes / 1048576.0
        biggest = (max([sz for _, sz in rows]) / 1048576.0) if rows else 0.0

        print(f"[体积] 合计 {mb:.1f} MB，history {hist_mb:.1f} MB，最大单文件 {biggest:.1f} MB")
        for f, sz in sorted(rows, key=lambda x: -x[1])[:8]:
            bud = FILE_BUDGET_MB.get(f)
            flag = (f"  [超预算 {bud} MB]" if (bud and sz / 1048576.0 > bud) else "")
            print(f"        {f:<24}{sz/1048576.0:>7.1f} MB{flag}")

        try:
            with open(STATE_FILE, encoding="utf-8") as fh:
                st = json.load(fh)
        except Exception:
            st = {}
        mode = st.get("mode", "normal")
        new_mode = mode
        why = ""

        bpd = int(hist_bytes / HISTORY_DAYS) if (hist_bytes and HISTORY_DAYS) else 0
        cap_days = int(st.get("capDays") or DATA_TARGET_DAYS)
        if bpd:
            by_single = int(SIZE_SINGLE_LIMIT_MB * 1048576 * 0.95 / bpd)
            by_total = int(SIZE_TOTAL_LIMIT_MB * 1048576 * 0.95 / bpd)
            cap_days = max(DAYS_FLOOR, min(by_single, by_total))

        over = [f for f, b in FILE_BUDGET_MB.items() if sizes.get(f, 0) / 1048576.0 > b]
        if mb > SIZE_TOTAL_LIMIT_MB or over:
            new_mode = "reduced"
            why = (f"总量 {mb:.0f} MB 超 {SIZE_TOTAL_LIMIT_MB} MB"
                   if mb > SIZE_TOTAL_LIMIT_MB else f"文件超预算：{','.join(over)}")
        elif mode == "reduced" and mb < SIZE_TOTAL_LIMIT_MB * SIZE_RESUME_RATIO:
            new_mode = "normal"
            why = f"总量已降到 {mb:.0f} MB（< {int(SIZE_TOTAL_LIMIT_MB * SIZE_RESUME_RATIO)} MB）"

        if new_mode != mode:
            print(f"[体积] 模式切换 {mode} -> {new_mode}（{why}）")
            print(f"[体积] 下次运行起数据天数设为 "
                  f"{DAYS_FLOOR if new_mode == 'reduced' else min(DATA_TARGET_DAYS, cap_days)} 天")
        elif cap_days < DATA_TARGET_DAYS and new_mode == "normal":
            print(f"[体积] 按体积上限，天数由 {DATA_TARGET_DAYS} 压到 {cap_days} 天")
        else:
            print("[体积] 正常")

        st.update({
            "mode": new_mode,
            "capDays": cap_days,
            "historyDays": HISTORY_DAYS,
            "bytesPerDay": bpd,
            "lastCheckAt": datetime.utcnow().isoformat() + "Z",
            "lastSizeMB": round(mb, 1),
            "lastHistoryMB": round(hist_mb, 1),
            "lastFiles": {f: round(sz / 1048576.0, 2) for f, sz in rows},
        })
        if new_mode != mode:
            st["lastSwitchAt"] = st["lastCheckAt"]
            st["lastSwitchReason"] = why
        save_json(STATE_FILE, st)
        return st
    except Exception as e:
        print(f"[体积] 自检失败（不影响主流程）: {e}")
        return {}


def main():
    t_start = time.time()

    print("=== 1. 拉名单 ===")
    codes = get_universe()
    print(f"名单 {len(codes)} 只")

    print("=== 2. 行业映射 ===")
    sector_map = get_sector_map()

    print("=== 3. 拉当日快照 ===")
    quotes, failed = fetch_all_quotes(codes)

    counter = {}
    for v in quotes.values():
        t = v.get("t") or ""
        if len(t) == 14:
            d = f"{t[0:4]}-{t[4:6]}-{t[6:8]}"
            counter[d] = counter.get(d, 0) + 1
    trade_date = max(counter, key=counter.get) if counter else None
    print(f"=== 4. 交易日 = {trade_date} ===")

    # ---------- 个股历史 ----------
    hist = load_json(HISTORY_FILE, {"dates": [], "close": {}})
    old_dates = hist.get("dates", [])
    old_close = hist.get("close", {})
    old_high = hist.get("high", {})
    old_low = hist.get("low", {})
    old_vol = hist.get("volume", {})

    targets = [c for c in codes if quotes.get(c, {}).get("v", 0) > 0]

    def _reset():
        return [], {}, {}, {}, {}

    # 历史天数不足 → 全部重新回补
    need_rebuild = len(old_dates) < HISTORY_DAYS * 0.8
    if need_rebuild and old_dates:
        print(f"  历史只有 {len(old_dates)} 天，低于目标 {HISTORY_DAYS} 天 → 重新回补")
        old_dates, old_close, old_high, old_low, old_vol = _reset()

    # 版本升级：v3 = close + high + low + volume(万股)
    if hist.get("ver", 1) < 4 and old_dates:
        print(f"  历史版本 {hist.get('ver', 1)} < 3（需前复权+单位修正）→ 全量重建")
        old_dates, old_close, old_high, old_low, old_vol = _reset()

    # ---- 黑名单：连续 2 次拉取失败（北交所/新股/停牌，腾讯无数据）→ 不再浪费额度 ----
    try:
        with open(STATE_FILE, encoding="utf-8") as _f:
            _bl_state = json.load(_f)
    except Exception:
        _bl_state = {}
    _black = {k for k, v in (_bl_state.get("blacklist") or {}).items() if v >= 2}
    if _black:
        print(f"  黑名单跳过 {len(_black)} 只（历史反复拉取失败）")

    missing = [c for c in targets if c not in old_close and c not in _black]

    def _priority(c):
        q = quotes.get(c, {})
        a = q.get("a", 0)
        tr = q.get("tr", 0)
        is_cand = (5e8 <= a <= 50e8) and (5 <= tr <= 15)
        return (0 if is_cand else 1, -a)

    missing.sort(key=_priority)
    cand_cnt = sum(1 for c in missing if _priority(c)[0] == 0)
    print(f"=== 5. 历史：已有 {len(old_close)} 只，还缺 {len(missing)} 只"
          f"（候选股 {cand_cnt} 只优先）===")

    todo = missing[:MAX_BACKFILL]
    if todo:
        print(f"  本次回补 {len(todo)} 只（上限 {MAX_BACKFILL}）")
        kclose, khigh, klow, kvol = backfill_history(todo)

        all_dates = sorted(set(old_dates) | {d for rows in kclose.values() for d, _ in rows})[-HISTORY_DAYS:]
        di = {d: i for i, d in enumerate(all_dates)}

        def merge(old_map, new_map):
            out = {}
            for c, arr in old_map.items():
                vals = [None] * len(all_dates)
                for od, v in zip(old_dates, arr):
                    if od in di:
                        vals[di[od]] = v
                out[c] = vals
            for c, rows in new_map.items():
                vals = [None] * len(all_dates)
                for d, v in rows:
                    if d in di:
                        vals[di[d]] = v
                out[c] = vals
            return out

        hist = {
            "ver": 4,
            "dates": all_dates,
            "close": merge(old_close, kclose),
            "high": merge(old_high, khigh),
            "low": merge(old_low, klow),
            "volume": merge(old_vol, kvol),
        }
    else:
        hist = {"ver": 4, "dates": old_dates, "close": old_close,
                "high": old_high, "low": old_low, "volume": old_vol}
        print("  无需回补")

    dates = hist["dates"]
    closes = hist["close"]
    vols = hist.get("volume", {})
    highs = hist.get("high", {})
    lows = hist.get("low", {})
    if trade_date and (not dates or dates[-1] != trade_date):
        dates.append(trade_date)
        for c, arr in closes.items():
            q = quotes.get(c)
            while len(arr) < len(dates) - 1:
                arr.append(None)
            arr.append(q["p"] if q and q["p"] > 0 else None)
        for c, arr in highs.items():
            q = quotes.get(c)
            while len(arr) < len(dates) - 1:
                arr.append(None)
            arr.append(q["h"] if q else None)
        for c, arr in lows.items():
            q = quotes.get(c)
            while len(arr) < len(dates) - 1:
                arr.append(None)
            arr.append(q["l"] if q else None)
        for c, arr in vols.items():
            q = quotes.get(c)
            while len(arr) < len(dates) - 1:
                arr.append(None)
            arr.append(round(vol_to_wan(c, q["v"]), 2) if q else None)   # → 万股
        if len(dates) > HISTORY_DAYS:
            cut = len(dates) - HISTORY_DAYS
            dates = dates[cut:]
            for m in (closes, highs, lows, vols):
                for c in list(m.keys()):
                    m[c] = m[c][cut:]
        hist = {"ver": 4, "dates": dates, "close": closes,
                "high": highs, "low": lows, "volume": vols}

    # high/low 只保留最近 HL_DAYS 天（KDJ 用），close/volume 保留全部
    trim = max(0, len(hist["dates"]) - HL_DAYS)
    if trim > 0:
        hist["high"] = {c: arr[-HL_DAYS:] for c, arr in hist.get("high", {}).items()}
        hist["low"] = {c: arr[-HL_DAYS:] for c, arr in hist.get("low", {}).items()}
        hist["hlStart"] = trim
    else:
        hist["hlStart"] = 0

    save_json(HISTORY_FILE, hist)
    print(f"  历史天数 {len(hist['dates'])} / 股票 {len(hist['close'])}"
          f" / 高低价 {HL_DAYS} 天 / 成交量 {len(hist.get('volume',{}))} 只")

    # ---------- 派生指标 ----------
    print("=== 6. 派生指标 ===")
    dates = hist["dates"]
    closes = hist["close"]

    def ma(arr, n):
        vals = [x for x in arr if x is not None]
        if len(vals) < n:
            return None
        return round(sum(vals[-n:]) / n, 3)

    stocks = {}
    chip_ok = 0
    chip_fail = 0
    highs_map = hist.get("high", {})
    lows_map = hist.get("low", {})
    vols_map = hist.get("volume", {})

    for c, q in quotes.items():
        arr = closes.get(c) or []
        highs = highs_map.get(c) or []
        lows = lows_map.get(c) or []
        vols = vols_map.get(c) or []
        vals = [x for x in arr if x is not None]
        chg5 = round((vals[-1] / vals[-6] - 1) * 100, 2) if len(vals) >= 6 and vals[-6] else None
        chg20 = round((vals[-1] / vals[-21] - 1) * 100, 2) if len(vals) >= 21 and vals[-21] else None
        chg60 = round((vals[-1] / vals[-61] - 1) * 100, 2) if len(vals) >= 61 and vals[-61] else None

        # 20 日波动率（低波动因子用，scoreStock 直接读 vol20）
        vol20 = None
        if len(vals) >= 21:
            rets = []
            for k in range(len(vals) - 20, len(vals)):
                if vals[k] and vals[k - 1]:
                    rets.append((vals[k] / vals[k - 1] - 1) * 100)
            if len(rets) >= 10:
                vol20 = round(statistics.pstdev(rets), 2)

        # 流通股本（万股）= 成交量(手)*100 / (换手率/100) / 10000
        float_shares = None
        if q["tr"] and q["tr"] > 0 and q["v"] > 0:
            shares = vol_to_shares(c, q["v"])          # 统一成股
            float_shares = round(shares / (q["tr"] / 100) / 10000, 1)

        # ---- 其他指标（RSI / 回撤 / 连涨跌 / 涨停次数）----
        try:
            extra = calc_extra(arr, highs, lows)
        except Exception as e:
            if chip_fail < 5:
                print(f"  [指标] {c} 计算失败: {e}")
            chip_fail += 1
            extra = {}

        # ---- 筹码三因子（用 OHLCV + 流通股本算）----
        try:
            # 只取最近 CHIP_DAYS 天：500 天全算会让筹码环节从 3 分钟涨到 12 分钟，
            # 而筹码分布本质是近期换手，120 天就够（与原口径一致）
            chip = calc_chips(arr[-CHIP_DAYS:], vols[-CHIP_DAYS:], float_shares)
            if chip:
                chip_ok += 1
        except Exception as e:
            if chip_fail < 5:
                print(f"  [筹码] {c} 计算失败: {e}")
            chip_fail += 1
            chip = None

        stocks[c] = {
            "industry": sector_map.get(c, ""),
            "floatShares": float_shares,
            "chipWinner": chip["chipWinner"] if chip else None,
            "chipCost": chip["chipCost"] if chip else None,
            "chipConc": chip["chipConc"] if chip else None,
            "ma5": ma(vals, 5), "ma10": ma(vals, 10),
            "ma20": ma(vals, 20), "ma60": ma(vals, 60), "ma250": ma(vals, 250),
            "chg5": chg5, "chg20": chg20, "chg60": chg60,
            "vol20": vol20,
            "rsi": extra.get("rsi"),
            "dd20": extra.get("dd20"),
            "dd60": extra.get("dd60"),
            "upDays": extra.get("upDays"),
            "downDays": extra.get("downDays"),
            "lu20": extra.get("lu20"),
            "kdjK": extra.get("kdjK"),
            "kdjD": extra.get("kdjD"),
            "kdjJ": extra.get("kdjJ"),
            "kdjGold": extra.get("kdjGold"),
            "hh60": extra.get("hh60"),
            "break20": extra.get("break20"),
            "vrange20": extra.get("vrange20"),
            "vratio": None,
        }

    # ---------- 量比（今日量 / 前 20 日均量）----------
    # 回测结论：缩量是加分项（量比 <0.5 时三个状态都提升），放量是负贡献
    vser = hist.get("volume") or {}
    vr_ok = 0
    for c, st in stocks.items():
        vs = [x for x in (vser.get(c) or []) if x]
        if len(vs) >= 21:
            base = sum(vs[-21:-1]) / 20.0
            st["vratio"] = round(vs[-1] / base, 2) if base > 0 else None
            if st["vratio"] is not None:
                vr_ok += 1
    print(f"  量比算出来 {vr_ok} 只")

    # ---------- 连板数 / 当日封板（打板策略用）----------
    # 用 quotes 的今日价 + history 的历史序列计算。
    # 不能直接用 hist 末行：同一次运行里当日 K 线未必已写入全部股票，会算成 0~1 只。
    cser_b = hist.get("close") or {}
    hser_b = hist.get("high") or {}
    board_ok = 0
    for c, st in stocks.items():
        st["isLimitUp"] = False
        st["board"] = 0
        try:
            q = quotes.get(c) or {}
            p = q.get("p") or 0
            h = q.get("h") or 0
            if not (p > 0):
                continue
            cs = [x for x in (cser_b.get(c) or []) if x]
            hs = [x for x in (hser_b.get(c) or []) if x]
            if not cs:
                continue
            # 若历史末行已是今日（收盘后重跑），去掉，避免重复计算
            if abs(cs[-1] - p) < 1e-6:
                cs = cs[:-1]
                hs = hs[:-1] if hs else hs
            if not cs:
                continue
            lim = 0.30 if c.startswith("bj") else (0.20 if c.startswith(("sh688", "sz30")) else 0.10)
            if p / cs[-1] - 1 >= lim * 0.98 and (h <= 0 or p >= h * 0.999):
                b = 1
                for k in range(1, 9):
                    if k + 1 > len(cs):
                        break
                    if not cs[-k - 1]:
                        break
                    ratio = cs[-k] / cs[-k - 1] - 1
                    touched_high = (len(hs) >= k) and hs[-k] and cs[-k] >= hs[-k] * 0.999
                    if ratio >= lim * 0.98 and touched_high:
                        b += 1
                    else:
                        break
                st["isLimitUp"] = True
                st["board"] = b
                board_ok += 1
        except Exception:
            continue
    print(f"  连板数算出来 {board_ok} 只（当日封板）")

    # ---------- 龙虎榜映射（打板用）----------
    # 说明：龙虎榜约 18:00 发布，本流程 16:35 运行，因此这里读到的是"上一次上榜"的记录，
    # 用 lhbDaysAgo 标明距今天数（1=昨日）。打板策略用「近期上榜 + 净买入>0 + 换手<10%」。
    try:
        _lhb_days = load_json(LHB_FILE, {"days": []}).get("days") or []
        _tm = {}
        for _d in sorted(_lhb_days, key=lambda z: z.get("date") or ""):
            for _x in (_d.get("list") or []):
                _tm[_x.get("code")] = {
                    "date": _d.get("date"),
                    "net": _x.get("net"),
                    "turnover": _x.get("turnover"),
                    "reason": _x.get("reason"),
                }
        _idx = {d: i for i, d in enumerate(hist.get("dates") or [])}
        _ti = len(hist.get("dates") or []) - 1
        ok_lhb = 0
        for c, st in stocks.items():
            z = _tm.get(c)
            st["lhbNet"] = None
            st["lhbTurnover"] = None
            st["lhbDaysAgo"] = None
            st["lhbReason"] = None
            if not z:
                continue
            zi = _idx.get(z.get("date"))
            st["lhbNet"] = z.get("net")
            st["lhbTurnover"] = z.get("turnover")
            st["lhbDaysAgo"] = (_ti - zi) if zi is not None else None
            st["lhbReason"] = z.get("reason")
            ok_lhb += 1
        print(f"  龙虎榜映射 {ok_lhb} 只")
    except Exception as _e:
        print(f"  [龙虎榜] 失败: {_e}")
        for st in stocks.values():
            st.setdefault("lhbNet", None)
            st.setdefault("lhbTurnover", None)
            st.setdefault("lhbDaysAgo", None)
            st.setdefault("lhbReason", None)

    # ---------- 短线策略字段：5日量比 / ATR14 / 60日位置 / 昨日高低 ----------
    # 逐只容错：任何一只算失败都不影响其他股票，也不影响主流程
    cser_s = hist.get("close") or {}
    hser_s = hist.get("high") or {}
    lser_s = hist.get("low") or {}
    vser_s = hist.get("volume") or {}
    ok_vr5 = ok_atr = ok_pos = ok_dif = 0
    for c, st in stocks.items():
        st["vr5"] = None
        st["atrPct"] = None
        st["pos60"] = None
        st["macdDif"] = None
        st["prevHigh"] = None
        st["prevChg"] = None
        try:
            cs = cser_s.get(c) or []
            hs = hser_s.get(c) or []
            ls = lser_s.get(c) or []
            vs = [x for x in (vser_s.get(c) or []) if x]

            # ① 5 日量比（次新股天数不足时用现有交易日均值）
            if len(vs) >= 4:
                bn = min(5, len(vs) - 1)
                base5 = sum(vs[-bn - 1:-1]) / float(bn)
                if base5 > 0:
                    st["vr5"] = round(vs[-1] / base5, 2)
                    ok_vr5 += 1

            # 对齐成 (收,高,低)，剔除空值，只用尾部 70 根
            rows = []
            n = min(len(cs), len(hs), len(ls))
            for i in range(max(0, n - 70), n):
                if cs[i] and hs[i] and ls[i]:
                    rows.append((float(cs[i]), float(hs[i]), float(ls[i])))

            # ② ATR 占现价百分比（不足 15 根时用现有根数，至少 6 根）
            if len(rows) >= 6:
                n_atr = min(14, len(rows) - 1)
                trs = []
                for k in range(len(rows) - n_atr, len(rows)):
                    cl, hh, ll = rows[k]
                    pcl = rows[k - 1][0]
                    trs.append(max(hh - ll, abs(hh - pcl), abs(ll - pcl)))
                last = rows[-1][0]
                if last > 0 and n_atr > 0:
                    st["atrPct"] = round((sum(trs) / float(n_atr)) / last * 100, 2)
                    ok_atr += 1

            # ③ 60 日波段位置（不足 60 天用现有窗口，至少 20 天）
            if len(rows) >= 20:
                seg = rows[-min(60, len(rows)):]
                mx = max(x[1] for x in seg)
                mn = min(x[2] for x in seg)
                if mx > mn:
                    st["pos60"] = round((rows[-1][0] - mn) / (mx - mn), 3)
                    ok_pos += 1

            # ⑤ MACD(6,13,5) 的 DIF 序列（近 20 日），用于底背离判断
            try:
                closes_full = [x for x in (cser_s.get(c) or []) if x]
                p_now = (quotes.get(c) or {}).get("p") or 0
                if p_now > 0 and (not closes_full or abs(closes_full[-1] - p_now) > 1e-6):
                    closes_full.append(p_now)
                st["macdDif"] = None
                if len(closes_full) >= 30:
                    k6, k13, k5 = 2.0 / 7.0, 2.0 / 14.0, 2.0 / 6.0
                    e6 = e13 = dea = None
                    difs = []
                    for x in closes_full:
                        e6 = x if e6 is None else x * k6 + e6 * (1 - k6)
                        e13 = x if e13 is None else x * k13 + e13 * (1 - k13)
                        d = e6 - e13
                        dea = d if dea is None else d * k5 + dea * (1 - k5)
                        difs.append(round(d, 4))
                    st["macdDif"] = difs[-20:]
                    ok_dif += 1
            except Exception:
                st["macdDif"] = None

            # ④ 昨日最高 / 昨日涨跌幅
            if len(rows) >= 2:
                st["prevHigh"] = round(rows[-2][1], 2)
            if len(rows) >= 3 and rows[-3][0] > 0:
                st["prevChg"] = round((rows[-2][0] / rows[-3][0] - 1) * 100, 2)
        except Exception:
            continue
    print(f"  vr5 {ok_vr5} 只 / atrPct {ok_atr} 只 / pos60 {ok_pos} 只 / macdDif {ok_dif} 只")

    # ---------- ST / 停牌标记 ----------
    try:
        ok_st = ok_sus = 0
        for c, st in stocks.items():
            q = quotes.get(c) or {}
            nm = q.get("n") or ""
            st["isST"] = bool(("ST" in nm.upper()) or ("退" in nm))
            if st["isST"]:
                ok_st += 1
            vs2 = [x for x in ((hist.get("volume") or {}).get(c) or []) if x is not None]
            tail = vs2[-3:] if vs2 else []
            st["isSuspended"] = bool(tail) and all((not v) for v in tail)
            if st["isSuspended"]:
                ok_sus += 1
        print(f"  ST {ok_st} 只 / 疑似停牌 {ok_sus} 只")
    except Exception as _e:
        print(f"  [ST/停牌] 失败: {_e}")
        for st in stocks.values():
            st.setdefault("isST", False)
            st.setdefault("isSuspended", False)


    # ---------- 布林带（20日，2倍标准差）----------
    # 中性市超短信号：收盘跌破下轨 + 未深跌(>MA60*0.92) + 当日收阳
    cser = hist.get("close") or {}
    boll_ok = 0
    for c, st in stocks.items():
        cs = [x for x in (cser.get(c) or []) if x][-20:]
        if len(cs) >= 20 and st.get("ma20"):
            m = st["ma20"]
            var = sum((x - m) ** 2 for x in cs) / len(cs)
            sd = var ** 0.5
            st["std20"] = round(sd, 3)
            st["bollLo"] = round(m - 2 * sd, 2)      # 2σ 下轨
            st["bollLo15"] = round(m - 1.5 * sd, 2)  # 1.5σ 下轨
            st["bollHi"] = round(m + 2 * sd, 2)
            boll_ok += 1
    print(f"  布林带算出来 {boll_ok} 只")

    # ---------- 市场环境 ----------
    print(f"  筹码算出来 {chip_ok} 只，失败 {chip_fail} 只")

    print("=== 7. 市场环境 ===")
    total_amount = sum(q["a"] for q in quotes.values()) / 1e8

    limit_up = limit_down = broken = 0
    for q in quotes.values():
        lu, ld, p, h = q["lu"], q["ld"], q["p"], q["h"]
        if lu > 0 and p > 0 and p >= lu * 0.999:
            limit_up += 1
        elif lu > 0 and h > 0 and h >= lu * 0.999:
            broken += 1
        if ld > 0 and p > 0 and p <= ld * 1.001:
            limit_down += 1

    ever = limit_up + broken
    broken_rate = round(broken / ever * 100, 2) if ever else 0

    idx = {}
    idx_series = {}
    for name, code in [("sh", "sh000001"), ("sz", "sz399001")]:
        rows = fetch_kline_closes(code, 120)
        idx_series[name] = rows
        vals = [v for _, v in rows]
        _m20 = ma(vals, 20)
        _m5 = ma(vals, 5)
        # MA20 斜率：最近 5 个交易日的 MA20 变化率（%），用于前端判断趋势方向
        _slope = None
        if _m20 and len(vals) >= 25:
            _prev = sum(vals[-25:-5]) / 20.0
            if _prev > 0:
                _slope = round((_m20 / _prev - 1) * 100, 2)
        idx[name] = {
            "close": vals[-1] if vals else None,
            "ma5": _m5, "ma10": ma(vals, 10),
            "ma20": _m20, "ma60": ma(vals, 60),
            "ma5Slope": round((_m5 / (sum(vals[-10:-5]) / 5.0) - 1) * 100, 2)
                        if (_m5 and len(vals) >= 10 and sum(vals[-10:-5]) > 0) else None,
            "ma20Slope": _slope,
            "aboveMa20": (vals[-1] > _m20) if (vals and _m20) else None,
        }

    # ---------- 板块聚合 ----------
    _sh_prev = load_json(SECTOR_HIS_FILE, {"dates": [], "close": {}})
    sector_hist_close = _sh_prev.get("close") or {}
    sec_agg = {}
    for c, q in quotes.items():
        ind = stocks.get(c, {}).get("industry") or "其他"
        a = sec_agg.setdefault(ind, {"count": 0, "chgSum": 0.0, "chgN": 0, "limitUp": 0})
        a["count"] += 1
        if q["cp"]:
            a["chgSum"] += q["cp"]
            a["chgN"] += 1
        if q["lu"] > 0 and q["p"] >= q["lu"] * 0.999:
            a["limitUp"] += 1

    sectors = []
    for ind, a in sec_agg.items():
        if a["count"] < 5 or a["chgN"] == 0:
            continue
        _sh = (sector_hist_close or {}).get(ind) or []
        _c5 = None
        if len(_sh) >= 6 and _sh[-1] and _sh[-6]:
            _c5 = round((_sh[-1] / _sh[-6] - 1) * 100, 2)
        _m20 = None
        if len(_sh) >= 20 and all(_sh[-20:]):
            _m20 = sum(_sh[-20:]) / 20.0
        _trend = None
        if _c5 is not None and _sh:
            _trend = "up" if (_c5 > 0.5 and _m20 and _sh[-1] > _m20) else (
                "down" if (_c5 < -0.5 and _m20 and _sh[-1] < _m20) else "flat")
        _chg = round(a["chgSum"] / a["chgN"], 2)
        # 当日方向：和 chg 同口径，供「板块多空」统计使用（trend 是 5 日 + MA20 的中期口径）
        _trend_day = "up" if _chg > 0.1 else ("down" if _chg < -0.1 else "flat")
        sectors.append({
            "name": ind,
            "chg": _chg,
            "chg5": _c5,
            "trend": _trend,          # 中期：5 日涨跌 + MA20 位置
            "trendDay": _trend_day,   # 当日：按今日平均涨跌
            "limitUp": a["limitUp"],
            "count": a["count"],
        })
    sectors.sort(key=lambda x: x["chg"], reverse=True)
    for i, s in enumerate(sectors):
        s["rank"] = i + 1

    # 板块多空：当日 / 中期 两套口径，避免前端用错
    _breadth_day = {"up": 0, "flat": 0, "down": 0}
    _breadth_mid = {"up": 0, "flat": 0, "down": 0}
    for s in sectors:
        if s.get("trendDay"):
            _breadth_day[s["trendDay"]] += 1
        if s.get("trend"):
            _breadth_mid[s["trend"]] += 1
    _breadth_day["total"] = len(sectors)
    _breadth_mid["total"] = len(sectors)

    rank_map = {s["name"]: s for s in sectors}
    for c, st in stocks.items():
        s = rank_map.get(st["industry"])
        if s:
            st["sectorChg"] = s["chg"]
            st["sectorRank"] = s["rank"]
            st["sectorLimitUp"] = s["limitUp"]

    above_ma = bool(idx["sh"]["ma5"] and idx["sh"]["close"] and idx["sh"]["close"] >= idx["sh"]["ma5"])
    if limit_down >= 40 or broken_rate > 65:
        status, cap = "空仓", 0
    elif above_ma and limit_up >= 30 and broken_rate <= 45:
        status, cap = "进攻", 0.8
    elif above_ma and limit_up >= 15:
        status, cap = "中性", 0.5
    else:
        status, cap = "防守", 0.2

    # ---- 与回测一致的市况：上证 MA20 + MA20 五日斜率（modelEval 用的同一套）----
    def _regime_hist(rows, days=20):
        ds = [d for d, _ in rows]
        vs = [v for _, v in rows]
        m20 = _ma_series(vs, 20) if False else None
        m20 = []
        acc = 0.0
        cnt = 0
        for i, v in enumerate(vs):
            acc += v
            cnt += 1
            if cnt > 20:
                acc -= vs[i - 20]
                cnt = 20
            m20.append(acc / 20 if cnt == 20 else None)
        out = []
        for i in range(max(0, len(vs) - days), len(vs)):
            if i < 5 or m20[i] is None or m20[i - 5] is None:
                continue
            c = vs[i]
            st = ("进攻" if (c > m20[i] and m20[i] - m20[i - 5] > 0)
                  else ("防守" if (c < m20[i] and m20[i] - m20[i - 5] < 0) else "中性"))
            out.append({"date": ds[i], "status": st,
                        "close": round(c, 2), "ma20": round(m20[i], 2)})
        return out

    regime_hist = _regime_hist(idx_series.get("sh") or [])
    regime_now = regime_hist[-1]["status"] if regime_hist else status

    # 市场宽度：收盘价 > MA20 的股票占比（打板/超短的动态开关）
    _b_tot = _b_up = 0
    for _c, _s in stocks.items():
        _p = (quotes.get(_c) or {}).get("p", 0)
        _m20 = _s.get("ma20")
        if not _p or not _m20:
            continue
        _b_tot += 1
        if _p > _m20:
            _b_up += 1
    breadth = round(_b_up / _b_tot * 100, 1) if _b_tot else None
    _CAP = {"进攻": 0.8, "中性": 0.5, "防守": 0.3, "空仓": 0.0}

    market = {
        # status 改为 regime 的同值别名：原来两套口径同日常常矛盾
        # （2026-10-20: status=进攻 / regime=防守），旧客户端读 status 也不会错。
        "status": regime_now,
        "regime": regime_now,                  # 唯一口径（index_ma20_slope）
        "regimeHistory": regime_hist,
        "regimeSource": "index_ma20_slope",
        "positionCap": _CAP.get(regime_now, 0.3),   # 由 regime 推导，不再跟旧的 cap
        "breadth": breadth,
        "breadthDef": "收盘价 > MA20 的股票占比(%)",
        "indexAboveMA5": above_ma,
        "shIndex": idx["sh"],
        "szIndex": idx["sz"],
        "totalAmount": round(total_amount, 1),
        "limitUpCount": limit_up,
        "limitDownCount": limit_down,
        "brokenCount": broken,
        "brokenRate": broken_rate,
        "sectors": sectors,
        "sectorBreadthDay": _breadth_day,
        "sectorBreadthMid": _breadth_mid,
    }

    # ---------- 指数与板块历史 ----------
    print("=== 8. 指数历史 ===")
    build_index_history()

    print("=== 9. 板块历史 ===")
    build_sector_history(hist, sector_map)

    print("=== 9.5 龙虎榜 ===")
    build_lhb(hist["dates"])

    print("=== 10. 财务数据 ===")
    build_financials()

    # ETF 模块已废弃，不再采集（省 26 个请求）

    # ---------- 输出 ----------
    coverage = round(len(quotes) / len(codes), 4) if codes else 0
    meta = {
        "date": trade_date,
        "total": len(codes),
        "count": len(quotes),
        "coverage": coverage,
        "updatedAt": datetime.utcnow().isoformat() + "Z",
    }

    full = dict(meta)
    full["failed"] = failed[:200]
    full["quotes"] = quotes

    print("=== 11.5 模型自评（算合理的预期收益）===")
    try:
        model_eval = build_model_eval(hist)
    except Exception as e:
        print(f"  [自评] 失败（不影响主流程）: {e}")
        model_eval = None

    analysis = {
        "date": trade_date,
        "updatedAt": meta["updatedAt"],
        "historyDays": len(hist["dates"]),
        "market": market,
        "stocks": stocks,
    }
    if model_eval:
        # 清除退化档：lo==hi 说明"没算出来时填了止损值"，不能当真实期望
        _bad = 0
        _clean = {}
        for _reg, _buckets in model_eval.items():
            _clean[_reg] = {}
            for _bk, _v in _buckets.items():
                if not _v:
                    continue
                if _v.get("lo") == _v.get("hi") or (_v.get("mid") == _v.get("lo") == _v.get("hi")):
                    _bad += 1
                    _clean[_reg][_bk] = {"n": _v.get("n"), "mid": None, "lo": None, "hi": None,
                                         "win": _v.get("win"), "invalid": True,
                                         "reason": "lo=hi，统计退化"}
                else:
                    _clean[_reg][_bk] = _v
        analysis["modelEval"] = {
            "asOf": trade_date,
            "holdDays": MODEL_HOLD,
            "stopPct": int(MODEL_STOP * 100),
            "byRegime": _clean,
            "degenerateCount": _bad,
        }
        if _bad:
            print(f"  [自评] 已置空 {_bad} 个退化档位（lo=hi）")

    save_json("data/meta.json", meta)
    save_json("data/quotes.json", full)
    save_json("data/analysis.json", analysis)

    print("=== 12. 完成 ===")
    for p in ["data/meta.json", "data/quotes.json", "data/analysis.json",
              "data/history.json", "data/sector_map.json",
              "data/index_history.json", "data/sector_history.json",
              "data/financials.json", "data/lhb.json"]:
        if os.path.exists(p):
            print(f"  {p}  {os.path.getsize(p)/1024:.0f} KB")
    print(f"  交易日 {trade_date}  覆盖 {coverage*100:.2f}%  用时 {int(time.time() - t_start)}s")
    print(f"  状态 {regime_now}  仓位 {int(market['positionCap']*100)}%  宽度 {breadth}%"
          f"   (旧口径 status={status} cap={int(cap*100)}% 仅日志用)")
    print(f"  涨停 {limit_up}  跌停 {limit_down}  炸板 {broken}({broken_rate}%)  成交额 {total_amount:.0f}亿")
    print(f"  行业 {len(sector_map)} 只  板块指数 {len(sectors)} 个")

    # ---------- 体积自检（学习体积、决定下次用多少天） ----------
    enforce_size_limit()


if __name__ == "__main__":
    try:
        main()
    except Exception:
        import traceback
        tb = traceback.format_exc()
        print(tb, flush=True)
        try:
            os.makedirs("data", exist_ok=True)
            with open("data/last_error.txt", "w", encoding="utf-8") as f:
                f.write(tb)
        except Exception:
            pass
