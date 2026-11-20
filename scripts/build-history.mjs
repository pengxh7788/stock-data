// 生成 history 分支数据（结构不变）：
//   out/kline/<code>.json   个股日K（前复权，250根）
//   out/flow/<code>.json    个股资金流
//   out/daily/<date>.json   全市场日K分片（60天）
//   out/flow_daily/<date>.json 全市场资金流分片（60天）
//   out/calendar.json / manifest.json
//
// 获取策略（重点优化）：
//   1) K 线：腾讯，并发 8；已是最新交易日的直接跳过；缺数据的才拉 250 根；
//   2) 资金流：默认**只拉一次全市场资金流排行**（约 30 个请求）拿到当天所有票，
//      再本地写入每股 flow 文件与 flow_daily，不再 6000 次逐票请求；
//   3) 首次（FULL=1）才逐票补 20 天资金流历史，并发 3，失败不阻塞。
import fs from 'node:fs/promises';
import path from 'node:path';

const OUT = 'out';
const KEEP = 250;
const DAILY_DAYS = 60;
const FULL = process.env.FULL === '1';
const KLINE_CONC = Number(process.env.KLINE_CONC || 8);
const FLOW_CONC = Number(process.env.FLOW_CONC || 3);
const UT = 'fa5fd1943c7b386f172d6893dbfba10b';
const OWNER_REPO = process.env.OWNER_REPO || 'pengxh7788/stock-data';
const HEADERS = { 'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)' };
let FLOAT_SHARES = new Map();   // code -> 流通股本(股)

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const secid = (code) => `${code.startsWith('sh') || /^6/.test(code) ? 1 : 0}.${code.replace(/\D/g, '')}`;
const isStar = (code) => code.startsWith('sh688') || code.startsWith('sh689');

async function fetchText(url, timeout = 8000, retry = 1) {
  for (let i = 0; i <= retry; i++) {
    try {
      const res = await fetch(url, { headers: HEADERS, signal: AbortSignal.timeout(timeout) });
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      return await res.text();
    } catch (e) {
      if (i === retry) throw e;
      await sleep(200 + Math.random() * 200);
    }
  }
}
const fetchJson = async (u, t = 8000, r = 1) => JSON.parse(await fetchText(u, t, r));

async function tencentKline(code, n) {
  const url = `https://web.ifzq.gtimg.cn/appstock/app/fqkline/get?param=${code},day,,,${n},qfq`;
  const d = await fetchJson(url);
  const block = d?.data?.[code];
  const rows = block?.qfqday || block?.day || [];
  return rows
    .filter((r) => Array.isArray(r) && r.length >= 6)
    .map((r) => {
      const open = +r[1], close = +r[2], high = +r[3], low = +r[4];
      let vol = +r[5];
      if (isStar(code)) vol = vol / 100;
      return [r[0], open, close, high, low, vol, Math.round(close * vol * 100)];
    })
    .filter((b) => b[1] > 0 && b[2] > 0);
}

function flowToObj(x) {
  if (Array.isArray(x)) {
    const [date, mainV, superV, bigV, midV, smallV] = x;
    return { date, main: +mainV || 0, retail: (+midV || 0) + (+smallV || 0), super: +superV || 0, big: +bigV || 0, mid: +midV || 0, small: +smallV || 0 };
  }
  return {
    date: String(x?.date || ''),
    src: x?.src || 'em',
    main: +x?.main || 0,
    retail: +(x?.retail ?? ((+x?.mid || 0) + (+x?.small || 0))),
    super: +x?.super || 0,
    big: x?.big == null ? null : +x.big,
    mid: x?.mid == null ? null : +x.mid,
    small: x?.small == null ? null : +x.small,
  };
}
function normalizeFlow(list) {
  return (list || []).map(flowToObj).filter((x) => /^\d{4}-\d{2}-\d{2}$/.test(x.date));
}

async function sinaFlowHistory(code, days = 60) {
  const url = `https://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php/MoneyFlow.ssl_qsfx_zjlrqs?page=1&num=${days}&sort=opendate&asc=0&daima=${code}`;
  const res = await fetch(url, { headers: { ...HEADERS, Referer: 'https://finance.sina.com.cn' }, signal: AbortSignal.timeout(8000) });
  if (!res.ok) throw new Error(`HTTP ${res.status}`);
  const arr = await res.json();
  return (arr || []).map((r) => ({
    date: String(r.opendate || ''),
    main: +r.netamount || 0,
    super: +r.r0_net || 0,
    big: null,
    mid: null,
    small: null,
    retail: null,
    src: 'sina',
  })).filter((x) => /^\d{4}-\d{2}-\d{2}$/.test(x.date));
}

async function emFlowHistory(code, lmt) {
  const url =
    `https://push2his.eastmoney.com/api/qt/stock/fflow/daykline/get?secid=${secid(code)}` +
    `&fields1=f1,f2,f3,f7&fields2=f51,f52,f53,f54,f55,f56&klt=101&lmt=${lmt}&ut=${UT}`;
  const d = await fetchJson(url);
  return (d?.data?.klines || [])
    .map((s) => {
      const p = String(s).split(',');
      // 东财: f52 主力 / f53 小单 / f54 中单 / f55 大单 / f56 超大单
      return flowToObj([p[0], +p[1], +p[5], +p[4], +p[3], +p[2]]);
    })
    .filter((f) => /^\d{4}-\d{2}-\d{2}$/.test(f.date));
}

// 全市场当日资金流排行（一次拿完，约 30 页）
async function marketFlowRanking() {
  const out = new Map();
  const fsParam = 'm:0+t:6,m:0+t:80,m:1+t:2,m:1+t:23,m:0+t:81+s:2048';
  for (let pn = 1; pn <= 60; pn++) {
    const url =
      `https://push2.eastmoney.com/api/qt/clist/get?pn=${pn}&pz=200&po=1&np=1&fltt=2&invt=2&fid=f62` +
      `&fs=${encodeURIComponent(fsParam)}&fields=f12,f62,f66,f72,f78,f84`;
    let d;
    try { d = await fetchJson(url, 10000, 1); } catch { break; }
    const diff = d?.data?.diff || [];
    if (!diff.length) break;
    for (const x of diff) {
      const code = String(x.f12 || '');
      if (!/^\d{6}$/.test(code)) continue;
      const norm = code.startsWith('6') ? 'sh' + code : /^[489]/.test(code) ? 'bj' + code : 'sz' + code;
      out.set(norm, [+x.f62 || 0, +x.f66 || 0, +x.f72 || 0, +x.f78 || 0, +x.f84 || 0]);
    }
    const total = d?.data?.total || 0;
    if (pn * 200 >= total) break;
    await sleep(120);
  }
  return out;
}

const readJson = async (f) => { try { return JSON.parse(await fs.readFile(f, 'utf8')); } catch { return null; } };
const writeJson = async (f, v) => { await fs.mkdir(path.dirname(f), { recursive: true }); await fs.writeFile(f, JSON.stringify(v)); };

function normalizeBars(bars, code) {
  const cap = FLOAT_SHARES.get(code) || 0;
  return (bars || []).map((b) => {
    const date = b[0], o = +b[1], c = +b[2], h = +b[3], l = +b[4], v = +b[5], a = +b[6];
    let t = b[7];
    if (t == null || t === 0) t = cap > 0 ? +((v * 100) / cap * 100).toFixed(2) : 0;
    return [date, o, c, h, l, v, a, +t || 0];
  });
}

function mergeByDate(oldList, newList, keep = KEEP) {
  const m = new Map();
  for (const x of [...(oldList || []), ...newList]) {
    const d = Array.isArray(x) ? x[0] : x?.date;
    if (!d) continue;
    const prev = m.get(d);
    // 同一日期：已有东财完整拆分（big 非空）时，不被新浪的 null 覆盖
    if (prev && !Array.isArray(prev) && prev.big != null && !Array.isArray(x) && x.big == null) continue;
    m.set(d, x);
  }
  return [...m.values()].sort((a, b) => {
    const da = Array.isArray(a) ? a[0] : a.date;
    const db = Array.isArray(b) ? b[0] : b.date;
    return da.localeCompare(db);
  }).slice(-keep);
}

async function pool(items, n, fn) {
  let i = 0;
  await Promise.all(Array.from({ length: n }, async () => {
    while (i < items.length) {
      const k = i++;
      try { await fn(items[k], k); } catch {}
      await sleep(15);
    }
  }));
}

async function loadFloatShares() {
  const bases = [
    `https://gh-proxy.com/https://raw.githubusercontent.com/${OWNER_REPO}/main/data/analysis.json`,
    `https://cdn.jsdelivr.net/gh/${OWNER_REPO}@main/data/analysis.json`,
  ];
  for (const u of bases) {
    try {
      const d = await fetchJson(u, 20000, 0);
      const stocks = d?.stocks || {};
      for (const [code, v] of Object.entries(stocks)) {
        const fs = Number(v?.floatShares);
        if (fs > 0) FLOAT_SHARES.set(code, fs * 1e4);   // 万股 -> 股
      }
      if (FLOAT_SHARES.size) return;
    } catch (e) { console.warn('[floatShares]', e.message); }
  }
}

async function loadCodes() {
  const bases = [
    `https://gh-proxy.com/https://raw.githubusercontent.com/${OWNER_REPO}/main/data/quotes.json`,
    `https://cdn.jsdelivr.net/gh/${OWNER_REPO}@main/data/quotes.json`,
    `https://raw.githubusercontent.com/${OWNER_REPO}/main/data/quotes.json`,
  ];
  for (const u of bases) {
    try {
      const d = await fetchJson(u, 15000, 0);
      const codes = Object.keys(d?.quotes || {});
      if (codes.length) return codes.filter((c) => /^(sh|sz|bj)\d{6}$/.test(c));
    } catch (e) { console.warn('[codes]', u, e.message); }
  }
  throw new Error('无法读取 quotes.json');
}

async function buildCalendar() {
  for (let i = 0; i < 3; i++) {
    try {
      const d = await fetchJson('https://web.ifzq.gtimg.cn/appstock/app/fqkline/get?param=sh000001,day,,,500,qfq', 10000, 1);
      const rows = d?.data?.sh000001?.qfqday || d?.data?.sh000001?.day || [];
      const days = rows.map((r) => r[0]).slice(-500);
      if (days.length > 20) return { tradingDays: days, lastTradingDate: days[days.length - 1] };
    } catch (e) { console.warn('[calendar]', i + 1, e.message); await sleep(1000); }
  }
  throw new Error('交易日历获取失败');
}

async function main() {
  const t0 = Date.now();
  const calendar = await buildCalendar();
  await loadFloatShares();
  const codes = await loadCodes();
  const lastDate = calendar.lastTradingDate;
  console.log(`[history] codes=${codes.length} full=${FULL} last=${lastDate}`);

  // ---------- K 线 ----------
  let klineCount = 0, skipped = 0, missingCount = 0;
  const missingCodes = [];
  await pool(codes, KLINE_CONC, async (code) => {
    const file = path.join(OUT, 'kline', `${code}.json`);
    const old = FULL ? null : await readJson(file);
    const last = old?.bars?.length ? old.bars[old.bars.length - 1][0] : null;
    if (!FULL && last === lastDate && Array.isArray(old?.bars?.[0]) && old.bars[0].length >= 8) { skipped++; return; }
    const n = last ? 5 : KEEP;
    let bars = [];
    try { bars = await tencentKline(code, n); } catch {}
    if (!bars.length) bars = old?.bars || [];
    const merged = normalizeBars(mergeByDate(old?.bars, bars), code);
    if (merged.length) {
      await writeJson(file, { code, adj: 'qfq', amountEst: true, floatShares: (FLOAT_SHARES.get(code) || 0) / 1e4, lastDate: merged[merged.length - 1][0], updated: merged[merged.length - 1][0], bars: merged });
      klineCount++;
    } else { missingCount++; missingCodes.push(code); }
  });
  console.log(`[history] kline done=${klineCount} skipped=${skipped} missing=${missingCount} ${Math.round((Date.now() - t0) / 1000)}s`);

  // 指数也写入，供 APP 的 getTradingDays / 大盘使用
  for (const idx of ['sh000001', 'sz399001', 'sz399006']) {
    try {
      const old = await readJson(path.join(OUT, 'kline', `${idx}.json`));
      const bars = mergeByDate(old?.bars, await tencentKline(idx, KEEP));
      if (bars.length) {
        await writeJson(path.join(OUT, 'kline', `${idx}.json`), { code: idx, adj: 'qfq', amountEst: true, lastDate: bars[bars.length - 1][0], updated: bars[bars.length - 1][0], bars: normalizeBars(bars, idx) });
      }
    } catch (e) { console.warn('[index]', idx, e.message); }
  }

  // ---------- 资金流 ----------
  let flowCount = 0;
  const FLOW_BATCH = Number(process.env.FLOW_BATCH || 2000);

  let flowFiles = [];
  try { flowFiles = await fs.readdir(path.join(OUT, 'flow')); } catch {}
  const flowHave = new Set(flowFiles.filter((f) => f.endsWith('.json')).map((f) => f.slice(0, -5)));

  // 需要更新的票（没有文件或最后日期不是最新交易日）
  const needFlow = [];
  for (const code of codes) {
    if (!flowHave.has(code)) { needFlow.push(code); continue; }
    const d = await readJson(path.join(OUT, 'flow', `${code}.json`));
    const last = d?.flow?.length ? d.flow[d.flow.length - 1].date : '';
    if (last !== lastDate) needFlow.push(code);
  }

  // ---- 第一优先：新浪（快，main/super 齐全） ----
  const sinaBatch = needFlow.slice(0, 6000);
  console.log(`[history] flow sina batch=${sinaBatch.length}`);
  await pool(sinaBatch, 8, async (code) => {
    const file = path.join(OUT, 'flow', `${code}.json`);
    const old = await readJson(file);
    try {
      const rows = await sinaFlowHistory(code, 60);
      if (!rows.length) return;
      const flow = mergeByDate(normalizeFlow(old?.flow), rows);
      await writeJson(file, { code, lastDate: flow[flow.length - 1].date, updated: flow[flow.length - 1].date, flow });
      flowCount++;
    } catch {}
  });

  // ---- 第二优先：东财补大/中/小单拆分（限量，后台慢慢补） ----
  const needEm = [];
  for (const code of codes) {
    const d = await readJson(path.join(OUT, 'flow', `${code}.json`));
    const last = d?.flow?.[d.flow.length - 1];
    if (!last || last.big == null) needEm.push(code);
  }
  const emBatch = needEm.slice(0, FLOW_BATCH);
  console.log(`[history] flow em split need=${needEm.length} batch=${emBatch.length}`);
  await pool(emBatch, 4, async (code) => {
    const file = path.join(OUT, 'flow', `${code}.json`);
    const old = await readJson(file);
    try {
      const flow = mergeByDate(normalizeFlow(old?.flow), await emFlowHistory(code, 60));
      await writeJson(file, { code, lastDate: flow[flow.length - 1].date, updated: flow[flow.length - 1].date, flow });
    } catch {}
  });

  // 统计最终 flow 文件数（含历史已存在的）
  try { flowFiles = await fs.readdir(path.join(OUT, 'flow')); } catch {}
  const flowTotal = flowFiles.filter((f) => f.endsWith('.json')).length;

  console.log(`[history] flow done=${flowCount} total=${flowTotal} ${Math.round((Date.now() - t0) / 1000)}s`);

  // ---------- 分片 ----------
  await writeJson(path.join(OUT, 'calendar.json'), calendar);
  const days = calendar.tradingDays.slice(-DAILY_DAYS);
  await buildDaily(days);
  await buildFlowDaily(days);
  await buildLhb(days);

  await writeJson(path.join(OUT, 'manifest.json'), {
    updatedAt: new Date().toISOString(),
    lastTradingDate: lastDate,
    klineCount, flowCount: flowTotal,
    dailyDays: days.length,
    dailyFrom: days[0],
    dailyTo: days[days.length - 1],
    flowDailyDays: days.length,
    lhbDays: days.length,
    missing: missingCodes.slice(0, 300),
  });
  console.log(`[history] ALL DONE ${Math.round((Date.now() - t0) / 1000)}s`);
}

async function fetchLhb(date) {
  const rows = {};
  for (let pn = 1; pn <= 5; pn++) {
    const filter = encodeURIComponent(`(TRADE_DATE='${date}')`);
    const url =
      'https://datacenter-web.eastmoney.com/api/data/v1/get?reportName=RPT_DAILYBILLBOARD_DETAILSNEW' +
      '&columns=SECURITY_CODE,SECUCODE,TRADE_DATE,BILLBOARD_NET_AMT,BILLBOARD_BUY_AMT,BILLBOARD_SELL_AMT,TURNOVERRATE,EXPLANATION' +
      `&filter=${filter}&pageNumber=${pn}&pageSize=500&source=WEB&client=WEB`;
    let d;
    try { d = await fetchJson(url, 12000, 1); } catch (e) { break; }
    const list = d?.result?.data || [];
    if (!list.length) break;
    for (const x of list) {
      const m = String(x.SECUCODE || '').split('.');
      if (m.length !== 2) continue;
      const code = (m[1] === 'SH' ? 'sh' : m[1] === 'BJ' ? 'bj' : 'sz') + m[0];
      const rec = {
        net: +x.BILLBOARD_NET_AMT || 0,
        buy: +x.BILLBOARD_BUY_AMT || 0,
        sell: +x.BILLBOARD_SELL_AMT || 0,
        tr: +x.TURNOVERRATE || 0,
        why: String(x.EXPLANATION || ''),
      };
      const prev = rows[code];
      if (!prev) {
        rows[code] = rec;
      } else {
        // 同一只票当天可能有多条上榜原因：保留 |净买额| 最大的一条，原因合并
        const whys = new Set(String(prev.why).split('；').concat(String(rec.why).split('；')).filter(Boolean));
        if (Math.abs(rec.net) > Math.abs(prev.net)) rows[code] = Object.assign({}, rec, { why: [...whys].join('；') });
        else rows[code] = Object.assign({}, prev, { why: [...whys].join('；') });
      }
    }
    if (list.length < 500) break;
    await sleep(150);
  }
  return rows;
}

async function buildLhb(days) {
  let count = 0;
  for (const d of days) {
    try {
      const rows = await fetchLhb(d);
      if (Object.keys(rows).length) {
        await writeJson(path.join(OUT, 'lhb', `${d}.json`), { date: d, rows });
        count++;
      }
    } catch (e) { console.warn('[lhb]', d, e.message); }
    await sleep(120);
  }
  try {
    const keep = new Set(days.map((d) => `${d}.json`));
    for (const f of await fs.readdir(path.join(OUT, 'lhb'))) if (!keep.has(f)) await fs.rm(path.join(OUT, 'lhb', f), { force: true });
  } catch {}
  console.log(`[lhb] days=${count}`);
}

async function buildDaily(days) {
  const set = new Set(days);
  const map = new Map(days.map((d) => [d, {}]));
  let files = [];
  try { files = await fs.readdir(path.join(OUT, 'kline')); } catch { return; }
  await pool(files, 32, async (f) => {
    if (!f.endsWith('.json')) return;
    const d = await readJson(path.join(OUT, 'kline', f));
    if (!d?.bars) return;
    for (const b of d.bars) if (set.has(b[0])) map.get(b[0])[d.code] = [b[1], b[2], b[3], b[4], b[5], b[6], b[7] || 0];
  });
  for (const d of days) await writeJson(path.join(OUT, 'daily', `${d}.json`), { date: d, bars: map.get(d) });
  console.log(`[daily] ${days.length}`);
}

async function buildFlowDaily(days) {
  const set = new Set(days);
  const map = new Map(days.map((d) => [d, {}]));
  let files = [];
  try { files = await fs.readdir(path.join(OUT, 'flow')); } catch { return; }
  await pool(files, 32, async (f) => {
    if (!f.endsWith('.json')) return;
    const d = await readJson(path.join(OUT, 'flow', f));
    if (!d?.flow) return;
    for (const x of normalizeFlow(d.flow)) if (set.has(x.date)) map.get(x.date)[d.code] = [x.main, x.super, x.big, x.mid, x.small];
  });
  for (const d of days) await writeJson(path.join(OUT, 'flow_daily', `${d}.json`), { date: d, flow: map.get(d) });
  console.log(`[flow_daily] ${days.length}`);
}

main().catch((e) => { console.error(e); process.exit(1); });
