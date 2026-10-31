// 每天从东财抓取：日K（前复权，250根）+ 资金流（最多250天），写成分股票 JSON。
// 输出目录 out/{kline,flow,calendar.json,manifest.json}，由 workflow 推到 history 分支。
import fs from 'node:fs/promises';
import path from 'node:path';

const OUT = 'out';
const CONC = Number(process.env.CONC || 2);
const FULL = process.env.FULL === '1';
const UT = 'fa5fd1943c7b386f172d6893dbfba10b';
const OWNER_REPO = process.env.OWNER_REPO || 'pengxh7788/stock-data';
const DAILY_DAYS = 60;   // 日线分片保留天数（全市场筛选用）

const HEADERS = {
  'User-Agent': 'Mozilla/5.0 (Linux; Android 14) AppleWebKit/537.36',
  Referer: 'https://quote.eastmoney.com/',
};

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const secid = (code) => `${/^(sh|6)/.test(code) ? 1 : 0}.${code.replace(/^\D+/, '')}`;

async function getJson(url, retry = 3) {
  for (let i = 0; i <= retry; i++) {
    try {
      const res = await fetch(url, { headers: HEADERS, signal: AbortSignal.timeout(12000) });
      if (res.status === 429) throw new Error('HTTP 429');
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      return await res.json();
    } catch (err) {
      if (i === retry) throw err;
      await sleep(600 * (i + 1) + Math.random() * 400);
    }
  }
}

async function fetchKline(code, fromDate) {
  const beg = fromDate ? fromDate.replace(/-/g, '') : '20240101';
  const url =
    `https://push2his.eastmoney.com/api/qt/stock/kline/get?secid=${secid(code)}` +
    `&klt=101&fqt=1&beg=${beg}&end=20500101` +
    `&fields1=f1,f2,f3,f4,f5,f6&fields2=f51,f52,f53,f54,f55,f56,f57&ut=${UT}`;
  const data = await getJson(url);
  return (data?.data?.klines || [])
    .map((line) => {
      const p = String(line).split(',');
      return [p[0], +p[1], +p[2], +p[3], +p[4], +p[5], +p[6]];
    })
    .filter((b) => b[1] > 0 && b[2] > 0);
}

async function fetchFlow(code, lmt) {
  const url =
    `https://push2his.eastmoney.com/api/qt/stock/fflow/daykline/get?secid=${secid(code)}` +
    `&fields1=f1,f2,f3,f7&fields2=f51,f52,f53,f54,f55,f56&klt=101&lmt=${lmt}&ut=${UT}`;
  const data = await getJson(url);
  return (data?.data?.klines || [])
    .map((line) => {
      const p = String(line).split(',');
      // [日期, 主力, 小单, 中单, 大单, 超大单]
      return [p[0], +p[1], +p[5], +p[4], +p[3], +p[2]];
    })
    .filter((f) => /^\d{4}-\d{2}-\d{2}$/.test(f[0]));
}

function mergeByDate(list, keep) {
  const map = new Map();
  for (const item of list) map.set(item[0], item);
  return [...map.values()].sort((a, b) => a[0].localeCompare(b[0])).slice(-keep);
}

async function readJson(file) {
  try {
    return JSON.parse(await fs.readFile(file, 'utf8'));
  } catch {
    return null;
  }
}

async function writeJson(file, value) {
  await fs.mkdir(path.dirname(file), { recursive: true });
  await fs.writeFile(file, JSON.stringify(value));
}

async function pool(items, n, fn) {
  let cursor = 0;
  await Promise.all(
    Array.from({ length: n }, async () => {
      while (cursor < items.length) {
        const idx = cursor++;
        await fn(items[idx], idx);
        await sleep(80);
      }
    }),
  );
}

async function loadQuotes() {
  const bases = [
    `https://gh-proxy.com/https://raw.githubusercontent.com/${OWNER_REPO}/main/data/quotes.json`,
    `https://cdn.jsdelivr.net/gh/${OWNER_REPO}@main/data/quotes.json`,
    `https://raw.githubusercontent.com/${OWNER_REPO}/main/data/quotes.json`,
  ];
  for (const url of bases) {
    try {
      const data = await getJson(url, 1);
      if (data?.quotes && Object.keys(data.quotes).length) return data;
    } catch (err) {
      console.warn('quotes source failed:', url, err.message);
    }
  }
  throw new Error('无法读取 data/quotes.json');
}

async function buildCalendar() {
  const bars = await fetchKline('sh000001', null);
  const days = bars.map((b) => b[0]).slice(-500);
  return { tradingDays: days, lastTradingDate: days[days.length - 1] };
}

async function main() {
  const quotes = await loadQuotes();
  const codes = Object.keys(quotes.quotes).filter((c) => /^(sh|sz|bj)\d{6}$/.test(c));
  console.log(`[history] codes=${codes.length} full=${FULL}`);

  const missing = [];
  let klineCount = 0;
  let flowCount = 0;
  let done = 0;

  await pool(codes, CONC, async (code) => {
    const kFile = path.join(OUT, 'kline', `${code}.json`);
    const fFile = path.join(OUT, 'flow', `${code}.json`);
    const oldK = FULL ? null : await readJson(kFile);
    const oldF = FULL ? null : await readJson(fFile);

    const lastK = oldK?.bars?.length ? oldK.bars[oldK.bars.length - 1][0] : null;
    try {
      const bars = mergeByDate([...(oldK?.bars || []), ...(await fetchKline(code, lastK))], 250);
      if (bars.length) {
        await writeJson(kFile, { code, adj: 'qfq', updated: bars[bars.length - 1][0], bars });
        klineCount++;
      } else {
        missing.push(code);
      }
    } catch {
      missing.push(code);
    }

    try {
      const flow = mergeByDate(
        [...(oldF?.flow || []), ...(await fetchFlow(code, oldF ? 10 : 120))],
        250,
      );
      if (flow.length) {
        await writeJson(fFile, { code, updated: flow[flow.length - 1][0], flow });
        flowCount++;
      }
    } catch {
      /* 资金流失败不影响 K 线，下次再补 */
    }

    if (++done % 200 === 0) console.log(`[history] ${done}/${codes.length}`);
  });

  const calendar = await buildCalendar();
  await writeJson(path.join(OUT, 'calendar.json'), calendar);

  const dailyDays = calendar.tradingDays.slice(-DAILY_DAYS);
  await buildDailyShards(dailyDays);
  await writeJson(path.join(OUT, 'manifest.json'), {
    updatedAt: new Date().toISOString(),
    lastTradingDate: calendar.lastTradingDate,
    klineCount,
    flowCount,
    dailyDays: dailyDays.length,
    dailyFrom: dailyDays[0],
    dailyTo: dailyDays[dailyDays.length - 1],
    missing: missing.slice(0, 300),
  });

  console.log(
    `[history] done kline=${klineCount} flow=${flowCount} missing=${missing.length} lastTradingDate=${calendar.lastTradingDate}`,
  );
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});

// 把逐票 K 线反转成「按天」的全市场分片，供 APP 一次拉全市场做筛选。
async function buildDailyShards(days) {
  const daySet = new Set(days);
  const map = new Map(days.map((d) => [d, {}]));
  let files = [];
  try {
    files = await fs.readdir(path.join(OUT, 'kline'));
  } catch {
    return;
  }
  let done = 0;
  await pool(files, 24, async (f) => {
    if (!f.endsWith('.json')) return;
    const data = await readJson(path.join(OUT, 'kline', f));
    if (!data?.bars?.length) return;
    for (const b of data.bars) {
      if (!daySet.has(b[0])) continue;
      map.get(b[0])[data.code] = [b[1], b[2], b[3], b[4], b[5], b[6]];
    }
    if (++done % 1000 === 0) console.log(`[daily] ${done}/${files.length}`);
  });

  for (const d of days) {
    await writeJson(path.join(OUT, 'daily', `${d}.json`), { date: d, bars: map.get(d) });
  }

  // 清理超出窗口的旧分片
  const keep = new Set(days.map((d) => `${d}.json`));
  try {
    for (const f of await fs.readdir(path.join(OUT, 'daily'))) {
      if (!keep.has(f)) await fs.rm(path.join(OUT, 'daily', f), { force: true });
    }
  } catch {}
  console.log(`[daily] shards=${days.length} ${days[0]}..${days[days.length - 1]}`);
}
