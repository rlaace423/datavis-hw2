import { csvParse } from 'https://cdn.jsdelivr.net/npm/d3-dsv@3/+esm';

export const DATE_START = '2021-01-01';
export const DATE_END = '2023-12-31';
const ds = new Date(DATE_START);
const de = new Date(DATE_END);
const fixedDomain = [DATE_START, DATE_END];
const heatMapKeys = ['BTC', 'ETH', 'BNB', 'ADA', 'XRP'];

async function loadCsv(path) {
  const text = await fetch(path).then((r) => r.text());
  return csvParse(text);
}
function toNumber(x) {
  const v = +x;
  return Number.isFinite(v) ? v : null;
}
function byDateAsc(a, b) {
  return new Date(a.date) - new Date(b.date);
}
function inRangeISO(s) {
  const d = new Date(s);
  return d >= ds && d <= de;
}
function filterRange(rows, valueKeys) {
  return rows
    .map((r) => {
      const out = { date: r.date };
      for (const k of valueKeys) out[k] = toNumber(r[k]);
      return out;
    })
    .filter((r) => inRangeISO(r.date))
    .sort(byDateAsc);
}
function joinOnDate(baseArr, ...others) {
  const maps = others.map((arr) => new Map(arr.map((r) => [r.date, r])));
  return baseArr
    .filter((r) => maps.every((m) => m.has(r.date)))
    .map((r) => {
      const merged = { ...r };
      maps.forEach((m) => Object.assign(merged, m.get(r.date)));
      return merged;
    });
}
// 4
function returnsFromKey(rows, key) {
  const out = [];
  for (let i = 1; i < rows.length; i++) {
    const p = rows[i - 1][key],
      c = rows[i][key];
    if (p != null && c != null && p !== 0) out.push((c - p) / p);
  }
  return out;
}
function pearson(x, y) {
  const n = Math.min(x.length, y.length);
  const X = x.slice(0, n),
    Y = y.slice(0, n);
  const mx = X.reduce((s, v) => s + v, 0) / n,
    my = Y.reduce((s, v) => s + v, 0) / n;
  let num = 0,
    dx = 0,
    dy = 0;
  for (let i = 0; i < n; i++) {
    const a = X[i] - mx,
      b = Y[i] - my;
    num += a * b;
    dx += a * a;
    dy += b * b;
  }
  return num / Math.sqrt(dx * dy);
}
// 5
function firstNonNull(arr, key) {
  for (const r of arr) {
    const v = r[key];
    if (v != null && v !== 0) return v;
  }
  return null;
}
function normalizePanel(arr, key, base) {
  if (!base) return [];
  return arr.filter((r) => r[key] != null).map((r) => ({ date: r.date, key, value: r[key] / base }));
}

export async function loadAllData() {
  // btc: https://www.kaggle.com/datasets/mczielinski/bitcoin-historical-data
  // dxy: https://finance.yahoo.com/quote/DX-Y.NYB/history
  // eth, xrp: https://www.kaggle.com/datasets/svaningelgem/crypto-currencies-daily-prices
  // nasdaq: https://www.nasdaq.com/market-activity/index/comp/historical
  // gold: https://www.kaggle.com/datasets/sahilwagh/gold-stock-prices (GC:CMX)
  const [dxyRaw, btcRaw, ethRaw, bnbRaw, adaRaw, xrpRaw, nasdaqRaw, goldRaw] = await Promise.all([
    loadCsv('data/dxy.csv'),
    loadCsv('data/btc.csv'),
    loadCsv('data/eth.csv'),
    loadCsv('data/BNB.csv'),
    loadCsv('data/ADA.csv'),
    loadCsv('data/xrp.csv'),
    loadCsv('data/nasdaq.csv'),
    loadCsv('data/goldstock v1.csv'),
  ]);

  const dxy = filterRange(dxyRaw, ['dxy']);
  const btc = filterRange(btcRaw, ['close_usd', 'volume']).map((r) => ({
    date: r.date,
    close: r.close_usd,
    volume: r.volume,
  }));
  const eth = filterRange(ethRaw, ['close']).map((r) => ({ date: r.date, close: r.close }));
  const bnb = filterRange(bnbRaw, ['close']).map((r) => ({ date: r.date, close: r.close }));
  const ada = filterRange(adaRaw, ['close']).map((r) => ({ date: r.date, close: r.close }));
  const xrp = filterRange(xrpRaw, ['close']).map((r) => ({ date: r.date, close: r.close }));
  const nasdaq = filterRange(nasdaqRaw, ['close']).map((r) => ({ date: r.date, nasdaq: r.close }));
  const gold = filterRange(goldRaw, ['Close', 'Volume']).map((r) => ({ date: r.date, gold: r.Close, volume: r.Volume }));

  // 1. BTC vs DXY
  const q1 = joinOnDate(btc, dxy); // [{date, close, volume, dxy}]
  const seriesBtc = q1.map((r) => ({ date: r.date, key: 'BTC', value: r.close }));
  const seriesDxy = q1.map((r) => ({ date: r.date, key: 'DXY', value: r.dxy }));

  // 2. BTC vs DXY Scatter Plot
  const q2_points = q1.map((r) => ({
    date: r.date,
    dxy: r.dxy,
    btc_usd: r.close,
    year: new Date(r.date).getUTCFullYear(),
  }));

  // 3. BTC/ETH/XRP HeatMap
  const q3 = joinOnDate(
    btc.map(({ date, close }) => ({ date, BTC: close })),
    eth.map(({ date, close }) => ({ date, ETH: close })),
    bnb.map(({ date, close }) => ({ date, BNB: close })),
    ada.map(({ date, close }) => ({ date, ADA: close })),
    xrp.map(({ date, close }) => ({ date, XRP: close })),
  );

  const returns = {};
  for (const key of heatMapKeys) {
    returns[key] = returnsFromKey(q3, key);
  }
  const corrs = [];
  for (const row of heatMapKeys) {
    for (const col of heatMapKeys) {
      corrs.push({ row, col, value: row === col ? 1 : pearson(returns[row], returns[col]) });
    }
  }

  // 4. BTC/NASDAQ/GOLD 정규화
  const q4 = joinOnDate(
    btc.map(({ date, close }) => ({ date, btc: close })),
    nasdaq,
    gold,
  );

  const baseBTC = firstNonNull(q4, 'btc');
  const baseNAS = firstNonNull(q4, 'nasdaq');
  const baseGOLD = firstNonNull(q4, 'gold');

  const normalized = [
    ...normalizePanel(q4, 'btc', baseBTC),
    ...normalizePanel(q4, 'nasdaq', baseNAS),
    ...normalizePanel(q4, 'gold', baseGOLD),
  ];

  // 5. BTC 거래량/가격 변동률
  const btcVolAbs = [];
  for (let i = 1; i < btc.length; i++) {
    const prev = btc[i - 1],
      curr = btc[i];
    if (prev.close != null && curr.close != null && prev.close !== 0) {
      btcVolAbs.push({
        date: curr.date,
        volume: curr.volume,
        vol_abs: Math.abs((curr.close - prev.close) / prev.close),
      });
    }
  }

  // 6. Gold 거래량/가격 변동률
  const goldVolAbs = [];
  for (let i = 1; i < gold.length; i++) {
    const prev = gold[i - 1],
      curr = gold[i];
    if (prev.gold != null && curr.gold != null && prev.gold !== 0) {
      goldVolAbs.push({
        date: curr.date,
        volume: curr.volume,
        vol_abs: Math.abs((curr.gold - prev.gold) / prev.gold),
      });
    }
  }

  return {
    series: { btc: seriesBtc, dxy: seriesDxy },
    q2_points,
    corrs,
    normalized,
    btcVolAbs,
    goldVolAbs,
  };
}

export function buildBtcDxy(data) {
  const { btc, dxy } = data.series;
  return {
    $schema: 'https://vega.github.io/schema/vega-lite/v6.json',
    layer: [
      {
        data: { values: [{ x: '2022-01-01', x2: '2022-12-31' }] },
        mark: { type: 'rect', color: '#cccccc', opacity: 0.3 },
        encoding: {
          x: { field: 'x', type: 'temporal', scale: { domain: fixedDomain } },
          x2: { field: 'x2', type: 'temporal' },
        },
      },
      // 기존 BTC 라인
      {
        data: { values: btc },
        mark: 'line',
        encoding: {
          x: {
            field: 'date',
            type: 'temporal',
            title: '기간 (2021년 1월 ~ 2023년 12월)',
            scale: { domain: fixedDomain },
          },
          y: { field: 'value', type: 'quantitative', title: 'BTC (USD)' },
          color: { value: '#1f77b4' },
        },
      },
      // 기존 DXY 라인
      {
        data: { values: dxy },
        mark: 'line',
        encoding: {
          x: { field: 'date', type: 'temporal', scale: { domain: fixedDomain } },
          y: { field: 'value', type: 'quantitative', title: 'DXY (미국 달러 지수)' },
          color: { value: '#ff7f0e' },
        },
      },
    ],
    resolve: { scale: { y: 'independent' } },
    width: 1920,
    height: 600,
  };
}

export function buildBtcDxyScatter(data) {
  return {
    $schema: 'https://vega.github.io/schema/vega-lite/v6.json',
    width: 1000,
    height: 600,
    layer: [
      {
        data: { values: data.q2_points },
        mark: { type: 'circle', opacity: 0.75 },
        encoding: {
          x: {
            field: 'dxy',
            type: 'quantitative',
            title: 'DXY (미국 달러 지수)',
            scale: { domain: [80, 120], clamp: true },
          },
          y: { field: 'btc_usd', type: 'quantitative', title: 'BTC (USD)' },
          color: { field: 'year', type: 'nominal' },
          tooltip: [
            { field: 'date', type: 'temporal' },
            { field: 'dxy', type: 'quantitative' },
            { field: 'btc_usd', type: 'quantitative' },
          ],
        },
      },
      {
        data: { values: data.q2_points },
        transform: [{ regression: 'btc_usd', on: 'dxy' }],
        mark: { type: 'line' },
        encoding: {
          x: { field: 'dxy', type: 'quantitative' },
          y: { field: 'btc_usd', type: 'quantitative' },
          color: { value: 'black' },
        },
      },
    ],
  };
}

export function buildBtcEthXrpCorrHeatMap(data) {
  return {
    $schema: 'https://vega.github.io/schema/vega-lite/v6.json',
    data: { values: data.corrs },
    layer: [
      // heatmap
      {
        mark: 'rect',
        encoding: {
          x: { field: 'col', type: 'nominal', title: null, sort: heatMapKeys },
          y: { field: 'row', type: 'nominal', title: null, sort: heatMapKeys },
          color: { field: 'value', type: 'quantitative', scale: { domain: [0, 1] }, title: 'Pearson r' },
          tooltip: [
            { field: 'row', type: 'nominal' },
            { field: 'col', type: 'nominal' },
            { field: 'value', type: 'quantitative', format: '.2f' },
          ],
        },
      },
      // 셀 중앙에 숫자 레이블
      {
        mark: { type: 'text', fontSize: 12, fontWeight: 'bold' },
        encoding: {
          x: { field: 'col', type: 'nominal', sort: heatMapKeys },
          y: { field: 'row', type: 'nominal', sort: heatMapKeys },
          text: { field: 'value', type: 'quantitative', format: '.2f' },
          // 배경색과 대비 맞추기: 색이 진하면 흰 글씨, 아니면 검정 글씨
          color: {
            condition: { test: 'datum.value >= 0.7', value: 'white' },
            value: 'black'
          }
        }
      }
    ],
    width: 300,
    height: 300,
  };
}

export function buildBtcNasdaqGoldNormalized(data) {
  return {
    $schema: 'https://vega.github.io/schema/vega-lite/v6.json',
    width: 1920,
    height: 600,
    data: { values: data.normalized }, // [{date,key,value}]
    transform: [
      // 30일 이동 평균/표준편차 계산 (시리즈별)
      {
        window: [
          { op: 'mean', field: 'value', as: 'ma30' },
          { op: 'stdev', field: 'value', as: 'sd30' },
        ],
        frame: [-29, 0],
        groupby: ['key'],
        sort: [{ field: 'date', order: 'ascending' }],
      },
      { calculate: 'datum.ma30 - datum.sd30', as: 'lo' },
      { calculate: 'datum.ma30 + datum.sd30', as: 'hi' },
    ],
    layer: [
      // (A) 에러밴드(±1σ)
      {
        mark: { type: 'errorband', opacity: 0.18, clip: true },
        encoding: {
          x: {
            field: 'date',
            type: 'temporal',
            title: '기간 (2021년 1월 ~ 2023년 12월)',
            scale: { domain: [DATE_START, DATE_END] },
          },
          y: { field: 'lo', type: 'quantitative', title: '정규화한 가격 (baseline=1.0)' },
          y2: { field: 'hi' },
          color: { field: 'key', type: 'nominal', title: null },
        },
      },
      // (B) 기존 라인 (정규화 값)
      {
        mark: { type: 'line', clip: true },
        encoding: {
          x: { field: 'date', type: 'temporal', scale: { domain: [DATE_START, DATE_END] } },
          y: { field: 'value', type: 'quantitative', title: '정규화한 가격 (baseline=1.0)' },
          color: { field: 'key', type: 'nominal', title: null },
          tooltip: [
            { field: 'date', type: 'temporal' },
            { field: 'key', type: 'nominal' },
            { field: 'value', type: 'quantitative', format: '.2f' },
          ],
        },
      },
    ],
  };
}

// 5. BTC 거래량 vs 변동성
export function buildVolumePrice(volAbs, title, yAxisDomain) {
  return {
    $schema: 'https://vega.github.io/schema/vega-lite/v6.json',
    width: 1000,
    height: 600,
    title,
    data: { values: volAbs },
    transform: [
      { filter: 'isValid(datum.volume) && isValid(datum.vol_abs) && datum.volume > 0' },
      { calculate: 'log(datum.volume)', as: 'ln_volume' },
    ],
    layer: [
      {
        mark: { type: 'rect', opacity: 0.3, stroke: null },
        encoding: {
          x: {
            field: 'ln_volume',
            type: 'quantitative',
            bin: { maxbins: 35 },
            title: `거래량 (${title})`,
            axis: { labelExpr: 'format(exp(datum.value), ".2s")' },
          },
          y: {
            field: 'vol_abs',
            type: 'quantitative',
            bin: { maxbins: 35 },
            title: '가격 변동률 ( |ΔP/P| )',
            scale: { domain: yAxisDomain },
          },
          color: {
            aggregate: 'count',
            type: 'quantitative',
            title: '밀집도',
            scale: { type: 'sqrt', scheme: 'viridis' },
          },
          tooltip: [{ aggregate: 'count', type: 'quantitative', title: 'Count' }],
        },
      },

      // (B) 산점도 — 살짝 투명하게
      {
        mark: { type: 'circle', opacity: 0.35 },
        encoding: {
          x: {
            field: 'ln_volume',
            type: 'quantitative',
            title: `거래량 (${title})`,
            axis: { labelExpr: 'format(exp(datum.value), ".2s")' },
          },
          y: {
            field: 'vol_abs',
            type: 'quantitative',
            title: '가격 변동률 ( |ΔP/P| )',
            scale: { domain: yAxisDomain },
          },
          tooltip: [
            { field: 'date', type: 'temporal', title: 'Date' },
            { field: 'volume', type: 'quantitative', title: 'Volume' },
            { field: 'vol_abs', type: 'quantitative', format: '.3f', title: '가격 변동률 ( |ΔP/P| )' },
          ],
        },
      },

      // (C) 회귀선 — 같은 축 범위로 고정
      {
        transform: [{ regression: 'vol_abs', on: 'ln_volume' }],
        mark: { type: 'line', color: 'black', clip: true },
        encoding: {
          x: {
            field: 'ln_volume',
            type: 'quantitative',
            title: `거래량 (${title})`,
            axis: { labelExpr: 'format(exp(datum.value), ".2s")' },
          },
          y: { field: 'vol_abs', type: 'quantitative', scale: { domain: yAxisDomain } },
        },
      },
    ],
  };
}

function parseEuroValue(str) {
  if (typeof str !== "string") return null;

  // "€1.31bn", "€920.60m" 같은 문자열 처리
  let s = str.trim().toLowerCase();

  // 화폐 기호, 콤마, 공백 제거
  s = s.replace(/€/g, "").replace(/,/g, "").replace(/\s+/g, "");

  let multiplier = 1;

  if (s.endsWith("bn")) {
    multiplier = 1e9;
    s = s.slice(0, -2); // 'bn' 제거
  } else if (s.endsWith("m")) {
    multiplier = 1e6;
    s = s.slice(0, -1); // 'm' 제거
  }

  const value = parseFloat(s);
  if (Number.isNaN(value)) return null;

  return value * multiplier; // 유로 단위
}

export async function loadClubValueData() {
  const raw = await loadCsv('data/club_values.csv');

  // Premier League만 사용 + value 파싱
  const rows = raw
    // .filter((r) => r.league_name === 'Premier League' && r.value)
    .map((r) => {
      const valueEur = parseEuroValue(r.value);
      if (valueEur == null) return null;

      return {
        // Vega-Lite가 자동으로 temporal로 파싱할 수 있도록 ISO 문자열 그대로 사용
        date: r.date,                 // "2025-01-01"
        club_name: r.club_name,       // "Arsenal FC"
        value_eur: valueEur,          // 숫자(유로)
        value_eur_million: valueEur / 1e6, // 보기 좋게 백만 단위로 변환
      };
    })
    .filter(Boolean);

  return rows;
}

export function buildClubValueLineSpec(clubValues, clubs, colorRange) {
  return {
    width: 900,
    height: 500,
    data: {
      values: clubValues,
    },
    params: [
      {
        name: 'clubHighlight',
        select: { type: 'point', fields: ['club_name'], bind: 'legend' },
      },
    ],
    mark: {
      type: 'line',
      point: true,
      tooltip: true,
    },
    encoding: {
      x: {
        field: 'date',
        type: 'temporal',
        title: 'Date',
      },
      y: {
        field: 'value_eur_million',
        type: 'quantitative',
        title: 'Club Market Value (million €)',
      },
      color: {
        field: 'club_name',
        type: 'nominal',
        title: 'Club',
        legend: { title: 'Club (click to highlight)' },
        // 🔥 여기서 도메인/레인지 고정
        scale: {
          domain: clubs,
          range: colorRange,
        },
      },
      opacity: {
        condition: { param: 'clubHighlight', value: 1 },
        value: 0.25,
      },
    },
  };
}
