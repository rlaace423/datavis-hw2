const fs = require('fs');
const readline = require('readline');

const INPUT = 'btcusd_1-min_data.csv';
const OUTPUT = 'btc.csv';

// UTC 기준 YYYY-MM-DD
function toUtcDateKey(tsMs) {
  return new Date(tsMs).toISOString().slice(0, 10);
}

(async () => {
  const rl = readline.createInterface({
    input: fs.createReadStream(INPUT, { encoding: 'utf8' }),
    crlfDelay: Infinity,
  });

  // 날짜별 집계: { volumeSum, lastTsMs, closeUsd }
  const byDate = Object.create(null);

  let isHeader = true;
  let idx = {};
  for await (const raw of rl) {
    const line = raw.trim();
    if (!line) continue;

    if (isHeader) {
      const headers = line.split(',');
      ['Timestamp', 'Close', 'Volume'].forEach((h) => {
        idx[h] = headers.indexOf(h);
        if (idx[h] === -1) throw new Error(`헤더에 ${h} 컬럼이 없습니다.`);
      });
      isHeader = false;
      continue;
    }

    const cols = line.split(',');

    const tsSec = parseFloat(cols[idx.Timestamp]); // 예: 1325412060.0
    if (!Number.isFinite(tsSec)) continue;

    const close = parseFloat(cols[idx.Close]);
    const vol = parseFloat(cols[idx.Volume]);

    const tsMs = Math.round(tsSec * 1000);
    const dateKey = toUtcDateKey(tsMs);

    const bucket = (byDate[dateKey] ||= { volumeSum: 0, lastTsMs: -1, closeUsd: null });

    if (Number.isFinite(vol)) bucket.volumeSum += vol;

    // 해당 날짜에서 가장 늦은 시각의 Close를 종가로 사용
    if (Number.isFinite(close) && tsMs > bucket.lastTsMs) {
      bucket.lastTsMs = tsMs;
      bucket.closeUsd = close;
    }
  }

  const out = fs.createWriteStream(OUTPUT, { encoding: 'utf8' });
  out.write('date,close_usd,volume\n');
  for (const date of Object.keys(byDate).sort()) {
    const { closeUsd, volumeSum } = byDate[date];
    if (closeUsd == null) continue; // 데이터 없는 날 스킵
    out.write(`${date},${closeUsd},${volumeSum}\n`);
  }
  out.end(() => {
    console.log(`✅ 완성: ${OUTPUT} (UTC 일자 기준)`);
  });
})();
