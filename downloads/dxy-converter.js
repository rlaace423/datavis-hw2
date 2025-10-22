const fs = require('fs');
const readline = require('readline');

const INPUT = 'dxy_raw.csv';
const OUTPUT = 'dxy_daily.csv';

const MONTH = {
  Jan: '01', Feb: '02', Mar: '03', Apr: '04', May: '05', Jun: '06',
  Jul: '07', Aug: '08', Sep: '09', Oct: '10', Nov: '11', Dec: '12',
};

// "Oct 16, 2025",98.39  → { date: "2025-10-16", dxy: 98.39 }
function parseLine(line) {
  // 헤더 pass
  if (/^\s*date\s*,\s*dxy\s*$/i.test(line)) return null;

  // CSV(따옴표 포함) 전용 간단 파서
  const m = line.match(/^"([A-Za-z]{3})\s+(\d{1,2}),\s*(\d{4})"\s*,\s*([+-]?\d+(?:\.\d+)?|null)\s*$/);
  if (!m) return null;

  const [, monStr, ddStr, yyyyStr, dxyStr] = m;
  if (dxyStr === 'null') return null; // 결측치 스킵(원하면 keep해도 됨)

  const mm = MONTH[monStr];
  const dd = ddStr.padStart(2, '0');
  const date = `${yyyyStr}-${mm}-${dd}`;
  const dxy = Number(dxyStr);
  if (!Number.isFinite(dxy)) return null;

  return { date, dxy };
}

(async () => {
  const rl = readline.createInterface({
    input: fs.createReadStream(INPUT, { encoding: 'utf8' }),
    crlfDelay: Infinity,
  });

  const rows = [];
  for await (const raw of rl) {
    const line = raw.trim();
    if (!line) continue;
    const rec = parseLine(line);
    if (rec) rows.push(rec);
  }

  // 날짜 오름차순 정렬
  rows.sort((a, b) => (a.date < b.date ? -1 : a.date > b.date ? 1 : 0));

  // 쓰기
  const out = fs.createWriteStream(OUTPUT, { encoding: 'utf8' });
  out.write('date,dxy\n');
  for (const r of rows) out.write(`${r.date},${r.dxy}\n`);
  out.end(() => console.log(`✅ 완료: ${OUTPUT}`));
})();
