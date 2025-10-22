const fs = require('fs');
const readline = require('readline');

const INPUT = 'NASDAQ_Composite_Index_HistoricalData_1760648499379.csv';
const OUTPUT = 'nasdaq.csv';

function formatDate(mdy) {
  // "10/15/2025" → "2025-10-15"
  const [month, day, year] = mdy.split('/').map(s => s.trim());
  return `${year}-${month.padStart(2, '0')}-${day.padStart(2, '0')}`;
}

(async () => {
  const rl = readline.createInterface({
    input: fs.createReadStream(INPUT, { encoding: 'utf8' }),
    crlfDelay: Infinity,
  });

  let isHeader = true;
  const rows = [];

  for await (const raw of rl) {
    const line = raw.trim();
    if (!line) continue;

    if (isHeader) {
      isHeader = false;
      continue; // 첫 줄(헤더) 건너뜀
    }

    const [dateStr, close, open, high, low] = line.split(',');
    const date = formatDate(dateStr);
    rows.push({ date, close, open, high, low });
  }

  // 날짜 오름차순 정렬
  rows.sort((a, b) => (a.date < b.date ? -1 : 1));

  const out = fs.createWriteStream(OUTPUT, { encoding: 'utf8' });
  out.write('date,close,open,high,low\n');
  for (const r of rows) {
    out.write(`${r.date},${r.close},${r.open},${r.high},${r.low}\n`);
  }
  out.end(() => console.log(`✅ 완료: ${OUTPUT}`));
})();
