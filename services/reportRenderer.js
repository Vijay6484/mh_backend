const fs = require('fs');
const path = require('path');
const puppeteer = require('puppeteer');

function safe(val) {
    if (val === null || val === undefined) return '—';
    const s = String(val).trim();
    return s.length ? s : '—';
}

function escapeHtml(value) {
    return String(value)
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;')
        .replace(/"/g, '&quot;')
        .replace(/'/g, '&#39;');
}

function fileToDataUrl(filePath, mime) {
    const buf = fs.readFileSync(filePath);
    return `data:${mime};base64,${buf.toString('base64')}`;
}

function renderRecordTable(record, idx, ctx) {
    const rows = [
        ['Document Number', safe(record.document_number)],
        ['Document Type', safe(record.document_type)],
        ['Registration Office', safe(record.registration_office)],
        ['Date of Registration', safe(record.date)],
        ['Seller / Executor Party', safe(record.seller_party)],
        ['Buyer / Claimant Party', safe(record.buyer_party)],
        ['Property Description', safe(record.text)],
        ['District', safe(ctx.district)],
        ['Taluka', safe(ctx.taluka)],
        ['Village', safe(ctx.village)],
    ];

    const trs = rows.map(([k, v]) => `
        <tr>
            <td class="key">${escapeHtml(k)}</td>
            <td class="val">${escapeHtml(v)}</td>
        </tr>
    `).join('');

    return `
        <section class="record">
            <div class="record-header">Document Record #${idx + 1}</div>
            <table class="record-table">
                <tbody>${trs}</tbody>
            </table>
        </section>
    `;
}

function renderHtml({ records, ctx, logoDataUrl, fontDataUrl }) {
    const recordsHtml = records.map((r, i) => renderRecordTable(r, i, ctx)).join('\n');
    return `<!doctype html>
<html lang="mr">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Property Search Report</title>
  <style>
    @font-face {
      font-family: "MahaDevanagari";
      src: url("${fontDataUrl}") format("truetype");
      font-weight: 100 900;
      font-style: normal;
      font-display: block;
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      color: #1e293b;
      font-family: "MahaDevanagari", "Noto Serif Devanagari", "Noto Sans Devanagari", sans-serif;
      -webkit-font-smoothing: antialiased;
      text-rendering: optimizeLegibility;
      font-kerning: normal;
      font-variant-ligatures: normal;
      font-feature-settings: "liga" 1, "kern" 1;
    }
    .page {
      padding: 30px 28px 36px;
    }
    .top {
      display: flex;
      align-items: center;
      gap: 14px;
      min-height: 76px;
    }
    .logo {
      height: 72px;
      width: auto;
      object-fit: contain;
    }
    .title-wrap {
      flex: 1;
      text-align: center;
      padding-right: 40px;
    }
    .title {
      margin: 0;
      color: #1e3a8a;
      font-size: 28px;
      line-height: 1.2;
      font-weight: 800;
    }
    .subtitle {
      margin: 6px 0 0;
      color: #475569;
      font-size: 14px;
      font-weight: 700;
    }
    .divider {
      margin: 14px 0 12px;
      border-top: 3px solid #1e3a8a;
    }
    .ctxbox {
      background: #f8fafc;
      border: 1px solid #e2e8f0;
      border-radius: 8px;
      padding: 11px 14px;
      margin-bottom: 16px;
      font-size: 13px;
      color: #334155;
      line-height: 1.55;
    }
    .ctxgrid {
      display: grid;
      grid-template-columns: 1fr 1fr;
      column-gap: 16px;
      row-gap: 4px;
    }
    .ctxgrid b { color: #0f172a; font-weight: 800; }
    .record {
      margin: 0 0 18px;
      page-break-inside: avoid;
      break-inside: avoid;
    }
    .record-header {
      background: #1e3a8a;
      color: #fff;
      font-size: 14px;
      font-weight: 800;
      padding: 7px 11px;
      border-radius: 6px 6px 0 0;
    }
    .record-table {
      width: 100%;
      border-collapse: collapse;
      border: 1px solid #cbd5e1;
      border-top: none;
      table-layout: fixed;
      font-size: 12.8px;
    }
    .record-table td {
      border: 1px solid #cbd5e1;
      padding: 7px 9px;
      vertical-align: top;
      white-space: pre-wrap;
      word-break: break-word;
    }
    .record-table .key {
      width: 34%;
      background: #f1f5f9;
      color: #334155;
      font-weight: 800;
    }
    .record-table .val {
      color: #0f172a;
      font-weight: 500;
    }
    .footer {
      position: fixed;
      left: 28px;
      right: 28px;
      bottom: 14px;
      font-size: 10px;
      color: #64748b;
      border-top: 1px solid #e2e8f0;
      padding-top: 6px;
      display: flex;
      justify-content: space-between;
      gap: 12px;
    }
  </style>
</head>
<body>
  <main class="page">
    <header class="top">
      <img class="logo" src="${logoDataUrl}" alt="Mahasuchi logo" />
      <div class="title-wrap">
        <h1 class="title">Property Search Report</h1>
        <p class="subtitle">Official Search Report — Mahasuchi</p>
      </div>
    </header>
    <div class="divider"></div>
    <section class="ctxbox">
      <div class="ctxgrid">
        <div><b>District:</b> ${escapeHtml(safe(ctx.district))}</div>
        <div><b>Taluka:</b> ${escapeHtml(safe(ctx.taluka))}</div>
        <div><b>Village:</b> ${escapeHtml(safe(ctx.village))}</div>
        <div><b>Property No.:</b> ${escapeHtml(safe(ctx.query))}</div>
        <div><b>Generated On:</b> ${escapeHtml(new Date().toLocaleString('en-IN'))}</div>
        <div><b>Total Records:</b> ${records.length}</div>
      </div>
    </section>
    ${recordsHtml}
  </main>
  <footer class="footer">
    <span>This report is generated by Mahasuchi and is for informational reference only. © ${new Date().getFullYear()} Mahasuchi.</span>
    <span>Page <span class="pageNumber"></span> / <span class="totalPages"></span></span>
  </footer>
</body>
</html>`;
}

function resolveChromePath() {
    const fromEnv = process.env.PUPPETEER_EXECUTABLE_PATH || process.env.CHROME_EXECUTABLE_PATH || '';
    if (fromEnv && fs.existsSync(fromEnv)) return fromEnv;

    const candidates = [
        '/usr/bin/google-chrome',
        '/usr/bin/google-chrome-stable',
        '/usr/bin/chromium-browser',
        '/usr/bin/chromium',
    ];
    for (const p of candidates) {
        if (fs.existsSync(p)) return p;
    }
    return undefined;
}

async function generatePropertyReportPdf({ records, ctx, assetsDir, chromeExecutablePath }) {
    if (!Array.isArray(records) || records.length === 0) {
        throw new Error('Cannot render PDF: records are empty');
    }

    const assetsBase = assetsDir || path.join(__dirname, '..', 'assets');
    const logoPath = path.join(assetsBase, 'logo.jpeg');
    const fontPath = path.join(assetsBase, 'NotoSerifDevanagari-Regular.ttf');

    if (!fs.existsSync(logoPath)) throw new Error(`Logo asset missing: ${logoPath}`);
    if (!fs.existsSync(fontPath)) throw new Error(`Font asset missing: ${fontPath}`);

    const logoDataUrl = fileToDataUrl(logoPath, 'image/jpeg');
    const fontDataUrl = fileToDataUrl(fontPath, 'font/ttf');
    const html = renderHtml({ records, ctx, logoDataUrl, fontDataUrl });

    const launchOpts = {
        headless: true,
        args: ['--no-sandbox', '--disable-setuid-sandbox', '--font-render-hinting=none'],
    };
    const resolvedPath = chromeExecutablePath || resolveChromePath();
    if (resolvedPath) launchOpts.executablePath = resolvedPath;

    const browser = await puppeteer.launch(launchOpts);
    try {
        const page = await browser.newPage();
        await page.setContent(html, { waitUntil: 'networkidle0' });
        const pdfBuffer = await page.pdf({
            format: 'A4',
            printBackground: true,
            margin: { top: '0mm', right: '0mm', bottom: '0mm', left: '0mm' },
            displayHeaderFooter: false,
            preferCSSPageSize: true,
        });
        return Buffer.from(pdfBuffer);
    } finally {
        await browser.close();
    }
}

module.exports = {
    generatePropertyReportPdf,
};

