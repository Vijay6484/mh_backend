#!/usr/bin/env node
/**
 * Generate the same "Property Search Report" PDF server-side (for support re-sends).
 *
 * Usage (run from repo root or anywhere):
 *   node Backend/tools/generate_report_pdf.js --district "पुणे" --taluka "मावळ" --village "इंदुरी" --query "572"
 *
 * Optional:
 *   --out "./Mahasuchi_Report_572.pdf"
 *   --indexDir "/abs/path/to/indexed_data"
 *   --logo "/abs/path/to/logo.jpeg"
 *   --font "/abs/path/to/NotoSansDevanagari-Regular.ttf"
 */

require('dotenv').config();
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const { jsPDF } = require('jspdf');
const autoTable = require('jspdf-autotable');

function parseArgs(argv) {
    const args = {};
    for (let i = 2; i < argv.length; i++) {
        const a = argv[i];
        if (!a.startsWith('--')) continue;
        const key = a.slice(2);
        const val = (i + 1 < argv.length && !argv[i + 1].startsWith('--')) ? argv[++i] : true;
        args[key] = val;
    }
    return args;
}

function normalizeFsKey(s) {
    return String(s ?? '').trim().normalize('NFC');
}

function resolveDirEntry(parentDir, requestedName) {
    const req = normalizeFsKey(requestedName);
    const exact = path.join(parentDir, requestedName);
    if (fs.existsSync(exact)) return requestedName;
    if (!fs.existsSync(parentDir)) return null;
    try {
        const entries = fs.readdirSync(parentDir, { withFileTypes: true })
            .filter(d => d.isDirectory() && !d.name.startsWith('.'))
            .map(d => d.name);
        const match = entries.find(name => normalizeFsKey(name) === req);
        return match || null;
    } catch {
        return null;
    }
}

// Must match Backend/server.js fingerprint logic
const IDENTITY_FIELDS = [
    'document_number',
    'document_type',
    'registration_office',
    'date',
    'seller_party',
    'buyer_party',
    'text',
    'property_numbers',
    'pdf_link',
];

const canonicalize = (value) => {
    if (value === null || value === undefined) return 'null';
    const t = typeof value;
    if (t === 'string') return JSON.stringify(String(value).trim().normalize('NFC'));
    if (t === 'number' || t === 'boolean') return JSON.stringify(value);
    if (Array.isArray(value)) {
        const items = value.map(canonicalize).sort();
        return `[${items.join(',')}]`;
    }
    if (t === 'object') {
        const keys = Object.keys(value).sort();
        return `{${keys.map(k => `${JSON.stringify(k)}:${canonicalize(value[k])}`).join(',')}}`;
    }
    return 'null';
};

const fingerprint = (record) => {
    const parts = IDENTITY_FIELDS.map(k => canonicalize(record ? record[k] : undefined));
    return crypto.createHash('sha256').update(parts.join('|')).digest('base64url');
};

function safe(val) {
    if (val === null || val === undefined) return '—';
    const s = String(val).trim();
    return s.length ? s : '—';
}

function toDataUrl(filePath) {
    const buf = fs.readFileSync(filePath);
    const ext = path.extname(filePath).toLowerCase();
    const mime = ext === '.png' ? 'image/png' : 'image/jpeg';
    return `data:${mime};base64,${buf.toString('base64')}`;
}

function ensureDevanagariFont(doc, fontTtfPath) {
    const fontDataB64 = fs.readFileSync(fontTtfPath).toString('base64');
    doc.addFileToVFS('NotoSansDevanagari-Regular.ttf', fontDataB64);
    doc.addFont('NotoSansDevanagari-Regular.ttf', 'NotoSansDevanagari', 'normal');
    // Bold mapped to same font file (works fine for our layout)
    doc.addFont('NotoSansDevanagari-Regular.ttf', 'NotoSansDevanagari', 'bold');
}

function buildPdf(records, ctx, opts) {
    const { district: d, taluka: t, village: v, query: q } = ctx;

    const doc = new jsPDF({ unit: 'mm', format: 'a4', orientation: 'portrait' });
    const PRIMARY = [30, 58, 138]; // #1e3a8a
    const SLATE_700 = [51, 65, 85];
    const SLATE_800 = [30, 41, 59];
    const SLATE_500 = [100, 116, 139];
    const BG_SOFT = [248, 250, 252];
    const BORDER = [226, 232, 240];

    // Embed Devanagari font (Marathi)
    ensureDevanagariFont(doc, opts.fontPath);
    const setPdfFont = (style) => doc.setFont('NotoSansDevanagari', style);

    // Logo
    try {
        const logoDataUrl = toDataUrl(opts.logoPath);
        // Keep aspect ratio; height fixed 28mm; width computed and capped
        const logoH = 28;
        let logoW = 40;
        try {
            const imgProps = doc.getImageProperties(logoDataUrl);
            const ratio = imgProps && imgProps.height ? (imgProps.width / imgProps.height) : 1;
            logoW = Math.min(52, Math.max(28, logoH * ratio));
        } catch { /* ignore */ }
        doc.addImage(logoDataUrl, 'JPEG', 14, 8, logoW, logoH);
    } catch { /* proceed without logo */ }

    // Header (centered + bold)
    doc.setTextColor(...PRIMARY);
    setPdfFont('bold');
    doc.setFontSize(18);
    doc.text('Property Search Report', 105, 18, { align: 'center' });
    doc.setFontSize(10.5);
    doc.setTextColor(...SLATE_500);
    doc.text('Official Search Report — Mahasuchi', 105, 24, { align: 'center' });

    // Divider
    doc.setDrawColor(...PRIMARY);
    doc.setLineWidth(1.2);
    doc.line(14, 40, 196, 40);

    // Context box
    doc.setFillColor(...BG_SOFT);
    doc.setDrawColor(...BORDER);
    doc.setLineWidth(0.3);
    doc.roundedRect(14, 44, 182, 22, 2.5, 2.5, 'FD');

    doc.setTextColor(...SLATE_800);
    doc.setFontSize(10);
    setPdfFont('bold');
    doc.text('District:', 18, 51);
    doc.text('Taluka:', 110, 51);
    doc.text('Village:', 18, 57);
    doc.text('Property No.:', 110, 57);

    setPdfFont('normal');
    doc.text(safe(d), 38, 51);
    doc.text(safe(t), 125, 51);
    doc.text(safe(v), 38, 57);
    doc.text(safe(q), 137, 57);

    doc.setTextColor(...SLATE_500);
    doc.setFontSize(9.5);
    doc.text(`Generated On: ${new Date().toLocaleString('en-IN')}`, 18, 63);
    doc.text(`Total Records: ${records.length}`, 150, 63);

    let cursorY = 74;
    for (let i = 0; i < records.length; i++) {
        const r = records[i] || {};

        // Section header bar
        doc.setFillColor(...PRIMARY);
        doc.setDrawColor(...PRIMARY);
        doc.roundedRect(14, cursorY - 6, 182, 8, 2, 2, 'F');
        setPdfFont('bold');
        doc.setTextColor(255, 255, 255);
        doc.setFontSize(11);
        doc.text(`Document Record #${i + 1}`, 18, cursorY - 1);

        doc.setTextColor(...SLATE_800);
        doc.setFontSize(10);
        cursorY += 6;

        const rows = [
            ['Document Type', safe(r.document_type)],
            ['Registration Office', safe(r.registration_office)],
            ['Date of Registration', safe(r.date)],
            ['Seller / Executor Party', safe(r.seller_party)],
            ['Buyer / Claimant Party', safe(r.buyer_party)],
            ['Property Description', safe(r.text)],
            ['District', safe(d)],
            ['Taluka', safe(t)],
            ['Village', safe(v)],
        ];

        autoTable(doc, {
            startY: cursorY,
            body: rows,
            theme: 'grid',
            styles: {
                font: 'NotoSansDevanagari',
                fontSize: 9.2,
                cellPadding: 2.2,
                overflow: 'linebreak',
                lineColor: BORDER,
                textColor: SLATE_800
            },
            tableLineColor: BORDER,
            tableLineWidth: 0.3,
            columnStyles: {
                0: { cellWidth: 55, fontStyle: 'bold', textColor: SLATE_700, fillColor: [241, 245, 249] },
                1: { cellWidth: 127, textColor: SLATE_800 },
            },
            margin: { left: 14, right: 14 },
        });

        const last = doc.lastAutoTable;
        cursorY = (last && last.finalY ? last.finalY : cursorY) + 10;
        if (cursorY > 270 && i < records.length - 1) {
            doc.addPage();
            cursorY = 24;
        }
    }

    // Footer
    const pageCount = doc.getNumberOfPages();
    for (let p = 1; p <= pageCount; p++) {
        doc.setPage(p);
        doc.setDrawColor(...BORDER);
        doc.setLineWidth(0.4);
        doc.line(14, 286, 196, 286);
        doc.setFontSize(9);
        doc.setTextColor(...SLATE_500);
        setPdfFont('normal');
        doc.text(
            `This report is generated by Mahasuchi and is for informational reference only. © ${new Date().getFullYear()} Mahasuchi.`,
            105,
            292,
            { align: 'center' }
        );
        doc.text(`Page ${p} of ${pageCount}`, 196, 292, { align: 'right' });
    }

    return doc.output('arraybuffer');
}

function main() {
    const args = parseArgs(process.argv);
    const district = args.district;
    const taluka = args.taluka;
    const village = args.village;
    const query = args.query;
    const out = args.out;

    if (!district || !taluka || !village || !query) {
        console.error('Missing required args.\n\nExample:\n  node Backend/tools/generate_report_pdf.js --district "पुणे" --taluka "मावळ" --village "इंदुरी" --query "572"\n');
        process.exit(2);
    }

    const INDEX_DIR = args.indexDir
        ? path.resolve(String(args.indexDir))
        : (process.env.INDEX_DIR ? path.resolve(process.env.INDEX_DIR) : path.join(__dirname, '..', 'indexed_data'));

    // Default to backend-local assets (so it works on VPS even without Frontend folder)
    const defaultLogo = path.join(__dirname, '..', 'assets', 'logo.jpeg');
    const defaultFont = path.join(__dirname, '..', 'assets', 'NotoSansDevanagari-Regular.ttf');

    const logoPath = path.resolve(String(args.logo || defaultLogo));
    const fontPath = path.resolve(String(args.font || defaultFont));

    if (!fs.existsSync(INDEX_DIR)) {
        console.error(`Index directory not found: ${INDEX_DIR}`);
        process.exit(1);
    }
    if (!fs.existsSync(logoPath)) {
        console.error(`Logo not found: ${logoPath}`);
        process.exit(1);
    }
    if (!fs.existsSync(fontPath)) {
        console.error(`Font not found: ${fontPath}`);
        process.exit(1);
    }

    const resolvedDistrict = resolveDirEntry(INDEX_DIR, district);
    if (!resolvedDistrict) {
        console.error(`District not found in index: ${district}`);
        process.exit(1);
    }
    const districtDir = path.join(INDEX_DIR, resolvedDistrict);

    const resolvedTaluka = resolveDirEntry(districtDir, taluka);
    if (!resolvedTaluka) {
        console.error(`Taluka not found in index: ${taluka}`);
        process.exit(1);
    }
    const talukaDir = path.join(districtDir, resolvedTaluka);

    const resolvedVillage = resolveDirEntry(talukaDir, village);
    if (!resolvedVillage) {
        console.error(`Village not found in index: ${village}`);
        process.exit(1);
    }

    const filePath = path.join(talukaDir, resolvedVillage, 'data.json');
    if (!fs.existsSync(filePath)) {
        console.error(`Data file not found: ${filePath}`);
        process.exit(1);
    }

    const queryMatch = String(query).trim().match(/\d+/);
    if (!queryMatch) {
        console.error('Query must contain a number.');
        process.exit(1);
    }
    const queryNumber = queryMatch[0];

    let records;
    try {
        records = JSON.parse(fs.readFileSync(filePath, 'utf8'));
    } catch (e) {
        console.error(`Failed to read/parse JSON: ${filePath}\n${e.message}`);
        process.exit(1);
    }

    // Match same as API: base number of any property_numbers entry
    const matched = [];
    for (const record of records) {
        if (!record || !Array.isArray(record.property_numbers)) continue;
        let isMatch = false;
        for (const prop of record.property_numbers) {
            const baseNumber = String(prop.value).split(/[\/\-]/)[0].trim();
            if (baseNumber === String(queryNumber)) { isMatch = true; break; }
        }
        if (isMatch) matched.push(record);
    }

    // Attach keys + dedupe (same as frontend & API expectation)
    const seen = new Set();
    const unique = [];
    for (const r of matched) {
        const k = fingerprint(r);
        const rec = { ...r, key: k };
        if (seen.has(k)) continue;
        seen.add(k);
        unique.push(rec);
    }

    if (unique.length === 0) {
        console.error('No matching records found for this query.');
        process.exit(1);
    }

    const ctx = { district, taluka, village, query: queryNumber };
    const pdfBuf = buildPdf(unique, ctx, { logoPath, fontPath });

    const defaultOut = path.resolve(process.cwd(), `Mahasuchi_Report_${queryNumber}.pdf`);
    const outPath = path.resolve(process.cwd(), String(out || defaultOut));
    fs.writeFileSync(outPath, Buffer.from(pdfBuf));
    console.log(`✔ PDF generated: ${outPath}`);
    console.log(`✔ Records: ${unique.length} (deduped by SHA key)`);
}

main();

