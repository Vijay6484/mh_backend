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
 *   --assetsDir "/abs/path/to/backend/assets"
 *   --chromePath "/usr/bin/google-chrome"
 */

require('dotenv').config();
const fs = require('fs');
const path = require('path');
const reportData = require('../services/reportData');
const { generatePropertyReportPdf } = require('../services/reportRenderer');

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

async function main() {
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

    const indexDir = args.indexDir
        ? path.resolve(String(args.indexDir))
        : (process.env.INDEX_DIR ? path.resolve(process.env.INDEX_DIR) : path.join(__dirname, '..', 'indexed_data'));
    try {
        const loaded = await reportData.loadRecordsByContext({
            indexDir, district, taluka, village, query
        });
        const unique = reportData.dedupeByKey(loaded.matched);
        if (!unique.length) {
            console.error('No matching records found for this query.');
            process.exit(1);
        }
        const assetsDir = path.resolve(String(args.assetsDir || path.join(__dirname, '..', 'assets')));
        const chromeExecutablePath = args.chromePath ? path.resolve(String(args.chromePath)) : undefined;
        const pdfBuf = await generatePropertyReportPdf({
            records: unique,
            ctx: { district, taluka, village, query: loaded.queryNumber },
            assetsDir,
            chromeExecutablePath,
        });
        const defaultOut = path.resolve(process.cwd(), `Mahasuchi_Report_${loaded.queryNumber}.pdf`);
        const outPath = path.resolve(process.cwd(), String(out || defaultOut));
        fs.writeFileSync(outPath, Buffer.from(pdfBuf));
        console.log(`✔ PDF generated: ${outPath}`);
        console.log(`✔ Records: ${unique.length} (deduped by SHA key)`);
    } catch (e) {
        console.error(`Failed to generate report: ${e.message}`);
        process.exit(1);
    }
}

main().catch((e) => {
    console.error(`Failed to generate report: ${e.message}`);
    process.exit(1);
});

