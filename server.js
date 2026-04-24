require('dotenv').config();
const express = require('express');
const cors = require('cors');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const { PDFDocument } = require('pdf-lib');
const { HttpsProxyAgent } = require('https-proxy-agent');
const mongoose = require('mongoose');
const axios = require('axios');
const reportData = require('./services/reportData');
const { generatePropertyReportPdf } = require('./services/reportRenderer');
const { getSmtpConfigFromEnv, validateSmtpConfig, sendReportEmail } = require('./services/mailer');
const { forEachRecordInDataJson } = require('./services/streamDataJson');

const app = express();
app.use(cors({ origin: '*' }));
app.use(express.json());
// PayU posts back as form-urlencoded
app.use(express.urlencoded({ extended: true }));

const INDEX_DIR = process.env.INDEX_DIR
    ? path.resolve(process.env.INDEX_DIR)
    : path.join(__dirname, 'indexed_data');

// locations_index.json: built by `node tools/build_locations_index.js` from locations.json
const LOCATIONS_INDEX_PATH = path.join(__dirname, 'locations_index.json');
let locationsIndexCache = null;

function loadLocationsIndex() {
    if (locationsIndexCache) return locationsIndexCache;
    if (!fs.existsSync(LOCATIONS_INDEX_PATH)) {
        locationsIndexCache = null;
        return null;
    }
    try {
        const data = JSON.parse(fs.readFileSync(LOCATIONS_INDEX_PATH, 'utf8'));
        locationsIndexCache = data;
        return data;
    } catch (e) {
        console.error('[locations] Failed to read locations_index.json', e && e.message);
        locationsIndexCache = null;
        return null;
    }
}

/**
 * Resolve numeric ids (d, t, v) to on-disk Marathi folder names.
 * Ids are stable per `locations_index.json` build order.
 */
function resolveMrFromIds(dStr, tStr, vStr) {
    const idx = loadLocationsIndex();
    if (!idx || !Array.isArray(idx.districts)) return null;
    const d = parseInt(String(dStr), 10);
    const t = parseInt(String(tStr), 10);
    const v = parseInt(String(vStr), 10);
    if (Number.isNaN(d) || Number.isNaN(t) || Number.isNaN(v)) return null;
    const dist = idx.districts.find(x => x.id === d);
    if (!dist) return null;
    const tal = (dist.talukas || []).find(x => x.id === t);
    if (!tal) return null;
    const vill = (tal.villages || []).find(x => x.id === v);
    if (!vill) return null;
    return { district: dist.mr, taluka: tal.mr, village: vill.mr };
}

function resolveMrFromIdsFixed(d, t, v) {
    const o = resolveMrFromIds(d, t, v);
    if (!o || !o.district || !o.taluka || !o.village) return null;
    return o;
}

/**
 * Get Marathi { district, taluka, village } from query/body: prefer ids (d,t,v) or legacy strings.
 */
function getMrLocationFromParams(params) {
    const d = params.d ?? params.districtId;
    const t = params.t ?? params.talukaId;
    const v = params.v ?? params.villageId;
    if (d != null && d !== '' && t != null && t !== '' && v != null && v !== '') {
        const o = resolveMrFromIdsFixed(d, t, v);
        if (o) return o;
    }
    const { district, taluka, village } = params;
    if (district && taluka && village) {
        return { district: String(district), taluka: String(taluka), village: String(village) };
    }
    return null;
}

// Short-lived tokens issued after successful PayU verification.
// Stored in-memory (process-local). If the server restarts, tokens are cleared.
const PAYMENT_TOKENS = new Map(); // token -> { txnid, exp }
const PAYMENT_TOKEN_TTL_MS = 10 * 60 * 1000; // 10 minutes

function issuePaymentToken(txnid) {
    const token = crypto.randomBytes(24).toString('base64url');
    const exp = Date.now() + PAYMENT_TOKEN_TTL_MS;
    PAYMENT_TOKENS.set(token, { txnid, exp });
    return { token, exp };
}

function normalizeFsKey(s) {
    // Make user input and on-disk names comparable across platforms.
    // - trim to avoid accidental spaces
    // - NFC to reduce Unicode normalization mismatches (common with Indic scripts)
    return String(s ?? '')
        .trim()
        .normalize('NFC')
        .replace(/[()]/g, '')
        .replace(/\s+/g, ' ')
        .trim();
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

// ─── PayU Config (from .env) ──────────────────────────────────────────────────
const PAYU_KEY     = process.env.PAYU_MERCHANT_KEY;
const PAYU_SALT    = process.env.PAYU_MERCHANT_SALT;
const PAYU_URL     = process.env.PAYU_BASE_URL || 'https://test.payu.in/_payment';
const FRONTEND_URL = process.env.FRONTEND_URL  || 'http://localhost:3000';

// ─── Proxy Config ─────────────────────────────────────────────────────────────
const PROXY_URL  = process.env.PROXY_URL || 'http://geonode_fLbRzuEUB8-type-residential-country-in:8f20b050-ac4e-474f-a486-8d33b366dfce@proxy.geonode.io:9000';
const proxyAgent = new HttpsProxyAgent(PROXY_URL);

// ─── MongoDB Connection ───────────────────────────────────────────────────────
const MONGODB_URI = process.env.MONGODB_URI || 'mongodb://localhost:27017/mahasuchi';
mongoose.connect(MONGODB_URI)
    .then(() => console.log('Connected to MongoDB: Mahasuchi Database'))
    .catch(err => console.error('MongoDB connection error:', err));

const LeadSchema = new mongoose.Schema({
    phone: String,
    email: String,
    district: String,
    taluka: String,
    village: String,
    query: String,
    status: { type: String, enum: ['pending', 'paid', 'failed'], default: 'pending' },
    txnid: { type: String, unique: true },
    reportEmailSentAt: { type: Date, default: null },
    reportEmailStatus: { type: String, enum: ['pending', 'sent', 'failed'], default: 'pending' },
    reportEmailError: { type: String, default: '' },
    createdAt: { type: Date, default: Date.now }
});

const Lead = mongoose.model('Lead', LeadSchema);

// ─── SMTP config visibility ────────────────────────────────────────────────────
const smtpCfg = getSmtpConfigFromEnv();
const smtpValidation = validateSmtpConfig(smtpCfg);
if (!smtpValidation.ok) {
    console.warn(`[email] SMTP disabled. Missing vars: ${smtpValidation.missing.join(', ')}`);
} else {
    console.log(`[email] SMTP configured (${smtpCfg.host}:${smtpCfg.port}, secure=${smtpCfg.secure})`);
}

async function getFullRecordsByKeys({ district, taluka, village, query, keys }) {
    const loaded = await reportData.loadRecordsByContext({
        indexDir: INDEX_DIR, district, taluka, village, query
    });
    const keySet = new Set((keys || []).filter(Boolean));
    const filtered = loaded.matched.filter(r => keySet.has(r.key));
    const deduped = reportData.dedupeByKey(filtered);
    return { queryNumber: loaded.queryNumber, records: deduped };
}

async function generatePdfForContext({ district, taluka, village, query, records }) {
    const deduped = reportData.dedupeByKey(records || []);
    if (!deduped.length) {
        throw new Error('No records available for report generation');
    }
    const queryMatch = String(query).trim().match(/\d+/);
    const queryNumber = queryMatch ? queryMatch[0] : String(query);
    const pdf = await generatePropertyReportPdf({
        records: deduped,
        ctx: { district, taluka, village, query: queryNumber },
        assetsDir: path.join(__dirname, 'assets'),
    });
    const filename = `Mahasuchi_Report_${queryNumber}.pdf`;
    return { pdf, filename, queryNumber, deduped };
}

async function tryAutoEmailReport(txnid) {
    try {
        const lead = await Lead.findOne({ txnid }).lean();
        if (!lead) return { sent: false, skipped: true, reason: 'Lead not found' };
        if (lead.reportEmailSentAt || lead.reportEmailStatus === 'sent') {
            return { sent: false, skipped: true, reason: 'Already emailed' };
        }
        if (!lead.email) {
            await Lead.findOneAndUpdate({ txnid }, {
                reportEmailStatus: 'failed',
                reportEmailError: 'Lead email missing'
            }).exec();
            return { sent: false, skipped: true, reason: 'Lead email missing' };
        }

        const loaded = await reportData.loadRecordsByContext({
            indexDir: INDEX_DIR,
            district: lead.district,
            taluka: lead.taluka,
            village: lead.village,
            query: lead.query,
        });
        const deduped = reportData.dedupeByKey(loaded.matched);
        if (!deduped.length) {
            await Lead.findOneAndUpdate({ txnid }, {
                reportEmailStatus: 'failed',
                reportEmailError: 'No records found for paid search context'
            }).exec();
            return { sent: false, skipped: true, reason: 'No matching records' };
        }

        const { pdf, filename } = await generatePdfForContext({
            district: lead.district,
            taluka: lead.taluka,
            village: lead.village,
            query: lead.query,
            records: deduped,
        });

        const emailRes = await sendReportEmail({
            to: lead.email,
            pdfBuffer: pdf,
            filename,
            ctx: { district: lead.district, taluka: lead.taluka, village: lead.village, query: lead.query }
        });

        if (emailRes.sent) {
            await Lead.findOneAndUpdate({ txnid }, {
                reportEmailSentAt: new Date(),
                reportEmailStatus: 'sent',
                reportEmailError: ''
            }).exec();
            return { sent: true };
        }

        await Lead.findOneAndUpdate({ txnid }, {
            reportEmailStatus: 'failed',
            reportEmailError: emailRes.reason || 'SMTP not configured'
        }).exec();
        return { sent: false, skipped: true, reason: emailRes.reason || 'SMTP disabled' };
    } catch (err) {
        console.error('[email] Auto report email failed', txnid, err && err.message);
        await Lead.findOneAndUpdate({ txnid }, {
            reportEmailStatus: 'failed',
            reportEmailError: err && err.message ? String(err.message).slice(0, 300) : 'Unknown email error'
        }).exec().catch(() => {});
        return { sent: false, skipped: true, reason: 'Exception' };
    }
}

// ─── Helpers ──────────────────────────────────────────────────────────────────
const DAILY_LIMIT_STRINGS = [
    'daily limit', 'limit exceeded', 'usage limit',
    'quota exceeded', 'too many', 'प्रतिदिन मर्यादा'
];

function isDailyLimitHit(bufferOrString) {
    try {
        const text = (Buffer.isBuffer(bufferOrString)
            ? bufferOrString.slice(0, 3000).toString('utf8')
            : String(bufferOrString).slice(0, 3000)
        ).toLowerCase();
        return DAILY_LIMIT_STRINGS.some(s => text.includes(s));
    } catch { return false; }
}

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

/**
 * PayU SHA512 hash for payment initiation
 * Formula: key|txnid|amount|productinfo|firstname|email|udf1|udf2|udf3|udf4|udf5|udf6|udf7|udf8|udf9|udf10|SALT
 */
function generatePayUHash({ txnid, amount, productinfo, firstname, email, udf1 = '' }) {
    const hashSeq = [
        PAYU_KEY, txnid, amount, productinfo, firstname, email,
        udf1, '', '', '', '', '', '', '', '', '', // udf1 to udf10 (udf1 is index 6)
        PAYU_SALT
    ];
    // Sequence check:
    // 0:key, 1:txnid, 2:amount, 3:productinfo, 4:firstname, 5:email,
    // 6:udf1, 7:udf2, 8:udf3, 9:udf4, 10:udf5, 11:udf6, 12:udf7, 13:udf8, 14:udf9, 15:udf10,
    // 16:SALT
    const str = hashSeq.join('|');
    return crypto.createHash('sha512').update(str).digest('hex');
}

/**
 * Verify PayU response hash (reverse hash for success/failure)
 * Formula: SALT|status|udf10|udf9|udf8|udf7|udf6|udf5|udf4|udf3|udf2|udf1|email|firstname|productinfo|amount|txnid|key
 */
function verifyPayUHash(params) {
    const { hash, status, txnid, amount, productinfo, firstname, email, udf1 = '', additionalCharges } = params;

    const hashSeq = [
        PAYU_SALT, status, '', '', '', '', '', '', '', '', '', // udf10 down to udf2 are empty
        udf1, email, firstname, productinfo, amount, txnid, PAYU_KEY
    ];
    // Sequence check:
    // 0:SALT, 1:status, 
    // 2:udf10, 3:udf9, 4:udf8, 5:udf7, 6:udf6, 7:udf5, 8:udf4, 9:udf3, 10:udf2,
    // 11:udf1, 12:email, 13:firstname, 14:productinfo, 15:amount, 16:txnid, 17:key

    let str = hashSeq.join('|');
    
    // PayU sends additionalCharges sometimes, if so, we must prepend its hash
    if (additionalCharges) {
        const primaryHash = crypto.createHash('sha512').update(str).digest('hex');
        const finalStr = `${additionalCharges}|${primaryHash}`;
        return crypto.createHash('sha512').update(finalStr).digest('hex') === hash;
    }

    const expected = crypto.createHash('sha512').update(str).digest('hex');
    const isMatched = expected === hash;

    if (isMatched && status === 'success') {
        // Asynchronously update status to 'paid'
        Lead.findOneAndUpdate({ txnid }, { status: 'paid' }).exec()
            .then(() => console.log(`[PayU] Lead ${txnid} marked as PAID`))
            .catch(err => console.error(`[PayU] Error updating lead status:`, err));
    } else if (isMatched && status !== 'success') {
        Lead.findOneAndUpdate({ txnid }, { status: 'failed' }).exec()
            .catch(() => {});
    }

    return isMatched;
}

// ─── PDF Proxy Fetch ──────────────────────────────────────────────────────────
async function fetchPdfWithProxy(url, maxRetries = 12) {
    for (let attempt = 1; attempt <= maxRetries; attempt++) {
        try {
            const response = await axios.get(url, {
                httpsAgent: proxyAgent,
                responseType: 'arraybuffer',
                timeout: 60000,
                headers: {
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                    'Accept': 'application/pdf,*/*',
                    'Accept-Language': 'en-US,en;q=0.9',
                    'Referer': 'https://pay2igr.igrmaharashtra.gov.in/'
                },
                maxRedirects: 5,
                validateStatus: s => s < 500
            });

            const buf = Buffer.from(response.data);
            if (buf.slice(0, 4).toString('ascii') !== '%PDF') {
                if (isDailyLimitHit(buf)) {
                    console.warn(`[merge-pdfs] Daily limit hit. Waiting 8s...`);
                    await sleep(8000);
                    attempt--;
                    continue;
                }
                console.warn(`[merge-pdfs] Not a PDF for ${url}. Skipping.`);
                return null;
            }
            console.log(`[merge-pdfs] ✔ Fetched ${url} (${buf.length} bytes)`);
            return buf;
        } catch (err) {
            console.error(`[merge-pdfs] Error attempt ${attempt}/${maxRetries}: ${err.message}`);
            await sleep(Math.min(4000 * attempt, 16000));
        }
    }
    return null;
}

// ─── /api/locations ───────────────────────────────────────────────────────────
// Default: indexed list with ids + mr + en (English transliteration for UI).
// ?legacy=1 returns raw locations.json (nested object, Marathi only) for old clients.
app.get('/api/locations', (req, res) => {
    if (String(req.query.legacy || '') === '1') {
        const locationsPath = path.join(__dirname, 'locations.json');
        if (!fs.existsSync(locationsPath)) return res.status(404).json({ error: 'Locations file not found' });
        try { res.json(JSON.parse(fs.readFileSync(locationsPath, 'utf8'))); }
        catch (e) { res.status(500).json({ error: 'Failed to read locations' }); }
        return;
    }
    const idx = loadLocationsIndex();
    if (!idx) {
        return res.status(500).json({
            error: 'Locations index not found. Run: node tools/build_locations_index.js',
            details: { path: LOCATIONS_INDEX_PATH }
        });
    }
    res.json(idx);
});

// ─── /api/search ──────────────────────────────────────────────────────────────
app.get('/api/search', async (req, res) => {
    const { query } = req.query;
    const loc = getMrLocationFromParams(req.query);
    if (!loc) {
        return res.status(400).json({
            error: 'Missing location parameters. Send either d, t, v (numeric ids) or district, taluka, village (Marathi).'
        });
    }
    const { district, taluka, village } = loc;
    if (!query) return res.status(400).json({ error: 'Missing required parameters' });
    // Accept common user inputs like "74," or "74/1" and extract the leading number.
    const queryMatch = String(query).trim().match(/\d+/);
    if (!queryMatch)
        return res.status(400).json({ error: 'Query must contain a number' });
    const queryNumber = queryMatch[0];

    if (!fs.existsSync(INDEX_DIR)) {
        return res.status(500).json({
            error: 'Index directory not found on server',
            details: { indexDir: INDEX_DIR }
        });
    }

    const resolvedDistrict = resolveDirEntry(INDEX_DIR, district);
    if (!resolvedDistrict) {
        return res.status(404).json({ error: 'District not found in index' });
    }

    const districtDir = path.join(INDEX_DIR, resolvedDistrict);
    const resolvedTaluka = resolveDirEntry(districtDir, taluka);
    if (!resolvedTaluka) {
        return res.status(404).json({ error: 'Taluka not found in index' });
    }

    const talukaDir = path.join(districtDir, resolvedTaluka);
    const resolvedVillage = resolveDirEntry(talukaDir, village);
    if (!resolvedVillage) {
        return res.status(404).json({ error: 'Village not found in index' });
    }

    const filePath = path.join(talukaDir, resolvedVillage, 'data.json');
    if (!fs.existsSync(filePath)) {
        return res.status(404).json({
            error: 'Data file not found for the selected location',
            details: { filePath }
        });
    }

    try {
        const matched = [];
        await forEachRecordInDataJson(filePath, (record) => {
            if (record.property_numbers && Array.isArray(record.property_numbers)) {
                for (const prop of record.property_numbers) {
                    const baseNumber = String(prop.value).split(/[\/\-]/)[0].trim();
                    if (baseNumber === String(queryNumber)) { matched.push(record); break; }
                }
            }
        });

        // Dedupe key is generated from the block of fields between `document_number`
        // and `pdf_link` (both inclusive), so records that differ only in fields
        // like `doc_id` / `serial_number` / `village` / `year` still collapse into one.
        //
        // Strings are normalized (trim + NFC). Objects/arrays are canonicalized
        // (object keys sorted, array items sorted) so order differences don't change the key.
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

        // SECURITY: This endpoint is public. Do not expose full indexed records here.
        // Default response only returns the document type needed for the blurred UI.
        //
        // If you need full records (e.g. post-payment), call this endpoint with:
        // - Header:  x-full-search-key: <SEARCH_FULL_API_KEY>
        // - Query:   full=1
        const wantsFull = String(req.query.full || '').toLowerCase() === '1';
        const providedKey = req.get('x-full-search-key') || '';
        const serverKey = process.env.SEARCH_FULL_API_KEY || '';
        const allowFull = Boolean(serverKey) && wantsFull && providedKey === serverKey;

        if (allowFull) {
            // For internal use you may still want a stable key for each record.
            const results = matched.map(r => ({ ...r, key: fingerprint(r) }));
            return res.json({ count: results.length, results });
        }

        const results = matched.map(r => ({
            document_type: r.document_type || 'Unknown',
            key: fingerprint(r)
        }));
        return res.json({ count: results.length, results });
    } catch (error) {
        console.error('[api/search] Failed reading data.json', {
            filePath,
            message: error && error.message
        });
        const msg = String(error && error.message || '');
        const isStructure = msg.includes('Top-level object should be an array');
        const isOom = /allocation|out of memory|string length|Invalid string length/i.test(msg);
        res.status(isStructure ? 422 : isOom ? 500 : 422).json({
            error: isStructure
                ? 'Indexed data file must be a top-level JSON array'
                : isOom
                    ? 'Indexed data file is too large to process on this server'
                    : (msg || 'Failed to read indexed data file'),
            details: { filePath }
        });
    }
});

// ─── /api/search/full-by-keys ─────────────────────────────────────────────────
// POST { token, d, t, v, query, keys } OR { token, district, taluka, village, query, keys }
// Returns full records matching those SHA keys (for post-payment PDF generation).
app.post('/api/search/full-by-keys', async (req, res) => {
    const body = req.body || {};
    const { token, query, keys } = body;

    if (!token) return res.status(401).json({ error: 'Missing token' });
    const payEntry = PAYMENT_TOKENS.get(token);
    if (!payEntry) return res.status(401).json({ error: 'Invalid token' });
    if (Date.now() > payEntry.exp) {
        PAYMENT_TOKENS.delete(token);
        return res.status(401).json({ error: 'Token expired' });
    }

    const loc = getMrLocationFromParams(body);
    if (!loc) {
        return res.status(400).json({
            error: 'Missing location parameters. Send either d, t, v (numeric ids) or district, taluka, village (Marathi).'
        });
    }
    const { district, taluka, village } = loc;

    if (!query) {
        return res.status(400).json({ error: 'Missing required parameters' });
    }
    if (!Array.isArray(keys) || keys.length === 0) {
        return res.status(400).json({ error: 'Missing keys' });
    }

    try {
        const { records } = await getFullRecordsByKeys({ district, taluka, village, query, keys });
        const results = reportData.dedupeByKey(records);
        return res.json({ count: results.length, results });
    } catch (error) {
        console.error('[api/search/full-by-keys] failed', error && error.message);
        return res.status(error.status || 500).json({
            error: error.message || 'Failed to load full records',
            details: error.details || undefined
        });
    }
});

// ─── /api/report/download ─────────────────────────────────────────────────────
// POST { token, d, t, v, query, keys } OR { token, district, taluka, village, query, keys }
// Returns attachment PDF generated server-side (Chromium) with Marathi-accurate rendering.
app.post('/api/report/download', async (req, res) => {
    const body = req.body || {};
    const { token, query, keys } = body;

    if (!token) return res.status(401).json({ error: 'Missing token' });
    const payEntry = PAYMENT_TOKENS.get(token);
    if (!payEntry) return res.status(401).json({ error: 'Invalid token' });
    if (Date.now() > payEntry.exp) {
        PAYMENT_TOKENS.delete(token);
        return res.status(401).json({ error: 'Token expired' });
    }

    const loc = getMrLocationFromParams(body);
    if (!loc) {
        return res.status(400).json({
            error: 'Missing location parameters. Send either d, t, v (numeric ids) or district, taluka, village (Marathi).'
        });
    }
    const { district, taluka, village } = loc;
    if (!query) return res.status(400).json({ error: 'Missing query' });
    if (!Array.isArray(keys) || keys.length === 0) return res.status(400).json({ error: 'Missing keys' });

    try {
        const { records } = await getFullRecordsByKeys({ district, taluka, village, query, keys });
        if (!records.length) return res.status(404).json({ error: 'No records returned for report generation' });

        const { pdf, filename } = await generatePdfForContext({
            district, taluka, village, query, records
        });

        res.setHeader('Content-Type', 'application/pdf');
        res.setHeader('Content-Disposition', `attachment; filename="${filename.replace(/[^a-zA-Z0-9_.-]/g, '_')}"`);
        res.setHeader('Content-Length', pdf.byteLength);
        return res.send(Buffer.from(pdf));
    } catch (error) {
        console.error('[api/report/download] failed', error && error.message);
        return res.status(error.status || 500).json({
            error: error.message || 'Failed to generate report PDF',
            details: error.details || undefined
        });
    }
});

// ─── /api/payu/initiate ─────────────────────────────────────────────────────
// POST { amount, productinfo, firstname, email, phone, query }
// Returns: PayU form fields + hash for frontend to submit
app.post('/api/payu/initiate', (req, res) => {
    const { productinfo, firstname, email, phone, searchQuery } = req.body;

    if (!productinfo || !firstname || !email || !phone) {
        return res.status(400).json({ error: 'Missing payment fields' });
    }

    // Pricing is enforced server-side so clients cannot tamper with amount.
    const REPORT_FEE_INR = 699;
    const GST_RATE = 0.18;
    const amtFixed = (REPORT_FEE_INR * (1 + GST_RATE)).toFixed(2);

    const txnid    = `MSC_${Date.now()}_${Math.random().toString(36).substr(2, 6).toUpperCase()}`;
    const udf1     = searchQuery || '';   // store search query for post-payment use

    const hash = generatePayUHash({
        txnid, amount: amtFixed, productinfo, firstname, email, udf1
    });

    res.json({
        key:         PAYU_KEY,
        txnid,
        amount:      amtFixed,
        productinfo,
        firstname,
        email,
        phone,
        udf1,
        // PayU typically POSTs to surl/furl. Our React SPA cannot receive POST directly.
        // So PayU posts to backend, we verify + mint token, then redirect to frontend.
        surl:        `${process.env.BACKEND_URL || 'https://api.mahasuchi.com'}/api/payu/success`,
        furl:        `${process.env.BACKEND_URL || 'https://api.mahasuchi.com'}/api/payu/failure`,
        hash,
        action:      PAYU_URL
    });
});

// ─── PayU redirect handlers (receive POST, then redirect to frontend) ──────────
app.post('/api/payu/success', (req, res) => {
    const params = req.body || {};
    const isValid = verifyPayUHash(params);
    const frontend = process.env.FRONTEND_URL || 'http://localhost:3000';

    if (!isValid || params.status !== 'success') {
        return res.redirect(302, `${frontend}/payment-failure`);
    }

    const { token } = issuePaymentToken(params.txnid);
    // Fire-and-forget support copy email right after successful payment callback.
    tryAutoEmailReport(params.txnid).then((r) => {
        if (r && r.sent) console.log(`[email] Report emailed for txnid ${params.txnid}`);
    }).catch(() => {});
    return res.redirect(302, `${frontend}/payment-success?txnid=${encodeURIComponent(params.txnid)}&token=${encodeURIComponent(token)}`);
});

app.post('/api/payu/failure', (req, res) => {
    const frontend = process.env.FRONTEND_URL || 'http://localhost:3000';
    return res.redirect(302, `${frontend}/payment-failure`);
});

// ─── /api/payu/verify ────────────────────────────────────────────────────────
// POST from frontend after PayU redirect — verifies hash server-side
app.post('/api/payu/verify', (req, res) => {
    const params = req.body;
    const isValid = verifyPayUHash(params);

    if (!isValid) {
        return res.status(400).json({ verified: false, error: 'Hash mismatch — possible tampered response' });
    }

    if (params.status !== 'success') {
        return res.status(200).json({ verified: false, status: params.status });
    }

    const { token } = issuePaymentToken(params.txnid);
    // For gateway flows that hit verify route, also trigger auto-email.
    tryAutoEmailReport(params.txnid).then((r) => {
        if (r && r.sent) console.log(`[email] Report emailed for txnid ${params.txnid}`);
    }).catch(() => {});
    res.json({ verified: true, status: 'success', txnid: params.txnid, token, expires_in_sec: Math.floor(PAYMENT_TOKEN_TTL_MS / 1000) });
});

// ─── /api/leads ─────────────────────────────────────────────────────────────
// Called from frontend popup to store initial lead data
app.post('/api/leads', async (req, res) => {
    const { phone, email, district, taluka, village, query, txnid } = req.body;
    try {
        const lead = new Lead({
            phone,
            email,
            district,
            taluka,
            village,
            query,
            txnid,
            status: 'pending'
        });
        await lead.save();
        console.log(`[leads] New lead saved: ${phone} / ${txnid}`);
        res.json({ success: true });
    } catch (err) {
        console.error('[leads] Save error:', err);
        // If txnid duplicate (user retrying), just return success
        if (err.code === 11000) return res.json({ success: true });
        res.status(500).json({ error: 'Failed to save lead' });
    }
});

// ─── /api/merge-pdfs ─────────────────────────────────────────────────────────
app.post('/api/merge-pdfs', async (req, res) => {
    const { urls, filename } = req.body;
    if (!urls || !Array.isArray(urls) || urls.length === 0)
        return res.status(400).json({ error: 'No PDF URLs provided' });

    console.log(`[merge-pdfs] Starting merge of ${urls.length} PDFs...`);

    try {
        const mergedDoc = await PDFDocument.create();
        let successCount = 0;

        for (let i = 0; i < urls.length; i++) {
            const url = urls[i];
            console.log(`[merge-pdfs] Fetching ${i + 1}/${urls.length}: ${url}`);
            const pdfBuf = await fetchPdfWithProxy(url);
            if (!pdfBuf) { console.warn(`[merge-pdfs] Skipping ${url}`); continue; }

            try {
                const srcDoc = await PDFDocument.load(pdfBuf, { ignoreEncryption: true });
                const pages = await mergedDoc.copyPages(srcDoc, srcDoc.getPageIndices());
                pages.forEach(p => mergedDoc.addPage(p));
                successCount++;
                console.log(`[merge-pdfs] ✔ Added ${pages.length} page(s) from doc ${i + 1}`);
            } catch (pdfErr) {
                console.error(`[merge-pdfs] Failed to parse PDF: ${pdfErr.message}`);
            }
        }

        if (successCount === 0)
            return res.status(502).json({ error: 'Could not fetch any PDFs. Please try again.' });

        const mergedBytes = await mergedDoc.save();
        const safeFilename = (filename || 'Mahasuchi_Report.pdf').replace(/[^a-zA-Z0-9_.-]/g, '_');
        console.log(`[merge-pdfs] ✅ Done. ${successCount}/${urls.length} merged → ${mergedBytes.byteLength} bytes`);

        res.setHeader('Content-Type', 'application/pdf');
        res.setHeader('Content-Disposition', `attachment; filename="${safeFilename}"`);
        res.setHeader('Content-Length', mergedBytes.byteLength);
        res.send(Buffer.from(mergedBytes));
    } catch (err) {
        console.error('[merge-pdfs] Error:', err);
        res.status(500).json({ error: 'Internal server error during PDF merge.' });
    }
});

// ─── Start ────────────────────────────────────────────────────────────────────
const PORT = process.env.PORT || 8000;
app.listen(PORT, () => console.log(`Mahasuchi Backend running on port ${PORT}`));
