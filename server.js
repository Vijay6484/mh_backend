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

const app = express();
app.use(cors({ origin: '*' }));
app.use(express.json());
// PayU posts back as form-urlencoded
app.use(express.urlencoded({ extended: true }));

const INDEX_DIR = process.env.INDEX_DIR
    ? path.resolve(process.env.INDEX_DIR)
    : path.join(__dirname, 'indexed_data');

function normalizeFsKey(s) {
    // Make user input and on-disk names comparable across platforms.
    // - trim to avoid accidental spaces
    // - NFC to reduce Unicode normalization mismatches (common with Indic scripts)
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
    createdAt: { type: Date, default: Date.now }
});

const Lead = mongoose.model('Lead', LeadSchema);

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
app.get('/api/locations', (req, res) => {
    const locationsPath = path.join(__dirname, 'locations.json');
    if (!fs.existsSync(locationsPath)) return res.status(404).json({ error: 'Locations file not found' });
    try { res.json(JSON.parse(fs.readFileSync(locationsPath, 'utf8'))); }
    catch (e) { res.status(500).json({ error: 'Failed to read locations' }); }
});

// ─── /api/search ──────────────────────────────────────────────────────────────
app.get('/api/search', (req, res) => {
    const { district, taluka, village, query } = req.query;
    if (!district || !taluka || !village || !query)
        return res.status(400).json({ error: 'Missing required parameters' });
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
        const raw = fs.readFileSync(filePath, 'utf8');
        const records = JSON.parse(raw);
        const matched = [];
        for (const record of records) {
            if (record.property_numbers && Array.isArray(record.property_numbers)) {
                for (const prop of record.property_numbers) {
                    const baseNumber = String(prop.value).split(/[\/\-]/)[0].trim();
                    if (baseNumber === String(queryNumber)) { matched.push(record); break; }
                }
            }
        }

        // Dedupe by full record equality (all key/value pairs).
        // Use a stable (key-order independent) JSON fingerprint so we can dedupe in O(n).
        const stableStringify = (value) => {
            if (value === null) return 'null';
            const t = typeof value;
            if (t === 'string' || t === 'number' || t === 'boolean') return JSON.stringify(value);
            if (Array.isArray(value)) return `[${value.map(stableStringify).join(',')}]`;
            if (t === 'object') {
                const keys = Object.keys(value).sort();
                return `{${keys.map(k => `${JSON.stringify(k)}:${stableStringify(value[k])}`).join(',')}}`;
            }
            // JSON.parse output shouldn't contain undefined/functions/symbols, but guard anyway.
            return JSON.stringify(null);
        };

        const fingerprint = (record) => {
            const canonical = stableStringify(record);
            return crypto.createHash('sha256').update(canonical).digest('base64url');
        };

        const seenDocs = new Set();
        const uniqueMatched = [];
        for (const r of matched) {
            const fp = fingerprint(r);
            if (seenDocs.has(fp)) continue;
            seenDocs.add(fp);
            uniqueMatched.push(r);
        }

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
            return res.json({ count: uniqueMatched.length, results: uniqueMatched });
        }

        const results = uniqueMatched.map(r => ({
            document_type: r.document_type || 'Unknown'
        }));
        return res.json({ count: results.length, results });
    } catch (error) {
        console.error('[api/search] Failed reading/parsing data.json', {
            filePath,
            message: error && error.message
        });
        // Most common real-world cause here is a partially-written or malformed JSON file.
        res.status(422).json({
            error: 'Indexed data file is not valid JSON',
            details: { filePath }
        });
    }
});

// ─── /api/payu/initiate ─────────────────────────────────────────────────────
// POST { amount, productinfo, firstname, email, phone, query }
// Returns: PayU form fields + hash for frontend to submit
app.post('/api/payu/initiate', (req, res) => {
    const { amount, productinfo, firstname, email, phone, searchQuery } = req.body;

    if (!amount || !productinfo || !firstname || !email || !phone) {
        return res.status(400).json({ error: 'Missing payment fields' });
    }

    const txnid    = `MSC_${Date.now()}_${Math.random().toString(36).substr(2, 6).toUpperCase()}`;
    const amtFixed = parseFloat(amount).toFixed(2);
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
        surl:        `${process.env.FRONTEND_URL || 'http://localhost:3000'}/payment-success`,
        furl:        `${process.env.FRONTEND_URL || 'http://localhost:3000'}/payment-failure`,
        hash,
        action:      PAYU_URL
    });
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

    res.json({ verified: true, status: 'success', txnid: params.txnid });
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
