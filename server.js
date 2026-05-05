require('dotenv').config();
const express = require('express');
const cors = require('cors');
const cookieParser = require('cookie-parser');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const jwt = require('jsonwebtoken');
const { PDFDocument } = require('pdf-lib');
const { HttpsProxyAgent } = require('https-proxy-agent');
const mongoose = require('mongoose');
const axios = require('axios');
const reportData = require('./services/reportData');
const { getSmtpConfigFromEnv, validateSmtpConfig, sendReportEmail } = require('./services/mailer');
const { forEachRecordInDataJson } = require('./services/streamDataJson');
const {
    isAdminEnvConfigured,
    getClientId,
    checkAdminLoginWindow,
    recordAdminLoginFailure,
    clearAdminLoginWindow,
    verifyPassword,
    signAdminToken,
    requireAdminAuth,
    safeEqualStr,
    getJwtExpires,
} = require('./services/adminAuth');

const app = express();

// Reflect any Origin (required when credentials: true; * is invalid with cookies).
app.use(cors({ origin: true, credentials: true }));
app.use(cookieParser());
// Large payload needed for uploading client-generated PDFs.
app.use(express.json({ limit: '30mb' }));
// PayU posts back as form-urlencoded
app.use(express.urlencoded({ extended: true, limit: '2mb' }));

// Public config for frontend runtime toggles (no auth).
app.get('/api/public-config', (req, res) => {
    const maintenance = parseBoolEnv(process.env.MAINTENANCE_MODE || '0');
    const liveIn = String(process.env.MAINTENANCE_LIVE_IN || 'one hour').trim() || 'one hour';
    return res.json({
        maintenance,
        maintenance_message: `We’ll be live in ${liveIn}.`,
        now_iso: new Date().toISOString(),
    });
});

// Maintenance gate for API: when enabled, block all API routes except allowlisted ones.
app.use('/api', (req, res, next) => {
    const maintenance = parseBoolEnv(process.env.MAINTENANCE_MODE || '0');
    if (!maintenance) return next();

    const allow = new Set([
        '/public-config',
        // Allow PayU callbacks/verification to avoid breaking in-flight payments.
        '/payu/success',
        '/payu/failure',
        '/payu/verify',
    ]);
    const pathOnly = String(req.path || '');
    if (allow.has(pathOnly)) return next();

    return res.status(503).json({
        error: 'Maintenance mode',
        message: 'Service temporarily unavailable. Please try again later.',
    });
});

const INDEX_DIR = process.env.INDEX_DIR
    ? path.resolve(process.env.INDEX_DIR)
    : path.join(__dirname, 'indexed_data');

// ─── Public runtime config (maintenance switch, etc.) ─────────────────────────
function parseBoolEnv(v) {
    const s = String(v ?? '').trim().toLowerCase();
    return s === '1' || s === 'true' || s === 'yes' || s === 'on';
}

function normalizeEmail(email) {
    return String(email || '').trim().toLowerCase();
}

function normalizePhone(phone) {
    const digits = String(phone || '').replace(/\D/g, '');
    if (!digits) return '';
    return digits.length > 10 ? digits.slice(-10) : digits;
}

function maskEmail(email) {
    const e = normalizeEmail(email);
    if (!e || !e.includes('@')) return '';
    const [name, host] = e.split('@');
    const visible = name.slice(0, 2);
    return `${visible}${'*'.repeat(Math.max(1, name.length - visible.length))}@${host}`;
}

function getCookieOptions(maxAgeMs) {
    const secure = parseBoolEnv(process.env.COOKIE_SECURE || (process.env.NODE_ENV === 'production' ? '1' : '0'));
    return {
        httpOnly: true,
        secure,
        sameSite: secure ? 'none' : 'lax',
        maxAge: maxAgeMs,
        path: '/',
    };
}

// locations_index.json: built by `node tools/build_locations_index.js` from locations.json.
// Override LOCATIONS_INDEX_PATH in .env if you want a small local testing index.
const LOCATIONS_INDEX_PATH = process.env.LOCATIONS_INDEX_PATH
    ? path.resolve(process.env.LOCATIONS_INDEX_PATH)
    : path.join(__dirname, 'locations_index.json');
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

// Payment tokens (for paid report download)
// - New format: JWT (stateless, survives refresh/server restart)
// - Backward compat: legacy in-memory tokens (older frontend builds)
const PAYMENT_TOKENS = new Map(); // legacy token -> { txnid, exp }
const PAYMENT_TOKEN_TTL_MS = 10 * 60 * 1000; // legacy tokens: 10 minutes

const PAYMENT_JWT_SECRET =
    process.env.PAYMENT_JWT_SECRET ||
    process.env.ADMIN_JWT_SECRET ||
    process.env.PAYU_MERCHANT_SALT ||
    '';
const CUSTOMER_JWT_SECRET = process.env.CUSTOMER_JWT_SECRET || PAYMENT_JWT_SECRET;
const CUSTOMER_JWT_EXPIRES = process.env.CUSTOMER_JWT_EXPIRES || '3650d';
const CUSTOMER_COOKIE_NAME = 'mahasuchi_customer_token';

function issuePaymentToken(txnid) {
    // If secret missing for some reason, fall back to legacy in-memory token.
    if (!PAYMENT_JWT_SECRET) {
        const token = crypto.randomBytes(24).toString('base64url');
        const exp = Date.now() + PAYMENT_TOKEN_TTL_MS;
        PAYMENT_TOKENS.set(token, { txnid, exp });
        return { token, exp, legacy: true };
    }

    // "Forever" token: keep it effectively non-expiring (10 years).
    const token = jwt.sign(
        { typ: 'pay', txnid: String(txnid || '') },
        PAYMENT_JWT_SECRET,
        { algorithm: 'HS256', expiresIn: '3650d' }
    );
    return { token, exp: null, legacy: false };
}

function verifyPaymentToken(token) {
    const t = String(token || '').trim();
    if (!t) return { ok: false, status: 401, error: 'Missing token' };

    // Prefer JWT verification (new format).
    if (PAYMENT_JWT_SECRET) {
        try {
            const payload = jwt.verify(t, PAYMENT_JWT_SECRET, { algorithms: ['HS256'] });
            if (!payload || payload.typ !== 'pay' || !payload.txnid) {
                return { ok: false, status: 401, error: 'Invalid token' };
            }
            return { ok: true, txnid: String(payload.txnid) };
        } catch (_) {
            // fall through to legacy
        }
    }

    // Legacy in-memory tokens (old frontend builds).
    const payEntry = PAYMENT_TOKENS.get(t);
    if (!payEntry) return { ok: false, status: 401, error: 'Invalid token' };
    if (Date.now() > payEntry.exp) {
        PAYMENT_TOKENS.delete(t);
        return { ok: false, status: 401, error: 'Token expired' };
    }
    return { ok: true, txnid: String(payEntry.txnid) };
}

function issueCustomerSessionToken({ email, phone }) {
    if (!CUSTOMER_JWT_SECRET) return '';
    return jwt.sign(
        {
            typ: 'cust',
            email: normalizeEmail(email),
            phone: normalizePhone(phone),
        },
        CUSTOMER_JWT_SECRET,
        { algorithm: 'HS256', expiresIn: CUSTOMER_JWT_EXPIRES }
    );
}

function verifyCustomerSessionToken(token) {
    if (!CUSTOMER_JWT_SECRET) return { ok: false, status: 503, error: 'Customer auth is not configured' };
    const t = String(token || '').trim();
    if (!t) return { ok: false, status: 401, error: 'Missing customer token' };
    try {
        const payload = jwt.verify(t, CUSTOMER_JWT_SECRET, { algorithms: ['HS256'] });
        if (!payload || payload.typ !== 'cust' || !payload.email || !payload.phone) {
            return { ok: false, status: 401, error: 'Invalid customer token' };
        }
        return {
            ok: true,
            customer: {
                email: normalizeEmail(payload.email),
                phone: normalizePhone(payload.phone),
            }
        };
    } catch (_) {
        return { ok: false, status: 401, error: 'Invalid or expired customer token' };
    }
}

function setCustomerSessionCookies(res, token) {
    if (!token) return;
    const tenYearsMs = 3650 * 24 * 60 * 60 * 1000;
    res.cookie(CUSTOMER_COOKIE_NAME, token, getCookieOptions(tenYearsMs));
}

function getCustomerTokenFromReq(req) {
    const hdr = String(req.get('authorization') || '').trim();
    if (hdr.toLowerCase().startsWith('bearer ')) return hdr.slice('bearer '.length).trim();
    const c = req.cookies && req.cookies[CUSTOMER_COOKIE_NAME];
    if (c) return String(c).trim();
    return '';
}

function requireCustomerAuth(req, res, next) {
    const token = getCustomerTokenFromReq(req);
    const verified = verifyCustomerSessionToken(token);
    if (!verified.ok) return res.status(verified.status).json({ error: verified.error });
    req.customer = verified.customer;
    return next();
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
// PAYU_MODE=test uses PAYU_TEST_* vars; anything else uses PAYU_* / PAYU_LIVE_*.
const PAYU_MODE = String(process.env.PAYU_MODE || 'live').trim().toLowerCase();
const PAYU_IS_TEST = PAYU_MODE === 'test' || PAYU_MODE === 'sandbox';
const PAYU_KEY = PAYU_IS_TEST
    ? (process.env.PAYU_TEST_MERCHANT_KEY || process.env.PAYU_MERCHANT_KEY)
    : (process.env.PAYU_LIVE_MERCHANT_KEY || process.env.PAYU_MERCHANT_KEY);
const PAYU_SALT = PAYU_IS_TEST
    ? (process.env.PAYU_TEST_MERCHANT_SALT || process.env.PAYU_MERCHANT_SALT)
    : (process.env.PAYU_LIVE_MERCHANT_SALT || process.env.PAYU_MERCHANT_SALT);
const PAYU_URL = PAYU_IS_TEST
    ? (process.env.PAYU_TEST_BASE_URL || 'https://test.payu.in/_payment')
    : (process.env.PAYU_LIVE_BASE_URL || process.env.PAYU_BASE_URL || 'https://secure.payu.in/_payment');
const FRONTEND_URL = PAYU_IS_TEST
    ? (process.env.PAYU_TEST_FRONTEND_URL || process.env.FRONTEND_URL || 'http://localhost:3000')
    : (process.env.PAYU_LIVE_FRONTEND_URL || process.env.FRONTEND_URL || 'https://mahasuchi.com');
const BACKEND_URL = PAYU_IS_TEST
    ? (process.env.PAYU_TEST_BACKEND_URL || process.env.BACKEND_URL || 'http://localhost:8000')
    : (process.env.PAYU_LIVE_BACKEND_URL || process.env.BACKEND_URL || 'https://api.mahasuchi.com');

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
    phoneNorm: { type: String, index: true },
    email: String,
    emailNorm: { type: String, index: true },
    district: String,
    taluka: String,
    village: String,
    query: String,
    selectedKeys: { type: [String], default: [] },
    selectedLocationIds: {
        d: { type: Number, default: null },
        t: { type: Number, default: null },
        v: { type: Number, default: null },
    },
    selectedLocationEn: {
        de: { type: String, default: '' },
        te: { type: String, default: '' },
        ve: { type: String, default: '' },
    },
    propertyType: { type: String, default: '' },
    status: { type: String, enum: ['pending', 'paid', 'failed'], default: 'pending' },
    txnid: { type: String, unique: true },
    reportEmailSentAt: { type: Date, default: null },
    reportEmailStatus: { type: String, enum: ['pending', 'sent', 'failed'], default: 'pending' },
    reportEmailError: { type: String, default: '' },
    createdAt: { type: Date, default: Date.now }
});

const Lead = mongoose.model('Lead', LeadSchema);

// NOTE: Server-side PDF caching removed (PDF is generated client-side now).

// ─── SMTP config visibility ────────────────────────────────────────────────────
const smtpCfg = getSmtpConfigFromEnv();
const smtpValidation = validateSmtpConfig(smtpCfg);
if (!smtpValidation.ok) {
    console.warn(`[email] SMTP disabled. Missing vars: ${smtpValidation.missing.join(', ')}`);
} else {
    console.log(`[email] SMTP configured (${smtpCfg.host}:${smtpCfg.port}, secure=${smtpCfg.secure})`);
}

if (isAdminEnvConfigured()) {
    console.log('[admin] /api/admin/* enabled (JWT login).');
} else {
    console.warn('[admin] /api/admin/* disabled. Set ADMIN_USER, ADMIN_PASS_HASH, ADMIN_JWT_SECRET.');
}

async function getFullRecordsByKeys({ district, taluka, village, query, keys }) {
    const loc = { district, taluka, village };
    const queryMatch = String(query).trim().match(/\d+/);
    const queryNumber = queryMatch ? queryMatch[0] : String(query);

    const r = await searchPropertyIndex(loc, queryNumber);
    if (!r.ok) {
        throw new Error(r.error || 'Failed to load records');
    }
    const matched = r.matched;
    const qNum = r.queryNumber;

    const keySet = new Set((keys || []).filter(Boolean));
    const filtered = matched.filter(r => keySet.has(reportData.fingerprint(r)));
    const deduped = reportData.dedupeByKey(filtered);
    return { queryNumber: qNum, records: deduped };
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

        const r = await searchPropertyIndex({ district: lead.district, taluka: lead.taluka, village: lead.village }, lead.query);
        if (!r.ok) {
            await Lead.findOneAndUpdate({ txnid }, {
                reportEmailStatus: 'failed',
                reportEmailError: 'Failed to load records for auto-email'
            }).exec();
            return { sent: false, skipped: true, reason: 'Failed to load records' };
        }
        const matched = r.matched;
        
        const deduped = reportData.dedupeByKey(matched);
        if (!deduped.length) {
            await Lead.findOneAndUpdate({ txnid }, {
                reportEmailStatus: 'failed',
                reportEmailError: 'No records found for paid search context'
            }).exec();
            return { sent: false, skipped: true, reason: 'No matching records' };
        }

        return { sent: false, skipped: true, reason: 'Server-side PDF generation disabled; use client upload email flow.' };
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
        // Per requirement: if payment fails, keep it as pending.
        Lead.findOneAndUpdate({ txnid }, { status: 'pending' }).exec()
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

/**
 * @param {{ district: string, taluka: string, village: string }} loc
 * @param {string} query
 * @returns {Promise<
 *   { ok: true, queryNumber: string, filePath: string, matched: any[] }
 *   | { ok: false, status: number, error: string, details?: any, filePath?: string }
 * >}
 */
async function searchPropertyIndex(loc, query) {
    const { district, taluka, village } = loc;
    if (!query) {
        return { ok: false, status: 400, error: 'Missing required parameters' };
    }
    const queryMatch = String(query).trim().match(/\d+/);
    if (!queryMatch) {
        return { ok: false, status: 400, error: 'Query must contain a number' };
    }
    const queryNumber = queryMatch[0];

    if (!fs.existsSync(INDEX_DIR)) {
        return { ok: false, status: 500, error: 'Index directory not found on server', details: { indexDir: INDEX_DIR } };
    }

    const resolvedDistrict = resolveDirEntry(INDEX_DIR, district);
    if (!resolvedDistrict) {
        return { ok: false, status: 404, error: 'District not found in index' };
    }
    const districtDir = path.join(INDEX_DIR, resolvedDistrict);
    const resolvedTaluka = resolveDirEntry(districtDir, taluka);
    if (!resolvedTaluka) {
        return { ok: false, status: 404, error: 'Taluka not found in index' };
    }
    const talukaDir = path.join(districtDir, resolvedTaluka);
    const resolvedVillage = resolveDirEntry(talukaDir, village);
    if (!resolvedVillage) {
        return { ok: false, status: 404, error: 'Village not found in index' };
    }

    const filePath = path.join(talukaDir, resolvedVillage, 'data.json');
    if (!fs.existsSync(filePath)) {
        return { ok: false, status: 404, error: 'Data file not found for the selected location', details: { filePath } };
    }

    try {
        const matched = [];
        const seenKey = new Set();
        await forEachRecordInDataJson(filePath, (record) => {
            if (record.property_numbers && Array.isArray(record.property_numbers)) {
                for (const prop of record.property_numbers) {
                    const baseNumber = String(prop.value).split(/[\/\-]/)[0].trim();
                    if (baseNumber === String(queryNumber)) {
                        const key = reportData.fingerprint(record);
                        if (seenKey.has(key)) return;
                        seenKey.add(key);
                        matched.push(record);
                        return;
                    }
                }
            }
        });
        return { ok: true, queryNumber, filePath, matched };
    } catch (error) {
        const msg = String(error && error.message || '');
        const isStructure = msg.includes('Top-level object should be an array');
        const isOom = /allocation|out of memory|string length|Invalid string length/i.test(msg);
        return {
            ok: false,
            status: isStructure ? 422 : isOom ? 500 : 422,
            error: isStructure
                ? 'Indexed data file must be a top-level JSON array'
                : isOom
                    ? 'Indexed data file is too large to process on this server'
                    : (msg || 'Failed to read indexed data file'),
            details: { filePath }
        };
    }
}

// ─── /api/search ──────────────────────────────────────────────────────────────
app.get('/api/search', async (req, res) => {
    const { query } = req.query;
    const loc = getMrLocationFromParams(req.query);
    if (!loc) {
        return res.status(400).json({
            error: 'Missing location parameters. Send either d, t, v (numeric ids) or district, taluka, village (Marathi).'
        });
    }

    const r = await searchPropertyIndex(loc, query);
    if (!r.ok) {
        if (r.filePath) {
            console.error('[api/search] Failed reading data.json', { filePath: r.filePath, error: r.error });
        }
        return res.status(r.status).json({ error: r.error, details: r.details || undefined });
    }
    const { queryNumber, filePath, matched } = r;

    // Duplicate identity rows in data.json (same document_number..pdf_link block) are
    // already collapsed by SHA key during streaming, same as the paid/PDF path.

    // SECURITY: This endpoint is public. Do not expose full indexed records here.
    // If you need full records (e.g. post-payment), use admin panel or
    // x-full-search-key with full=1.
    const wantsFull = String(req.query.full || '').toLowerCase() === '1';
    const providedKey = req.get('x-full-search-key') || '';
    const serverKey = process.env.SEARCH_FULL_API_KEY || '';
    const allowFull = Boolean(serverKey) && wantsFull && providedKey === serverKey;

    if (allowFull) {
        const results = matched.map((row) => ({ ...row, key: reportData.fingerprint(row) }));
        return res.json({ count: results.length, results, queryNumber });
    }

    const results = matched.map((row) => ({
        document_type: row.document_type || 'Unknown',
        key: reportData.fingerprint(row)
    }));
    return res.json({ count: results.length, results, queryNumber });
});

// ─── /api/search/full-by-keys ─────────────────────────────────────────────────
// POST { token, d, t, v, query, keys } OR { token, district, taluka, village, query, keys }
// Returns full records matching those SHA keys (for post-payment PDF generation).
app.post('/api/search/full-by-keys', async (req, res) => {
    const body = req.body || {};
    const { token, query, keys } = body;

    const tok = verifyPaymentToken(token);
    if (!tok.ok) return res.status(tok.status).json({ error: tok.error });

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
        const lead = await Lead.findOne({ txnid: tok.txnid }).lean();
        if (!lead) {
            return res.status(404).json({ error: 'Purchase not found for token' });
        }
        if (lead.status !== 'paid') {
            return res.status(403).json({ error: 'Purchase is not marked as paid' });
        }

        const expectedQuery = String(lead.query || '').trim();
        const requestedQuery = String(query || '').trim();
        if (expectedQuery && requestedQuery && expectedQuery !== requestedQuery) {
            return res.status(403).json({ error: 'Query mismatch for this purchase token' });
        }

        const reqLoc = getMrLocationFromParams(body);
        if (!reqLoc) {
            return res.status(400).json({ error: 'Missing location for this purchase token' });
        }
        const expectedByIds = lead.selectedLocationIds && lead.selectedLocationIds.d && lead.selectedLocationIds.t && lead.selectedLocationIds.v
            ? resolveMrFromIdsFixed(lead.selectedLocationIds.d, lead.selectedLocationIds.t, lead.selectedLocationIds.v)
            : null;
        const expectedLoc = expectedByIds || {
            district: String(lead.district || ''),
            taluka: String(lead.taluka || ''),
            village: String(lead.village || ''),
        };
        if (
            normalizeFsKey(expectedLoc.district) !== normalizeFsKey(reqLoc.district) ||
            normalizeFsKey(expectedLoc.taluka) !== normalizeFsKey(reqLoc.taluka) ||
            normalizeFsKey(expectedLoc.village) !== normalizeFsKey(reqLoc.village)
        ) {
            return res.status(403).json({ error: 'Location mismatch for this purchase token' });
        }

        const allowedKeys = new Set((lead.selectedKeys || []).filter(Boolean));
        if (!allowedKeys.size) {
            return res.status(403).json({ error: 'No downloadable records are mapped to this purchase' });
        }
        const invalidRequestedKey = keys.find(k => !allowedKeys.has(String(k)));
        if (invalidRequestedKey) {
            return res.status(403).json({ error: 'Requested record is not part of this paid purchase' });
        }

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

// ─── /api/report/email-upload ────────────────────────────────────────────────
// POST JSON:
// {
//   token, to, filename,
//   pdf_base64, // base64 string (no data: prefix)
//   ctx: { district, taluka, village, query }
// }
// Emails the exact PDF generated by the browser.
app.post('/api/report/email-upload', async (req, res) => {
    const body = req.body || {};
    const { token } = body;
    const to = String(body.to || '').trim();
    const filename = String(body.filename || 'Mahasuchi_Report.pdf');
    const pdfBase64 = String(body.pdf_base64 || '').trim();
    const ctx = (body && typeof body.ctx === 'object' && body.ctx) ? body.ctx : {};

    const tok = verifyPaymentToken(token);
    if (!tok.ok) return res.status(tok.status).json({ sent: false, error: tok.error });
    if (!to) return res.status(400).json({ sent: false, error: 'Missing recipient email (to)' });
    if (!pdfBase64) return res.status(400).json({ sent: false, error: 'Missing pdf_base64' });

    console.log(`[email-upload] request txnid=${tok.txnid || 'unknown'} to=${to} file=${filename}`);

    let pdfBuf;
    try {
        pdfBuf = Buffer.from(pdfBase64, 'base64');
        if (!pdfBuf.length || pdfBuf.slice(0, 4).toString('ascii') !== '%PDF') {
            return res.status(400).json({ sent: false, error: 'Invalid PDF payload' });
        }
        if (pdfBuf.byteLength > 25 * 1024 * 1024) {
            return res.status(413).json({ sent: false, error: 'PDF too large' });
        }
    } catch (_) {
        return res.status(400).json({ sent: false, error: 'Could not parse pdf_base64' });
    }

    try {
        const emailRes = await sendReportEmail({
            to,
            pdfBuffer: pdfBuf,
            filename,
            ctx: {
                district: String(ctx.district || ''),
                taluka: String(ctx.taluka || ''),
                village: String(ctx.village || ''),
                query: String(ctx.query || ''),
            }
        });

        // Best-effort update in Mongo (for admin panel visibility).
        // Do not fail the API if DB is down.
        const txnid = tok.txnid;
        if (txnid) {
            const upd = emailRes.sent
                ? { reportEmailSentAt: new Date(), reportEmailStatus: 'sent', reportEmailError: '' }
                : { reportEmailStatus: 'failed', reportEmailError: String(emailRes.reason || 'Email not sent').slice(0, 300) };
            Lead.findOneAndUpdate({ txnid }, upd).exec().catch(() => {});
        }

        if (!emailRes.sent) {
            console.warn(`[email-upload] not sent txnid=${tok.txnid || 'unknown'} reason=${emailRes.reason || 'Email not sent'}`);
            return res.status(503).json({ sent: false, error: emailRes.reason || 'Email not sent' });
        }
        console.log(`[email-upload] sent txnid=${tok.txnid || 'unknown'} to=${to}`);
        return res.json({ sent: true });
    } catch (err) {
        console.error('[api/report/email-upload] failed', err && err.message);
        return res.status(500).json({ sent: false, error: err && err.message ? String(err.message) : 'Failed to send email' });
    }
});

// ─── /api/payu/initiate ─────────────────────────────────────────────────────
// POST { amount, productinfo, firstname, email, phone, searchQuery, txnid? }
// Returns: PayU form fields + hash for frontend to submit
app.post('/api/payu/initiate', (req, res) => {
    const { productinfo, firstname, email, phone, searchQuery, txnid: txnidFromClient } = req.body;

    if (!productinfo || !firstname || !email || !phone) {
        return res.status(400).json({ error: 'Missing payment fields' });
    }

    // Pricing is enforced server-side so clients cannot tamper with amount.
    const REPORT_FEE_INR = 999;
    const GST_RATE = 0.18;
    const amtFixed = (REPORT_FEE_INR * (1 + GST_RATE)).toFixed(2);

    // IMPORTANT:
    // Frontend creates txnid and stores Lead({txnid}) before calling this route.
    // If we generate a different txnid here, PayU callbacks won't match the Lead, and auto-email will not run.
    const safeClientTxnid = String(txnidFromClient || '').trim();
    const txnid = (/^MSC_[0-9]{10,}_[A-Z0-9]{4,10}$/.test(safeClientTxnid) && safeClientTxnid.length <= 80)
        ? safeClientTxnid
        : `MSC_${Date.now()}_${Math.random().toString(36).slice(2, 8).toUpperCase()}`;
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
        surl:        `${BACKEND_URL}/api/payu/success`,
        furl:        `${BACKEND_URL}/api/payu/failure`,
        hash,
        action:      PAYU_URL
    });
});

// ─── PayU redirect handlers (receive POST, then redirect to frontend) ──────────
app.post('/api/payu/success', async (req, res) => {
    const params = req.body || {};
    const isValid = verifyPayUHash(params);
    const frontend = FRONTEND_URL;

    if (!isValid || params.status !== 'success') {
        return res.redirect(302, `${frontend}/payment-failure`);
    }

    try {
        await Lead.findOneAndUpdate({ txnid: String(params.txnid || '') }, { status: 'paid' }).exec();
        const lead = await Lead.findOne({ txnid: String(params.txnid || '') }).lean();
        if (lead && lead.email && lead.phone) {
            const customerToken = issueCustomerSessionToken({
                email: lead.emailNorm || lead.email,
                phone: lead.phoneNorm || lead.phone,
            });
            setCustomerSessionCookies(res, customerToken);
        }
    } catch (e) {
        console.error('[payu/success] failed to set customer session', e && e.message);
    }

    const { token } = issuePaymentToken(params.txnid);
    // Auto-email moved to client-generated PDF upload flow.
    return res.redirect(302, `${frontend}/payment-success?txnid=${encodeURIComponent(params.txnid)}&token=${encodeURIComponent(token)}`);
});

app.post('/api/payu/failure', (req, res) => {
    // Best-effort: if PayU gives us txnid in failure callback, keep Lead as pending.
    // (Some setups may not include all fields here.)
    try {
        const txnid = req.body && req.body.txnid ? String(req.body.txnid) : '';
        if (txnid) {
            Lead.findOneAndUpdate({ txnid }, { status: 'pending' }).exec().catch(() => {});
        }
    } catch (_) {}
    const frontend = FRONTEND_URL;
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
    // Auto-email moved to client-generated PDF upload flow.
    res.json({ verified: true, status: 'success', txnid: params.txnid, token, expires_in_sec: 3650 * 24 * 60 * 60 });
});

// ─── /api/leads ─────────────────────────────────────────────────────────────
// Called from frontend popup to store initial lead data
app.post('/api/leads', async (req, res) => {
    const {
        phone,
        email,
        district,
        taluka,
        village,
        query,
        txnid,
        selectedKeys,
        d,
        t,
        v,
        de,
        te,
        ve,
        propertyType,
    } = req.body;
    const sessionToken = getCustomerTokenFromReq(req);
    let sessionCustomer = null;
    if (sessionToken) {
        const verified = verifyCustomerSessionToken(sessionToken);
        if (!verified.ok) {
            return res.status(401).json({ error: 'Invalid customer session. Please login again.' });
        }
        sessionCustomer = verified.customer;
    }

    // If customer session already exists, force lead identity to same email/phone.
    // This keeps admin panel attribution stable across repeat purchases.
    const emailNorm = sessionCustomer ? sessionCustomer.email : normalizeEmail(email);
    const phoneNorm = sessionCustomer ? sessionCustomer.phone : normalizePhone(phone);
    const emailToStore = sessionCustomer ? sessionCustomer.email : String(email || '').trim();
    const phoneToStore = sessionCustomer ? sessionCustomer.phone : String(phone || '').trim();
    if (!emailNorm || !phoneNorm) {
        return res.status(400).json({ error: 'Missing email or phone' });
    }
    const cleanKeys = Array.isArray(selectedKeys)
        ? Array.from(new Set(selectedKeys.map(k => String(k || '').trim()).filter(Boolean))).slice(0, 1000)
        : [];
    try {
        const lead = new Lead({
            phone: phoneToStore,
            phoneNorm,
            email: emailToStore,
            emailNorm,
            district,
            taluka,
            village,
            query,
            txnid,
            selectedKeys: cleanKeys,
            selectedLocationIds: {
                d: Number.isFinite(Number(d)) ? Number(d) : null,
                t: Number.isFinite(Number(t)) ? Number(t) : null,
                v: Number.isFinite(Number(v)) ? Number(v) : null,
            },
            selectedLocationEn: {
                de: String(de || ''),
                te: String(te || ''),
                ve: String(ve || ''),
            },
            propertyType: String(propertyType || ''),
            status: 'pending'
        });
        await lead.save();
        console.log(`[leads] New lead saved: ${phone} / ${txnid}`);
        const customerToken = issueCustomerSessionToken({ email: emailNorm, phone: phoneNorm });
        setCustomerSessionCookies(res, customerToken);
        res.json({ success: true, customerToken });
    } catch (err) {
        console.error('[leads] Save error:', err);
        // If txnid duplicate (user retrying), just return success
        if (err.code === 11000) {
            const customerToken = issueCustomerSessionToken({ email: emailNorm, phone: phoneNorm });
            setCustomerSessionCookies(res, customerToken);
            return res.json({ success: true, customerToken });
        }
        res.status(500).json({ error: 'Failed to save lead' });
    }
});

app.post('/api/customer/session/login', async (req, res) => {
    const emailNorm = normalizeEmail(req.body && req.body.email);
    const phoneNorm = normalizePhone(req.body && req.body.phone);
    if (!emailNorm || !phoneNorm) {
        return res.status(400).json({ error: 'Email and phone are required' });
    }
    const leadExists = await Lead.exists({ emailNorm, phoneNorm });
    if (!leadExists) {
        return res.status(401).json({ error: 'Customer not found' });
    }
    const customerToken = issueCustomerSessionToken({ email: emailNorm, phone: phoneNorm });
    setCustomerSessionCookies(res, customerToken);
    return res.json({
        ok: true,
        customerToken,
        customer: { email: emailNorm, phone: phoneNorm, emailMasked: maskEmail(emailNorm) },
    });
});

app.get('/api/customer/session/me', requireCustomerAuth, async (req, res) => {
    const emailNorm = req.customer.email;
    const phoneNorm = req.customer.phone;
    const paidCount = await Lead.countDocuments({ emailNorm, phoneNorm, status: 'paid' });
    return res.json({
        ok: true,
        customer: {
            email: emailNorm,
            phone: phoneNorm,
            emailMasked: maskEmail(emailNorm),
            hasPaidPurchases: paidCount > 0,
        },
    });
});

app.get('/api/customer/downloads', requireCustomerAuth, async (req, res) => {
    const emailNorm = req.customer.email;
    const phoneNorm = req.customer.phone;
    const leads = await Lead.find({ emailNorm, phoneNorm, status: 'paid' })
        .sort({ createdAt: -1 })
        .limit(200)
        .lean();
    const downloads = leads.map((lead) => ({
        txnid: lead.txnid,
        query: lead.query,
        district: lead.district,
        taluka: lead.taluka,
        village: lead.village,
        createdAt: lead.createdAt,
        keyCount: Array.isArray(lead.selectedKeys) ? lead.selectedKeys.length : 0,
        selectedLocationIds: lead.selectedLocationIds || null,
        selectedLocationEn: lead.selectedLocationEn || null,
    }));
    return res.json({ count: downloads.length, downloads });
});

app.post('/api/customer/downloads/:txnid/bootstrap', requireCustomerAuth, async (req, res) => {
    const txnid = String(req.params.txnid || '').trim();
    if (!txnid) return res.status(400).json({ error: 'Missing txnid' });
    const emailNorm = req.customer.email;
    const phoneNorm = req.customer.phone;
    const lead = await Lead.findOne({ txnid, emailNorm, phoneNorm, status: 'paid' }).lean();
    if (!lead) {
        return res.status(404).json({ error: 'Paid purchase not found for this customer' });
    }
    const keys = Array.isArray(lead.selectedKeys) ? lead.selectedKeys.filter(Boolean) : [];
    if (!keys.length) {
        return res.status(403).json({ error: 'No paid records were saved for this transaction' });
    }
    const { token } = issuePaymentToken(txnid);
    return res.json({
        txnid,
        token,
        records: keys.map((key) => ({ key })),
        ctx: {
            d: lead.selectedLocationIds && lead.selectedLocationIds.d != null ? String(lead.selectedLocationIds.d) : '',
            t: lead.selectedLocationIds && lead.selectedLocationIds.t != null ? String(lead.selectedLocationIds.t) : '',
            v: lead.selectedLocationIds && lead.selectedLocationIds.v != null ? String(lead.selectedLocationIds.v) : '',
            de: lead.selectedLocationEn && lead.selectedLocationEn.de ? String(lead.selectedLocationEn.de) : '',
            te: lead.selectedLocationEn && lead.selectedLocationEn.te ? String(lead.selectedLocationEn.te) : '',
            ve: lead.selectedLocationEn && lead.selectedLocationEn.ve ? String(lead.selectedLocationEn.ve) : '',
            district: String(lead.district || ''),
            taluka: String(lead.taluka || ''),
            village: String(lead.village || ''),
            query: String(lead.query || ''),
            propertyType: String(lead.propertyType || ''),
        }
    });
});

// ─── /api/admin/* (JWT + bcrypt; configure ADMIN_USER, ADMIN_PASS_HASH, ADMIN_JWT_SECRET) ─
// Generate password hash:  node tools/hash_admin_password.js "YourPassword"
app.post('/api/admin/login', async (req, res) => {
    if (!isAdminEnvConfigured()) {
        return res.status(503).json({ error: 'Admin login is not enabled (missing env config).' });
    }
    const clientId = getClientId(req);
    const windowCheck = checkAdminLoginWindow(clientId);
    if (!windowCheck.ok) {
        return res.status(429).json({
            error: 'Too many failed attempts. Try again later.',
            retryAfterSec: windowCheck.retryAfterSec
        });
    }
    const { username, password } = req.body || {};
    if (!username || !password) {
        recordAdminLoginFailure(clientId);
        return res.status(400).json({ error: 'Missing username or password' });
    }
    if (!safeEqualStr(username, process.env.ADMIN_USER || '')) {
        recordAdminLoginFailure(clientId);
        return res.status(401).json({ error: 'Invalid credentials' });
    }
    let passOk = false;
    try {
        passOk = await verifyPassword(String(password), process.env.ADMIN_PASS_HASH);
    } catch (e) {
        console.error('[api/admin/login] password check error', e && e.message);
        return res.status(500).json({ error: 'Server misconfiguration' });
    }
    if (!passOk) {
        recordAdminLoginFailure(clientId);
        return res.status(401).json({ error: 'Invalid credentials' });
    }
    clearAdminLoginWindow(clientId);
    const token = signAdminToken();
    return res.json({ token, expires: getJwtExpires() });
});

app.get('/api/admin/stats', requireAdminAuth, async (req, res) => {
    try {
        const [confirmed, totalLeads, pending, failed, emailed, emailFailed] = await Promise.all([
            Lead.countDocuments({ status: 'paid' }),
            Lead.countDocuments({}),
            Lead.countDocuments({ status: 'pending' }),
            // Keep failed for backward compatibility (may be 0 if you keep failures as pending).
            Lead.countDocuments({ status: 'failed' }),
            Lead.countDocuments({ reportEmailStatus: 'sent' }),
            Lead.countDocuments({ reportEmailStatus: 'failed' }),
        ]);
        // Backward compat fields:
        const purchased = confirmed;
        return res.json({ confirmed, purchased, totalLeads, pending, failed, emailed, emailFailed });
    } catch (err) {
        console.error('[api/admin/stats]', err);
        return res.status(500).json({ error: 'Failed to load stats' });
    }
});

app.get('/api/admin/leads', requireAdminAuth, async (req, res) => {
    const limit = Math.min(500, Math.max(1, parseInt(String(req.query.limit || '200'), 10) || 200));
    try {
        const leads = await Lead.find({})
            .sort({ createdAt: -1 })
            .limit(limit)
            .lean();
        return res.json({ count: leads.length, leads });
    } catch (err) {
        console.error('[api/admin/leads]', err);
        return res.status(500).json({ error: 'Failed to load leads' });
    }
});

app.get('/api/admin/search', requireAdminAuth, async (req, res) => {
    const { query } = req.query;
    const loc = getMrLocationFromParams(req.query);
    if (!loc) {
        return res.status(400).json({
            error: 'Missing location. Send d, t, v (ids) or district, taluka, village (Marathi).'
        });
    }
    const r = await searchPropertyIndex(loc, query);
    if (!r.ok) {
        if (r.filePath) {
            console.error('[api/admin/search] index read error', { filePath: r.filePath, error: r.error });
        }
        return res.status(r.status).json({ error: r.error, details: r.details || undefined });
    }
    const results = r.matched.map((row) => ({ ...row, key: reportData.fingerprint(row) }));
    return res.json({ count: results.length, results, queryNumber: r.queryNumber });
});

// PDF generation has been moved client-side; this endpoint is disabled.
app.post('/api/admin/report', requireAdminAuth, async (_req, res) => {
    return res.status(410).json({
        error: 'PDF generation moved to client side. This endpoint is deprecated.',
    });
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
