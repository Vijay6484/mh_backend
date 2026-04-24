const crypto = require('crypto');
const bcrypt = require('bcryptjs');
const jwt = require('jsonwebtoken');

const LOGIN_WINDOW_MS = 15 * 60 * 1000; // 15 minutes
const MAX_ATTEMPTS = 5;
const attemptStore = new Map();

function getClientId(req) {
    const xff = req.headers['x-forwarded-for'];
    if (xff) {
        const first = String(xff).split(',')[0].trim();
        if (first) return `ip:${first}`;
    }
    return `ip:${req.socket?.remoteAddress || 'local'}`;
}

/**
 * @returns {{ ok: true, record: { count: number, resetAt: number } } | { ok: false, retryAfterSec: number }}
 */
function checkAdminLoginWindow(clientId) {
    const now = Date.now();
    let rec = attemptStore.get(clientId);
    if (!rec || now > rec.resetAt) {
        rec = { count: 0, resetAt: now + LOGIN_WINDOW_MS };
        attemptStore.set(clientId, rec);
    }
    if (rec.count >= MAX_ATTEMPTS) {
        return { ok: false, retryAfterSec: Math.max(0, Math.ceil((rec.resetAt - now) / 1000)) };
    }
    return { ok: true, record: rec };
}

function recordAdminLoginFailure(clientId) {
    const rec = attemptStore.get(clientId);
    if (rec) rec.count += 1;
}

function clearAdminLoginWindow(clientId) {
    attemptStore.delete(clientId);
}

function isAdminEnvConfigured() {
    return Boolean(
        process.env.ADMIN_USER
        && process.env.ADMIN_PASS_HASH
        && process.env.ADMIN_JWT_SECRET
    );
}

function getJwtExpires() {
    return process.env.ADMIN_JWT_EXPIRES || '8h';
}

/**
 * @param {string} plain
 * @param {string} hash
 * @returns {Promise<boolean>}
 */
function verifyPassword(plain, hash) {
    if (!hash || !plain) return false;
    return bcrypt.compare(plain, hash);
}

/**
 * @returns {string}
 */
function signAdminToken() {
    return jwt.sign(
        { t: 'admin' },
        process.env.ADMIN_JWT_SECRET,
        { expiresIn: getJwtExpires(), algorithm: 'HS256' }
    );
}

/**
 * @param {string} token
 * @returns {object | null}
 */
function verifyAdminToken(token) {
    if (!process.env.ADMIN_JWT_SECRET) return null;
    try {
        return jwt.verify(token, process.env.ADMIN_JWT_SECRET, { algorithms: ['HS256'] });
    } catch {
        return null;
    }
}

/**
 * @param {import('express').Request} req
 * @param {import('express').Response} res
 * @param {import('express').NextFunction} next
 */
function requireAdminAuth(req, res, next) {
    const h = req.headers.authorization || '';
    if (!h.startsWith('Bearer ')) {
        return res.status(401).json({ error: 'Unauthorized' });
    }
    const token = h.slice(7).trim();
    if (!token) {
        return res.status(401).json({ error: 'Unauthorized' });
    }
    const payload = verifyAdminToken(token);
    if (!payload || payload.t !== 'admin') {
        return res.status(401).json({ error: 'Invalid or expired session' });
    }
    return next();
}

/**
 * Constant-time string compare (same length only).
 */
function safeEqualStr(a, b) {
    const sa = String(a);
    const sb = String(b);
    if (sa.length !== sb.length) return false;
    return crypto.timingSafeEqual(Buffer.from(sa, 'utf8'), Buffer.from(sb, 'utf8'));
}

module.exports = {
    isAdminEnvConfigured,
    getClientId,
    checkAdminLoginWindow,
    recordAdminLoginFailure,
    clearAdminLoginWindow,
    verifyPassword,
    signAdminToken,
    verifyAdminToken,
    requireAdminAuth,
    safeEqualStr,
    getJwtExpires,
};

