const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const { forEachRecordInDataJson } = require('./streamDataJson');

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

function normalizeFsKey(s) {
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

function fingerprint(record) {
    const parts = IDENTITY_FIELDS.map(k => canonicalize(record ? record[k] : undefined));
    return crypto.createHash('sha256').update(parts.join('|')).digest('base64url');
}

function dedupeByKey(records) {
    const seen = new Set();
    const out = [];
    for (const r of records || []) {
        if (!r) continue;
        const key = r.key || fingerprint(r);
        if (seen.has(key)) continue;
        seen.add(key);
        out.push({ ...r, key });
    }
    return out;
}

async function loadRecordsByContext({ indexDir, district, taluka, village, query }) {
    if (!district || !taluka || !village || !query) {
        const e = new Error('Missing required parameters');
        e.status = 400;
        throw e;
    }

    if (!fs.existsSync(indexDir)) {
        const e = new Error('Index directory not found on server');
        e.status = 500;
        e.details = { indexDir };
        throw e;
    }

    const resolvedDistrict = resolveDirEntry(indexDir, district);
    if (!resolvedDistrict) {
        const e = new Error('District not found in index');
        e.status = 404;
        throw e;
    }
    const districtDir = path.join(indexDir, resolvedDistrict);

    const resolvedTaluka = resolveDirEntry(districtDir, taluka);
    if (!resolvedTaluka) {
        const e = new Error('Taluka not found in index');
        e.status = 404;
        throw e;
    }
    const talukaDir = path.join(districtDir, resolvedTaluka);

    const resolvedVillage = resolveDirEntry(talukaDir, village);
    if (!resolvedVillage) {
        const e = new Error('Village not found in index');
        e.status = 404;
        throw e;
    }

    const filePath = path.join(talukaDir, resolvedVillage, 'data.json');
    if (!fs.existsSync(filePath)) {
        const e = new Error('Data file not found for the selected location');
        e.status = 404;
        e.details = { filePath };
        throw e;
    }

    const queryMatch = String(query).trim().match(/\d+/);
    if (!queryMatch) {
        const e = new Error('Query must contain a number');
        e.status = 400;
        throw e;
    }
    const queryNumber = queryMatch[0];

    const matched = [];
    try {
        await forEachRecordInDataJson(filePath, (record) => {
            if (!Array.isArray(record.property_numbers)) return;
            let isMatch = false;
            for (const prop of record.property_numbers) {
                const baseNumber = String(prop.value).split(/[\/\-]/)[0].trim();
                if (baseNumber === String(queryNumber)) { isMatch = true; break; }
            }
            if (!isMatch) return;
            matched.push({ ...record, key: fingerprint(record) });
        });
    } catch (err) {
        const e = new Error(
            err && (err.message || '').includes('Top-level object should be an array')
                ? 'Indexed data file must be a JSON array'
                : 'Failed to read indexed data file'
        );
        e.status = 422;
        e.details = { filePath, cause: err && err.message };
        throw e;
    }

    return {
        queryNumber,
        filePath,
        resolved: { district: resolvedDistrict, taluka: resolvedTaluka, village: resolvedVillage },
        matched,
    };
}

module.exports = {
    IDENTITY_FIELDS,
    normalizeFsKey,
    resolveDirEntry,
    fingerprint,
    dedupeByKey,
    loadRecordsByContext,
};

