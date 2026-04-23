#!/usr/bin/env node
/**
 * Reads Backend/locations.json (nested { district: { taluka: [villages] } })
 * and writes Backend/locations_index.json with stable numeric ids + English labels
 * (IT transliteration via Sanscript, title-cased).
 *
 * Usage: node tools/build_locations_index.js
 */
const fs = require('fs');
const path = require('path');
const Sanscript = require('@indic-transliteration/sanscript');

const ROOT = path.join(__dirname, '..');
const INPUT = path.join(ROOT, 'locations.json');
const OUTPUT = path.join(ROOT, 'locations_index.json');

function toEnglishLabel(mr) {
    const s = String(mr ?? '').trim();
    if (!s) return '';
    try {
        const raw = Sanscript.t(s, 'devanagari', 'itrans').toLowerCase();
        return raw
            .split(/(\s+|[-/,()])/)
            .map((part) => {
                if (!part || /^\s+$/.test(part)) return part;
                if (/^[-/,()]$/.test(part)) return part;
                return part.charAt(0).toUpperCase() + part.slice(1);
            })
            .join('');
    } catch {
        return s;
    }
}

function main() {
    const raw = fs.readFileSync(INPUT, 'utf8');
    const legacy = JSON.parse(raw);

    const districts = Object.keys(legacy).sort((a, b) => a.localeCompare(b, 'mr'));
    const out = {
        version: 1,
        generated: new Date().toISOString(),
        districts: [],
    };

    let dId = 0;
    for (const dMr of districts) {
        dId += 1;
        const talukaMap = legacy[dMr] || {};
        const talukaNames = Object.keys(talukaMap).sort((a, b) => a.localeCompare(b, 'mr'));
        const talukas = [];
        let tId = 0;
        for (const tMr of talukaNames) {
            tId += 1;
            const villagesMr = Array.isArray(talukaMap[tMr]) ? talukaMap[tMr] : [];
            const villages = [];
            let vId = 0;
            const vSorted = [...villagesMr].sort((a, b) => String(a).localeCompare(String(b), 'mr'));
            for (const vMr of vSorted) {
                vId += 1;
                villages.push({
                    id: vId,
                    mr: vMr,
                    en: toEnglishLabel(vMr),
                });
            }
            talukas.push({
                id: tId,
                mr: tMr,
                en: toEnglishLabel(tMr),
                villages,
            });
        }
        out.districts.push({
            id: dId,
            mr: dMr,
            en: toEnglishLabel(dMr),
            talukas,
        });
    }

    const tmp = OUTPUT + '.tmp';
    fs.writeFileSync(tmp, JSON.stringify(out, null, 2), 'utf8');
    fs.renameSync(tmp, OUTPUT);
    console.log(`Wrote ${OUTPUT} (${out.districts.length} districts)`);
}

main();
