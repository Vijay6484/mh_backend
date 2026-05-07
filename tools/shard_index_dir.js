/**
 * Shard an existing village `data.json` index into `<gut>.json` files.
 *
 * Input layout (existing):
 *   <indexDir>/<district>/<taluka>/<village>/data.json
 *
 * Output layout (new, fast lookup):
 *   <indexDir>/<district>/<taluka>/<village>/<gut>.json
 *
 * Notes:
 * - Only shards `property_numbers` entries where type === "gut_number"
 * - "540/2/3" and "128-2" shard into "540.json" and "128.json"
 * - A document can be written into multiple gut files if it mentions multiple gut numbers
 */
const fs = require('fs');
const path = require('path');

function baseGutNumber(raw) {
  const s = String(raw || '').trim();
  const m = s.match(/^(\d+)/);
  return m ? m[1] : '';
}

function atomicWriteJson(filePath, obj) {
  const tmp = `${filePath}.tmp`;
  fs.writeFileSync(tmp, JSON.stringify(obj, null, 2), 'utf8');
  fs.renameSync(tmp, filePath);
}

function shardVillageFile(dataPath) {
  const dir = path.dirname(dataPath);
  const raw = fs.readFileSync(dataPath, 'utf8');
  const arr = JSON.parse(raw);
  if (!Array.isArray(arr)) {
    throw new Error(`Expected top-level array: ${dataPath}`);
  }

  /** @type {Record<string, any[]>} */
  const byGut = {};

  for (const doc of arr) {
    const props = doc && doc.property_numbers;
    if (!Array.isArray(props)) continue;
    const guts = new Set();
    for (const p of props) {
      if (!p || p.type !== 'gut_number') continue;
      const g = baseGutNumber(p.value);
      if (g) guts.add(g);
    }
    if (!guts.size) continue;
    for (const g of guts) {
      (byGut[g] ||= []).push(doc);
    }
  }

  const guts = Object.keys(byGut).sort((a, b) => Number(a) - Number(b));
  for (const g of guts) {
    const outPath = path.join(dir, `${g}.json`);
    atomicWriteJson(outPath, byGut[g]);
  }

  return { dir, wrote: guts.length, guts };
}

function walkForVillageDataJson(rootDir) {
  /** @type {string[]} */
  const found = [];
  /** @type {string[]} */
  const stack = [rootDir];
  while (stack.length) {
    const cur = stack.pop();
    if (!cur) break;
    let entries = [];
    try {
      entries = fs.readdirSync(cur, { withFileTypes: true });
    } catch {
      continue;
    }
    for (const e of entries) {
      const full = path.join(cur, e.name);
      if (e.isDirectory()) stack.push(full);
      else if (e.isFile() && e.name === 'data.json') found.push(full);
    }
  }
  return found;
}

function main() {
  const indexDir = process.argv[2] ? path.resolve(process.argv[2]) : path.resolve(__dirname, '..', 'indexed_data_dummy');
  if (!fs.existsSync(indexDir)) {
    console.error(`❌ indexDir not found: ${indexDir}`);
    process.exitCode = 1;
    return;
  }

  const files = walkForVillageDataJson(indexDir);
  if (!files.length) {
    console.warn(`⚠️ No data.json files found under: ${indexDir}`);
    return;
  }

  console.log(`🔎 Found ${files.length} village data.json file(s).`);
  let totalWrote = 0;
  for (const fp of files) {
    try {
      const r = shardVillageFile(fp);
      totalWrote += r.wrote;
      console.log(`✅ Sharded ${fp} → wrote ${r.wrote} file(s): ${r.guts.join(', ') || 'none'}`);
    } catch (e) {
      console.warn(`⚠️ Failed to shard ${fp}: ${e && e.message ? e.message : e}`);
    }
  }
  console.log(`\n✨ Done. Total gut files written: ${totalWrote}`);
}

if (require.main === module) {
  main();
}

