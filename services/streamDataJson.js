/**
 * Stream-parse top-level JSON arrays in data.json without loading the whole file.
 * Fixes V8 max string / memory limits for very large village files (e.g. 500MB+).
 */
const fs = require('fs');
const { chain } = require('stream-chain');
const { parser } = require('stream-json');
const streamArray = require('stream-json/streamers/stream-array.js');

/**
 * @param {string} filePath
 * @param {(record: object) => void} onRecord - called for each array element (object)
 * @returns {Promise<void>}
 */
function forEachRecordInDataJson(filePath, onRecord) {
    return new Promise((resolve, reject) => {
        const pipeline = chain([
            fs.createReadStream(filePath, { highWaterMark: 1024 * 1024 }),
            parser(),
            streamArray(),
        ]);
        pipeline.on('data', (chunk) => {
            try {
                const value = chunk && chunk.value;
                if (value && typeof value === 'object' && !Array.isArray(value)) {
                    onRecord(value);
                }
            } catch (e) {
                reject(e);
            }
        });
        pipeline.on('end', () => resolve());
        pipeline.on('error', (err) => reject(err));
    });
}

module.exports = { forEachRecordInDataJson };
