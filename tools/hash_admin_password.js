#!/usr/bin/env node
/**
 * Usage: node tools/hash_admin_password.js "YourStrongPassword"
 * Put the printed hash in .env as ADMIN_PASS_HASH=...
 */
const bcrypt = require('bcryptjs');

const pwd = process.argv[2];
if (!pwd || pwd.length < 8) {
    console.error('Usage: node tools/hash_admin_password.js "YourPassword" (min 8 chars)');
    process.exit(1);
}
const cost = 12;
bcrypt.hash(pwd, cost).then((hash) => {
    console.log(hash);
    process.exit(0);
}).catch((e) => {
    console.error(e);
    process.exit(1);
});
