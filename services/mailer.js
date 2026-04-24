const nodemailer = require('nodemailer');

function parseBool(v) {
    return String(v || '').toLowerCase() === 'true';
}

function getSmtpConfigFromEnv() {
    return {
        host: process.env.SMTP_HOST || '',
        port: Number(process.env.SMTP_PORT || 587),
        secure: parseBool(process.env.SMTP_SECURE),
        user: process.env.SMTP_USER || '',
        pass: process.env.SMTP_PASS || '',
        from: process.env.SMTP_FROM || '',
        replyTo: process.env.SMTP_REPLY_TO || '',
        connectionTimeout: Number(process.env.SMTP_CONNECTION_TIMEOUT_MS || 15000),
    };
}

function validateSmtpConfig(cfg) {
    const missing = [];
    if (!cfg.host) missing.push('SMTP_HOST');
    if (!cfg.port) missing.push('SMTP_PORT');
    if (!cfg.user) missing.push('SMTP_USER');
    if (!cfg.pass) missing.push('SMTP_PASS');
    if (!cfg.from) missing.push('SMTP_FROM');
    return { ok: missing.length === 0, missing };
}

function createTransportFromEnv() {
    const cfg = getSmtpConfigFromEnv();
    const { ok, missing } = validateSmtpConfig(cfg);
    if (!ok) return { enabled: false, reason: `Missing SMTP vars: ${missing.join(', ')}` };

    const transport = nodemailer.createTransport({
        host: cfg.host,
        port: cfg.port,
        secure: cfg.secure,
        auth: { user: cfg.user, pass: cfg.pass },
        connectionTimeout: cfg.connectionTimeout,
    });
    return { enabled: true, transport, cfg };
}

async function sendReportEmail({ to, pdfBuffer, filename, ctx }) {
    const tx = createTransportFromEnv();
    if (!tx.enabled) return { sent: false, skipped: true, reason: tx.reason };

    await tx.transport.sendMail({
        from: tx.cfg.from,
        to,
        replyTo: tx.cfg.replyTo || undefined,
        subject: `Mahasuchi Property Search Report - ${ctx.query}`,
        text: [
            'Dear Customer,',
            '',
            'Your property search report is attached to this email.',
            `District: ${ctx.district}`,
            `Taluka: ${ctx.taluka}`,
            `Village: ${ctx.village}`,
            `Property No.: ${ctx.query}`,
            '',
            'Regards,',
            'Mahasuchi Team'
        ].join('\n'),
        attachments: [
            { filename, content: pdfBuffer, contentType: 'application/pdf' }
        ]
    });

    return { sent: true };
}

module.exports = {
    getSmtpConfigFromEnv,
    validateSmtpConfig,
    createTransportFromEnv,
    sendReportEmail,
};

