// NOTE:
// Server-side PDF generation has been removed.
// PDFs are generated in the browser and uploaded to `/api/report/email-upload` for emailing.

async function generatePropertyReportPdf({ records, ctx, assetsDir, chromeExecutablePath }) {
    void records; void ctx; void assetsDir; void chromeExecutablePath;
    throw new Error('Server-side PDF generation is disabled. Generate PDF in browser and upload for email.');
}

module.exports = {
    generatePropertyReportPdf,
};

