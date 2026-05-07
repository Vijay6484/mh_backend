// AWS Lambda entrypoint for this Express backend.
// Use with API Gateway (HTTP API or REST API) using Lambda proxy integration.
const serverless = require('serverless-http');

let cachedHandler = null;

function getHandler() {
    if (cachedHandler) return cachedHandler;
    // Import the Express app without starting a listener.
    const { app } = require('./server');
    cachedHandler = serverless(app);
    return cachedHandler;
}

module.exports.handler = async (event, context) => {
    // Because we keep long-lived connections (Mongo), let Lambda freeze the process
    // without waiting for the event loop to empty.
    context.callbackWaitsForEmptyEventLoop = false;
    return await getHandler()(event, context);
};

