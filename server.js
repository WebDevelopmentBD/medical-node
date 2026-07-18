const http = require('http');

// Parse --port=XXXX from argv, default to 5000
const portArg = process.argv.find((arg) => arg.startsWith('--port='));
const PORT = portArg ? parseInt(portArg.split('=')[1], 10) : 5000;

// Print all environment Key => Value pairs to the log on every connection.
// TEST/DEBUG ONLY — remove this and the /env endpoint before running
// anything with real secrets in its environment.
function logEnv(context) {
  console.log(`--- Environment on ${context} ---`);
  for (const [key, value] of Object.entries(process.env)) {
    console.log(`${key} => ${value}`);
  }
  console.log('--- end environment ---');
}

const server = http.createServer((req, res) => {
  logEnv(`connection to ${req.url}`);

  if (req.url == '/health') {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ status: 'ok', port: PORT, uptime: process.uptime() }));
    return;
  }

  if (req.url == '/env') {
    // TEST/DEBUG ONLY — strip this route for production
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify(process.env, null, 2));
    return;
  }

  if (req.url == '/net') {
    // TEST/DEBUG ONLY — strip this route for production
	const localAddress = Object.values(require('os').networkInterfaces()).flatMap(iface => iface).map(info => info.address);
	let responseBody = '', bytesReceived = 0;
    res.writeHead(200, { 'Content-Type': 'application/json' });
	const httpReq = require('https').request("https://ipinfo.io/ip", (resp) => {
		resp.setEncoding('utf8');
		resp.on('data', (chunk) => {
			responseBody  += chunk;
			bytesReceived += chunk.length;
		});
		resp.on('end', ()=>{
		  if(res.statusCode != 200) console.error("Body:", responseBody, resp.headers, "Bytes:", bytesReceived);
		  res.end(JSON.stringify([localAddress, responseBody.trim()], null, 2));
		});
		console.log('net/https:', `HTTP Response Code: ${resp.statusCode}`, `${responseBody}`);
	});
	httpReq.on('error', (ex)=>{
		console.error("net/https:", ex);
		res.end(JSON.stringify([localAddress, "Remote request fail!"], null, 2));
	});
	httpReq.end(()=> console.log("net/https, request sent"));
    return;
  }

  res.writeHead(200, { 'Content-Type': 'text/plain' });
  res.end(`Hello from data-broker on port ${PORT}\n`);
});

server.listen(PORT, '0.0.0.0', () => {
  console.log(`Server listening on 0.0.0.0:${PORT}`);
  logEnv('startup');
});

// Graceful shutdown — lets tini/RouterOS stop-signal actually work as intended
function shutdown(signal) {
  console.log(`Received ${signal}, shutting down gracefully`);
  server.close(() => {
    console.log('Server closed');
    process.exit(0);
  });
  // Force-exit if close() hangs longer than RouterOS's stop-signal timeout (10s)
  setTimeout(() => process.exit(1), 8000).unref();
}

process.on('SIGTERM', () => shutdown('SIGTERM'));
process.on('SIGINT', () => shutdown('SIGINT'));

// Don't let an uncaught error hang silently — exit and let auto-restart-interval catch it
process.on('uncaughtException', (err) => {
  console.error('Uncaught exception:', err);
  process.exit(1);
});
process.on('unhandledRejection', (reason) => {
  console.error('Unhandled rejection:', reason);
  process.exit(1);
});
