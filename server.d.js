#!/usr/bin/env node
const net = require('net'), path = require('path'), fs = require('fs'), os = require('os');
const https = require('https'), URL = require('url').URL;
//const zlib = require('zlib');

//Check for the command-line argument first
const portArg = process.argv.find((arg) => arg.startsWith('--port='));
const rawPort = portArg ? portArg.split('=').pop() : process.env.TCP_PORT;
const deviceDriverArg = process.argv.find((arg) => arg.startsWith('--driver='));
const deviceDriver = deviceDriverArg ? deviceDriverArg.split('=').pop() : null;
const deviceLabel = process.argv.find((arg) => arg.startsWith('--device=')) || "device=ASTM_K";


if( !rawPort || !deviceDriver ){
  console.error("Error: Port not specified. Provide --port=XXXX --driver=XXXX or set the `TCP_PORT` environment variable.");
  process.exit(1);
}

const branchArg = process.argv.find((arg) => arg.startsWith('--branch='));
const API_BRANCH = branchArg ? branchArg.split('=').pop() : process.env.API_BRANCH

// Group all required values to validate them in one clean pass
const requiredEnv = {
  TCP_PORT: parseInt(rawPort, 10),
  DEVICE_NAME: deviceLabel.split('=').pop(),
  API_HOST: process.env.API_HOST,
  API_TOKEN: process.env.API_TOKEN,
  API_BRANCH: API_BRANCH // Uses the resolved value from argument
};

const DRIVER = require('./driver.'+deviceDriver+'.js');

//FIX: Wire the query function so driver can respond to Maglumi sample queries
if(typeof DRIVER.setQueryFn == 'function') {
  DRIVER.setQueryFn( DRIVER.query );
}

const ipList = Object.values(os.networkInterfaces())
  .flatMap(iface => iface)
  .filter(info => (info.family == 'IPv4' || info.family === 4) && !info.internal)
  .map(info => info.address);

console.log("IP Address(es):", ipList);

const credentials = {
	token: requiredEnv.API_TOKEN,
	base_url: "https://" +requiredEnv.API_HOST+ "/legacy/api/log-astm.php",
	branch_id: requiredEnv.API_BRANCH
};

// Any failed query will Queue to local database, then API Call later.
const dbQueue = function(deviceName, json){
	console.log(deviceName, json);
};

const apiQueue = function(deviceName, logs, clientAddress){
  //FIX: Removed redundant second .replace('Z', '') — already handled by regex
  const timeStamp = (new Date()).toISOString()
		.replace('T', ' ')       // separate date & time
		.replace(/\.\d+Z$/, ''); // drop milliseconds and trailing Z

  return httpCallback(logs.join('\n'), {
		'Content-Type' : 'text/plain', "X-App": deviceName, "X-Total": logs.length,
		'X-Forwarded-Host' : clientAddress.split(':').pop(), 'X-Time' : timeStamp
	}).then(resp=>{
		console.log("apiQueue(txt):", resp.replace(/\r|\n/g, ', ').trim(), "for", logs.length, "rows.");
	}, ex=>console.error("apiQueue(text):",ex));

	/**Compressed stream for save bandwidth
	  zlib.gzip(logs.join('\n'), (err, compressedBuffer) => {
		if(err) return console.error(err.message);
		httpCallback(compressedBuffer, {
			'Content-Type' : 'text/x-gzip', "X-App": deviceName, "X-Total": logs.length,
			'X-Forwarded-Host' : clientAddress, 'X-Time' : timeStamp
		}).then(resp=>{
			console.log("apiQueue(gz):", resp.replace(/\r|\n/g, ', ').trim(), "for",logs.length,"rows.");
		}, ex=>console.error("apiQueue(gz):",ex));
	  });
	*/
 };

function httpCallback( postData, headerCols ){
	const cookies = "API=ERPCallback; VER=1.2.0; File="+escape(path.basename(__filename))+"; SOURCE=ASTM";
	postData = Buffer.from( postData );

	let options = {
        method: 'PUT',
        headers: {
			'Accept': 'text/html,application/json,application/xml;q=0.9,*/*;q=0.8',
			'Authorization': `Bearer ${credentials.token}`,
			'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:1.0) Gecko/20100101 Node/'+process.versions.node,
			'Cookie': cookies,
			'Content-Type': 'text/plain',
			'Content-Length': postData.length,
			'X-App': "MedicalD",
			'X-Branch': credentials.branch_id
        }
    };
	if( headerCols ) for(let col in headerCols) options.headers[col]=headerCols[col];

	  return new Promise((resolve, reject) => {
        let responseBody = '', bytesReceived = 0, endpoint = new URL(credentials.base_url);

        const req = https.request(credentials.base_url + "?filename=ASTMLog&source=" + path.basename(__filename), options, (res) => {
                res.on('data', (chunk) => {
                  responseBody += chunk;
                  bytesReceived += chunk.length;
                });
                res.on('end', ()=>{
                  if(res.statusCode != 200) console.error("Body:", responseBody, res.headers, "Bytes:", bytesReceived);
                  resolve( responseBody );
                });
                console.log('httpCallback():', `HTTP Response Code: ${res.statusCode}`);
        });
        req.on('finish', ()=>{
          //unused — available for debug
        });
        req.on('error', (error)=>{
                console.error("httpCallback("+endpoint.hostname+") API-endpoint:", error);
                reject( error.message );
        });

        req.write( postData );
        req.end(()=>console.log("httpCallback("+endpoint.hostname+"): processed, OK"));
  });
}


const server = DRIVER.start(requiredEnv.TCP_PORT, requiredEnv.DEVICE_NAME, dbQueue);

DRIVER.monitor.status = (device, info) => {
  console.log(`[${device}] STATUS`, info);
  if(info.event == 'EOT' && info.data){
	apiQueue(device, info.data, info.client);
  }
};
DRIVER.monitor.error = (device, err) => {
  console.error(`[${device}]`, err);
};

DRIVER.monitor.heartbeat = (device, state) => {
  // Optional
};

function shutdown( signal ){
  console.log(`${requiredEnv.DEVICE_NAME} Received ${signal}. Starting graceful shutdown...`);
  
  //Stop accepting new connections and close the TCP server
  server.close(function(){
    console.log('TCP/ASTM server on '+requiredEnv.TCP_PORT+' closed.');
    process.exit(0);
  });
  // Safety net: force exit if graceful shutdown hangs
  setTimeout(function(){ process.exit(1); }, 1500);
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