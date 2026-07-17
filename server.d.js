
const net = require('net'), path = require('path'), fs = require('fs');
const https = require('https'), zlib = require('zlib'), URL = require('url').URL;

//Check for the command-line argument first
const portArg = process.argv.find((arg) => arg.startsWith('--port='));
//Fallback to process.env.TCP_PORT if the argument isn't provided
const rawPort = portArg ? portArg.split('=')[1] : process.env.TCP_PORT;

//Check for traffic handle driver
const deviceDriver = process.argv.find((arg) => arg.startsWith('--driver=')).split('=').pop();
const deviceLabel = process.argv.find((arg) => arg.startsWith('--device=')) || "device=ASTM_K";


if( !rawPort || !deviceDriver ){
  console.error("Error: Port not specified. Provide --port=XXXX --driver=XXXX or set the `TCP_PORT` environment variable.");
  process.exit(1);
}

const branchArg = process.argv.find((arg) => arg.startsWith('--branch='));
// Fallback to process.env.API_BRANCH if the CLI argument isn't provided
const API_BRANCH = branchArg ? branchArg.split('=')[1] : process.env.API_BRANCH

// Group all required values to validate them in one clean pass
const requiredEnv = {
  TCP_PORT: parseInt(rawPort, 10),
  DEVICE_NAME: deviceLabel.split('=').pop(),
  API_HOST: process.env.API_HOST,
  API_TOKEN: process.env.API_TOKEN,
  API_BRANCH: API_BRANCH // Uses the resolved value from argument
};

//const maglumix3 = require('./maglumix3');
//const cobas = require('./cobas');
//const iFlash = require('./iFlash');

const DRIVER = require('./driver.'+deviceDriver+'.js');

const credentials = {
	token: requiredEnv.API_TOKEN,
	base_url: "https://" +requiredEnv.API_HOST+ "/legacy/api/log-astm.php",
	branch_id: requiredEnv.API_BRANCH
};

const dbQueue = function(deviceName, json){
	console.log(deviceName, json);
};

const apiQueue = function(deviceName, logs, clientAddress){
	const timeStamp = (new Date()).toISOString()
            .replace('T', ' ')   // separate date & time
            .replace(/\.\d+Z$/, '') // drop milliseconds and trailing Z
            .replace('Z', '');

  zlib.gzip(logs.join('\n'), (err, compressedBuffer) => {
	if(err) return console.error(err.message);
	httpCallback(compressedBuffer, {
		'Content-Type' : 'text/x-gzip', "X-App": deviceName, "X-Total": logs.length,
		'X-Forwarded-Host' : clientAddress, 'X-Time' : timeStamp
	}).then(resp=>{
		console.log("apiQueue(gz):", resp.replace(/\r|\n/g, ', ').trim(), "for",logs.length,"rows.");
	}, ex=>console.error("apiQueue(gz):",ex));
  });
 };

function httpCallback( postData, headerCols ){
	const cookies = "API=ERPCallback; VER=1.2.0; File="+escape(path.basename(__filename))+"; SOURCE=ASTM";
	//uri = new URL(credentials.base_url + "?filename=ASTM&source=RelyClient-MedicalD"),
	postData = Buffer.from( postData );

	let options = {
        //hostname: uri.hostname,
        //port: uri.port || (uri.protocol == 'https:' ? 443 : 80),
        //path: uri.pathname,
        method: 'PUT',
        headers: {
			'Accept': 'text/html,application/json,application/xml;q=0.9,*/*;q=0.8',
 			'Authorization': `Bearer ${credentials.token}`,
			'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:1.0) Gecko/20100101 Node/'+process.versions.node,
 			'Cookie': cookies,
            'Content-Type': 'text/x-gzip',
            'Content-Length': postData.length,
			'X-App': "MedicalD",
			'X-Branch': credentials.branch_id
        }
    };
	if( headerCols ) for(let col in headerCols) options.headers[col]=headerCols[col];

  return new Promise((resolve, reject) => {
	  
	  
	/* Debug purpose DUMP
	let rawHeaders = "";
	for(let h in options.headers){rawHeaders += h+': '+options.headers[h]+"\n";}
	return fs.writeFile(path.join(__dirname, 'output.csv'), Buffer.concat([Buffer.from(rawHeaders+"\n"),postData]), (err) => {
        if( err ) return reject( err );
		resolve('CSV successfully written to output.csv', 0);
    });
	*/
	let responseBody = '', bytesReceived = 0, endpoint = new URL(credentials.base_url);

	const req = https.request(credentials.base_url + "?filename=ASTMLog&source=" + path.basename(__filename), options, (res) => {
		//res.setEncoding('binary');
		res.on('data', (chunk) => {
		  responseBody += chunk;
		  bytesReceived += chunk.length;
		  //console.log("Resp:", chunk);
		});
		res.on('end', ()=>{
		  if(res.statusCode != 200) console.error("Body:", responseBody, res.headers, "Bytes:", bytesReceived);
		  resolve( responseBody );
		});
		console.log('httpCallback():', `HTTP Response Code: ${res.statusCode}`);
		//console.log('HEADERS: ', res.headers);
	});
	req.on('finish', ()=>{
	  //console.log(`Total ${req.socket.bytesWritten} bytes written to ${endpoint.pathname}`);
	});
	req.on('error', (error)=>{
		console.error("httpCallback("+endpoint.hostname+") API-endpoint:", error);
		reject( error.message );
	});
	
	//==Lower Level tracking
	/*req.on('socket', (socket) => {
	  socket.on('close', () => {
		console.log(`Connection closed, Total-payload: ${socket.bytesWritten} bytes`);
	  });
	});*/
	req.write( postData );
	req.end(()=>console.log("httpCallback("+endpoint.hostname+"): porcessed, OK"));
  });
}


/**
Device: Roche cobas e411 analyzer
Protocol: ASTM
Interface: RS-232C 
Baud rate: 9600bps
Config: 8-bit, 1 stop bit, no parity
Frame length: around 247 bytes

const devices = [
    { name: 'Sysmex_XN', port: 5001, handler: ASTM },
    { name: 'Roche_Cobas', port: 5002, handler: cobas },
    { name: 'Lifotronic_GH', port: 5003, handler: ASTM },
    { name: 'YHLO_iFlash', port: 5004, handler: iFlash },
    { name: 'Indiko_Plus', port: 5005, handler: ASTM },
    { name: 'Maglumi_X3', port: 6001, handler: ASTM }
];
*/

const server = DRIVER.start(requiredEnv.TCP_PORT, requiredEnv.DEVICE_NAME, dbQueue);

DRIVER.monitor.status = (device, info) => {
  // Push to dashboard / Prometheus / DB
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

//==Self daemon to receive command from SERVER
function handleCommand(line){
  const [cmd, ...args] = line.trim().split(/\s+/);
  switch (cmd.toUpperCase()) {
    case 'PING':
      return 'PONG';

    case 'ECHO':
      return args.join(' ');

    case 'TIME':
      return new Date().toISOString();

	case 'RELOAD':
		devices.forEach(d =>{
			d.handler.start(d.port, d.name, dbQueue);
		});
      return "Success!";

	// ---------- RESTART ----------
    case 'RESTART':
      // optional: give the client a quick ack before we exit
      setTimeout(() => {
        // exit cleanly; a supervisor will bring the process back up
        process.exit(0);
      }, 100);
      return 'OK: restarting';

    default:
      return `ERROR: unknown command "${cmd}"`;
  }
}
// -------------------------------------------------------------------------


function shutdown(signal) {
  console.log(`${requiredEnv.DEVICE_NAME} Received ${signal}. Starting graceful shutdown...`);
  
  // 1. Stop accepting new HTTP requests
  server.close((e) => {
    console.log('TCP/ASTM server on '+requiredEnv.TCP_PORT+' closed.');
    process.exit(0);

    // 2. Safely close database connections
    /*db.close().then(() => {
      console.log('Database connections closed.');
      
      // 3. Finally, exit the process safely
      process.exit(0);
    });*/
  });
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
