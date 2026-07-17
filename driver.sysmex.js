'use strict';

const net = require('net'), fs = require('fs');

const CTRL = {
  STX: '\x02',
  ETX: '\x03',
  EOT: '\x04',
  ENQ: '\x05',
  ACK: '\x06',
  NAK: '\x15',
  ETB: '\x17'
};

const escapeChars = ['\x02','\x03','\x04','\x05', '\x06','\x15', '\x17'];

const replaceCTRL = function(s){
	return String(s).replace(/[\x03\x04]/g, '').replace(/[\x05\x02]/g, '').replace(/[\x06]/g, '');
};

let ipuQuery = () => console.debug("No HOST->IPU Query interface.");

exports.start = exports.boot = function (port, name, queueFn) {
  name = name || "Sysmex_XN";
  const serv = net.createServer(socket => handleClient(socket, name, queueFn));
  serv.listen(port, function(e){
	  console.log(`TCP server ${name}/LIS running port ${port}`);
  });
  return serv;
};

exports.query = async function(device, sampleId){
  // Fetch from DB / API / cache
  return {
    sampleId,
    tests: ['WBC', 'RBC', 'HGB']
  };
};

exports.monitor = {
  status: (device, info) =>
    console.debug(`[STATUS:${device}]`, info),

  error: (device, err) =>
    console.debug(`[ERROR:${device}]`, err),

  heartbeat: (device, state) =>
    console.debug(`[HEARTBEAT:${device}]`, state.connected ? 'ALIVE' : 'DOWN')
};


function emitStatus(name, info){
  try{
    exports.monitor?.status(name, info);
  }
  catch{}
}

function emitError(name, err){
  try{
    exports.monitor?.error(name, err);
  }
  catch{}
}

function emitHeartbeat(name, state){
  try{
    exports.monitor?.heartbeat(name, state);
  }
  catch{}
}


/* =========================
   CLIENT HANDLER
========================= */
function handleClient(socket, name, queueFn){
  let buffer = '', pendingQuery = null, expectedFrame = 1, rawFrame = [];
  
  const state = {connected: true, endPoint:socket.remoteAddress, lastDataTs: Date.now(), lastFrameTs: null, lastEotTs: null, errorCount: 0};

  console.log(`[${name}] Connected ${socket.remoteAddress}`);
  emitStatus(name, { event: 'CONNECTED', ts: Date.now(), client: socket.remoteAddress });

  socket.on('data', data => {
	state.lastDataTs = Date.now();
    try {
      const raw = data.toString('binary');
      fs.appendFileSync(`${name}.bin`, data);

	  //if(escapeChars.indexOf( data ) < 0)
	  rawFrame.push(replaceCTRL( data ));

      if(raw.includes( CTRL.ENQ )){
        socket.write(CTRL.ACK);
        return;
      }

	  /* ===== End of Terminator (Finish) ===== */
      if(raw.includes( CTRL.EOT ) || raw.includes("L|1|N\r")){
		  state.lastEotTs = Date.now();
		  emitStatus(name, {event: 'EOT', ts: state.lastEotTs, client: socket.remoteAddress, data: rawFrame.join('').split("\r") });
		  if(pendingQuery && ipuQuery){
			respondToQuery(socket, pendingQuery, name, ipuQuery);
			pendingQuery = null;
		  }
		  finalize(buffer, name, queueFn);

		  buffer = '';
		  rawFrame = [];
		  expectedFrame = 1;   // RESET HERE
		  return;
      }

	  //==Record received for processing
      if(raw.includes( CTRL.STX )){
		const frame = extractFrame(raw);
		if( !frame ){
			state.errorCount++;
			socket.write(CTRL.NAK);
			return;
		}

		/* ===== FRAME SEQUENCE CHECK ===== */
		if(frame.frameNo !== expectedFrame){
			console.error(
			  `[${name}] FRAME SEQ ERROR exp=${expectedFrame} got=${frame.frameNo} from ${socket.remoteAddress}`
			);
			socket.write(CTRL.NAK);
			state.errorCount++;
			emitError(name, {type: 'FRAME_ERROR', expected, received, ts: Date.now()});
			return;
		}

		/* ===== CHECKSUM CHECK ===== */
		const expected = calcChecksum(frame.frameNo + frame.payload);

		if(frame.checksum !== expected){
			console.error(`[${name}] CHECKSUM FAIL exp=${expected} got=${frame.checksum}`);
			socket.write( CTRL.NAK );
			let dt = new Date();
			fs.appendFileSync(`${name}.checksum.log`, `[${socket.remoteAddress}] ${frame.checksum} != ${expected} `, dt.toISOString()+"\n");
			return;
		}

		emitHeartbeat(name, state);//Respond

		if(frame.payload.startsWith('Q|')) {
		  const parts = frame.payload.split('|');
		  pendingQuery = parts[2]?.replace(/\^/g, '').trim();
		}

		/* ===== ACCEPT FRAME ===== */
		buffer += frame.payload.replace(/\x0D\x0A/g, '') + '\r';
		expectedFrame = nextFrame(expectedFrame);
		socket.write(CTRL.ACK);
      }

    }
	catch(err){
	  state.errorCount++;
      console.error(`[${name}] DATA ERROR`, err.message, "from:", socket.remoteAddress);
      socket.write(CTRL.NAK);
    }
  });

  socket.on('error', err => {
	state.errorCount++;
    console.error(`[${name}] SOCKET ERROR`, err.message, "from:", socket.remoteAddress);
	emitError(name, {type: 'TIMEOUT', lastDataTs: state.lastDataTs, ts: Date.now()});
  });
  
  socket.on('timeout', ()=>{
	state.errorCount++;
    console.error(`[${name}] SOCKET Timeout`, socket.remoteAddress);
	socket.end(); // Use end() to initiate a graceful shutdown
  });

  socket.on('close', ()=>{
	state.connected = false;
	emitStatus(name, { event: 'DISCONNECTED', ts: Date.now() });
	console.log(`[${name}] Connection closed`)
  });
}

/* =========================
   FINALIZE
========================= */
function finalize(raw, name, queueFn) {
  try {
    const json = parseASTM( raw );
    if( json.length ){
      console.log(`[${name}] Queued ${json.length} order(s)`);
      if(typeof(queueFn) != "undefined") queueFn.call(this, name, json);
    }
  }
  catch( err ){
    console.error(`[${name}] FINALIZE ERROR`, err);
  }
}


/* =========================
   CHECKSUM UTILITIES
========================= */
function calcChecksum(data) {
  let sum = 0;
  for (let i = 0; i < data.length; i++)
  {
    sum += data.charCodeAt(i);
  }
  return (sum & 0xFF)
    .toString(16)
    .toUpperCase()
    .padStart(2, '0');
}

function nextFrame(n) {
  return n >= 7 ? 1 : n + 1;
}


function extractFrame( raw ){
  const stx = raw.indexOf(CTRL.STX);
  if (stx === -1) return null;

  const etx = raw.indexOf(CTRL.ETX);
  const etb = raw.indexOf(CTRL.ETB);
  const end = etx !== -1 ? etx : etb;
  if (end === -1) return null;

  const body = raw.substring(stx + 1, end);   // "1H|..."
  const frameNo = parseInt(body[0], 10);

  if (isNaN(frameNo)) return null;

  const payload = body.substring(1);           // strip frame number
  const checksum = raw.substring(end + 1, end + 3);

  return { frameNo, payload, checksum };
}


// Talkback to IPU
async function respondToQuery(socket, sampleId, name, queryFn){
  try {
    const order = await queryFn(name, sampleId);
    if (!order) return;

    let frame = 1;
    const records = [];

    records.push(`H|\\^&|||LIS|||||||P|1`);
    records.push(
      `O|1|${order.sampleId}||${order.tests.map(t => `^^^${t}`).join('\\')}`
    );
    records.push(`L|1|N`);

    socket.write(CTRL.ENQ);

    for(const rec of records)
	{
      const payload = frame + rec;
      const cs = calcChecksum(payload);
      socket.write(
        CTRL.STX + payload + CTRL.ETX + cs + '\r\n'
      );
      frame = nextFrame(frame);
    }

    socket.write(CTRL.EOT);

    console.log(`[${name}] Sent query response to ${socket.remoteAddress} for ${sampleId}`);

  }catch(err){
    console.error(`[${name}] QUERY RESPONSE ERROR`, err, 'from', socket.remoteAddress);
  }
}


/* =========================
   SYSMEX XN PARSER: ASTM E1394
========================= */
function template() {
  return {
    header: {},
    patient: {},
    order: {
      sampleId: '',
      testDate: '',
      comments: []
    },
    results: []
  };
}

function parseASTM(raw) {

  const messages = [];
  let current = template();
  let lastResult = null;

  const lines = raw.split(/\r\n|\r|\n/);

  for (const line of lines)
  {
    if (!line.includes('|')) continue;

    const f = line.split('|');
    const type = f[0].replace(/[^A-Z]/g, '');

    switch (type)
	{

      /* ================= HEADER ================= */
      case 'H':
        current.header = {
          analyzer: f[4]?.trim(),
          version: f[12]
        };
        break;

      /* ================= PATIENT ================= */
      case 'P':
        current.patient.id = f[3] || '';
        break;

      /* ================= ORDER ================= */
      case 'O':
        const parts = (f[2] || f[3] || '').split('^');
        current.order.sampleId =
          parts.find(v => v.trim()) || '';
        break;

      /* ================= RESULT ================= */
      case 'R':
        lastResult = null;

        const test = f[2]?.split('^')[4];
        const dt = f[12];

        if (dt && !current.order.testDate && dt.length >= 14) {
          current.order.testDate =
            `${dt.slice(0,4)}-${dt.slice(4,6)}-${dt.slice(6,8)} ` +
            `${dt.slice(8,10)}:${dt.slice(10,12)}:${dt.slice(12,14)}`;
        }

        if(test && f[3]) {
          lastResult = {
            name: test,
            value: f[3],
            unit: f[4],
            flag: f[6] || 'N',
            comments: []
          };
          current.results.push(lastResult);
        }
        break;

      /* ================= COMMENT ================= */
      case 'C':
        const comment = f[3]?.trim();
        if (!comment) break;

        // Attach comment to LAST RESULT if exists
        if (lastResult) {
          lastResult.comments.push(comment);
        } 
        // Otherwise attach to ORDER
        else {
          current.order.comments =
            current.order.comments || [];
          current.order.comments.push(comment);
        }
        break;

      /* ================= TERMINATOR ================= */
      case 'L':
        messages.push(current);
        current = template();
        lastResult = null;
        break;

      /* ================= HOST -> IPU Query ========== */
      case 'Q':
		current.query = {sampleId: f[2]?.replace(/\^/g, '').trim()};
		break;
    }
  }

  return messages;
}