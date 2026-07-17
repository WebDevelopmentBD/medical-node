'use strict';

const net = require('net');
const fs = require('fs');

const CTRL = {
  STX: '\x02',
  ETX: '\x03',
  EOT: '\x04',
  ENQ: '\x05',
  ACK: '\x06',
  NAK: '\x15',
  ETB: '\x17'
};

exports.start = function (port, name, queueFn) {
  boot(port, name, queueFn);
};

/* =========================
   SERVER BOOT
========================= */
function boot(port, name, queueFn) {
  try {
    const server = net.createServer(socket =>
      handleClient(socket, name, queueFn)
    );

    server.on('error', err => {
      console.error(`[${name}] SERVER ERROR`, err.message);
      restart(port, name, queueFn);
    });

    server.listen(port, '0.0.0.0', () =>
      console.log(`[${name}] Listening on ${port}`)
    );

  } catch (err) {
    console.error(`[${name}] BOOT ERROR`, err);
    restart(port, name, queueFn);
  }
}

function restart(port, name, queueFn) {
  console.log(`[${name}] Restarting in 3s...`);
  setTimeout(() => boot(port, name, queueFn), 3000);
}

/* =========================
   CLIENT HANDLER
========================= */
function handleClient(socket, name, queueFn) {
  let buffer = '';

  console.log(`[${name}] Connected ${socket.remoteAddress}`);

  socket.on('data', data => {
    try {
      const raw = data.toString('binary');
      fs.appendFileSync(`${name}.bin`, raw);

      if(raw.includes( CTRL.ENQ )){
        socket.write(CTRL.ACK);
        return;
      }

      if (raw.includes(CTRL.EOT)) {
        finalize(buffer, name, queueFn);
        buffer = '';
        return;
      }

      if(raw.includes( CTRL.STX )){
		  const frame = extractFrame(raw);
		  if( !frame ){
			socket.write(CTRL.NAK);
			return;
		  }

		  const expected = calcChecksum(frame.payload), received = frame.checksum;

		  if(expected !== received){
			console.error(`[${name}] CHECKSUM FAIL exp=${expected} got=${received}`);
			socket.write( CTRL.NAK );
			let dt = new Date();
			fs.appendFileSync(`${name}.checksum.log`, `[${socket.remoteAddress}] ${received} != ${expected} `, dt.toISOString()+"\n");
			return;
		  }

		  buffer += frame.payload.replace(/\x0D\x0A/g, '') + '\r';
		  socket.write(CTRL.ACK);
      }

    } catch (err) {
      console.error(`[${name}] DATA ERROR`, err.message, "from:", socket.remoteAddress);
      socket.write(CTRL.NAK);
    }
  });

  socket.on('error', err =>
    console.error(`[${name}] SOCKET ERROR`, err.message, "from:", socket.remoteAddress)
  );

  socket.on('close', () =>
    console.log(`[${name}] Connection closed`)
  );
}

/* =========================
   FINALIZE
========================= */
function finalize(raw, name, queueFn) {
  try {
    const json = parseASTM( raw );
    if( json.length ){
      console.log(`[${name}] ✔ Queued ${json.length} order(s)`);
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

function extractFrame(raw) {
  const stx = raw.indexOf(CTRL.STX);
  if (stx === -1) return null;

  const etx = raw.indexOf(CTRL.ETX);
  const etb = raw.indexOf(CTRL.ETB);
  const end = etx !== -1 ? etx : etb;

  if (end === -1) return null;

  const payload = raw.substring(stx + 1, end);
  const checksum = raw.substring(end + 1, end + 3);

  return { payload, checksum };
}


/* =========================
   SYSMEX XN PARSER
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

  for (const line of lines) {
    if (!line.includes('|')) continue;

    const f = line.split('|');
    const type = f[0].replace(/[^A-Z]/g, '');

    switch (type) {

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

        if (test && f[3]) {
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
    }
  }

  return messages;
}