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

let ipuQuery = () => console.debug("No HOST->IPU Query interface.");

exports.start = exports.boot = function (port, name, queueFn) {
  name = name || "Maglumi_X3";
  const serv = net.createServer(socket => handleClient(socket, name, queueFn));
  serv.listen(port, '0.0.0.0', function(e){
          console.log(`TCP server ${name}/LIS running port ${port}`);
  });
  return serv;
};

exports.query = async function(device, sampleId){
  /* Fetch from DB / API / cache — override via setQueryFn() */
  return {
    sampleId,
    tests: ['WBC', 'RBC', 'HGB']
  };
};

/**
 * Wire a real query function so respondToQuery can fetch orders.
 * Call from server.d.js:  DRIVER.setQueryFn(DRIVER.query);
 */
exports.setQueryFn = function(fn){
  ipuQuery = fn;
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
  try{ exports.monitor?.status(name, info); } catch(e){}
}
function emitError(name, err){
  try{ exports.monitor?.error(name, err); } catch(e){}
}
function emitHeartbeat(name, state){
  try{ exports.monitor?.heartbeat(name, state); } catch(e){}
}


/* =========================
   Maglumi CLIENT HANDLER
   ========================= */
function handleClient(socket, name, queueFn){

  let buffer = '';
  let pendingQuery = null;
  let expectedFrame = 1;
  let rawFrame = [];

  /* String-based TCP reassembly buffer.
     Accumulates binary string chunks so that frames split across
     multiple TCP segments are reassembled before extraction. */
  let tcpBuf = '';

  /* Persistent binary log stream (non-blocking) */
  let logStream = null;
  try {
    logStream = fs.createWriteStream(`${name}.bin`, { flags: 'a' });
    logStream.on('error', function() { logStream = null; });
  } catch(e) { logStream = null; }

  /* Outbound transmission state (for query responses) */
  let txState    = 'idle';   // 'idle' | 'enq_sent' | 'frame_sent'
  let txFrames   = [];
  let txFrameIdx = 0;

  const state = {
    connected: true, endPoint: socket.remoteAddress,
    lastDataTs: Date.now(), lastFrameTs: null, lastEotTs: null, errorCount: 0
  };

  console.log(`[${name}] Connected ${socket.remoteAddress}`);
  emitStatus(name, { event: 'CONNECTED', ts: Date.now(), client: socket.remoteAddress });

  socket.on('data', function(data) {
    state.lastDataTs = Date.now();
    try {
      const raw = data.toString('binary');

      /* Log every raw byte for investigation */
      if (logStream) logStream.write(data);

      /* Append to reassembly buffer */
      tcpBuf += raw;

      /* ============================================================
         PROCESSING ORDER IS CRITICAL:
           Phase 1 — ACK / NAK  (responses to OUR outbound frames)
           Phase 2 — ENQ        (device requests to send)
           Phase 3 — STX frames (extract, validate, accumulate data)
           Phase 4 — EOT        (end-of-transmission, AFTER frames)
         ============================================================ */

      /* === PHASE 1: Handle ACK from device (responses to our outbound) === */
      while (tcpBuf.indexOf(CTRL.ACK) !== -1) {
        tcpBuf = tcpBuf.replace(CTRL.ACK, '');
        if (txState === 'enq_sent') {
          if (txFrames.length > 0) {
            socket.write(txFrames[0]);
            txState = 'frame_sent';
          } else {
            socket.write(CTRL.EOT);
            txState = 'idle';
          }
        } else if (txState === 'frame_sent') {
          if (txFrameIdx + 1 < txFrames.length) {
            txFrameIdx++;
            socket.write(txFrames[txFrameIdx]);
          } else {
            socket.write(CTRL.EOT);
            txState = 'idle';
          }
        }
      }

      /* === PHASE 1b: Handle NAK from device === */
      while (tcpBuf.indexOf(CTRL.NAK) !== -1) {
        tcpBuf = tcpBuf.replace(CTRL.NAK, '');
        if (txState === 'enq_sent') {
          socket.write(CTRL.ENQ);
        } else if (txState === 'frame_sent') {
          if (txFrameIdx < txFrames.length) {
            socket.write(txFrames[txFrameIdx]);
          }
        }
      }

      /* === PHASE 2: Handle ENQ from device === */
      while (tcpBuf.indexOf(CTRL.ENQ) !== -1) {
        tcpBuf = tcpBuf.replace(CTRL.ENQ, '');
        if (txState === 'idle') {
          socket.write(CTRL.ACK);
        }
      }

      /* === PHASE 3+4: Extract frames AND handle EOT between transmissions ===
         When the device sends multiple transmissions in a single TCP chunk
         (e.g. STX..ETX EOT ENQ STX..ETX EOT ...), the EOTs between frames
         must be honored to finalize each transmission individually. */

      /* Helper: finalize the current transmission batch */
      function flushTransmission() {
        state.lastEotTs = Date.now();

        /* Emit EOT event with raw ASTM record lines */
        emitStatus(name, {
          event: 'EOT',
          ts: state.lastEotTs,
          client: socket.remoteAddress,
          data: rawFrame.length ? rawFrame.join('').split("\r") : []
        });

        /* If device sent a Q record, respond with test selection */
        if (pendingQuery && ipuQuery) {
          var sid = pendingQuery;
          pendingQuery = null;
          respondToQuery(socket, sid, name, ipuQuery, function onReady(frames) {
            txFrames   = frames;
            txFrameIdx = 0;
            socket.write(CTRL.ENQ);
            txState = 'enq_sent';
          });
        }

        finalize(buffer, name, queueFn);

        buffer = '';
        rawFrame = [];
        expectedFrame = 1;
      }

      /* Main extraction loop — handles multiple transmissions per chunk */
      while (true) {

        /* Discard any bytes before the next STX */
        var stxPos = tcpBuf.indexOf(CTRL.STX);
        if (stxPos === -1) break;  /* no more frame data */

        if (stxPos > 0) {
          /* Check if an EOT is sitting in the gap (end of previous transmission) */
          var gap = tcpBuf.substring(0, stxPos);
          if (gap.indexOf(CTRL.EOT) !== -1) {
            tcpBuf = tcpBuf.replace(CTRL.EOT, '');
            flushTransmission();
            continue;  /* re-scan after flush & reset */
          }
          console.warn(`[${name}] Skipping ${stxPos} byte(s) before STX`);
          tcpBuf = tcpBuf.substring(stxPos);
        }

        /* Try to extract a complete frame */
        var frame = extractFrame(tcpBuf);
        if (!frame) break;  /* incomplete frame — wait for more data */

        /* ===== FRAME SEQUENCE CHECK ===== */
        if (frame.frameNo !== expectedFrame) {
          console.error(
            `[${name}] FRAME SEQ ERROR exp=${expectedFrame} got=${frame.frameNo} from ${socket.remoteAddress}`
          );
          socket.write(CTRL.NAK);
          state.errorCount++;
          emitError(name, {type: 'FRAME_ERROR', expected: expectedFrame, received: frame.frameNo, ts: Date.now()});
          tcpBuf = tcpBuf.substring(frame.consumeEnd);
          continue;
        }

        /* ===== CHECKSUM CHECK (only when device sends a checksum) ===== */
        if (frame.hasChecksum) {
          var csExpected = calcChecksum(frame.body);
          if (frame.checksum !== csExpected) {
            console.error(`[${name}] CHECKSUM FAIL exp=${csExpected} got=${frame.checksum}`);
            socket.write(CTRL.NAK);
            state.errorCount++;
            var dt = new Date();
            fs.appendFileSync(`${name}.checksum.log`,
              `[${socket.remoteAddress}] ${frame.checksum} != ${csExpected} ` + dt.toISOString() + "\n");
            tcpBuf = tcpBuf.substring(frame.consumeEnd);
            continue;
          }
        }

        emitHeartbeat(name, state);
        state.lastFrameTs = Date.now();

        /* ===== DETECT QUERY RECORD (Q) ===== */
        if (frame.payload.startsWith('Q|')) {
          var parts = frame.payload.split('|');
          pendingQuery = parts[2] ? parts[2].split('^')[0].trim() : '';
        }

        /* ===== ACCEPT FRAME ===== */
        rawFrame.push(frame.payload.replace(/\x0D\x0A/g, ''));
        buffer += frame.payload.replace(/\x0D\x0A/g, '') + '\r';

        /* Remove the processed frame from tcpBuf */
        tcpBuf = tcpBuf.substring(frame.consumeEnd);

        expectedFrame = nextFrame(expectedFrame);
        socket.write(CTRL.ACK);
      }

      /* Handle trailing EOT (last transmission in this chunk) */
      if (tcpBuf.indexOf(CTRL.EOT) !== -1) {
        tcpBuf = tcpBuf.replace(CTRL.EOT, '');
        flushTransmission();
      }

    }
    catch(err) {
      state.errorCount++;
      console.error(`[${name}] DATA ERROR`, err.message, "from:", socket.remoteAddress);
      socket.write(CTRL.NAK);
    }
  });

  socket.on('error', function(err) {
    state.errorCount++;
    console.error(`[${name}] SOCKET ERROR`, err.message, "from:", socket.remoteAddress);
    emitError(name, {type: 'SOCKET_ERROR', message: err.message, lastDataTs: state.lastDataTs, ts: Date.now()});
  });

  socket.on('timeout', function() {
    state.errorCount++;
    console.error(`[${name}] SOCKET Timeout`, socket.remoteAddress);
    socket.end();
  });

  socket.on('close', function() {
    state.connected = false;
    emitStatus(name, { event: 'DISCONNECTED', ts: Date.now() });
    console.log(`[${name}] Connection closed`);
    if (logStream) logStream.end();
  });
}


/* =========================
   FINALIZE
   ========================= */
function finalize(raw, name, queueFn) {
  try {
    var json = parseASTM(raw);
    if (json.length) {
      console.log(`[${name}] Queued ${json.length} order(s)`);
      if (typeof(queueFn) != "undefined") queueFn.call(this, name, json);
    }
  }
  catch(err) {
    console.error(`[${name}] FINALIZE ERROR`, err);
  }
}


/* =========================
   CHECKSUM UTILITIES
   ========================= */
function calcChecksum(data) {
  var sum = 0;
  for (var i = 0; i < data.length; i++) {
    sum += data.charCodeAt(i);
  }
  return (sum & 0xFF).toString(16).toUpperCase().padStart(2, '0');
}

function nextFrame(n) {
  return n >= 7 ? 1 : n + 1;
}

/* Check if a byte value (charCode) is an ASCII hex digit [0-9A-Fa-f] */
function isHexByte(b) {
  return (b >= 0x30 && b <= 0x39)   /* 0-9 */
      || (b >= 0x41 && b <= 0x46)  /* A-F */
      || (b >= 0x61 && b <= 0x66); /* a-f */
}


/* =========================
   FRAME EXTRACTION
   ========================= */
function extractFrame(raw) {
  var stxIdx = raw.indexOf(CTRL.STX);
  if (stxIdx === -1) return null;

  /* Find frame terminator (ETX or ETB) AFTER the STX */
  var etxIdx = raw.indexOf(CTRL.ETX, stxIdx + 1);
  var etbIdx = raw.indexOf(CTRL.ETB, stxIdx + 1);
  var termIdx = -1;
  if (etxIdx !== -1) termIdx = etxIdx;
  else if (etbIdx !== -1) termIdx = etbIdx;
  if (termIdx === -1) return null;   /* no terminator — incomplete frame */

  var body = raw.substring(stxIdx + 1, termIdx);
  if (!body.length) return null;

  /* --- Optional frame number ---
     Per ASTM: a single digit 1-7 followed by a record-type letter (H/P/O/R/Q/C/L).
     If the device omits the frame number (e.g. starts directly with 'H|'),
     the entire body is the payload and frameNo defaults to 1. */
  var frameNo = 1;
  var payload = body;
  var ASTM_TYPES = 'HPOQRCLMS';
  if (body.length >= 2
      && body.charCodeAt(0) >= 0x31 && body.charCodeAt(0) <= 0x37
      && ASTM_TYPES.indexOf(body[1]) !== -1) {
    frameNo = body.charCodeAt(0) - 0x30;
    payload = body.substring(1);
  }

  /* --- Optional checksum ---
     Manual format: <STX>data<CR><ETX>CS<CR>
     The checksum CS is exactly 2 uppercase hex chars followed by CR.
     We require the trailing CR to confirm a real checksum — this prevents
     false positives when non-checksum bytes (e.g. EOT, ENQ, or frame
     data from the next transmission) happen to be hex-like. */
  var hasChecksum = false;
  var checksum = '';
  var consumeEnd = termIdx + 1;   /* default: consume up to and including ETX/ETB */

  /* Need 4 bytes after terminator: 2 hex + 1 CR + at least 1 more byte
     (the CR proves these are a checksum, not random data) */
  if (termIdx + 4 <= raw.length) {
    var b1 = raw.charCodeAt(termIdx + 1);
    var b2 = raw.charCodeAt(termIdx + 2);
    var b3 = raw.charCodeAt(termIdx + 3);
    if (isHexByte(b1) && isHexByte(b2) && b3 === 0x0D) {
      hasChecksum = true;
      checksum = raw.substring(termIdx + 1, termIdx + 3).toUpperCase();
      consumeEnd = termIdx + 3;  /* past ETX + 2 checksum bytes (CR consumed below) */
    }
  }
  /* If the 2 bytes after ETX are hex but there is NO trailing CR,
     they are NOT a checksum — they are likely the start of the next
     transmission's data.  hasChecksum stays false. */

  /* Skip optional trailing CR after checksum or after ETX (no-checksum) */
  if (consumeEnd < raw.length && raw.charCodeAt(consumeEnd) === 0x0D) {
    consumeEnd++;
  }

  return {
    frameNo:    frameNo,
    payload:    payload,
    body:       body,       /* exact text between STX and ETX (for checksum calc) */
    checksum:   checksum,
    hasChecksum: hasChecksum,
    consumeEnd: consumeEnd
  };
}


/* =========================
   TALKBACK TO IPU (LIS → Maglumi)
   ========================= */
async function respondToQuery(socket, sampleId, name, queryFn, onReady) {
  try {
    var order = await queryFn(name, sampleId);

    if (!order) {
      /* Per Maglumi manual: "If LIS didn't find the sample,
         must send: ENQ  EOT" (no data frames) */
      console.log(`[${name}] No order found for ${sampleId} — sending ENQ+EOT`);
      onReady([]);
      return;
    }

    /* Build all records for a single-frame response (matches Maglumi
       behaviour observed in logs: one STX…ETX block per transmission). */
    var records = [];
    records.push('H|\\^&');

    if (order.patientId) {
      records.push('P|1|' + order.patientId);
    }

    var testIds = order.tests.map(function(t) { return '^^^' + t; }).join('\\');
    records.push('O|1|' + order.sampleId + '||' + testIds);
    records.push('L|1|N');

    /* Frame format per manual:  <STX>frameNo + records<CR><ETX>checksum<CR> */
    var frameNum = 1;
    var payload  = frameNum + records.join('\r');
    var cs       = calcChecksum(payload);
    var frame    = CTRL.STX + payload + '\r' + CTRL.ETX + cs + '\r';

    onReady([frame]);
    console.log(`[${name}] Prepared query response for ${sampleId} (${order.tests.length} test(s))`);

  }
  catch(err) {
    console.error(`[${name}] QUERY RESPONSE ERROR`, err, 'for', sampleId);
    onReady([]);  /* fail-safe: ENQ + EOT */
  }
}


/* =========================
   MAGLUMI PARSER: ASTM E1394
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

  var messages = [];
  var current = template();
  var lastResult = null;

  var lines = raw.split(/\r\n|\r|\n/);

  for (var li = 0; li < lines.length; li++) {
    var line = lines[li];
    if (!line.includes('|')) continue;

    var f = line.split('|');
    var type = f[0].replace(/[^A-Z]/g, '');

    switch (type) {

      /* ================= HEADER ================= */
      case 'H':
        current.header = {
          analyzer:  (f[4] || '').trim(),
          receiver:  (f[9] || '').trim(),
          /* Version may be at f[12] or f[13] depending on whether
             the Password field (f[3]) is present */
          version:   (f[12] && f[12].length > 1) ? f[12].trim() : ((f[13] || '').trim()),
          datetime:  (f[13] && f[13].length === 14) ? f[13].trim() : ((f[14] || '').trim())
        };
        break;

      /* ================= PATIENT ================= */
      case 'P':
        current.patient.id   = f[3] || f[4] || '';
        current.patient.name = f[5] || '';
        current.patient.sex  = f[8] || '';
        if (f[14]) {
          var ageParts = f[14].split('^');
          if (ageParts[0]) current.patient.age     = ageParts[0];
          if (ageParts[1]) current.patient.ageUnit = ageParts[1];
        }
        break;

      /* ================= ORDER =================
         f[2] = Specimen ID components (first = Sample ID) */
      case 'O':
        var specComponents = (f[2] || f[3] || '').split('^');
        current.order.sampleId = specComponents.find(function(v) { return v.trim(); }) || '';
        current.order.priority  = f[5] || '';
        current.order.dateTime  = f[6] || '';
        current.order.specimenDescriptor = f[15] || '';
        break;

      /* ================= RESULT =================
         f[2] Universal Test ID:  POC^Specimen^Priority^TestName
         Component index 3 = test name (after 3 carets). */
      case 'R':
        lastResult = null;

        var testComponents = f[2] ? f[2].split('^') : [];
        /* FIX: was [4], correct is [3] — '^^^TSH'.split('^') = ['','','','TSH'] */
        var test = testComponents[3] || '';
        var dt   = f[12];

        if (dt && !current.order.testDate && dt.length >= 14) {
          current.order.testDate =
            dt.slice(0,4) + '-' + dt.slice(4,6) + '-' + dt.slice(6,8) + ' ' +
            dt.slice(8,10) + ':' + dt.slice(10,12) + ':' + dt.slice(12,14);
        }

        if (test && f[3]) {
          lastResult = {
            name:     test,
            value:    f[3],
            unit:     f[4],
            refRange: f[5] || '',
            flag:     f[6] || 'N',
            status:   f[8] || 'F',
            datetime: dt || '',
            comments: []
          };
          current.results.push(lastResult);
        }
        break;

      /* ================= COMMENT ================= */
      case 'C':
        var comment = (f[3] || '').trim();
        if (!comment) break;

        if (lastResult) {
          lastResult.comments.push(comment);
        } else {
          current.order.comments = current.order.comments || [];
          current.order.comments.push(comment);
        }
        break;

      /* ================= TERMINATOR ================= */
      case 'L':
        messages.push(current);
        current = template();
        lastResult = null;
        break;

      /* ================= QUERY =================
         Q|1|SampleID^^DiskNo^PosNo^Diluent| |ALL|...|Status */
      case 'Q':
        var qParts = f[2] ? f[2].split('^') : [];
        current.query = { sampleId: (qParts[0] || '').trim() };
        break;
    }
  }

  return messages;
}