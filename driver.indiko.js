'use strict';

/**
 * Thermo Fisher Indiko / Gallery ASTM E1394 Driver — Pure Broker Mode
 *
 * Protocol: CLSI LIS01-A (low-level) + CLSI LIS2-A (records).
 * Based on document N12027 rev 7.0A (October 2018).
 *
 * This driver does NOT parse or process any ASTM records.
 * It only handles the TCP wire protocol (ENQ/ACK/EOT/STX/ETX),
 * extracts raw text records from frames, and delivers them as-is
 * to the monitor.callback and queueFn for shipping to the ERP.
 *
 * Indiko/Gallery frame format (WITH checksum, CRLF trailer):
 *   ENQ → ACK → <STX>frameNo records<CR>...<CR><ETX>CS<CR><LF> → ACK → ... → EOT
 *
 * Checksum: SUM of all bytes between STX and ETX (includes frame number
 * digit and the CR before ETX), modulo 256, 2 uppercase hex chars.
 *
 * Frame numbers cycle: 1,2,3,4,5,6,7,0,1,2,...  (wraps to 0, not 1)
 *
 * Each record line is CR-separated (0x0D) inside the frame body.
 * Default TCP port: 10100.
 */

var net = require('net'), fs = require('fs');

var CTRL = {
  STX: '\x02', ETX: '\x03', EOT: '\x04',
  ENQ: '\x05', ACK: '\x06', NAK: '\x15', ETB: '\x17'
};

/* ── Public API ── */

exports.start = exports.boot = function(port, name, queueFn) {
  name = name || 'Indiko_Plus';
  var serv = net.createServer(function(socket) {
    handleClient(socket, name, queueFn);
  });
  serv.listen(port, '0.0.0.0', function() {
    console.log('TCP server ' + name + '/LIS running port ' + port);
  });
  return serv;
};

/* Stub: backward-compat for server.d.js */
exports.query = async function() { return {}; };
exports.setQueryFn = function() {};

exports.monitor = {
  status: function(device, info) { console.log('[' + device + '] STATUS', info); },
  error:  function(device, err)  { console.error('[' + device + ']', err); },
  heartbeat: function() {}
};


/* ═══════════════════════════════════════════════════════
   CHECKSUM — SUM mod 256 (per CLSI LIS01-A / Indiko doc)
   Computed over the raw body between STX and ETX,
   which includes the frame number digit and the CR before ETX.
   ═══════════════════════════════════════════════════════ */
function calcChecksum(data) {
  var sum = 0;
  for (var i = 0; i < data.length; i++) {
    sum += data.charCodeAt(i);
  }
  return (sum & 0xFF).toString(16).toUpperCase().padStart(2, '0');
}

function isHexByte(b) {
  return (b >= 0x30 && b <= 0x39) || (b >= 0x41 && b <= 0x46) || (b >= 0x61 && b <= 0x66);
}

/**
 * Indiko frame number wraps: 1,2,3,4,5,6,7,0,1,2,...
 * This differs from Maglumi/Sysmex which wrap 7→1.
 */
function nextFrame(n) {
  return (n >= 7) ? 0 : n + 1;
}


/* ═══════════════════════════════════════════
   CLIENT HANDLER — pure broker, no parsing
   ═══════════════════════════════════════════ */
function handleClient(socket, name, queueFn) {

  var tcpBuf   = '';    /* reassembly buffer for TCP stream */
  var records  = [];    /* accumulated raw record lines for current transmission */
  var logStream = null;
  var expectedFrame = 1;

  /* Persistent binary log (append-only, non-blocking) */
  try {
    logStream = fs.createWriteStream(name + '.bin', { flags: 'a' });
    logStream.on('error', function() { logStream = null; });
  } catch(e) { logStream = null; }

  console.log('[' + name + '] Connected ' + socket.remoteAddress);
  exports.monitor.status(name, {
    event: 'CONNECTED', ts: Date.now(), client: socket.remoteAddress
  });

  /* ── Deliver accumulated records and reset ── */
  function flushTransmission() {
    var lines = records.length ? records.slice() : [];
    records = [];
    expectedFrame = 1;

    if (!lines.length) return;

    /* Primary delivery: monitor.status callback → apiQueue in server.d.js */
    exports.monitor.status(name, {
      event: 'EOT',
      ts: Date.now(),
      client: socket.remoteAddress,
      data: lines
    });

    /* Secondary delivery: queueFn (db fallback in server.d.js) */
    if (typeof queueFn === 'function') {
      queueFn(name, lines);
    }
  }

  /* ── Main data handler ── */
  socket.on('data', function(data) {
    try {
      var raw = data.toString('binary');
      if (logStream) logStream.write(data);
      tcpBuf += raw;

      /* ═══════════════════════════════════════════════
         PROCESSING ORDER (critical for correctness):
           1. ACK / NAK  — responses from device to our outbound
           2. ENQ        — device requests permission to send
           3. STX frames — extract raw text, validate checksum, accumulate
           4. EOT        — finalize current transmission batch
         ═══════════════════════════════════════════════ */

      /* Phase 1: Consume ACK/NAK (device responses to our outbound frames) */
      while (tcpBuf.indexOf(CTRL.ACK) !== -1) tcpBuf = tcpBuf.replace(CTRL.ACK, '');
      while (tcpBuf.indexOf(CTRL.NAK) !== -1) tcpBuf = tcpBuf.replace(CTRL.NAK, '');

      /* Phase 2: ENQ → reply ACK */
      while (tcpBuf.indexOf(CTRL.ENQ) !== -1) {
        tcpBuf = tcpBuf.replace(CTRL.ENQ, '');
        socket.write(CTRL.ACK);
      }

      /* Phase 3: Extract STX…ETX/ETB frames, validate, accumulate raw records */
      while (true) {
        var stxPos = tcpBuf.indexOf(CTRL.STX);
        if (stxPos === -1) break;

        if (stxPos > 0) {
          /* Check if EOT is in the gap → end of previous transmission */
          var gap = tcpBuf.substring(0, stxPos);
          if (gap.indexOf(CTRL.EOT) !== -1) {
            tcpBuf = tcpBuf.replace(CTRL.EOT, '');
            flushTransmission();
            continue;
          }
          /* Non-protocol bytes before STX — skip them */
          console.warn('[' + name + '] Skipping ' + stxPos + ' byte(s) before STX');
          tcpBuf = tcpBuf.substring(stxPos);
        }

        /* Find matching ETX or ETB */
        var etxIdx = tcpBuf.indexOf(CTRL.ETX, stxPos + 1);
        var etbIdx = tcpBuf.indexOf(CTRL.ETB, stxPos + 1);
        var termIdx = -1;
        if (etxIdx !== -1) termIdx = etxIdx;
        else if (etbIdx !== -1) termIdx = etbIdx;
        if (termIdx === -1) break;  /* incomplete frame — wait for more data */

        /* Body = everything between STX and terminator.
           Per Indiko doc: includes frame number + records + CR before ETX.
           This is also what the checksum is computed over. */
        var body = tcpBuf.substring(stxPos + 1, termIdx);
        if (!body.length) {
          tcpBuf = tcpBuf.substring(termIdx + 1);
          continue;
        }

        /* ── Checksum detection ──
           Indiko format after ETX: 2 hex bytes + CR + LF
           Detect by: 2 hex chars followed by CR.  The LF is consumed separately. */
        var hasChecksum = false;
        var checksum = '';
        var consumeEnd = termIdx + 1;

        if (termIdx + 3 < tcpBuf.length) {
          var b1 = tcpBuf.charCodeAt(termIdx + 1);
          var b2 = tcpBuf.charCodeAt(termIdx + 2);
          var b3 = tcpBuf.charCodeAt(termIdx + 3);
          if (isHexByte(b1) && isHexByte(b2) && b3 === 0x0D) {
            hasChecksum = true;
            checksum = tcpBuf.substring(termIdx + 1, termIdx + 3).toUpperCase();
            consumeEnd = termIdx + 3; /* past ETX + 2 checksum bytes */
          }
        }

        /* Skip trailing CR (after checksum or after bare ETX) */
        if (consumeEnd < tcpBuf.length && tcpBuf.charCodeAt(consumeEnd) === 0x0D) {
          consumeEnd++;
        }

        /* Skip trailing LF (Indiko uses CR+LF after checksum) */
        if (consumeEnd < tcpBuf.length && tcpBuf.charCodeAt(consumeEnd) === 0x0A) {
          consumeEnd++;
        }

        /* ── Optional frame number ──
           Indiko always sends frame number (0-7) as first char of body.
           Frame 0 is valid (used after frame 7 in multi-frame transmissions). */
        var frameNo = 1;
        var payload = body;
        var ASTM_TYPES = 'HPOQRCLMS';
        if (body.length >= 2
            && body.charCodeAt(0) >= 0x30 && body.charCodeAt(0) <= 0x37
            && ASTM_TYPES.indexOf(body[1]) !== -1) {
          frameNo = body.charCodeAt(0) - 0x30;
          payload = body.substring(1);
        }

        /* ── Checksum validation (warn only, never drop data) ──
           Compute over the raw body (between STX and ETX), which includes
           the frame number digit and CR before ETX — exactly as the
           Indiko document specifies. */
        if (hasChecksum) {
          var csExpected = calcChecksum(body);
          if (checksum !== csExpected) {
            console.warn('[' + name + '] CHECKSUM MISMATCH exp=' + csExpected +
              ' got=' + checksum + ' — accepting data anyway (frame ' + frameNo + ')');
          }
        }

        /* ── Frame sequence check (warn only, never drop data) ── */
        if (frameNo !== expectedFrame) {
          console.warn('[' + name + '] FRAME SEQ exp=' + expectedFrame + ' got=' + frameNo +
            ' — accepting data anyway');
        }

        /* ── Split payload on CR to get individual record lines ── */
        var lines = payload.split('\r');
        for (var i = 0; i < lines.length; i++) {
          if (lines[i].length > 0) {
            records.push(lines[i]);
          }
        }

        /* Remove processed frame from buffer */
        tcpBuf = tcpBuf.substring(consumeEnd);

        expectedFrame = nextFrame(expectedFrame);

        /* ACK every frame */
        socket.write(CTRL.ACK);
      }

      /* Phase 4: Trailing EOT → finalize */
      if (tcpBuf.indexOf(CTRL.EOT) !== -1) {
        tcpBuf = tcpBuf.replace(CTRL.EOT, '');
        flushTransmission();
      }

    } catch(err) {
      console.error('[' + name + '] DATA ERROR', err.message, 'from:', socket.remoteAddress);
      /* Don't NAK — log and continue to avoid losing data */
    }
  });

  socket.on('error', function(err) {
    console.error('[' + name + '] SOCKET ERROR', err.message, 'from:', socket.remoteAddress);
    exports.monitor.error(name, {
      type: 'SOCKET_ERROR', message: err.message, ts: Date.now()
    });
  });

  socket.on('timeout', function() {
    console.error('[' + name + '] SOCKET Timeout', socket.remoteAddress);
    socket.end();
  });

  socket.on('close', function() {
    exports.monitor.status(name, { event: 'DISCONNECTED', ts: Date.now() });
    console.log('[' + name + '] Connection closed');
    if (logStream) logStream.end();
  });
}