'use strict';

/**
 * Maglumi X3 ASTM E1394 Driver — Pure Broker Mode
 *
 * This driver does NOT parse or process any ASTM records.
 * It only handles the TCP wire protocol (ENQ/ACK/EOT/STX/ETX),
 * extracts raw text records from frames, and delivers them as-is
 * to the monitor.callback and queueFn for shipping to the ERP.
 *
 * Frame format observed from Maglumi X3 (NO checksum):
 *   ENQ → ACK → <STX>records<CR>...<CR><ETX> → ACK → EOT
 *
 * Each record line is CR-separated (0x0D) inside the frame body.
 */

var net = require('net'), fs = require('fs');

var CTRL = {
  STX: '\x02', ETX: '\x03', EOT: '\x04',
  ENQ: '\x05', ACK: '\x06', NAK: '\x15'
};

/* ── Public API ── */

exports.start = exports.boot = function(port, name, queueFn) {
  name = name || 'Maglumi_X3';
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


/* ═══════════════════════════════════════════
   CLIENT HANDLER — pure broker, no parsing
   ═══════════════════════════════════════════ */
function handleClient(socket, name, queueFn) {

  var tcpBuf   = '';    /* reassembly buffer for TCP stream */
  var records  = [];    /* accumulated raw record lines for current transmission */
  var logStream = null;

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
           3. STX frames — extract raw text, accumulate records
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

      /* Phase 3: Extract STX…ETX frames, accumulate raw record lines */
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

        /* Find matching ETX */
        var etxIdx = tcpBuf.indexOf(CTRL.ETX, stxPos + 1);
        if (etxIdx === -1) break;  /* incomplete frame — wait for more data */

        /* Body = everything between STX and ETX (may include optional
           leading frame number digit 1-7, and CR-separated records) */
        var body = tcpBuf.substring(stxPos + 1, etxIdx);
        if (!body.length) {
          tcpBuf = tcpBuf.substring(etxIdx + 1);
          continue;
        }

        /* Strip optional frame number: single digit 1-7 before a
           record-type letter (H/P/O/R/Q/C/L/M/S) */
        var ASTM_TYPES = 'HPOQRCLMS';
        if (body.length >= 2
            && body.charCodeAt(0) >= 0x31 && body.charCodeAt(0) <= 0x37
            && ASTM_TYPES.indexOf(body[1]) !== -1) {
          body = body.substring(1);
        }

        /* Split body on CR to get individual record lines */
        var lines = body.split('\r');
        for (var i = 0; i < lines.length; i++) {
          if (lines[i].length > 0) {
            records.push(lines[i]);
          }
        }

        /* Consume the frame from the buffer (past ETX, skip optional CR) */
        var consumeEnd = etxIdx + 1;
        if (consumeEnd < tcpBuf.length && tcpBuf.charCodeAt(consumeEnd) === 0x0D) {
          consumeEnd++;
        }
        tcpBuf = tcpBuf.substring(consumeEnd);

        /* ACK the frame */
        socket.write(CTRL.ACK);
      }

      /* Phase 4: Trailing EOT → finalize */
      if (tcpBuf.indexOf(CTRL.EOT) !== -1) {
        tcpBuf = tcpBuf.replace(CTRL.EOT, '');
        flushTransmission();
      }

    } catch(err) {
      console.error('[' + name + '] DATA ERROR', err.message, 'from:', socket.remoteAddress);
      socket.write(CTRL.NAK);
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