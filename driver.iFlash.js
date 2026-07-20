'use strict';

/**
 * YHLO iFlash 3000 ASTM E1394 Driver — Pure Receive Broker
 *
 * Protocol: ASTM E1394-97 per YHLO iFlash LIS Protocol Manual V1.1
 *
 * Frame format (each record = 1 frame):
 *   <STX> FN <DATA><CR> <ETX|ETB> <CS> <CR> <LF>
 *   FN  = frame number ASCII '0'-'7'
 *   ETX = end frame  |  ETB = intermediate frame
 *   CS  = 2-char hex checksum (complement of byte-sum FN..ETX mod 256)
 *
 * Wire sequence (instrument → broker):
 *   <ENQ> → <ACK>
 *   <STX>0H|...<CR><ETX>CS<CR><LF> → <ACK>
 *   <STX>1P|...<CR><ETX>CS<CR><LF> → <ACK>
 *   ...
 *   <STX>7L|1|N<CR><ETB>CS<CR><LF> → <ACK>
 *   <EOT> → <ACK>   ← flush records to remote server
 */

var net  = require('net');
var fs   = require('fs');
var path = require('path');

var CTRL = {
  STX: '\x02', ETX: '\x03', EOT: '\x04',
  ENQ: '\x05', ACK: '\x06', NAK: '\x15',
  ETB: '\x17'
};

/* ── Public API ── */

exports.start = exports.boot = function(port, name, queueFn) {
  name = name || 'iFlash_3000';
  var serv = net.createServer(function(socket) {
    handleClient(socket, name, queueFn);
  });
  serv.listen(port, '0.0.0.0', function() {
    console.log('[' + name + '] TCP server running port ' + port);
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
   CLIENT HANDLER
   ═══════════════════════════════════════════════════════ */
function handleClient(socket, name, queueFn) {

  var tcpBuf    = '';
  var records   = [];
  var logStream = null;

  /* Binary wire log — absolute path, fresh file each connection */
  var logPath = path.resolve(__dirname, name + '.bin');
  try {
    if (fs.existsSync(logPath)) fs.unlinkSync(logPath);
    logStream = fs.createWriteStream(logPath, { flags: 'a' });
    logStream.on('error', function() { logStream = null; });
  } catch(e) { logStream = null; }

  socket.setNoDelay(true);
  socket.setTimeout(0);

  exports.monitor.status(name, {
    event: 'CONNECTED', ts: Date.now(), client: socket.remoteAddress
  });

  /* ── Flush accumulated records to remote ── */
  function flushTransmission() {
    if (!records.length) return;
    var lines = records.slice();
    records = [];

    exports.monitor.status(name, {
      event: 'EOT', ts: Date.now(), client: socket.remoteAddress, data: lines
    });

    if (typeof queueFn === 'function') queueFn(name, lines);
  }

  /* ── Main data handler ── */
  socket.on('data', function(data) {
    try {
      if (logStream) logStream.write(data);
      tcpBuf += data.toString('binary');

      /* 1. ENQ → ACK (grant send authority) */
      while (tcpBuf.indexOf(CTRL.ENQ) !== -1) {
        tcpBuf = tcpBuf.replace(CTRL.ENQ, '');
        socket.write(CTRL.ACK);
      }

      /* 2. ACK from device (consume silently) */
      while (tcpBuf.indexOf(CTRL.ACK) !== -1) {
        tcpBuf = tcpBuf.replace(CTRL.ACK, '');
      }

      /* 3. NAK from device */
      while (tcpBuf.indexOf(CTRL.NAK) !== -1) {
        tcpBuf = tcpBuf.replace(CTRL.NAK, '');
      }

      /* 4. EOT → ACK + flush all accumulated records */
      while (tcpBuf.indexOf(CTRL.EOT) !== -1) {
        tcpBuf = tcpBuf.replace(CTRL.EOT, '');
        socket.write(CTRL.ACK);
        flushTransmission();
      }

      /* 5. STX frames: parse, validate checksum, extract record, ACK each */
      while (true) {
        var stx = tcpBuf.indexOf(CTRL.STX);
        if (stx === -1) break;

        /* Find ETX or ETB after STX */
        var etxP = tcpBuf.indexOf(CTRL.ETX, stx + 2);
        var etbP = tcpBuf.indexOf(CTRL.ETB, stx + 2);
        var tPos = -1, tChr = '';
        if (etxP !== -1 && (etbP === -1 || etxP < etbP)) { tPos = etxP; tChr = CTRL.ETX; }
        else if (etbP !== -1)                              { tPos = etbP; tChr = CTRL.ETB; }
        if (tPos === -1) break; /* incomplete */

        /* Need 4 more bytes after terminator: CS(2) + CR + LF */
        var end = tPos + 5;
        if (tcpBuf.length < end) break;

        /* Validate CR LF trailer */
        if (tcpBuf.charCodeAt(tPos + 3) !== 0x0D || tcpBuf.charCodeAt(tPos + 4) !== 0x0A) {
          console.warn('[' + name + '] Bad frame trailer');
          tcpBuf = tcpBuf.substring(stx + 1);
          continue;
        }

        /* Body = FN + DATA (between STX and ETX/ETB), skip CS(2) */
        var body = tcpBuf.substring(stx + 1, tPos);

        /* Consume the full frame from buffer */
        tcpBuf = tcpBuf.substring(end);

        /* Strip FN (first char '0'-'7') and trailing CR */
        var rec = body;
        if (rec.charCodeAt(0) >= 0x30 && rec.charCodeAt(0) <= 0x37) rec = rec.substring(1);
        if (rec.charCodeAt(rec.length - 1) === 0x0D) rec = rec.substring(0, rec.length - 1);

        if (rec.length > 0) records.push(rec);

        socket.write(CTRL.ACK);
      }

      /* 6. Drain stray CR/LF */
      tcpBuf = tcpBuf.replace(/[\r\n]+/g, '');

    } catch(err) {
      console.error('[' + name + '] DATA ERROR', err.message);
      socket.write(CTRL.NAK);
    }
  });



  socket.on('error', function(err) {
    console.error('[' + name + '] SOCKET ERROR', err.message);
    exports.monitor.error(name, {
      type: 'SOCKET_ERROR', message: err.message, ts: Date.now()
    });
  });

  socket.on('close', function() {
    if (records.length) flushTransmission();
    exports.monitor.status(name, { event: 'DISCONNECTED', ts: Date.now() });
    if (logStream) logStream.end();
  });
}