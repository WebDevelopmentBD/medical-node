'use strict';

const net = require('net'), fs = require('fs');

const CTRL = {
  STX: '\x02',
  ETX: '\x03',
  EOT: '\x04',
  ENQ: '\x05',
  ACK: '\x06',
  NAK: '\x15',
  ETB: '\x17',
  CR:  '\x0D',
  LF:  '\x0A'
};

// Simulated Database Lookups — Replace this with real DB queries!
function getPatientDataFromDB(sampleId) {
  return {
    patientId: "PAT" + sampleId,
    name: "Smith^John", 
    dob: "19850615", // YYYYMMDD format calculates Age automatically on iFlash
    sex: "M",
    tests: "^^^TSH\\^^^FT4" // YHLO specific test codes separated by \
  };
}

exports.start = exports.boot = function (port, name, queueFn) {
  name = name || "iFlash";
  return net.createServer(socket => handleClient(socket, name, queueFn)).listen(port);
};

function handleClient(socket, name, queueFn) {
  // Accumulator buffer to safely handle TCP stream fragmentation
  let streamAccumulator = ''; 
  let astmMessageBlock = '';

  console.log(`[${name}] Connected ${socket.remoteAddress}`);

  socket.on('data', data => {
    try {
      const raw = data.toString('binary');
      fs.appendFileSync(`${name}_raw.log`, raw);
      streamAccumulator += raw;

      // Processing byte-by-byte or frame-by-frame
      while (streamAccumulator.length > 0) {
        
        // 1. Handle Out-of-Frame Control Characters
        if (streamAccumulator.startsWith(CTRL.ENQ)) {
          socket.write(CTRL.ACK);
          streamAccumulator = streamAccumulator.slice(1);
          continue;
        }

        if (streamAccumulator.startsWith(CTRL.EOT)) {
          // Analyzer finished sending its chunk. Parse what we got.
          const queries = parseASTM(astmMessageBlock);
          astmMessageBlock = ''; 
          streamAccumulator = streamAccumulator.slice(1);

          // If a query record was captured, initiate the transmission response sequence
          if (queries.length > 0 && queries[0].query.sampleId) {
            sendQueryResponse(socket, queries[0].query.sampleId, name);
          }
          continue;
        }

        if (streamAccumulator.startsWith(CTRL.ACK) || streamAccumulator.startsWith(CTRL.NAK)) {
          // These handle transmissions originating from LIS side (ignored here for brevity)
          streamAccumulator = streamAccumulator.slice(1);
          continue;
        }

        // 2. Handle STX Encapsulated Data Frames
        if (streamAccumulator.startsWith(CTRL.STX)) {
          const etxIdx = streamAccumulator.indexOf(CTRL.ETX);
          const etbIdx = streamAccumulator.indexOf(CTRL.ETB);
          const endIdx = etxIdx !== -1 ? etxIdx : etbIdx;

          // Frame is incomplete, wait for more data from TCP stream
          if (endIdx === -1 || streamAccumulator.length < endIdx + 3) {
            break; 
          }

          const completeFrame = streamAccumulator.substring(0, endIdx + 3);
          streamAccumulator = streamAccumulator.slice(endIdx + 3);

          const frame = extractFrame(completeFrame);
          if (!frame) {
            socket.write(CTRL.NAK);
            continue;
          }

          const expected = calcChecksum(frame.payload);
          if (expected !== frame.checksum) {
            console.error(`[${name}] CHECKSUM FAIL exp=${expected} got=${frame.checksum}`);
            socket.write(CTRL.NAK);
            continue;
          }

          // Clean frame frame-handling sequence markers (e.g., "1H|...", "2P|...")
          const cleanLine = frame.payload.substring(1); 
          astmMessageBlock += cleanLine; // \r is already preserved at end of frame payload
          socket.write(CTRL.ACK);
          continue;
        }

        // Unhandled garbage characters fallback
        streamAccumulator = streamAccumulator.slice(1);
      }
    } catch (err) {
      console.error(`[${name}] DATA ERROR`, err.message);
      socket.write(CTRL.NAK);
    }
  });

  socket.on('close', () => console.log(`[${name}] Connection closed`));
  socket.on('error', err => console.error(`[${name}] SOCKET ERROR`, err.message));
}

/* =========================================
   BIDIRECTIONAL RESPONSE TRANSMITTER
========================================= */
function sendQueryResponse(socket, sampleId, name) {
  console.log(`[${name}] Processing Query for Sample: ${sampleId}`);
  const patient = getPatientDataFromDB(sampleId);

  // Fallback if sample barcode doesn't exist in LIS database
  if( !patient ){
    const records = [ "H|\\^&|||LIS||||||P|1|", "L|1|I|" ];
    writeASTMRecords(socket, records);
    return;
  }

  // Construct standard ASTM Records array matching YHLO expectations
  const records = [
    `H|\\^&|||LIS||||||P|1|`,
    `P|1||${patient.patientId}||${patient.name}|||${patient.dob}|${patient.sex}||||||||`,
    `O|1|${sampleId}||${patient.tests}||||||||||||||||||||F`,
    `L|1|N`
  ];

  // Perform half-duplex line negotiation 
  socket.write(CTRL.ENQ);
  
  // Quick absolute sync sleep setup for the engine to deliver lines sequentially
  let seq = 1;
  let recordIdx = 0;

  // Note: For a robust engine, build a state machine checking for analyzer ACKs 
  // before firing subsequent strings. This linear loop acts as a structural baseline:
  setTimeout(() => {
    for (let record of records) {
      const payload = `${seq % 8}${record}`;
      const checksum = calcChecksum(payload);
      const frame = `${CTRL.STX}${payload}${CTRL.ETX}${checksum}${CTRL.CR}${CTRL.LF}`;
      socket.write(frame);
      seq++;
    }
    socket.write(CTRL.EOT);
    console.log(`[${name}] Sent demographic mapping for ${sampleId}`);
  }, 300); 
}

/* =========================================
   UTILITIES & REWORKED PARSER
========================================= */
function calcChecksum(data) {
  let sum = 0;
  for (let i = 0; i < data.length; i++) {
    sum += data.charCodeAt(i);
  }
  return (sum & 0xFF).toString(16).toUpperCase().padStart(2, '0');
}

function extractFrame(frameStr) {
  const etx = frameStr.indexOf(CTRL.ETX);
  const etb = frameStr.indexOf(CTRL.ETB);
  const end = etx !== -1 ? etx : etb;
  if (end === -1) return null;

  return {
    payload: frameStr.substring(1, end),
    checksum: frameStr.substring(end + 1, end + 3)
  };
}

function template() {
  return { header: {}, patient: {}, order: { sampleId: '', testDate: '', comments: [] }, results: [], query: { sampleId: '' } };
}

function parseASTM(raw) {
  const messages = [];
  let current = template();
  const lines = raw.split('\r'); // Lines inside payload end in \r

  for (const line of lines) {
    if (!line.includes('|')) continue;
    const f = line.split('|');
    const type = f[0].replace(/[^A-Z]/g, '');

    switch (type) {
      case 'H':
        current.header = { analyzer: f[4]?.trim(), version: f[12] };
        break;
      case 'P':
        current.patient.id = f[3] || '';
        current.patient.name = f[5] || '';
        break;
      case 'Q':
        // Parse incoming Query criteria from YHLO 
        // Typically field 3: ^BARCODE^^^^O or similar
        const qParts = (f[2] || '').split('^');
        current.query.sampleId = qParts.find(v => v.trim()) || '';
        break;
      case 'O':
        const parts = (f[2] || f[3] || '').split('^');
        current.order.sampleId = parts.find(v => v.trim()) || '';
        break;
      case 'R':
        const test = f[2]?.split('^')[4];
        if (test && f[3]) {
          current.results.push({ name: test, value: f[3], unit: f[4], flag: f[6] || 'N' });
        }
        break;
      case 'L':
        messages.push(current);
        current = template();
        break;
    }
  }
  return messages;
}
