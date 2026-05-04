const { default: makeWASocket, useMultiFileAuthState, DisconnectReason, downloadMediaMessage } = require('@whiskeysockets/baileys');
const { Boom } = require('@hapi/boom');
const express = require('express');
const multer = require('multer');
const csv = require('csv-parser');
const fs = require('fs');
const path = require('path');
const pino = require('pino');

const app = express();
app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

const upload = multer({ dest: 'uploads/' });

let sock = null;
let qrCode = null;
let isConnected = false;
let sendingQueue = [];
let isSending = false;
let sendLog = [];
let currentJob = null;

// ── Connect to WhatsApp ──────────────────────────────────────────────────────
async function connectToWhatsApp() {
  const { state, saveCreds } = await useMultiFileAuthState('auth_info');

  sock = makeWASocket({
    auth: state,
    printQRInTerminal: false,
    logger: pino({ level: 'silent' }),
  });

  sock.ev.on('connection.update', ({ connection, lastDisconnect, qr }) => {
    if (qr) {
      qrCode = qr;
      isConnected = false;
      console.log('QR ready');
    }
    if (connection === 'close') {
      isConnected = false;
      qrCode = null;
      const shouldReconnect = lastDisconnect?.error instanceof Boom
        ? lastDisconnect.error.output.statusCode !== DisconnectReason.loggedOut
        : true;
      console.log('Disconnected. Reconnecting:', shouldReconnect);
      if (shouldReconnect) setTimeout(connectToWhatsApp, 3000);
    }
    if (connection === 'open') {
      isConnected = true;
      qrCode = null;
      console.log('WhatsApp connected!');
    }
  });

  sock.ev.on('creds.update', saveCreds);
}

// ── Send one message ─────────────────────────────────────────────────────────
async function sendMessage(contact, message, imagePath) {
  const phone = contact.phone.replace(/[^0-9]/g, '');
  const jid = phone.startsWith('20') ? `${phone}@s.whatsapp.net` : `20${phone}@s.whatsapp.net`;

  if (imagePath && fs.existsSync(imagePath)) {
    const imageBuffer = fs.readFileSync(imagePath);
    await sock.sendMessage(jid, {
      image: imageBuffer,
      caption: message,
    });
  } else {
    await sock.sendMessage(jid, { text: message });
  }
}

// ── Process queue ────────────────────────────────────────────────────────────
async function processQueue() {
  if (isSending || sendingQueue.length === 0) return;
  isSending = true;

  while (sendingQueue.length > 0 && currentJob?.active) {
    const item = sendingQueue.shift();
    const message = currentJob.template
      .replace(/\{الاسم\}/g, item.name || '')
      .replace(/\{الفرع\}/g, item.branch || '')
      .replace(/\{الرقم\}/g, item.phone || '');

    try {
      await sendMessage(item, message, currentJob.imagePath);
      const logEntry = { time: new Date().toLocaleTimeString('ar-EG'), name: item.name, phone: item.phone, status: 'تم' };
      sendLog.push(logEntry);
      currentJob.done++;
      console.log(`Sent to ${item.phone}`);
    } catch (err) {
      const logEntry = { time: new Date().toLocaleTimeString('ar-EG'), name: item.name, phone: item.phone, status: 'فشل: ' + err.message };
      sendLog.push(logEntry);
      currentJob.failed++;
      console.error(`Failed ${item.phone}:`, err.message);
    }

    if (sendingQueue.length > 0 && currentJob?.active) {
      const minD = (currentJob.delayMin || 3) * 60 * 1000;
      const maxD = (currentJob.delayMax || 5) * 60 * 1000;
      const delay = Math.floor(minD + Math.random() * (maxD - minD));
      currentJob.nextIn = delay;
      currentJob.nextAt = Date.now() + delay;
      await new Promise(r => setTimeout(r, delay));
    }
  }

  isSending = false;
  if (currentJob) currentJob.active = false;
}

// ── API Routes ───────────────────────────────────────────────────────────────

// Status + QR
app.get('/api/status', (req, res) => {
  res.json({
    connected: isConnected,
    qr: qrCode,
    sending: isSending,
    job: currentJob ? {
      total: currentJob.total,
      done: currentJob.done,
      failed: currentJob.failed,
      remaining: sendingQueue.length,
      active: currentJob.active,
      nextAt: currentJob.nextAt || null,
    } : null,
    log: sendLog.slice(-50),
  });
});

// Upload CSV
app.post('/api/upload-csv', upload.single('file'), (req, res) => {
  if (!req.file) return res.status(400).json({ error: 'لم يتم رفع ملف' });
  const results = [];
  fs.createReadStream(req.file.path)
    .pipe(csv())
    .on('data', row => {
      const keys = Object.keys(row);
      const nameKey = keys.find(k => k.includes('اسم') || k.toLowerCase().includes('name'));
      const phoneKey = keys.find(k => k.includes('رقم') || k.includes('هاتف') || k.toLowerCase().includes('phone') || k.toLowerCase().includes('mobile'));
      const branchKey = keys.find(k => k.includes('فرع') || k.toLowerCase().includes('branch'));
      const statusKey = keys.find(k => k.includes('status') || k.includes('حالة'));
      const status = statusKey ? row[statusKey] : '';
      if (['done', 'تم', 'no whatsapp', 'لا واتساب'].includes((status||'').toLowerCase())) return;
      if (phoneKey && row[phoneKey]) {
        results.push({
          name: nameKey ? row[nameKey] : '',
          phone: row[phoneKey].replace(/[^0-9+]/g, ''),
          branch: branchKey ? row[branchKey] : '',
        });
      }
    })
    .on('end', () => {
      fs.unlinkSync(req.file.path);
      res.json({ contacts: results, count: results.length });
    })
    .on('error', err => res.status(500).json({ error: err.message }));
});

// Upload image
app.post('/api/upload-image', upload.single('image'), (req, res) => {
  if (!req.file) return res.status(400).json({ error: 'لم يتم رفع صورة' });
  res.json({ path: req.file.path, filename: req.file.originalname });
});

// Start sending
app.post('/api/start', (req, res) => {
  if (!isConnected) return res.status(400).json({ error: 'واتساب غير متصل' });
  const { contacts, template, imagePath, delayMin, delayMax } = req.body;
  if (!contacts || contacts.length === 0) return res.status(400).json({ error: 'لا توجد جهات اتصال' });

  sendLog = [];
  sendingQueue = [...contacts];
  currentJob = {
    total: contacts.length,
    done: 0,
    failed: 0,
    active: true,
    template,
    imagePath: imagePath || null,
    delayMin: delayMin || 3,
    delayMax: delayMax || 5,
  };

  processQueue();
  res.json({ started: true, total: contacts.length });
});

// Stop sending
app.post('/api/stop', (req, res) => {
  if (currentJob) currentJob.active = false;
  sendingQueue = [];
  isSending = false;
  res.json({ stopped: true });
});

// ── Start ────────────────────────────────────────────────────────────────────
const PORT = process.env.PORT || 3000;
app.listen(PORT, '0.0.0.0', () => {
  console.log(`Server running on port ${PORT}`);
  connectToWhatsApp();
});
