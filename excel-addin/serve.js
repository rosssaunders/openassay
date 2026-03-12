#!/usr/bin/env node
/**
 * Development server for the openassay Excel Add-in.
 * Serves the add-in files over HTTPS (required by Office Add-ins).
 *
 * Usage:
 *   node serve.js
 *
 * Then sideload the manifest.xml into Excel:
 *   - Excel Online: Insert → Office Add-ins → Upload My Add-in → manifest.xml
 *   - Excel Desktop: See https://learn.microsoft.com/office/dev/add-ins/testing/sideload-office-add-ins
 */

const https = require('https');
const fs = require('fs');
const path = require('path');
const { execSync } = require('child_process');

const PORT = 3000;
const DIR = __dirname;

// Generate self-signed cert if not present
const certDir = path.join(DIR, '.certs');
if (!fs.existsSync(certDir)) {
  fs.mkdirSync(certDir);
}
const keyPath = path.join(certDir, 'key.pem');
const certPath = path.join(certDir, 'cert.pem');

if (!fs.existsSync(keyPath)) {
  console.log('Generating self-signed certificate...');
  execSync(
    `openssl req -x509 -newkey rsa:2048 -keyout "${keyPath}" -out "${certPath}" ` +
    `-days 365 -nodes -subj "/CN=localhost"`,
    { stdio: 'inherit' }
  );
}

const MIME_TYPES = {
  '.html': 'text/html',
  '.js': 'application/javascript',
  '.wasm': 'application/wasm',
  '.json': 'application/json',
  '.xml': 'application/xml',
  '.css': 'text/css',
  '.png': 'image/png',
  '.svg': 'image/svg+xml',
};

const server = https.createServer(
  {
    key: fs.readFileSync(keyPath),
    cert: fs.readFileSync(certPath),
  },
  (req, res) => {
    // CORS headers (needed for Office Add-ins)
    res.setHeader('Access-Control-Allow-Origin', '*');
    res.setHeader('Access-Control-Allow-Methods', 'GET');

    const urlPath = req.url.split('?')[0];
    let filePath = urlPath === '/' ? '/taskpane.html' : urlPath;
    filePath = path.join(DIR, filePath);

    // Security: prevent directory traversal
    if (!filePath.startsWith(DIR)) {
      res.writeHead(403);
      res.end('Forbidden');
      return;
    }

    const ext = path.extname(filePath);
    const contentType = MIME_TYPES[ext] || 'application/octet-stream';

    fs.readFile(filePath, (err, data) => {
      if (err) {
        res.writeHead(404);
        res.end(`Not found: ${req.url}`);
        return;
      }
      res.writeHead(200, { 'Content-Type': contentType });
      res.end(data);
    });
  }
);

server.listen(PORT, () => {
  console.log(`\n🐘 openassay Excel Add-in dev server`);
  console.log(`   https://localhost:${PORT}/`);
  console.log(`   https://localhost:${PORT}/taskpane.html`);
  console.log(`\n📋 To use in Excel:`);
  console.log(`   1. Open Excel Online (excel.office.com)`);
  console.log(`   2. Insert → Office Add-ins → Upload My Add-in`);
  console.log(`   3. Upload manifest.xml from this directory`);
  console.log(`   4. The "openassay SQL" panel will appear\n`);
});
