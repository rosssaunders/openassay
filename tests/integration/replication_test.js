#!/usr/bin/env node
/**
 * End-to-end replication integration test.
 *
 * 1. Spins up a real PostgreSQL via Docker
 * 2. Creates a table and publication on upstream PG
 * 3. Starts postrust's pg_server
 * 4. Executes CREATE SUBSCRIPTION on postrust to replicate from upstream
 * 5. Inserts data into upstream PG
 * 6. Asserts the data appears in postrust via PG wire protocol
 *
 * Requirements: docker, cargo (postrust), node, npm (pg)
 *
 * Usage: npm test (or node replication_test.js)
 */

const { Client } = require('pg');
const { execSync, spawn } = require('child_process');
const { setTimeout: sleep } = require('timers/promises');

const PG_PORT = 15432;
const PG_USER = 'postgres';
const PG_PASS = 'testpass';
const PG_DB = 'testdb';
const PG_CONTAINER = 'postrust-test-pg';

const POSTRUST_PORT = 55433;

let postrustProc = null;
let passed = 0;
let failed = 0;

function assert(condition, message) {
  if (!condition) {
    console.error(`  ✗ FAIL: ${message}`);
    failed++;
  } else {
    console.log(`  ✓ PASS: ${message}`);
    passed++;
  }
}

function assertEqual(actual, expected, message) {
  if (JSON.stringify(actual) !== JSON.stringify(expected)) {
    console.error(`  ✗ FAIL: ${message}`);
    console.error(`    expected: ${JSON.stringify(expected)}`);
    console.error(`    actual:   ${JSON.stringify(actual)}`);
    failed++;
  } else {
    console.log(`  ✓ PASS: ${message}`);
    passed++;
  }
}

async function startPostgres() {
  console.log('\n📦 Starting PostgreSQL container...');

  // Stop any existing container
  try {
    execSync(`docker rm -f ${PG_CONTAINER} 2>/dev/null`, { stdio: 'ignore' });
  } catch {}

  // Start PostgreSQL with logical replication enabled
  execSync(
    `docker run -d --name ${PG_CONTAINER} ` +
    `-e POSTGRES_PASSWORD=${PG_PASS} ` +
    `-e POSTGRES_DB=${PG_DB} ` +
    `-p ${PG_PORT}:5432 ` +
    `postgres:16 ` +
    `-c wal_level=logical ` +
    `-c max_replication_slots=4 ` +
    `-c max_wal_senders=4`,
    { stdio: 'inherit' }
  );

  // Wait for PG to be ready
  console.log('  Waiting for PostgreSQL to be ready...');
  for (let i = 0; i < 30; i++) {
    try {
      execSync(
        `docker exec ${PG_CONTAINER} pg_isready -U ${PG_USER}`,
        { stdio: 'ignore' }
      );
      console.log('  PostgreSQL is ready.');
      return;
    } catch {
      await sleep(1000);
    }
  }
  throw new Error('PostgreSQL failed to start within 30s');
}

async function setupUpstream() {
  console.log('\n📝 Setting up upstream tables and publication...');
  const client = new Client({
    host: 'localhost',
    port: PG_PORT,
    user: PG_USER,
    password: PG_PASS,
    database: PG_DB,
  });
  await client.connect();

  await client.query(`
    CREATE TABLE users (
      id SERIAL PRIMARY KEY,
      name TEXT NOT NULL,
      email TEXT,
      created_at TIMESTAMP DEFAULT NOW()
    )
  `);

  await client.query(`
    CREATE TABLE orders (
      id SERIAL PRIMARY KEY,
      user_id INTEGER REFERENCES users(id),
      amount NUMERIC(10,2) NOT NULL,
      status TEXT DEFAULT 'pending'
    )
  `);

  // Create publication for all tables
  await client.query(`CREATE PUBLICATION postrust_pub FOR ALL TABLES`);

  console.log('  Created tables: users, orders');
  console.log('  Created publication: postrust_pub');

  await client.end();
}

function startPostrust() {
  console.log('\n🚀 Starting postrust pg_server...');

  postrustProc = spawn(
    'cargo',
    ['run', '--bin', 'pg_server', '--', `127.0.0.1:${POSTRUST_PORT}`],
    {
      cwd: `${__dirname}/../..`,
      stdio: ['ignore', 'pipe', 'pipe'],
    }
  );

  return new Promise((resolve, reject) => {
    let output = '';
    const timeout = setTimeout(() => {
      reject(new Error('postrust failed to start within 30s'));
    }, 30000);

    postrustProc.stdout.on('data', (data) => {
      output += data.toString();
      if (output.includes('listening on')) {
        clearTimeout(timeout);
        console.log(`  postrust listening on port ${POSTRUST_PORT}`);
        resolve();
      }
    });

    postrustProc.stderr.on('data', (data) => {
      const msg = data.toString();
      // cargo build output goes to stderr
      if (msg.includes('error') && !msg.includes('Compiling') && !msg.includes('Downloading')) {
        clearTimeout(timeout);
        reject(new Error(`postrust error: ${msg}`));
      }
    });

    postrustProc.on('error', (err) => {
      clearTimeout(timeout);
      reject(err);
    });
  });
}

async function connectPostrust() {
  const client = new Client({
    host: 'localhost',
    port: POSTRUST_PORT,
    user: 'postgres',
    password: 'postgres',
    database: 'postgres',
  });
  await client.connect();
  return client;
}

async function connectUpstream() {
  const client = new Client({
    host: 'localhost',
    port: PG_PORT,
    user: PG_USER,
    password: PG_PASS,
    database: PG_DB,
  });
  await client.connect();
  return client;
}

async function testBasicQuery() {
  console.log('\n🧪 Test: Basic postrust query works');
  const pr = await connectPostrust();
  const res = await pr.query('SELECT 1 AS num, \'hello\' AS greeting');
  assertEqual(res.rows.length, 1, 'Should return 1 row');
  assertEqual(res.rows[0].num, '1', 'num should be 1');
  assertEqual(res.rows[0].greeting, 'hello', 'greeting should be hello');
  await pr.end();
}

async function testCreateSubscription() {
  console.log('\n🧪 Test: CREATE SUBSCRIPTION on postrust');
  const pr = await connectPostrust();

  // First create the target tables (postrust needs them before replication)
  await pr.query(`
    CREATE TABLE users (
      id SERIAL PRIMARY KEY,
      name TEXT NOT NULL,
      email TEXT,
      created_at TIMESTAMP DEFAULT NOW()
    )
  `);

  await pr.query(`
    CREATE TABLE orders (
      id SERIAL PRIMARY KEY,
      user_id INTEGER,
      amount NUMERIC(10,2) NOT NULL,
      status TEXT DEFAULT 'pending'
    )
  `);

  // Create subscription
  try {
    await pr.query(`
      CREATE SUBSCRIPTION test_sub
        CONNECTION 'host=localhost port=${PG_PORT} dbname=${PG_DB} user=${PG_USER} password=${PG_PASS}'
        PUBLICATION postrust_pub
        WITH (copy_data = true)
    `);
    console.log('  ✓ PASS: CREATE SUBSCRIPTION succeeded');
    passed++;
  } catch (err) {
    console.log(`  ✗ FAIL: CREATE SUBSCRIPTION failed: ${err.message}`);
    failed++;
  }

  await pr.end();
}

async function testInitialSync() {
  console.log('\n🧪 Test: Initial data sync');

  // Insert data into upstream BEFORE subscription (tests copy_data)
  const upstream = await connectUpstream();
  await upstream.query(`
    INSERT INTO users (name, email) VALUES
      ('Alice', 'alice@example.com'),
      ('Bob', 'bob@example.com'),
      ('Charlie', 'charlie@example.com')
  `);
  await upstream.query(`
    INSERT INTO orders (user_id, amount, status) VALUES
      (1, 99.99, 'completed'),
      (2, 49.50, 'pending'),
      (3, 199.00, 'shipped')
  `);
  await upstream.end();

  // Wait for replication to catch up
  console.log('  Waiting for replication sync...');
  await sleep(3000);

  // Query postrust
  const pr = await connectPostrust();
  const users = await pr.query('SELECT name, email FROM users ORDER BY name');
  assertEqual(users.rows.length, 3, 'Should have 3 users after initial sync');
  assertEqual(users.rows[0].name, 'Alice', 'First user should be Alice');

  const orders = await pr.query('SELECT amount, status FROM orders ORDER BY amount');
  assertEqual(orders.rows.length, 3, 'Should have 3 orders after initial sync');

  await pr.end();
}

async function testStreamingReplication() {
  console.log('\n🧪 Test: Streaming replication (INSERT after subscription)');

  // Insert new data into upstream
  const upstream = await connectUpstream();
  await upstream.query(`
    INSERT INTO users (name, email) VALUES ('Dave', 'dave@example.com')
  `);
  await upstream.query(`
    INSERT INTO orders (user_id, amount, status) VALUES (4, 75.00, 'pending')
  `);
  await upstream.end();

  // Wait for replication
  console.log('  Waiting for streaming replication...');
  await sleep(2000);

  // Verify in postrust
  const pr = await connectPostrust();
  const users = await pr.query('SELECT name FROM users ORDER BY name');
  assertEqual(users.rows.length, 4, 'Should have 4 users after streaming insert');
  assert(
    users.rows.some(r => r.name === 'Dave'),
    'Dave should appear via streaming replication'
  );

  const orders = await pr.query('SELECT COUNT(*) AS cnt FROM orders');
  assertEqual(orders.rows[0].cnt, '4', 'Should have 4 orders');

  await pr.end();
}

async function testUpdateReplication() {
  console.log('\n🧪 Test: Streaming replication (UPDATE)');

  const upstream = await connectUpstream();
  await upstream.query(`UPDATE users SET email = 'alice-new@example.com' WHERE name = 'Alice'`);
  await upstream.end();

  await sleep(2000);

  const pr = await connectPostrust();
  const res = await pr.query(`SELECT email FROM users WHERE name = 'Alice'`);
  assertEqual(res.rows.length, 1, 'Alice should still exist');
  assertEqual(res.rows[0].email, 'alice-new@example.com', 'Alice email should be updated');
  await pr.end();
}

async function testDeleteReplication() {
  console.log('\n🧪 Test: Streaming replication (DELETE)');

  const upstream = await connectUpstream();
  await upstream.query(`DELETE FROM orders WHERE status = 'pending'`);
  await upstream.end();

  await sleep(2000);

  const pr = await connectPostrust();
  const res = await pr.query(`SELECT COUNT(*) AS cnt FROM orders WHERE status = 'pending'`);
  assertEqual(res.rows[0].cnt, '0', 'Pending orders should be deleted');
  await pr.end();
}

async function cleanup() {
  console.log('\n🧹 Cleaning up...');
  if (postrustProc) {
    postrustProc.kill('SIGTERM');
    await sleep(500);
  }
  try {
    execSync(`docker rm -f ${PG_CONTAINER}`, { stdio: 'ignore' });
  } catch {}
}

async function main() {
  console.log('═══════════════════════════════════════════════');
  console.log('  postrust Replication Integration Test');
  console.log('═══════════════════════════════════════════════');

  try {
    await startPostgres();
    await setupUpstream();
    await startPostrust();

    await testBasicQuery();
    await testCreateSubscription();
    await testInitialSync();
    await testStreamingReplication();
    await testUpdateReplication();
    await testDeleteReplication();

  } catch (err) {
    console.error(`\n💥 Test harness error: ${err.message}`);
    failed++;
  } finally {
    await cleanup();
  }

  console.log('\n═══════════════════════════════════════════════');
  console.log(`  Results: ${passed} passed, ${failed} failed`);
  console.log('═══════════════════════════════════════════════\n');

  process.exit(failed > 0 ? 1 : 0);
}

main();
