'use strict';

// I7: publisher ErrorEvent (C12) в System Error Watcher и вызов events.error()
// на сайтах сбоев Gate + заполнение correlation на событиях.
//
// Тесты поднимают настоящие loopback-серверы (watcher и фасад Gate) и
// наблюдают wire-форму: POST /errors с x-watcher-key и x-watcher-scopes,
// dropped count при сбое доставки и C12-поля, записанные events.error().

const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('fs');
const http = require('http');
const path = require('path');

const { createErrorPublisher, resolveErrorPublisher } = require('../src/error-publisher.js');
const { createEventLog } = require('../src/contract/events.js');
const { createIntegrationGate } = require('../src/gate/index.js');
const { createWebhookInbox } = require('../src/gate/webhook-inbox.js');
const { createHhAdapter } = require('../src/adapters/hh-adapter.js');
const { startHttpServer } = require('../src/http/server.js');
const { sign: signWebhook } = require('../src/gate/webhook-signature.js');

const ROOT = path.join(__dirname, '..');
const START = Date.parse('2026-10-04T09:00:00.000Z');
const now = () => new Date(START);

function makeRoot(name) {
  const root = path.join(ROOT, '.sandbox', `error-publisher-test-${process.pid}`, name);
  fs.rmSync(root, { recursive: true, force: true });
  fs.mkdirSync(root, { recursive: true, mode: 0o700 });
  return root;
}

function c12Event(overrides = {}) {
  return {
    schemaVersion: 1,
    eventId: 'err_sample_1',
    occurredAt: '2026-10-04T09:00:00.000Z',
    source: { service: 'integration-gate', release: 'sandbox', environment: 'sandbox' },
    scope: { kind: 'profile', tenantId: null, profileId: 'profile-1' },
    correlation: { userTaskId: 'ut-1', runId: 'run-1', traceId: null },
    replyContext: { channel: null, destinationRef: null, status: 'not_applicable' },
    error: {
      code: 'OPERATION_BLOCKED',
      operation: 'invoke',
      severity: 'error',
      retryable: true,
      outcome: 'failed',
      safeSummary: 'доступ отклонён',
      privateDetailsRef: null,
    },
    origin: { kind: 'application', incidentId: null, diagnosticDepth: 0 },
    ...overrides,
  };
}

function spyErrorLog(root) {
  const log = createEventLog({ file: path.join(root, 'events.jsonl'), now });
  const calls = [];
  const original = log.error;
  log.error = params => {
    calls.push(params);
    return original(params);
  };
  return { log, calls };
}

function waitFor(predicate, { timeoutMs = 5000, intervalMs = 20 } = {}) {
  return new Promise((resolve, reject) => {
    const deadline = Date.now() + timeoutMs;
    const step = () => {
      if (predicate()) return resolve(true);
      if (Date.now() > deadline) return reject(new Error('waitFor timed out'));
      setTimeout(step, intervalMs);
    };
    step();
  });
}

const teardowns = [];
async function teardownAll() {
  for (const teardown of teardowns.splice(0).reverse()) {
    await teardown();
  }
}

async function startWatcher({ respondWith = 202 } = {}) {
  const received = [];
  const server = http.createServer((request, response) => {
    const chunks = [];
    request.on('data', chunk => chunks.push(chunk));
    request.on('end', () => {
      received.push({
        method: request.method,
        url: request.url,
        headers: request.headers,
        body: JSON.parse(Buffer.concat(chunks).toString('utf8')),
      });
      response.writeHead(respondWith, { 'content-type': 'application/json' });
      response.end('{}');
    });
  });
  await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
  teardowns.push(() => new Promise(resolve => server.close(() => resolve())));
  return { received, baseUrl: `http://127.0.0.1:${server.address().port}` };
}

async function deadWatcherUrl() {
  const server = http.createServer();
  await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
  const { port } = server.address();
  await new Promise(resolve => server.close(resolve));
  return `http://127.0.0.1:${port}`;
}

test.afterEach(async () => {
  await teardownAll();
});

test('publishError отправляет C12 ErrorEvent на POST /errors с ключом и scope', async () => {
  const { received, baseUrl } = await startWatcher();
  const publisher = createErrorPublisher({ watcherUrl: baseUrl, watcherKey: 'wk-integration-gate', environment: 'staging' });
  const event = c12Event();

  const result = await publisher.publishError(event);

  assert.deepEqual(result, { published: true });
  assert.equal(publisher.getDroppedCount(), 0);
  assert.equal(received.length, 1);
  const [request] = received;
  assert.equal(request.method, 'POST');
  assert.equal(request.url, '/errors');
  assert.equal(request.headers['x-watcher-key'], 'wk-integration-gate');
  assert.equal(request.headers['x-watcher-scopes'], 'error:write');
  assert.match(request.headers['content-type'], /application\/json/);
  assert.deepEqual(request.body, { ...event, source: { ...event.source, environment: 'staging' } });
});

test('сбой доставки не бросает и увеличивает dropped count', async () => {
  const publisher = createErrorPublisher({ watcherUrl: await deadWatcherUrl(), watcherKey: 'wk-test' });

  const result = await publisher.publishError(c12Event());

  assert.deepEqual(result, { published: false });
  assert.equal(publisher.getDroppedCount(), 1);
  assert.equal(publisher.getSpool().length, 1);
  assert.equal(publisher.getSpool()[0].eventId, 'err_sample_1');
});

test('ответ watcher 500 считается сбоем доставки', async () => {
  const { baseUrl } = await startWatcher({ respondWith: 500 });
  const publisher = createErrorPublisher({ watcherUrl: baseUrl, watcherKey: 'wk-test' });

  const result = await publisher.publishError(c12Event());

  assert.deepEqual(result, { published: false });
  assert.equal(publisher.getDroppedCount(), 1);
});

test('spool ограничен 100 событиями, dropped count растёт', async () => {
  const publisher = createErrorPublisher({ watcherUrl: await deadWatcherUrl(), watcherKey: 'wk-test' });

  for (let i = 0; i < 105; i += 1) {
    await publisher.publishError(c12Event({ eventId: `err_${i}` }));
  }

  assert.equal(publisher.getDroppedCount(), 105);
  assert.equal(publisher.getSpool().length, 100);
  assert.equal(publisher.getSpool()[0].eventId, 'err_5', 'вытесняются самые старые события');
});

test('resolveErrorPublisher без полного набора переменных окружения возвращает null', () => {
  assert.equal(resolveErrorPublisher({}), null);
  assert.equal(resolveErrorPublisher({ ERROR_WATCHER_URL: 'http://127.0.0.1:9' }), null);
  assert.equal(resolveErrorPublisher({ ERROR_WATCHER_KEY: 'wk-only' }), null);

  const configured = resolveErrorPublisher({
    ERROR_WATCHER_URL: 'http://127.0.0.1:9',
    ERROR_WATCHER_KEY: 'wk-test',
    NODE_ENV: 'production',
  });
  assert.equal(typeof configured.publishError, 'function');
  assert.equal(configured.getDroppedCount(), 0);
  assert.deepEqual(configured.getSpool(), []);
});

test('operation.blocked вызывает events.error() с C12-полями и correlation', async () => {
  const root = makeRoot('operation-blocked');
  const { log, calls } = spyErrorLog(root);
  const gate = createIntegrationGate({ dataRoot: root, now, log });

  const result = await gate.invoke({
    integrationBindingId: 'missing/binding',
    capability: 'hh.search_resumes',
    payload: { query: 'node', area: ['1'] },
    profileId: 'profile-1',
    userTaskId: 'ut-blocked-1',
    runId: 'run-blocked-1',
    replyContext: { channel: 'telegram', destinationRef: 'chat-42' },
  });
  assert.equal(result.outcome, 'blocked');
  assert.equal(result.code, 'BINDING_NOT_FOUND');

  assert.equal(calls.length, 1, 'events.error() вызван ровно один раз');
  const [error] = calls;
  assert.equal(error.code, 'OPERATION_BLOCKED');
  assert.equal(error.severity, 'error');
  assert.equal(error.retryable, true);
  assert.equal(error.outcome, 'failed');
  assert.equal(error.scope.kind, 'profile');
  assert.equal(error.scope.tenantId, null);
  assert.equal(error.scope.profileId, 'profile-1');
  assert.deepEqual(error.correlation, { userTaskId: 'ut-blocked-1', runId: 'run-blocked-1', traceId: null });
  assert.deepEqual(error.replyContext, { channel: 'telegram', destinationRef: 'chat-42', status: 'not_applicable' });
  assert.equal(error.origin.kind, 'application');
  assert.equal(error.origin.diagnosticDepth, 0);
  assert.ok(error.safeSummary.length > 0 && error.safeSummary.length <= 240);

  const entry = log.entries().find(item => item.event === 'error');
  assert.ok(entry, 'events.error() записал ErrorEvent в общий лог');
  assert.match(entry.eventId, /^err_[0-9a-f]{24}$/);
  assert.equal(entry.error.code, 'OPERATION_BLOCKED');
  assert.deepEqual(entry.correlation, error.correlation);

  const blocked = log.entries().find(item => item.event === 'operation.blocked');
  assert.deepEqual(blocked.correlation, { userTaskId: 'ut-blocked-1', runId: 'run-blocked-1', traceId: null }, 'correlation заполнен и на обычных событиях');
});

test('GATE_ERROR вызывает events.error() без профиля и корреляции', async () => {
  const root = makeRoot('gate-error');
  const { log, calls } = spyErrorLog(root);
  const gate = createIntegrationGate({ dataRoot: root, now, log });
  const facade = await startHttpServer(gate);
  teardowns.push(facade.stop);

  const response = await fetch(`${facade.baseUrl}/v1/invoke`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: '{broken json',
  });
  assert.equal(response.status, 500);
  assert.equal((await response.json()).code, 'GATE_ERROR');

  assert.equal(calls.length, 1);
  assert.equal(calls[0].code, 'GATE_ERROR');
  assert.equal(calls[0].operation, 'invoke');
  assert.equal(calls[0].scope.kind, 'platform');
  assert.equal(calls[0].scope.profileId, null);
  assert.deepEqual(calls[0].correlation, { userTaskId: null, runId: null, traceId: null });
  assert.ok(calls[0].safeSummary.length > 0 && calls[0].safeSummary.length <= 240);
});

test('callback.dispatch_failed вызывает events.error()', async () => {
  const root = makeRoot('dispatch-failed');
  const { log, calls } = spyErrorLog(root);
  const inbox = createWebhookInbox({
    root: path.join(root, 'inbox'),
    log,
    clock: now,
    dispatch: async () => {
      throw new Error('dispatch exploded');
    },
  });

  const rawBody = JSON.stringify({
    provider: 'hh',
    kind: 'application.decision.recorded',
    providerEventId: 'evt-dispatch-1',
    operationId: 'op-dispatch-1',
    profileId: 'profile-1',
    userTaskId: 'ut-dispatch-1',
    runId: 'run-dispatch-1',
  });
  const timestamp = String(START);
  const headers = {
    'x-gate-signature': signWebhook({ secret: 'whsec_test', timestamp, rawBody }),
    'x-gate-timestamp': timestamp,
    'x-gate-delivery': 'evt-dispatch-1',
  };

  const ack = inbox.receive({ headers, rawBody, provider: 'hh', secret: 'whsec_test' });
  assert.equal(ack.status, 202);
  await inbox.drain();

  assert.equal(calls.length, 1);
  assert.equal(calls[0].code, 'CALLBACK_DISPATCH_FAILED');
  assert.equal(calls[0].operation, 'dispatchCallback');
  assert.equal(calls[0].scope.kind, 'profile');
  assert.deepEqual(calls[0].correlation, { userTaskId: 'ut-dispatch-1', runId: 'run-dispatch-1', traceId: null });
  assert.equal(calls[0].replyContext.status, 'not_applicable');
});

test('provider.auth_refresh_failed вызывает events.error()', async () => {
  const root = makeRoot('auth-refresh-failed');
  const { log, calls } = spyErrorLog(root);
  const adapter = createHhAdapter({
    baseUrl: 'http://127.0.0.1:1',
    log,
    fetchImpl: async () => ({ status: 401, text: async () => JSON.stringify({ message: 'expired' }) }),
    refreshAccessToken: async () => {
      throw new Error('the host rejected the refresh grant');
    },
  });

  const result = await adapter.invoke({
    operationId: 'op-auth-refresh-1',
    capability: 'hh.search_resumes',
    payload: { query: 'node', area: ['1'] },
    scope: 'hh#read',
    binding: { value: 'hh-access-token', binding: { ref: 'sbx/hh#default' } },
    profileId: 'profile-1',
    userTaskId: 'ut-auth-1',
    runId: 'run-auth-1',
    replyContext: { channel: 'telegram', destinationRef: 'chat-42' },
  });

  assert.equal(result.outcome, 'blocked');
  assert.equal(result.code, 'PROVIDER_AUTH_EXPIRED');
  assert.equal(calls.length, 1);
  assert.equal(calls[0].code, 'PROVIDER_AUTH_REFRESH_FAILED');
  assert.equal(calls[0].operation, 'refreshAccessToken');
  assert.equal(calls[0].scope.kind, 'profile');
  assert.deepEqual(calls[0].correlation, { userTaskId: 'ut-auth-1', runId: 'run-auth-1', traceId: null });
  assert.deepEqual(calls[0].replyContext, { channel: 'telegram', destinationRef: 'chat-42', status: 'not_applicable' });
  assert.ok(calls[0].safeSummary.length <= 240);
});

test('ErrorEvent из Gate уходит в Error Watcher через errorPublisher', async () => {
  const { received, baseUrl } = await startWatcher();
  const publisher = createErrorPublisher({ watcherUrl: baseUrl, watcherKey: 'wk-gate', environment: 'sandbox' });
  const root = makeRoot('gate-publisher');
  const gate = createIntegrationGate({ dataRoot: root, now, errorPublisher: publisher });

  await gate.invoke({
    integrationBindingId: 'missing/binding',
    capability: 'hh.search_resumes',
    payload: { query: 'node', area: ['1'] },
    profileId: 'profile-1',
    userTaskId: 'ut-pub-1',
    runId: 'run-pub-1',
  });

  await waitFor(() => received.length === 1);
  const [request] = received;
  assert.equal(request.method, 'POST');
  assert.equal(request.url, '/errors');
  assert.equal(request.headers['x-watcher-key'], 'wk-gate');
  assert.equal(request.headers['x-watcher-scopes'], 'error:write');
  assert.equal(request.body.event, undefined, 'имя события не уходит на wire');
  assert.equal(request.body.error.code, 'OPERATION_BLOCKED');
  assert.equal(request.body.source.service, 'integration-gate');
  assert.equal(request.body.source.environment, 'sandbox');
  assert.match(request.body.eventId, /^err_/);
  assert.deepEqual(request.body.correlation, { userTaskId: 'ut-pub-1', runId: 'run-pub-1', traceId: null });
});
