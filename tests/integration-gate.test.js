'use strict';

// Приёмка P25 (карточка #64, эпик E6 #22, этап I08).
//
// Тесты проверяют внешний контракт Gate, а не внутренние вызовы: каждый сценарий
// поднимает изолированный стек (Gate + эмулятор провайдера на loopback) и
// наблюдает только исходы, квитанции, логи и эффекты на диске провайдера.
//
// Покрытие: AC-150/AC-250 (webhook: подпись → inbox + dedup → быстрый ACK →
// dispatch), AC-151/AC-251 (timeout = unknown до reconcile), AC-152 (одна
// реализация адаптера, бизнес-правила вне Gate), AC-252 (principal из binding),
// AC-253 (callback существующей операции не создаёт вторую задачу), логи этапа
// I08 (AC-154).

const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('fs');
const path = require('path');

const { createIntegrationGate } = require('../src/gate/index.js');
const { createProviderEmulator } = require('../src/provider-emulator/index.js');
const { createProviderAdapter } = require('../src/adapters/hh-sandbox-adapter.js');
const { createAdapterRegistry } = require('../src/gate/adapter-registry.js');
const { startHttpServer } = require('../src/http/server.js');
const { sign: signWebhook } = require('../src/gate/webhook-signature.js');
const { sanitizeFields } = require('../src/contract/events.js');

const ROOT = path.join(__dirname, '..');
const START = Date.parse('2026-10-03T09:00:00.000Z');
let tick = 0;
const now = () => new Date(START + tick);
const advance = ms => {
  tick += ms;
};

function makeRoot(name) {
  const root = path.join(ROOT, '.sandbox', `p25-test-${process.pid}`, name);
  fs.rmSync(root, { recursive: true, force: true });
  fs.mkdirSync(root, { recursive: true, mode: 0o700 });
  return root;
}

const teardowns = [];
async function teardownAll() {
  for (const teardown of teardowns.splice(0).reverse()) {
    await teardown();
  }
}

async function buildStack({ fault = 'success', bindingOptions = {}, providerOptions = {}, subscribe = true } = {}) {
  const root = makeRoot(`stack-${fault}-${teardowns.length}`);
  const gate = createIntegrationGate({ dataRoot: root, now, readProbePayload: { searchId: 'search-demo-1' } });
  const facade = await startHttpServer(gate);
  teardowns.push(facade.stop);

  const emulator = createProviderEmulator({
    root: path.join(root, 'provider'),
    now,
    fault,
    webhookUrl: facade.baseUrl,
    ...(providerOptions.delayMs ? { delayMs: providerOptions.delayMs } : {}),
    ...(providerOptions.listen === false ? { listen: false } : {}),
  });
  const started = await emulator.start();
  teardowns.push(() => (started.listening ? emulator.stop() : Promise.resolve()));

  const adapter = createProviderAdapter({ baseUrl: `http://127.0.0.1:${started.port}`, log: gate.log });
  gate.registry.register(adapter);

  const bindingRef = bindingOptions.ref || 'sbx/hh#default';
  const profileId = bindingOptions.profileId || 'profile-sandbox-1';
  gate.writeBindingValue(bindingRef, bindingOptions.value || 'synthetic-binding-value', {
    profileId,
    provider: adapter.provider,
    scopes: bindingOptions.scopes || ['hh#read', 'hh#write'],
    ...(bindingOptions.expiresAt ? { expiresAt: bindingOptions.expiresAt } : {}),
  });

  let subscription = null;
  let secret = null;
  if (started.listening && subscribe) {
    subscription = await gate.subscribe({
      integrationBindingId: bindingRef,
      eventType: 'application.decision.recorded',
      webhookUrl: facade.baseUrl,
      profileId,
    });
    if (subscription.webhookSubscriptionId) {
      secret = gate.subscriptionStore.secretOf(subscription.webhookSubscriptionId);
      emulator.setWebhookSecret(secret);
      emulator.setSubscriptionId(subscription.webhookSubscriptionId);
    } else if (!bindingOptions.expiresAt) {
      throw new Error(`webhook subscription was not established: ${JSON.stringify(subscription)}`);
    }
  }

  return { root, gate, facade, emulator, adapter, bindingRef, profileId, subscription, secret };
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

async function settle(gate, emulator) {
  await waitFor(() => gate.inbox.receivedCount() >= emulator.deliveryLog.length);
  await gate.drain();
}

function signedCallback({ secret, envelope, timestamp }) {
  const rawBody = JSON.stringify(envelope);
  return {
    headers: {
      'x-gate-signature': signWebhook({ secret, timestamp, rawBody }),
      'x-gate-timestamp': timestamp,
      'x-gate-delivery': envelope.providerEventId,
    },
    rawBody,
  };
}

test.afterEach(async () => {
  await teardownAll();
});

test('AC-150/AC-250: подпись → durable inbox + dedup → быстрый ACK → async dispatch', async () => {
  const { gate, emulator, bindingRef, profileId, secret, subscription } = await buildStack({ fault: 'success' });

  const envelope = {
    provider: 'hh-sandbox',
    kind: 'application.decision.recorded',
    providerEventId: 'evt-ac150',
    subscriptionId: subscription.webhookSubscriptionId,
    operationId: 'op-ac150',
    profileId,
    applicationId: 'app-demo-1',
    decision: 'advance',
  };
  const { headers, rawBody } = signedCallback({ secret, envelope, timestamp: String(now().getTime()) });

  const ack = gate.receiveCallback({ headers, rawBody, provider: 'hh-sandbox' });
  assert.equal(ack.status, 202);
  assert.equal(ack.reasonCode, 'CALLBACK_ACCEPTED');
  assert.equal(ack.applied, true);
  assert.equal(ack.duplicate, false);
  assert.equal(gate.inbox.receivedCount(), 1, 'durable receipt пишется до ACK');
  assert.equal(ack.dispatched, true, 'dispatch запланирован после ACK');

  await gate.drain();
  assert.equal(gate.outbox.count(), 1, 'dispatch публикует ровно одно событие');

  const replay = gate.receiveCallback({ headers, rawBody, provider: 'hh-sandbox' });
  assert.equal(replay.reasonCode, 'DUPLICATE_CALLBACK_IGNORED');
  assert.equal(replay.duplicate, true);
  assert.equal(gate.outbox.count(), 1, 'дубль не создаёт второго события');

  const forged = gate.receiveCallback({
    headers: { ...headers, 'x-gate-signature': 'v1=forged' },
    rawBody,
    provider: 'hh-sandbox',
  });
  assert.equal(forged.status, 401);
  assert.equal(forged.reasonCode, 'CALLBACK_SIGNATURE_INVALID');

  assert.equal(emulator.count(), 0, 'эффект провайдера не создаётся из обратного вызова');
});

test('AC-150: обратный вызов с новым callbackId, но тем же operationId — дубликат', async () => {
  const { gate, secret, subscription } = await buildStack({ fault: 'success' });

  const base = {
    provider: 'hh-sandbox',
    kind: 'application.decision.recorded',
    subscriptionId: subscription.webhookSubscriptionId,
    operationId: 'op-same-effect',
    profileId: 'profile-sandbox-1',
    applicationId: 'app-demo-1',
    decision: 'advance',
  };
  const first = signedCallback({ secret, envelope: { ...base, providerEventId: 'evt-a' }, timestamp: String(now().getTime()) });
  const second = signedCallback({ secret, envelope: { ...base, providerEventId: 'evt-b' }, timestamp: String(now().getTime()) });

  assert.equal(gate.receiveCallback({ ...first, provider: 'hh-sandbox' }).reasonCode, 'CALLBACK_ACCEPTED');
  const duplicate = gate.receiveCallback({ ...second, provider: 'hh-sandbox' });
  assert.equal(duplicate.reasonCode, 'DUPLICATE_CALLBACK_IGNORED');
  await gate.drain();
  assert.equal(gate.outbox.count(), 1);
});

test('AC-151/AC-251: таймаут мутации = unknown до reconcile; повтор только после reconcile', async () => {
  const { gate, emulator, bindingRef, profileId } = await buildStack({ fault: 'delay', providerOptions: { delayMs: 900 } });

  const mutation = await gate.invoke({
    integrationBindingId: bindingRef,
    capability: 'hh.application_decide',
    payload: { applicationId: 'app-demo-1', decision: 'advance' },
    operationId: 'op-ac151',
    profileId,
    userTaskId: 'ut-ac151',
    deadlineMs: 120,
  });
  assert.equal(mutation.outcome, 'outcome_unknown');
  assert.equal(mutation.code, 'EFFECT_STATE_UNKNOWN');
  assert.equal(mutation.retryAllowed, false);
  assert.equal(emulator.count(), 1, 'эффект уже произошёл, хотя ответа не было');

  const blind = await gate.invoke({
    integrationBindingId: bindingRef,
    capability: 'hh.application_decide',
    payload: { applicationId: 'app-demo-1', decision: 'advance' },
    operationId: 'op-ac151',
    profileId,
    deadlineMs: 120,
  });
  assert.equal(blind.code, 'RETRY_BEFORE_RECONCILE');
  assert.equal(blind.retryAllowed, false);
  assert.equal(emulator.count(), 1, 'слепой повтор не создаёт второй эффект');

  await waitFor(() => Boolean(emulator.lookup('op-ac151').receiptId));
  const reconciled = await gate.reconcile({ operationId: 'op-ac151', profileId });
  assert.equal(reconciled.outcome, 'result');
  assert.equal(reconciled.reconciled, true);
  assert.equal(reconciled.retryAllowed, true);

  const retry = await gate.invoke({
    integrationBindingId: bindingRef,
    capability: 'hh.application_decide',
    payload: { applicationId: 'app-demo-1', decision: 'advance' },
    operationId: 'op-ac151',
    profileId,
    deadlineMs: 120,
  });
  assert.equal(retry.outcome, 'result');
  assert.equal(retry.externalOperationRef, mutation.externalOperationRef || retry.externalOperationRef);
  assert.equal(emulator.count(), 1, 'после reconcile эффект по-прежнему один');
});

test('AC-251: operationId записан до вызова провайдера', async () => {
  const { gate, bindingRef, profileId } = await buildStack({ fault: 'success' });

  const pending = gate.invoke({
    integrationBindingId: bindingRef,
    capability: 'hh.application_decide',
    payload: { applicationId: 'app-demo-1', decision: 'advance' },
    operationId: 'op-before-call',
    profileId,
    deadlineMs: 120,
  });
  const entry = gate.ledger.get('op-before-call');
  assert.ok(entry, 'запись существует до ответа провайдера');
  assert.equal(entry.state, 'pending');
  assert.equal(entry.outcome, null);

  const result = await pending;
  assert.equal(result.outcome, 'result');
  assert.equal(gate.ledger.get('op-before-call').outcome, 'result');
});

test('AC-152: у провайдера одна реализация адаптера; вторая отклоняется', async () => {
  const { gate, adapter } = await buildStack({ fault: 'success' });

  assert.deepEqual(gate.registry.list(), ['hh-sandbox']);
  assert.equal(adapter.protocolVersion, 'provider-adapter/v1');

  assert.throws(
    () => gate.registry.register({ provider: 'hh-sandbox', protocolVersion: adapter.protocolVersion, capabilities: [] }),
    error => error.code === 'ADAPTER_ALREADY_REGISTERED'
  );
  assert.throws(
    () => gate.registry.register({ provider: 'hh-other', protocolVersion: 'provider-adapter/v0', capabilities: [] }),
    error => error.code === 'ADAPTER_PROTOCOL_UNSUPPORTED'
  );
  assert.deepEqual(gate.registry.list(), ['hh-sandbox'], 'реестр не изменился');
});

test('AC-152: бизнес-правила не живут в Gate', async () => {
  const businessRuleMarkers = [
    'prompt',
    'systemPrompt',
    'funnel',
    'score',
    'threshold',
    'playbook',
    'agent',
    'executor',
    'resume',
    'coverLetter',
    'interviewQuestion',
  ];
  const gateFiles = [];
  (function collect(dir) {
    for (const entry of fs.readdirSync(dir, { withFileTypes: true })) {
      const full = path.join(dir, entry.name);
      if (entry.isDirectory()) collect(full);
      else if (entry.name.endsWith('.js')) gateFiles.push(full);
    }
  })(path.join(ROOT, 'src', 'gate'));

  assert.ok(gateFiles.length > 0, 'в Gate должны быть модули');
  for (const file of gateFiles) {
    const source = fs.readFileSync(file, 'utf8');
    for (const marker of businessRuleMarkers) {
      assert.ok(
        !source.includes(marker),
        `${path.relative(ROOT, file)} содержит бизнес-правило "${marker}"; правила остаются в домене`
      );
    }
  }
});

test('AC-252: principal берётся из binding, а не от вызывающего', async () => {
  const { gate, emulator, bindingRef, profileId } = await buildStack({ fault: 'success' });
  const foreign = 'profile-sandbox-2';

  const read = await gate.invoke({
    integrationBindingId: bindingRef,
    capability: 'hh.search_status',
    payload: { searchId: 'search-demo-1' },
    profileId: foreign,
    userTaskId: 'ut-foreign',
  });
  assert.equal(read.code, 'BINDING_SCOPE_MISMATCH');
  assert.equal(read.outcome, 'blocked');
  assert.equal(emulator.count(), 0, 'чужой profile не дойдёт до провайдера');

  const poll = await gate.poll({ integrationBindingId: bindingRef, cursor: 0, limit: 10, profileId: foreign });
  assert.equal(poll.code, 'BINDING_SCOPE_MISMATCH');

  const own = await gate.invoke({
    integrationBindingId: bindingRef,
    capability: 'hh.search_status',
    payload: { searchId: 'search-demo-1' },
    profileId,
    userTaskId: 'ut-own',
  });
  assert.equal(own.outcome, 'result');
  assert.equal(own.profileId, profileId);
});

test('AC-253: callback существующей операции обновляет её и не создаёт вторую задачу', async () => {
  const { gate, emulator, bindingRef, profileId, secret, subscription } = await buildStack({ fault: 'success' });

  const mutation = await gate.invoke({
    integrationBindingId: bindingRef,
    capability: 'hh.application_decide',
    payload: { applicationId: 'app-demo-1', decision: 'advance' },
    operationId: 'op-ac253',
    profileId,
    userTaskId: 'ut-ac253',
  });
  assert.equal(mutation.outcome, 'result');
  await settle(gate, emulator);

  const entry = gate.ledger.get('op-ac253');
  assert.equal(entry.outcome, 'result');
  assert.ok(entry.reconciledAt, 'операция сверена обратным вызовом');
  assert.equal(gate.outbox.count(), 1);
  assert.equal(gate.taskPort.count(), 0, 'вторая задача не создаётся');

  const envelope = {
    provider: 'hh-sandbox',
    kind: 'application.decision.recorded',
    providerEventId: 'evt-ac253-replay',
    subscriptionId: subscription.webhookSubscriptionId,
    operationId: 'op-ac253',
    profileId,
    applicationId: 'app-demo-1',
    decision: 'advance',
  };
  const { headers, rawBody } = signedCallback({ secret, envelope, timestamp: String(now().getTime()) });
  const replay = gate.receiveCallback({ headers, rawBody, provider: 'hh-sandbox' });
  assert.equal(replay.reasonCode, 'DUPLICATE_CALLBACK_IGNORED', 'повтор той же операции не создаёт второго эффекта');
  await gate.drain();
  assert.equal(gate.outbox.count(), 1);
  assert.equal(gate.taskPort.count(), 0);
});

test('read/poll/webhook маршруты сохраняют scope', async () => {
  const { gate, emulator, bindingRef, profileId, secret, subscription } = await buildStack({ fault: 'success' });
  const foreign = 'profile-sandbox-2';

  const read = await gate.invoke({
    integrationBindingId: bindingRef,
    capability: 'hh.search_status',
    payload: { searchId: 'search-demo-1' },
    profileId: foreign,
  });
  assert.equal(read.code, 'BINDING_SCOPE_MISMATCH');

  const poll = await gate.poll({ integrationBindingId: bindingRef, profileId: foreign });
  assert.equal(poll.code, 'BINDING_SCOPE_MISMATCH');

  const envelope = {
    provider: 'hh-sandbox',
    kind: 'application.decision.recorded',
    providerEventId: 'evt-scope',
    subscriptionId: subscription.webhookSubscriptionId,
    operationId: 'op-scope',
    profileId: foreign,
    applicationId: 'app-demo-1',
    decision: 'advance',
  };
  const { headers, rawBody } = signedCallback({ secret, envelope, timestamp: String(now().getTime()) });
  const callback = gate.receiveCallback({ headers, rawBody, provider: 'hh-sandbox' });
  assert.equal(callback.reasonCode, 'CALLBACK_ACCEPTED');
  await gate.drain();

  const published = gate.outbox.entries().find(entry => entry.providerEventId === 'evt-scope');
  assert.equal(published.profileId, profileId, 'principal события берётся из подписки, а не из тела провайдера');
  assert.notEqual(published.profileId, foreign);

  assert.equal(emulator.count(), 0, 'ни один маршрут не вызвал провайдер от чужого профиля');

  const own = await gate.poll({ integrationBindingId: bindingRef, profileId });
  assert.equal(own.outcome, 'result');
  assert.equal(own.profileId, profileId);

  const unknownSubscription = signedCallback({
    secret,
    envelope: { ...envelope, subscriptionId: 'whsub_unknown', providerEventId: 'evt-scope-2' },
    timestamp: String(now().getTime()),
  });
  const rejected = gate.receiveCallback({ ...unknownSubscription, provider: 'hh-sandbox' });
  assert.equal(rejected.reasonCode, 'CALLBACK_SIGNATURE_INVALID', 'без подписки секрет не разрешается');
});

test('AC-154: логи этапа I08 содержат operationId, подписи/дедуп и unknown→reconciled без секретов', async () => {
  const { gate, emulator, bindingRef, profileId, secret, subscription } = await buildStack({ fault: 'delay', providerOptions: { delayMs: 300 } });

  const mutation = await gate.invoke({
    integrationBindingId: bindingRef,
    capability: 'hh.application_decide',
    payload: { applicationId: 'app-demo-1', decision: 'advance' },
    operationId: 'op-logs-1',
    profileId,
    userTaskId: 'ut-logs-1',
    deadlineMs: 120,
  });
  assert.equal(mutation.outcome, 'outcome_unknown');

  const envelope = {
    provider: 'hh-sandbox',
    kind: 'application.decision.recorded',
    providerEventId: 'evt-logs-1',
    subscriptionId: subscription.webhookSubscriptionId,
    operationId: 'op-logs-1',
    profileId,
    applicationId: 'app-demo-1',
    decision: 'advance',
  };
  const { headers, rawBody } = signedCallback({ secret, envelope, timestamp: String(now().getTime()) });
  gate.receiveCallback({ headers, rawBody, provider: 'hh-sandbox' });
  await gate.drain();

  const entries = gate.log.entries();
  const byEvent = name => entries.filter(entry => entry.event === name);

  assert.ok(byEvent('operation.started').some(e => e.operationId === 'op-logs-1'), 'operationId виден в логе');
  assert.ok(byEvent('operation.outcome').some(e => e.operationId === 'op-logs-1' && e.outcome === 'outcome_unknown'), 'unknown записан');
  assert.ok(byEvent('callback.accepted').some(e => e.providerEventId === 'evt-logs-1'), 'обратный вызов принят по providerEventId');
  assert.ok(byEvent('operation.reconciled_by_callback').some(e => e.operationId === 'op-logs-1'), 'unknown → reconciled виден');
  assert.ok(byEvent('event.published').some(e => e.eventId && e.operationId === 'op-logs-1'), 'нормализованное событие опубликовано');
  assert.ok(entries.some(e => e.profileId === profileId && e.userTaskId === 'ut-logs-1'), 'profile/userTask корреляция сохранена');

  const serialized = JSON.stringify(entries);
  assert.ok(!serialized.includes('synthetic-binding-value'), 'значение binding\'а не попадает в лог');
  assert.ok(!serialized.includes(secret), 'секрет подписи не попадает в лог');
  assert.ok(!serialized.includes('funnel') && !serialized.includes('vacancy'), 'сырой payload провайдера не попадает в лог');
  assert.ok(serialized.includes('op-logs-1'), 'внешние идентификаторы остаются в логе');
});

test('AC-154: сырой payload провайдера не публикуется в общий лог', async () => {
  const { gate, bindingRef, profileId } = await buildStack({ fault: 'success' });

  await gate.invoke({
    integrationBindingId: bindingRef,
    capability: 'hh.search_status',
    payload: { searchId: 'search-demo-1' },
    profileId,
    userTaskId: 'ut-raw-1',
  });

  const entries = gate.log.entries();
  const published = entries.filter(entry => entry.event === 'event.published');
  assert.equal(published.length, 0, 'read-операция не публикует сырой payload');
  const sanitized = sanitizeFields({ token: 'x', payload: { a: 1 }, safe: 1 });
  assert.deepEqual(sanitized, { safe: 1 });
});

test('истёкшая выдача — это blocked с человеческим текстом, а не машинный код', async () => {
  const { gate, emulator, bindingRef, profileId } = await buildStack({ fault: 'auth_expiry' });

  const read = await gate.invoke({
    integrationBindingId: bindingRef,
    capability: 'hh.search_status',
    payload: { searchId: 'search-demo-1' },
    profileId,
    userTaskId: 'ut-auth-1',
  });
  assert.equal(read.outcome, 'blocked');
  assert.equal(read.code, 'PROVIDER_AUTH_EXPIRED');
  assert.match(read.safeSummary, /обновите/i);
  assert.equal(emulator.count(), 0);

  const readiness = await gate.capabilities({ integrationBindingId: bindingRef, profileId, probe: true });
  assert.equal(readiness.authHealth, 'expired');
  assert.equal(readiness.capabilities.every(cap => cap.enabled === false), true);
});

test('истёкший binding отклоняется до обращения к провайдеру', async () => {
  const { gate, emulator, bindingRef, profileId } = await buildStack({
    fault: 'success',
    bindingOptions: { expiresAt: '2026-10-02T09:00:00.000Z' },
  });
  advance(60 * 1000);

  const read = await gate.invoke({
    integrationBindingId: bindingRef,
    capability: 'hh.search_status',
    payload: { searchId: 'search-demo-1' },
    profileId,
  });
  assert.equal(read.outcome, 'blocked');
  assert.equal(read.code, 'BINDING_EXPIRED');
  assert.match(read.safeSummary, /обновите/i);
  assert.equal(emulator.count(), 0, 'провайдер не вызывался');

  const readiness = await gate.capabilities({ integrationBindingId: bindingRef, profileId, probe: true });
  assert.equal(readiness.authHealth, 'expired');
});

test('недоступный провайдер — это failed, а не неизвестность', async () => {
  const { gate, emulator, bindingRef, profileId } = await buildStack({ fault: 'unreachable', providerOptions: { listen: false }, subscribe: false });

  const mutation = await gate.invoke({
    integrationBindingId: bindingRef,
    capability: 'hh.application_decide',
    payload: { applicationId: 'app-demo-1', decision: 'advance' },
    operationId: 'op-unreachable',
    profileId,
    deadlineMs: 120,
  });
  assert.equal(mutation.outcome, 'failed');
  assert.equal(mutation.code, 'PROVIDER_UNREACHABLE');
  assert.equal(emulator.count(), 0);
});

test('ошибка провайдера не создаёт эффект', async () => {
  const { gate, emulator, bindingRef, profileId } = await buildStack({ fault: 'error' });

  const mutation = await gate.invoke({
    integrationBindingId: bindingRef,
    capability: 'hh.application_decide',
    payload: { applicationId: 'app-demo-1', decision: 'advance' },
    operationId: 'op-error',
    profileId,
  });
  assert.equal(mutation.outcome, 'failed');
  assert.equal(mutation.code, 'PROVIDER_ERROR');
  assert.equal(emulator.count(), 0);
  assert.equal(gate.outbox.count(), 0);
});

test('подписка и отписка: webhookSubscriptionId и lifecycle state', async () => {
  const { gate, bindingRef, profileId } = await buildStack({ fault: 'success' });

  const subscription = await gate.subscribe({
    integrationBindingId: bindingRef,
    eventType: 'application.decision.recorded',
    webhookUrl: 'http://127.0.0.1:9/v1/callbacks',
    profileId,
  });
  assert.equal(subscription.outcome, 'result');
  assert.equal(subscription.lifecycleState, 'active');
  assert.equal(subscription.transport, 'webhook');

  const closed = await gate.unsubscribe({ webhookSubscriptionId: subscription.webhookSubscriptionId, profileId });
  assert.equal(closed.lifecycleState, 'inactive');

  const foreign = await gate.unsubscribe({ webhookSubscriptionId: subscription.webhookSubscriptionId, profileId: 'profile-sandbox-2' });
  assert.equal(foreign.code, 'BINDING_SCOPE_MISMATCH');
});

test('read и poll возвращают одинаковый нормализованный результат', async () => {
  const { gate, bindingRef, profileId } = await buildStack({ fault: 'success' });

  const read = await gate.invoke({
    integrationBindingId: bindingRef,
    capability: 'hh.search_status',
    payload: { searchId: 'search-demo-1' },
    profileId,
    userTaskId: 'ut-read-parity',
  });
  assert.equal(read.outcome, 'result');
  assert.equal(typeof read.eventId, 'string');

  const poll = await gate.poll({ integrationBindingId: bindingRef, cursor: 0, limit: 10, profileId });
  assert.equal(poll.outcome, 'result');
  assert.equal(poll.events.length, 0, 'poll не изобретает события, которых нет у провайдера');
  assert.equal(poll.hasMore, false);
});

test('реестр адаптеров отказывает в регистрации второй реализации того же провайдера', async () => {
  const registry = createAdapterRegistry();
  const adapter = { provider: 'hh-sandbox', protocolVersion: 'provider-adapter/v1', capabilities: [] };
  registry.register(adapter);
  assert.throws(
    () => registry.register({ provider: 'hh-sandbox', protocolVersion: 'provider-adapter/v1', capabilities: [] }),
    error => error.code === 'ADAPTER_ALREADY_REGISTERED'
  );
  assert.deepEqual(registry.list(), ['hh-sandbox']);
});

test(' Gate не создаёт GTD по факту внешнего события', async () => {
  const { gate, bindingRef, profileId, secret, subscription } = await buildStack({ fault: 'success' });

  const envelope = {
    provider: 'hh-sandbox',
    kind: 'application.decision.recorded',
    providerEventId: 'evt-no-gtd',
    subscriptionId: subscription.webhookSubscriptionId,
    operationId: null,
    profileId,
    applicationId: 'app-demo-1',
    decision: 'advance',
  };
  const { headers, rawBody } = signedCallback({ secret, envelope, timestamp: String(now().getTime()) });
  gate.receiveCallback({ headers, rawBody, provider: 'hh-sandbox' });
  await gate.drain();

  assert.equal(gate.taskPort.count(), 0, 'независимое событие без явной политики не создаёт задачу');
  const published = gate.outbox.entries()[0];
  assert.equal(published.createsGtd, false);
  assert.equal(published.gtdId, null);
});
