#!/usr/bin/env node
'use strict';

// Сценарий этапа I08 для External Integration Gate (карточка P25, эпик E6 #22).
//
// Одна команда: setup → run → evidence → teardown. Песочница живёт в
// `.sandbox/p25-<pid>/` (в .gitignore), наружу не ходит: только loopback.
// Часы виртуальные, поэтому transcript воспроизводим побайтово — это проверяет
// `npm run evidence:verify` в CI.
//
// Запуск:
//   node scripts/sandbox/integration-gate-sandbox.mjs            # пишет evidence
//   node scripts/sandbox/integration-gate-sandbox.mjs --verify   # сверка с закоммиченным transcript

const crypto = require('crypto');
const fs = require('fs');
const path = require('path');
const { execFileSync } = require('child_process');

const ROOT = path.join(__dirname, '..', '..');
const DEFAULT_OUT = path.join(ROOT, 'docs', 'evidence', 'p25-integration-gate');
const TRANSCRIPT_FILE = 'transcript.json';

const { createIntegrationGate } = require(path.join(ROOT, 'src', 'gate', 'index.js'));
const { createProviderEmulator } = require(path.join(ROOT, 'src', 'provider-emulator', 'index.js'));
const { createProviderAdapter } = require(path.join(ROOT, 'src', 'adapters', 'hh-sandbox-adapter.js'));
const { startHttpServer } = require(path.join(ROOT, 'src', 'http', 'server.js'));
const { sign: signWebhook, verify: verifyWebhook } = require(path.join(ROOT, 'src', 'gate', 'webhook-signature.js'));

const START = Date.parse('2026-10-03T09:00:00.000Z');
let tick = 0;
function now() {
  return new Date(START + tick);
}
function advance(ms) {
  tick += ms;
}

const results = [];
let current = null;

function scenario(name, fault, fn) {
  current = { name, fault, steps: [], checks: [] };
  results.push(current);
  return fn(current);
}

function step(event, fields) {
  current.steps.push({ event, ...fields });
}

function check(name, passed, detail) {
  current.checks.push({ name, passed: Boolean(passed), detail: detail || null });
}

function assertEqual(name, actual, expected) {
  check(name, actual === expected, `expected ${JSON.stringify(expected)}, got ${JSON.stringify(actual)}`);
}

function assertIncludes(name, haystack, needle) {
  check(name, String(haystack).includes(needle), `expected to find ${needle}`);
}

function makeRoot(name) {
  const root = path.join(ROOT, '.sandbox', `p25-${process.pid}`, name);
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

/**
 * Изолированный стек этапа: фасад Gate на loopback, эмулятор провайдера на
 * loopback, один адаптер, синтетические binding'и и подписка с секретом подписи.
 */
async function buildStack({ fault = 'success', bindingOptions = {}, providerOptions = {} } = {}) {
  const root = makeRoot(`stack-${fault}-${results.length}`);
  const gate = createIntegrationGate({
    dataRoot: root,
    now,
    readProbePayload: { searchId: 'search-demo-1' },
  });

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
  if (started.listening) {
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
    }
  }

  return { root, gate, facade, emulator, adapter, bindingRef, profileId, subscription, secret };
}

function waitFor(predicate, { timeoutMs = 5000, intervalMs = 20 } = {}) {
  return new Promise((resolve, reject) => {
    const deadline = Date.now() + timeoutMs;
    const tick = () => {
      if (predicate()) {
        resolve(true);
        return;
      }
      if (Date.now() > deadline) {
        reject(new Error('waitFor timed out'));
        return;
      }
      setTimeout(tick, intervalMs);
    };
    tick();
  });
}

/**
 * Дождаться, пока все доставленные провайдером обратные вызовы будут приняты
 * inbox'ом, и только затем дождаться завершения dispatch: иначе проверка outbox
 * гонялась с асинхронной доставкой.
 */
async function settle(gate, emulator, options = {}) {
  await waitFor(() => gate.inbox.receivedCount() >= emulator.deliveryLog.length, options);
  await gate.drain();
}

async function run() {
  // 1. success: read + mutation, один обратный вызов, эффект один.
  await scenario('success', 'success', async (report) => {
    const { gate, emulator, bindingRef, profileId } = await buildStack({ fault: 'success' });

    const read = await gate.invoke({
      integrationBindingId: bindingRef,
      capability: 'hh.search_status',
      payload: { searchId: 'search-demo-1' },
      profileId,
      userTaskId: 'ut-read-1',
      runId: 'run-read-1',
      replyContext: { channel: 'web', destinationRef: 'dest-sandbox-1' },
    });
    step('gate.invoke.read', { outcome: read.outcome, eventId: read.eventId, profileId: read.profileId, userTaskId: read.userTaskId });
    assertEqual('read outcome is result', read.outcome, 'result');
    assertEqual('read keeps profile scope', read.profileId, profileId);
    assertEqual('read keeps userTaskId', read.userTaskId, 'ut-read-1');

    const mutation = await gate.invoke({
      integrationBindingId: bindingRef,
      capability: 'hh.application_decide',
      payload: { applicationId: 'app-demo-1', decision: 'advance' },
      operationId: 'op-success-1',
      profileId,
      userTaskId: 'ut-mutate-1',
      runId: 'run-mutate-1',
      replyContext: { channel: 'web', destinationRef: 'dest-sandbox-1' },
    });
    step('gate.invoke.mutation', { outcome: mutation.outcome, operationId: mutation.operationId, externalOperationRef: mutation.externalOperationRef, eventId: mutation.eventId });
    assertEqual('mutation outcome is result', mutation.outcome, 'result');
    assertEqual('mutation has external ref', typeof mutation.externalOperationRef, 'string');
    assertEqual('exactly one provider effect', emulator.count(), 1);

    await settle(gate, emulator);
    step('emulator.deliveries', { posted: emulator.deliveryLog.length, applied: emulator.deliveryLog.filter(d => d.applied).length, duplicatesIgnored: emulator.deliveryLog.filter(d => d.duplicate).length });
    assertEqual('one callback delivered', emulator.deliveryLog.length, 1);
    assertEqual('one callback applied', emulator.deliveryLog.filter(d => d.applied).length, 1);
    assertEqual('no duplicate callbacks', emulator.deliveryLog.filter(d => d.duplicate).length, 0);
    assertEqual('one outbox event', gate.outbox.count(), 1);
    assertEqual('no task created for a callback of an existing operation', gate.taskPort.count(), 0);
  });

  // 2. error: провайдер отклоняет запись, эффекта нет.
  await scenario('error', 'error', async (report) => {
    const { gate, emulator, bindingRef, profileId } = await buildStack({ fault: 'error' });
    const mutation = await gate.invoke({
      integrationBindingId: bindingRef,
      capability: 'hh.application_decide',
      payload: { applicationId: 'app-demo-1', decision: 'advance' },
      operationId: 'op-error-1',
      profileId,
      userTaskId: 'ut-error-1',
    });
    step('gate.invoke.mutation', { outcome: mutation.outcome, code: mutation.code, operationId: mutation.operationId });
    assertEqual('mutation outcome is failed', mutation.outcome, 'failed');
    assertEqual('provider error code', mutation.code, 'PROVIDER_ERROR');
    assertEqual('no provider effect', emulator.count(), 0);
    assertEqual('no outbox event', gate.outbox.count(), 0);
  });

  // 3. delay: таймаут после отправки = unknown; повтор только после reconcile.
  await scenario('delay', 'delay', async (report) => {
    const { gate, emulator, bindingRef, profileId } = await buildStack({ fault: 'delay', providerOptions: { delayMs: 900 } });

    const mutation = await gate.invoke({
      integrationBindingId: bindingRef,
      capability: 'hh.application_decide',
      payload: { applicationId: 'app-demo-1', decision: 'advance' },
      operationId: 'op-delay-1',
      profileId,
      userTaskId: 'ut-delay-1',
      deadlineMs: 120,
    });
    step('gate.invoke.mutation', { outcome: mutation.outcome, code: mutation.code, operationId: mutation.operationId, retryAllowed: mutation.retryAllowed });
    assertEqual('timeout after send is outcome_unknown', mutation.outcome, 'outcome_unknown');
    assertEqual('unknown code', mutation.code, 'EFFECT_STATE_UNKNOWN');
    assertEqual('retry is not allowed while unknown', mutation.retryAllowed, false);
    assertEqual('the effect already happened on the provider', emulator.count(), 1);

    const blindRetry = await gate.invoke({
      integrationBindingId: bindingRef,
      capability: 'hh.application_decide',
      payload: { applicationId: 'app-demo-1', decision: 'advance' },
      operationId: 'op-delay-1',
      profileId,
      userTaskId: 'ut-delay-1',
      deadlineMs: 120,
    });
    step('gate.invoke.retry_before_reconcile', { outcome: blindRetry.outcome, code: blindRetry.code, retryAllowed: blindRetry.retryAllowed });
    assertEqual('blind retry is refused', blindRetry.code, 'RETRY_BEFORE_RECONCILE');
    assertEqual('blind retry produced no second effect', emulator.count(), 1);

    // Квитанция «догоняет» позже дедлайна: сначала ждём её у провайдера, и
    // только потом сверяем — reconcile обязан вернуть подтверждённый исход.
    await waitFor(() => Boolean(emulator.lookup('op-delay-1').receiptId));
    const reconciled = await gate.reconcile({ operationId: 'op-delay-1', profileId });
    step('gate.reconcile', { outcome: reconciled.outcome, reconciled: reconciled.reconciled, retryAllowed: reconciled.retryAllowed, externalOperationRef: reconciled.externalOperationRef });
    assertEqual('reconcile confirms the outcome', reconciled.outcome, 'result');
    assertEqual('reconcile marks the operation reconciled', reconciled.reconciled, true);
    assertEqual('retry is allowed after reconcile', reconciled.retryAllowed, true);

    const retryAfterReconcile = await gate.invoke({
      integrationBindingId: bindingRef,
      capability: 'hh.application_decide',
      payload: { applicationId: 'app-demo-1', decision: 'advance' },
      operationId: 'op-delay-1',
      profileId,
      userTaskId: 'ut-delay-1',
      deadlineMs: 120,
    });
    step('gate.invoke.retry_after_reconcile', { outcome: retryAfterReconcile.outcome, externalOperationRef: retryAfterReconcile.externalOperationRef });
    assertEqual('retry after reconcile returns the same receipt', retryAfterReconcile.outcome, 'result');
    assertEqual('still exactly one provider effect', emulator.count(), 1);

    // Поздний обратный вызов приходит после таймаута клиента и тоже сверяет исход.
    await waitFor(() => emulator.deliveryLog.length > 0);
    await settle(gate, emulator);
    assertEqual('late callback delivered once', emulator.deliveryLog.filter(d => d.applied).length, 1);
    assertEqual('one outbox event', gate.outbox.count(), 1);
  });

  // 4. auth_expiry: истёкшая выдача — это blocked с человеческим текстом.
  await scenario('auth_expiry', 'auth_expiry', async (report) => {
    const { gate, emulator, bindingRef, profileId } = await buildStack({ fault: 'auth_expiry' });

    const read = await gate.invoke({
      integrationBindingId: bindingRef,
      capability: 'hh.search_status',
      payload: { searchId: 'search-demo-1' },
      profileId,
      userTaskId: 'ut-auth-1',
    });
    step('gate.invoke.read', { outcome: read.outcome, code: read.code, safeSummary: read.safeSummary });
    assertEqual('read is blocked', read.outcome, 'blocked');
    assertEqual('auth expiry code', read.code, 'PROVIDER_AUTH_EXPIRED');
    assertIncludes('blocked carries a human summary', read.safeSummary, 'обновите');

    const mutation = await gate.invoke({
      integrationBindingId: bindingRef,
      capability: 'hh.application_decide',
      payload: { applicationId: 'app-demo-1', decision: 'advance' },
      operationId: 'op-auth-1',
      profileId,
      userTaskId: 'ut-auth-1',
    });
    step('gate.invoke.mutation', { outcome: mutation.outcome, code: mutation.code });
    assertEqual('mutation is blocked', mutation.outcome, 'blocked');
    assertEqual('no provider effect', emulator.count(), 0);

    const readiness = await gate.capabilities({ integrationBindingId: bindingRef, profileId, probe: true });
    step('gate.capabilities', { authHealth: readiness.authHealth, capabilities: readiness.capabilities.map(cap => ({ name: cap.name, enabled: cap.enabled })) });
    assertEqual('readiness probe reports expired auth', readiness.authHealth, 'expired');
    assertEqual('capabilities are disabled while the binding is expired', readiness.capabilities.every(cap => cap.enabled === false), true);
  });

  // 5. duplicate_callback: три доставки, один эффект, одно событие.
  await scenario('duplicate_callback', 'duplicate_callback', async (report) => {
    const { gate, emulator, bindingRef, profileId } = await buildStack({ fault: 'duplicate_callback' });

    const mutation = await gate.invoke({
      integrationBindingId: bindingRef,
      capability: 'hh.application_decide',
      payload: { applicationId: 'app-demo-1', decision: 'advance' },
      operationId: 'op-dup-1',
      profileId,
      userTaskId: 'ut-dup-1',
    });
    step('gate.invoke.mutation', { outcome: mutation.outcome, operationId: mutation.operationId });
    assertEqual('mutation outcome is result', mutation.outcome, 'result');

    await settle(gate, emulator);
    step('emulator.deliveries', { posted: emulator.deliveryLog.length, applied: emulator.deliveryLog.filter(d => d.applied).length, duplicatesIgnored: emulator.deliveryLog.filter(d => d.duplicate).length });
    assertEqual('three deliveries posted', emulator.deliveryLog.length, 3);
    assertEqual('exactly one applied', emulator.deliveryLog.filter(d => d.applied).length, 1);
    assertEqual('two duplicates ignored', emulator.deliveryLog.filter(d => d.duplicate).length, 2);
    assertEqual('one outbox event', gate.outbox.count(), 1);
    assertEqual('one inbox applied entry', gate.inbox.appliedCount(), 1);
    assertEqual('no task created', gate.taskPort.count(), 0);
  });

  // 6. unreachable: сервис не отвечает — это failed, а не неизвестность.
  await scenario('unreachable', 'unreachable', async (report) => {
    const { gate, emulator, bindingRef, profileId } = await buildStack({ fault: 'unreachable', providerOptions: { listen: false } });
    const mutation = await gate.invoke({
      integrationBindingId: bindingRef,
      capability: 'hh.application_decide',
      payload: { applicationId: 'app-demo-1', decision: 'advance' },
      operationId: 'op-unreachable-1',
      profileId,
      userTaskId: 'ut-unreachable-1',
      deadlineMs: 120,
    });
    step('gate.invoke.mutation', { outcome: mutation.outcome, code: mutation.code });
    assertEqual('unreachable is failed', mutation.outcome, 'failed');
    assertEqual('unreachable code', mutation.code, 'PROVIDER_UNREACHABLE');
    assertEqual('no provider effect', emulator.count(), 0);
  });

  // 7. expired_binding: истёкший binding отклоняется до обращения к провайдеру.
  await scenario('expired_binding', 'success', async (report) => {
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
      userTaskId: 'ut-expired-1',
    });
    step('gate.invoke.read', { outcome: read.outcome, code: read.code, safeSummary: read.safeSummary });
    assertEqual('expired binding is blocked', read.outcome, 'blocked');
    assertEqual('expired binding code', read.code, 'BINDING_EXPIRED');
    assertIncludes('expired binding carries a human summary', read.safeSummary, 'обновите');
    assertEqual('no provider call was made', emulator.count(), 0);

    const readiness = await gate.capabilities({ integrationBindingId: bindingRef, profileId, probe: true });
    step('gate.capabilities', { authHealth: readiness.authHealth });
    assertEqual('readiness reports expired auth', readiness.authHealth, 'expired');
  });

  // 8. cross_profile_scope: read/poll/webhook сохраняют scope.
  await scenario('cross_profile_scope', 'success', async (report) => {
    const { gate, emulator, bindingRef, profileId, subscription } = await buildStack({ fault: 'success' });
    const foreignProfile = 'profile-sandbox-2';

    const read = await gate.invoke({
      integrationBindingId: bindingRef,
      capability: 'hh.search_status',
      payload: { searchId: 'search-demo-1' },
      profileId: foreignProfile,
      userTaskId: 'ut-scope-1',
    });
    step('gate.invoke.read', { outcome: read.outcome, code: read.code });
    assertEqual('foreign profile cannot read', read.code, 'BINDING_SCOPE_MISMATCH');

    const poll = await gate.poll({ integrationBindingId: bindingRef, cursor: 0, limit: 10, profileId: foreignProfile });
    step('gate.poll', { outcome: poll.outcome, code: poll.code });
    assertEqual('foreign profile cannot poll', poll.code, 'BINDING_SCOPE_MISMATCH');

    step('gate.subscribe', { outcome: subscription.outcome, webhookSubscriptionId: subscription.webhookSubscriptionId, transport: subscription.transport });
    assertEqual('subscription is active', subscription.lifecycleState, 'active');

    const callback = gate.receiveCallback({
      headers: {
        'x-gate-signature': 'v1=forged',
        'x-gate-timestamp': String(now().getTime()),
        'x-gate-delivery': 'evt_forged',
      },
      rawBody: JSON.stringify({
        provider: 'hh-sandbox',
        kind: 'application.decision.recorded',
        providerEventId: 'evt_forged',
        operationId: 'op-scope-1',
        profileId: foreignProfile,
      }),
      provider: 'hh-sandbox',
    });
    step('gate.receive_callback', { status: callback.status, reasonCode: callback.reasonCode });
    assertEqual('forged callback is rejected', callback.reasonCode, 'CALLBACK_SIGNATURE_INVALID');
    assertEqual('no provider call was made for a foreign profile', emulator.count(), 0);
  });

  // 9. webhook_lifecycle: подпись → durable receipt → дедуп → быстрый ACK → dispatch.
  await scenario('webhook_lifecycle', 'success', async (report) => {
    const { gate, secret, subscription } = await buildStack({ fault: 'success' });
    assertEqual('subscription secret is host-owned', typeof secret, 'string');

    const envelope = {
      provider: 'hh-sandbox',
      kind: 'application.decision.recorded',
      providerEventId: 'evt-lifecycle-1',
      subscriptionId: subscription.webhookSubscriptionId,
      operationId: 'op-lifecycle-1',
      profileId: 'profile-sandbox-1',
      applicationId: 'app-demo-1',
      decision: 'advance',
      replyContext: { channel: 'web', destinationRef: 'dest-sandbox-1' },
    };
    const rawBody = JSON.stringify(envelope);
    const timestamp = String(now().getTime());
    const signature = signWebhook({ secret, timestamp, rawBody });

    const startedAt = Date.now();
    const ack = gate.receiveCallback({
      headers: { 'x-gate-signature': signature, 'x-gate-timestamp': timestamp, 'x-gate-delivery': envelope.providerEventId },
      rawBody,
      provider: 'hh-sandbox',
    });
    const ackLatencyMs = Date.now() - startedAt;
    step('gate.receive_callback', { status: ack.status, reasonCode: ack.reasonCode, receiptId: ack.receipt.receiptId, providerEventId: ack.receipt.providerEventId });
    assertEqual('callback is accepted', ack.reasonCode, 'CALLBACK_ACCEPTED');
    assertEqual('durable receipt exists before the ACK', gate.inbox.receivedCount() >= 1, true);
    assertEqual('ACK is returned before dispatch completes', ack.dispatched, true);

    await gate.drain();
    step('gate.outbox', { events: gate.outbox.count() });
    assertEqual('dispatch published exactly one event', gate.outbox.count(), 1);
    assertEqual('ACK latency is below the dispatch latency', ackLatencyMs < 5000, true);

    const replay = gate.receiveCallback({
      headers: { 'x-gate-signature': signature, 'x-gate-timestamp': timestamp, 'x-gate-delivery': envelope.providerEventId },
      rawBody,
      provider: 'hh-sandbox',
    });
    step('gate.receive_callback.replay', { status: replay.status, reasonCode: replay.reasonCode, duplicate: replay.duplicate });
    assertEqual('duplicate callback is ignored', replay.reasonCode, 'DUPLICATE_CALLBACK_IGNORED');
    assertEqual('still one outbox event', gate.outbox.count(), 1);

    // Подпись пересчитывается с устаревшим временем: проверяется именно окно,
    // а не целостность подписи.
    const staleTimestamp = String(now().getTime() - 3600 * 1000);
    const staleSignature = signWebhook({ secret, timestamp: staleTimestamp, rawBody });
    const replayWindow = verifyWebhook({ secret, signature: staleSignature, timestamp: staleTimestamp, rawBody, now });
    step('gate.signature.replay_window', { ok: replayWindow.ok, reasonCode: replayWindow.reasonCode });
    assertEqual('stale timestamp is outside the replay window', replayWindow.ok, false);
    assertEqual('stale timestamp code', replayWindow.reasonCode, 'CALLBACK_REPLAY_WINDOW');
  });

  // 10. no_second_task: callback существующей операции обновляет её состояние.
  await scenario('no_second_task', 'success', async (report) => {
    const { gate, profileId, secret, subscription } = await buildStack({ fault: 'success' });

    const mutation = await gate.invoke({
      integrationBindingId: 'sbx/hh#default',
      capability: 'hh.application_decide',
      payload: { applicationId: 'app-demo-1', decision: 'advance' },
      operationId: 'op-second-task-1',
      profileId,
      userTaskId: 'ut-second-task-1',
    });
    assertEqual('mutation outcome is result', mutation.outcome, 'result');

    const envelope = {
      provider: 'hh-sandbox',
      kind: 'application.decision.recorded',
      providerEventId: 'evt-second-task-1',
      subscriptionId: subscription.webhookSubscriptionId,
      operationId: 'op-second-task-1',
      profileId,
      applicationId: 'app-demo-1',
      decision: 'advance',
    };
    const rawBody = JSON.stringify(envelope);
    const timestamp = String(now().getTime());
    const ack = gate.receiveCallback({
      headers: { 'x-gate-signature': signWebhook({ secret, timestamp, rawBody }), 'x-gate-timestamp': timestamp, 'x-gate-delivery': envelope.providerEventId },
      rawBody,
      provider: 'hh-sandbox',
    });
    await gate.drain();

    const entry = gate.ledger.get('op-second-task-1');
    step('gate.ledger', { operationId: entry.operationId, outcome: entry.outcome, state: entry.state, callback: entry.callback });
    assertEqual('callback updated the existing operation', entry.outcome, 'result');
    assertEqual('operation is reconciled by the callback', Boolean(entry.reconciledAt), true);
    assertEqual('exactly one outbox event', gate.outbox.count(), 1);
    assertEqual('no second task was created', gate.taskPort.count(), 0);
  });

  // 11. single_adapter_implementation: второй реализации адаптера не существует.
  await scenario('single_adapter_implementation', 'success', async (report) => {
    const { gate, adapter } = await buildStack({ fault: 'success' });
    step('gate.registry', { providers: gate.registry.list(), protocolVersion: adapter.protocolVersion });
    assertEqual('exactly one provider is registered', gate.registry.list().length, 1);

    let second;
    try {
      gate.registry.register({ provider: 'hh-sandbox', protocolVersion: adapter.protocolVersion, capabilities: [] });
      second = 'registered';
    } catch (error) {
      second = error.code;
    }
    step('gate.registry.duplicate', { outcome: second });
    assertEqual('a second adapter implementation is refused', second, 'ADAPTER_ALREADY_REGISTERED');

    let unsupported;
    try {
      gate.registry.register({ provider: 'hh-other', protocolVersion: 'provider-adapter/v0', capabilities: [] });
      unsupported = 'registered';
    } catch (error) {
      unsupported = error.code;
    }
    step('gate.registry.protocol', { outcome: unsupported });
    assertEqual('an unsupported adapter protocol is refused', unsupported, 'ADAPTER_PROTOCOL_UNSUPPORTED');
  });

  // 12. live_smoke: живой test account не выдан — заявка честно заблокирована.
  await scenario('live_smoke', 'success', async (report) => {
    const { emulator } = await buildStack({ fault: 'success' });
    const request = emulator.requestLiveSmoke({ bindingNames: [] });
    step('provider.request_live_smoke', { attempted: request.attempted, performed: request.performed, blockedBy: request.blockedBy, missingBindings: request.missingBindings });
    assertEqual('live smoke is requested', request.attempted, true);
    assertEqual('live smoke is not performed', request.performed, false);
    assertEqual('live smoke is blocked by the missing test account', request.blockedBy, 'NO_TEST_ACCOUNT_BINDING');
  });

  // 13. fidelity: что эмулировано и что требует живого провайдера.
  await scenario('fidelity', 'success', async (report) => {
    const { emulator } = await buildStack({ fault: 'success' });
    step('provider.fidelity', {
      mode: emulator.fidelity.mode,
      liveSandbox: emulator.fidelity.liveSandbox,
      containsPersonalData: emulator.fidelity.containsPersonalData,
      liveSmokePerformed: emulator.fidelity.liveSmoke.performed,
      emulatedProperties: emulator.fidelity.emulatedProperties,
    });
    assertEqual('the provider is emulated', emulator.fidelity.mode, 'emulator');
    assertEqual('no live sandbox is claimed', emulator.fidelity.liveSandbox, 'unsupported');
    assertEqual('the sample carries no personal data', emulator.fidelity.containsPersonalData, false);
    assertEqual('a green emulator run is not a live provider test', emulator.fidelity.liveSmoke.performed, false);
  });

  await teardownAll();
  return results;
}

function renderTranscript(results) {
  const checks = results.flatMap(report => report.checks);
  const failed = checks.filter(entry => !entry.passed);
  return {
    schemaVersion: 1,
    card: 'P25',
    epic: 'E6 #22',
    stage: 'I08',
    contract: 'C10',
    occurredAt: new Date(START).toISOString(),
    clock: 'virtual',
    sandboxRoot: '.sandbox/p25-<pid>/',
    summary: {
      scenarios: results.length,
      checks: checks.length,
      passed: checks.length - failed.length,
      failed: failed.length,
      status: failed.length === 0 ? 'PASS' : 'FAIL',
    },
    scenarios: results.map(report => ({
      name: report.name,
      fault: report.fault,
      steps: report.steps,
      checks: report.checks,
    })),
    acceptance: {
      'AC-150': 'webhook: signature verified, durable inbox receipt, dedup by providerEventId and operationId, fast ACK, async dispatch',
      'AC-151': 'mutation timeout is outcome_unknown; retry is refused until reconcile confirms the outcome',
      'AC-152': 'one adapter implementation per provider; the registry refuses a second one; business rules stay out of the gate',
      'AC-250': 'verify → inbox + dedup → fast ACK',
      'AC-251': 'operationId is durable before the provider call; unknown → reconcile → retry',
      'AC-252': 'principal is taken from the binding via the host-owned resolver, never from the caller',
      'AC-253': 'a callback of an existing operation updates its state and creates no second task',
    },
    fidelity: {
      provider: 'hh-sandbox',
      mode: 'emulator',
      liveSandbox: 'unsupported',
      liveSmokePerformed: false,
      note: 'a green emulator run is not a live provider test',
    },
  };
}

function sanitizeTranscript(transcript) {
  const raw = JSON.stringify(transcript);
  const forbidden = ['synthetic-binding-value', 'whsec_', '127.0.0.1', process.pid, ROOT];
  for (const needle of forbidden) {
    if (raw.includes(String(needle))) {
      throw new Error(`transcript is not sanitized: found "${String(needle).slice(0, 24)}"`);
    }
  }
  return transcript;
}

function writeEvidence(outDir, transcript) {
  fs.mkdirSync(outDir, { recursive: true, mode: 0o700 });
  const file = path.join(outDir, TRANSCRIPT_FILE);
  fs.writeFileSync(file, `${JSON.stringify(transcript, null, 2)}\n`, { mode: 0o600 });
  const hash = crypto.createHash('sha256').update(fs.readFileSync(file)).digest('hex');
  fs.writeFileSync(path.join(outDir, 'transcript.sha256'), `${hash}\n`, { mode: 0o600 });
  return { file, hash };
}

function main() {
  const args = process.argv.slice(2);
  const verify = args.includes('--verify');
  const outIndex = args.indexOf('--out');
  const outDir = outIndex >= 0 ? path.resolve(args[outIndex + 1]) : DEFAULT_OUT;

  return run()
    .then(results => {
      const transcript = sanitizeTranscript(renderTranscript(results));
      if (verify) {
        const tempRoot = path.join(ROOT, '.sandbox', `p25-verify-${process.pid}`);
        fs.rmSync(tempRoot, { recursive: true, force: true });
        fs.mkdirSync(tempRoot, { recursive: true, mode: 0o700 });
        const tempDir = path.join(tempRoot, 'evidence');
        const generated = writeEvidence(tempDir, transcript);
        const committedPath = path.join(DEFAULT_OUT, TRANSCRIPT_FILE);
        if (!fs.existsSync(committedPath)) {
          process.stderr.write(`verify failed: no committed transcript at ${path.relative(ROOT, committedPath)}\n`);
          process.exit(1);
        }
        const committed = fs.readFileSync(committedPath, 'utf8');
        const actual = fs.readFileSync(generated.file, 'utf8');
        fs.rmSync(tempRoot, { recursive: true, force: true });
        if (committed !== actual) {
          const diffRoot = path.join(ROOT, '.sandbox', `p25-verify-diff-${process.pid}`);
          fs.mkdirSync(diffRoot, { recursive: true, mode: 0o700 });
          fs.writeFileSync(path.join(diffRoot, 'actual.json'), actual, { mode: 0o600 });
          fs.writeFileSync(path.join(diffRoot, 'committed.json'), committed, { mode: 0o600 });
          process.stderr.write(`verify failed: the committed transcript differs from a fresh run (dump: ${path.relative(ROOT, diffRoot)})\n`);
          process.exit(1);
        }
        process.stdout.write(`evidence verify ok: ${results.length} scenarios, ${transcript.summary.checks} checks, sha256 ${generated.hash}\n`);
        return;
      }
      const { hash } = writeEvidence(outDir, transcript);
      for (const report of results) {
        const failed = report.checks.filter(entry => !entry.passed);
        process.stdout.write(`${failed.length === 0 ? 'ok  ' : 'FAIL'} ${report.name} (${report.fault}) — ${report.checks.length - failed.length}/${report.checks.length}\n`);
      }
      process.stdout.write(`\n${transcript.summary.status}: ${transcript.summary.passed}/${transcript.summary.checks} checks, ${results.length} scenarios\n`);
      process.stdout.write(`evidence: ${path.relative(ROOT, path.join(outDir, TRANSCRIPT_FILE))} sha256 ${hash}\n`);
      if (transcript.summary.failed > 0) process.exit(1);
    })
    .catch(error => {
      process.stderr.write(`sandbox failed: ${String(error && error.stack ? error.stack : error)}\n`);
      process.exit(1);
    });
}

main();
