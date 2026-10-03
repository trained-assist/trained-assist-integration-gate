#!/usr/bin/env node
'use strict';

// Сценарий этапа I08 для карточки P26 (эпик E6 #22): реалистичный адаптер HH и
// существующий cron холодного поиска → срабатывания расписания.
//
// Одна команда: setup → run → evidence → teardown. Песочница живёт в
// `.sandbox/p26-<pid>/` (в .gitignore), наружу не ходит: только loopback. Часы
// виртуальные, поэтому transcript воспроизводим побайтово — это проверяет
// `npm run evidence:verify:p26` в CI.
//
// Запуск:
//   node scripts/sandbox/hh-pilot-sandbox.cjs            # пишет evidence
//   node scripts/sandbox/hh-pilot-sandbox.cjs --verify   # сверка с закоммиченным transcript

const crypto = require('crypto');
const fs = require('fs');
const path = require('path');

const ROOT = path.join(__dirname, '..', '..');
const DEFAULT_OUT = path.join(ROOT, 'docs', 'evidence', 'p26-hh-pilot');
const TRANSCRIPT_FILE = 'transcript.json';

const { createIntegrationGate } = require(path.join(ROOT, 'src', 'gate', 'index.js'));
const { createHhApiEmulator } = require(path.join(ROOT, 'src', 'provider-emulator', 'hh-api.js'));
const { createHhAdapter } = require(path.join(ROOT, 'src', 'adapters', 'hh-adapter.js'));
const { createColdSearchPilot } = require(path.join(ROOT, 'src', 'pilot', 'cold-search.js'));
const { startHttpServer } = require(path.join(ROOT, 'src', 'http', 'server.js'));

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
  const root = path.join(ROOT, '.sandbox', `p26-${process.pid}`, name);
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

// Декларации существующего cron холодного поиска: те же значения, что выдаёт
// доменный `intervalToCron` (trained-assist-hh-skill, hh-cold-search-cron.js).
const DECLARATIONS = {
  hourly: { profileId: 'profile-sandbox-1', vacancyId: 'vacancy-8801', cron: '39 * * * *', timezone: 'Europe/Moscow', intervalHours: 1, goal: 'cold search for vacancy-8801' },
  halfHourly: { profileId: 'profile-sandbox-1', vacancyId: 'vacancy-8801', cron: '9,39 * * * *', timezone: 'Europe/Moscow', intervalHours: 0.5, goal: 'cold search for vacancy-8801' },
  threeHourly: { profileId: 'profile-sandbox-1', vacancyId: 'vacancy-8801', cron: '39 0-23/3 * * *', timezone: 'Europe/Moscow', intervalHours: 3, goal: 'cold search for vacancy-8801' },
  daily: { profileId: 'profile-sandbox-1', vacancyId: 'vacancy-9102', cron: '4 17 * * *', timezone: 'Europe/Moscow', intervalHours: 24, goal: 'cold search for vacancy-9102' },
};

/** Общий task flow песочницы: тот же контракт, что у задачи расписания P22. */
function createTaskFlowRecorder() {
  const submitted = [];
  const completed = [];
  let refusals = 0;
  return {
    submitted,
    completed,
    refuseNextSubmit() {
      refusals += 1;
    },
    async submit(input) {
      if (refusals > 0) {
        refusals -= 1;
        throw new Error('the common task flow refused the occurrence');
      }
      const record = { ...input, taskId: input.userTaskId, runId: `run-${input.occurrenceKey.replace(/[^a-zA-Z0-9]/g, '-')}`, created: true };
      submitted.push(record);
      return record;
    },
    async complete(input) {
      completed.push(input);
      return { accepted: true };
    },
  };
}

/**
 * Изолированный стек пилота: фасад Gate на loopback, эмулятор HH в форме
 * настоящего API на loopback, один адаптер, синтетическая выдача и общий
 * task flow-рекордер.
 */
async function buildStack({ fault = 'ok', withRefresh = false, bindingOptions = {}, providerOptions = {} } = {}) {
  const root = makeRoot(`stack-${fault}-${results.length}`);
  const gate = createIntegrationGate({ dataRoot: root, now });
  const facade = await startHttpServer(gate, { pilot: null });
  teardowns.push(facade.stop);

  const emulator = createHhApiEmulator({
    root: path.join(root, 'provider'),
    now,
    fault,
    refreshToken: 'synthetic-refresh-token',
    ...(providerOptions.listen === false ? { listen: false } : {}),
  });
  const started = await emulator.start();
  teardowns.push(() => (started.listening ? emulator.stop() : Promise.resolve()));

  const adapter = createHhAdapter({
    baseUrl: `http://127.0.0.1:${started.port}`,
    log: gate.log,
    retryDelayMs: 0,
    // Приватный артефакт провайдера пишет хост: сырые резюме не идут в лог.
    recordArtifact: params => emulator.writeArtifact(params.ref, params),
    ...(withRefresh
      ? {
          refreshAccessToken: async () => {
            const result = await emulator.refreshAccess({ grant_type: 'refresh_token', refresh_token: 'synthetic-refresh-token' });
            return result.status === 200 ? result.body.access_token : null;
          },
        }
      : {}),
  });
  gate.registry.register(adapter);

  const bindingRef = bindingOptions.ref || 'sbx/hh#default';
  const profileId = bindingOptions.profileId || 'profile-sandbox-1';
  gate.writeBindingValue(bindingRef, bindingOptions.value || 'synthetic-binding-value', {
    profileId,
    provider: adapter.provider,
    scopes: bindingOptions.scopes || ['hh#read'],
    ...(bindingOptions.expiresAt ? { expiresAt: bindingOptions.expiresAt } : {}),
  });

  const taskFlow = createTaskFlowRecorder();
  const pilot = createColdSearchPilot({ gate, dataRoot: root, now, log: gate.log, taskFlow });

  return { root, gate, facade, emulator, adapter, bindingRef, profileId, taskFlow, pilot };
}

function occurrenceFor(declaration, at) {
  const scheduledFor = at || Date.parse('2026-10-03T10:00:00.000Z');
  const occurrenceKey = new Date(scheduledFor).toISOString().replace(/\.\d{3}Z$/, 'Z');
  return {
    occurrenceId: `occ-${declaration.vacancyId}-${occurrenceKey}`,
    scheduleId: `cold-search:${declaration.vacancyId}`,
    occurrenceKey,
    userTaskId: `ut-${declaration.vacancyId}-${occurrenceKey}`,
    profileId: declaration.profileId,
    scheduledFor,
  };
}

async function run() {
  // 1. Почасовая декларация маппится на запрос расписания без пересчёта cron.
  await scenario('cron_mapped_hourly', 'ok', async (report) => {
    const { pilot } = await buildStack({ fault: 'ok' });

    const hourly = pilot.declareSchedule(DECLARATIONS.hourly);
    step('pilot.declare_schedule', {
      outcome: hourly.outcome,
      cron: hourly.scheduleRequest.cron,
      timezone: hourly.scheduleRequest.timezone,
      intervalHours: hourly.provenance.effectiveIntervalHours,
      intervalSource: hourly.provenance.intervalSource,
      sourceJobName: hourly.provenance.sourceJobName,
    });
    assertEqual('hourly cron is taken from the domain declaration', hourly.scheduleRequest.cron, '39 * * * *');
    assertEqual('hourly timezone is Europe/Moscow', hourly.scheduleRequest.timezone, 'Europe/Moscow');
    assertEqual('the effective interval is reported, not recomputed', hourly.provenance.intervalSource, 'domain_cron_declaration');
    assertEqual('hourly is mapped', hourly.pilotStatus.hourlyMapped, true);
    assertEqual('hourly is not production enabled', hourly.pilotStatus.hourlyProductionEnabled, false);
    assertEqual('the prod trigger is not configured', hourly.pilotStatus.prodTriggerConfigured, false);

    const halfHourly = pilot.declareSchedule(DECLARATIONS.halfHourly);
    step('pilot.declare_schedule.half_hourly', { cron: halfHourly.scheduleRequest.cron, intervalHours: halfHourly.provenance.effectiveIntervalHours });
    assertEqual('sub-hourly declaration is accepted as data', halfHourly.scheduleRequest.cron, '9,39 * * * *');

    const daily = pilot.declareSchedule(DECLARATIONS.daily);
    step('pilot.declare_schedule.daily', { cron: daily.scheduleRequest.cron, intervalHours: daily.provenance.effectiveIntervalHours });
    assertEqual('daily declaration is accepted as data', daily.scheduleRequest.cron, '4 17 * * *');
  });

  // 2. Запрос на прод отказывает: почасовая поддержка не выдаётся за production enabled.
  await scenario('production_enable_refused', 'ok', async (report) => {
    const { pilot } = await buildStack({ fault: 'ok' });

    const production = pilot.declareSchedule({ ...DECLARATIONS.hourly, production: true });
    step('pilot.declare_schedule.production', { outcome: production.outcome, code: production.code, scheduleRequest: production.scheduleRequest });
    assertEqual('a production request is refused', production.code, 'PILOT_PRODUCTION_NOT_ENABLED');
    assertEqual('no schedule request is produced', production.scheduleRequest, null);
    assertEqual('the refusal keeps hourly unmapped in production', production.pilotStatus.hourlyProductionEnabled, false);

    const status = pilot.status();
    step('pilot.status', { mode: status.mode, hourlyProductionEnabled: status.hourlyProductionEnabled, prodTriggerConfigured: status.prodTriggerConfigured, liveTaskFlowWired: status.liveTaskFlowWired });
    assertEqual('the pilot runs in the sandbox mode', status.mode, 'sandbox');
    assertEqual('the live task flow is not wired', status.liveTaskFlowWired, false);
    assertEqual('no live smoke was performed', status.liveSmoke.performed, false);
  });

  // 3. Occurrence → задача через общий task flow → поиск → результат задачи.
  await scenario('occurrence_delivers_result', 'ok', async (report) => {
    const { gate, emulator, bindingRef, taskFlow, pilot } = await buildStack({ fault: 'ok' });
    const occurrence = occurrenceFor(DECLARATIONS.hourly);

    const result = await pilot.runOccurrence({
      occurrence,
      integrationBindingId: bindingRef,
      query: 'node developer',
      area: ['1', '113'],
      replyContext: { channel: 'web', destinationRef: 'dest-sandbox-1' },
    });
    step('pilot.run_occurrence', {
      outcome: result.outcome,
      deduplicated: result.deduplicated,
      userTaskId: result.userTaskId,
      runId: result.runId,
      operationId: result.operationId,
      itemCount: result.result.itemCount,
      privateDetailsRef: result.result.privateDetailsRef,
    });
    assertEqual('the read succeeds', result.outcome, 'result');
    assertEqual('the task carries the schedule userTaskId', result.userTaskId, occurrence.userTaskId);
    assertEqual('the run id comes from the common task flow', typeof result.runId, 'string');
    assertEqual('the sanitized sample is bounded', result.result.itemCount, 4);
    assertEqual('heavy data stays an artifact ref', typeof result.result.privateDetailsRef, 'string');
    assertEqual('exactly one provider read', emulator.reads(), 1);

    step('task_flow.submitted', { count: taskFlow.submitted.length, source: taskFlow.submitted[0].source, autoRun: taskFlow.submitted[0].autoRun, scheduleId: taskFlow.submitted[0].scheduleId, occurrenceKey: taskFlow.submitted[0].occurrenceKey });
    assertEqual('exactly one task was submitted', taskFlow.submitted.length, 1);
    assertEqual('the task is submitted by the schedule source', taskFlow.submitted[0].source, 'schedule');
    assertEqual('the task runs without clarifications', taskFlow.submitted[0].autoRun, true);

    step('task_flow.completed', { count: taskFlow.completed.length, outcome: taskFlow.completed[0].outcome, itemCount: taskFlow.completed[0].result.itemCount });
    assertEqual('the integration data returns as the task result', taskFlow.completed[0].result.itemCount, 4);
    assertEqual('the task result carries the operation id', taskFlow.completed[0].result.operationId, result.operationId);

    step('gate.side_channels', { taskPortTasks: gate.taskPort.count(), outboxEvents: gate.outbox.count(), gtdId: gate.ledger.get(result.operationId).gtdId });
    assertEqual('no second task is created by the gate', gate.taskPort.count(), 0);
    assertEqual('no separate delivery channel is created', gate.outbox.count(), 0);
    assertEqual('no GTD is created', gate.ledger.get(result.operationId).gtdId, null);
  });

  // 4. 429: ограниченные повторы, затем failed; выдача не обновляется.
  await scenario('rate_limited_bounded_retries', 'rate_limited', async (report) => {
    const { gate, emulator, bindingRef, taskFlow, pilot } = await buildStack({ fault: 'rate_limited' });
    const occurrence = occurrenceFor(DECLARATIONS.hourly);

    const result = await pilot.runOccurrence({ occurrence, integrationBindingId: bindingRef, query: 'node', area: ['1'] });
    step('pilot.run_occurrence', { outcome: result.outcome, code: result.code, providerAttempts: result.providerAttempts, pilotAttempts: result.attempts });
    assertEqual('the read fails after the bounded retries', result.outcome, 'failed');
    assertEqual('the provider code is the shared provider error', result.code, 'PROVIDER_ERROR');
    assertEqual('the adapter made one attempt and two bounded retries', result.providerAttempts, 3);
    assertEqual('the pilot made one attempt', result.attempts, 1);
    assertEqual('the provider saw three reads', emulator.reads(), 3);
    assertEqual('a rate limit never refreshes the credential', emulator.refreshes(), 0);

    const entries = gate.log.entries();
    const retries = entries.filter(entry => entry.event === 'provider.read_retry');
    step('gate.log.retries', { count: retries.length, reasonCodes: [...new Set(retries.map(entry => entry.reasonCode))], httpStatuses: [...new Set(retries.map(entry => entry.httpStatus))] });
    assertEqual('every retry is logged with its reason', retries.length, 2);
    assertEqual('the retry reason is the transient provider status', retries.every(entry => entry.reasonCode === 'PROVIDER_TRANSIENT' && entry.httpStatus === 429), true);

    step('task_flow.completed', { outcome: taskFlow.completed[0].outcome, code: taskFlow.completed[0].result.code });
    assertEqual('the failure is returned as the task result', taskFlow.completed[0].outcome, 'failed');
    assertEqual('the failure carries the provider code', taskFlow.completed[0].result.code, 'PROVIDER_ERROR');
  });

  // 5. 401: ровно одна попытка обновить выдачу, затем blocked с человеческим текстом.
  await scenario('auth_expiry_single_refresh', 'unauthorized', async (report) => {
    const { emulator, bindingRef, taskFlow, pilot } = await buildStack({ fault: 'unauthorized', withRefresh: true });
    const occurrence = occurrenceFor(DECLARATIONS.hourly);

    const result = await pilot.runOccurrence({ occurrence, integrationBindingId: bindingRef, query: 'node', area: ['1'] });
    step('pilot.run_occurrence', { outcome: result.outcome, code: result.code, providerAttempts: result.providerAttempts, detail: result.detail });
    assertEqual('the read is blocked', result.outcome, 'blocked');
    assertEqual('the auth expiry code is the shared provider code', result.code, 'PROVIDER_AUTH_EXPIRED');
    assertEqual('the credential was refreshed exactly once', emulator.refreshes(), 1);
    assertEqual('the read was attempted once before and once after the refresh', result.providerAttempts, 2);
    assertEqual('the provider saw two reads', emulator.reads(), 2);

    step('task_flow.completed', { outcome: taskFlow.completed[0].outcome, safeSummary: taskFlow.completed[0].result.safeSummary });
    assertEqual('the task result carries a human summary', typeof taskFlow.completed[0].result.safeSummary, 'string');
    assertIncludes('the human summary asks to update the access', taskFlow.completed[0].result.safeSummary, 'обновите');
  });

  // 6. 200 без читаемой полезной нагрузки — это не успех.
  await scenario('invalid_items_not_success', 'invalid_items', async (report) => {
    const { emulator, bindingRef, pilot } = await buildStack({ fault: 'invalid_items' });
    const occurrence = occurrenceFor(DECLARATIONS.hourly);

    const result = await pilot.runOccurrence({ occurrence, integrationBindingId: bindingRef, query: 'node', area: ['1'] });
    step('pilot.run_occurrence', { outcome: result.outcome, code: result.code, itemCount: result.result.itemCount });
    assertEqual('an unreadable payload is a failure', result.outcome, 'failed');
    assertEqual('the unreadable state code is the shared provider code', result.code, 'PROVIDER_STATE_UNREADABLE');
    assertEqual('no partial result is published', result.result.itemCount, null);
    assertEqual('the provider was read once', emulator.reads(), 1);
  });

  // 7. Недоступный провайдер — failed, а не неизвестность, и без шторма повторов.
  await scenario('unreachable_is_failed', 'unreachable', async (report) => {
    const { emulator, bindingRef, pilot } = await buildStack({ fault: 'unreachable', providerOptions: { listen: false } });
    const occurrence = occurrenceFor(DECLARATIONS.hourly);

    const result = await pilot.runOccurrence({ occurrence, integrationBindingId: bindingRef, query: 'node', area: ['1'] });
    step('pilot.run_occurrence', { outcome: result.outcome, code: result.code, providerAttempts: result.providerAttempts });
    assertEqual('an unreachable provider is a failure', result.outcome, 'failed');
    assertEqual('the unreachable code is the shared provider code', result.code, 'PROVIDER_UNREACHABLE');
    assertEqual('no retry storm is produced', result.providerAttempts, 1);
    assertEqual('the provider was never reached', emulator.reads(), 0);
  });

  // 8. Повторная доставка того же occurrence: один запрос, одна задача, один результат.
  await scenario('duplicate_occurrence_deduplicated', 'ok', async (report) => {
    const { emulator, bindingRef, taskFlow, pilot } = await buildStack({ fault: 'ok' });
    const occurrence = occurrenceFor(DECLARATIONS.hourly);

    const first = await pilot.runOccurrence({ occurrence, integrationBindingId: bindingRef, query: 'node', area: ['1'] });
    const second = await pilot.runOccurrence({ occurrence, integrationBindingId: bindingRef, query: 'node', area: ['1'] });
    step('pilot.run_occurrence.duplicate', { deduplicated: second.deduplicated, outcome: second.outcome, userTaskId: second.userTaskId, operationId: second.operationId });
    assertEqual('the second delivery is deduplicated', second.deduplicated, true);
    assertEqual('the deduplicated result keeps the same task', second.userTaskId, first.userTaskId);
    assertEqual('the deduplicated result keeps the same operation', second.operationId, first.operationId);
    assertEqual('the provider was read once', emulator.reads(), 1);
    assertEqual('exactly one task exists', taskFlow.submitted.length, 1);
    assertEqual('exactly one task result exists', taskFlow.completed.length, 1);
    assertEqual('the pilot ledger holds one entry', pilot.occurrences().length, 1);
  });

  // 9. Неподдержанный webhook и поток событий не обещаны.
  await scenario('webhook_not_promised', 'ok', async (report) => {
    const { gate, adapter, bindingRef, profileId } = await buildStack({ fault: 'ok' });

    const subscription = await gate.subscribe({
      integrationBindingId: bindingRef,
      eventType: 'resume.search.completed',
      webhookUrl: 'http://127.0.0.1:9/v1/callbacks',
      profileId,
    });
    step('gate.subscribe', { outcome: subscription.outcome, code: subscription.code, webhookSubscriptionId: subscription.webhookSubscriptionId, lifecycleState: subscription.lifecycleState, supportsWebhook: subscription.supportsWebhook });
    assertEqual('a webhook subscription is refused', subscription.code, 'WEBHOOK_NOT_SUPPORTED');
    assertEqual('no fake subscription id is issued', subscription.webhookSubscriptionId, null);
    assertEqual('no lifecycle state is faked', subscription.lifecycleState, null);
    assertEqual('the adapter declares no webhooks', adapter.events.webhooks.supported, false);

    const poll = await gate.poll({ integrationBindingId: bindingRef, cursor: 0, limit: 10, profileId });
    step('gate.poll', { outcome: poll.outcome, code: poll.code, events: poll.events.length });
    assertEqual('an event stream is refused', poll.code, 'PROVIDER_EVENTS_NOT_SUPPORTED');
    assertEqual('no empty event list is returned as "no events"', poll.events.length, 0);
    assertEqual('the adapter declares no event stream', adapter.events.eventStream.supported, false);

    const readiness = await gate.capabilities({ integrationBindingId: bindingRef, profileId });
    step('gate.capabilities', { transports: readiness.transports, providerEvents: { webhooks: readiness.providerEvents.webhooks.supported, eventStream: readiness.providerEvents.eventStream.supported }, capabilities: readiness.capabilities.map(cap => cap.name) });
    assertEqual('no event transport is advertised', readiness.transports.length, 0);
    assertEqual('the only supported operation is the read', readiness.capabilities.map(cap => cap.name).join(','), 'hh.search_resumes');
  });

  // 10. Отказ общего task flow ограничен попытками.
  await scenario('task_flow_failure_bounded', 'ok', async (report) => {
    const { bindingRef, taskFlow, pilot } = await buildStack({ fault: 'ok' });
    const occurrence = occurrenceFor(DECLARATIONS.hourly);
    taskFlow.refuseNextSubmit();
    taskFlow.refuseNextSubmit();

    const first = await pilot.runOccurrence({ occurrence, integrationBindingId: bindingRef, query: 'node', area: ['1'] });
    const second = await pilot.runOccurrence({ occurrence, integrationBindingId: bindingRef, query: 'node', area: ['1'] });
    const third = await pilot.runOccurrence({ occurrence, integrationBindingId: bindingRef, query: 'node', area: ['1'] });
    step('pilot.run_occurrence.attempts', [
      { code: first.code, attempts: first.attempts },
      { code: second.code, attempts: second.attempts },
      { code: third.code, attempts: third.attempts },
    ]);
    assertEqual('the first refusal is reported', first.code, 'TASK_SUBMIT_FAILED');
    assertEqual('the second refusal is reported', second.code, 'TASK_SUBMIT_FAILED');
    assertEqual('the attempt limit is enforced', third.code, 'ADMIT_ATTEMPTS_EXHAUSTED');
    assertEqual('no task was submitted', taskFlow.submitted.length, 0);
  });

  // 11. География — часть контракта провайдера: молча расширить поиск нельзя.
  await scenario('geography_required', 'ok', async (report) => {
    const { emulator, bindingRef, pilot } = await buildStack({ fault: 'ok' });

    const missing = await pilot.runOccurrence({ occurrence: occurrenceFor(DECLARATIONS.hourly), integrationBindingId: bindingRef, query: 'node' });
    step('pilot.run_occurrence.missing_area', { outcome: missing.outcome, code: missing.code });
    assertEqual('a missing geography is refused', missing.code, 'CAPABILITY_PAYLOAD_INVALID');
    assertIncludes('the refusal carries the domain message', missing.detail, 'география');
    assertEqual('the provider was not called', emulator.reads(), 0);

    const notRegionId = await pilot.runOccurrence({ occurrence: occurrenceFor(DECLARATIONS.hourly, Date.parse('2026-10-03T11:00:00.000Z')), integrationBindingId: bindingRef, query: 'node', area: ['Moscow'] });
    step('pilot.run_occurrence.non_region_id', { code: notRegionId.code });
    assertEqual('a non-region geography is refused', notRegionId.code, 'CAPABILITY_PAYLOAD_INVALID');
    assertIncludes('the refusal asks for a region id', notRegionId.detail, 'ID региона');

    const unrestricted = await pilot.runOccurrence({ occurrence: occurrenceFor(DECLARATIONS.hourly, Date.parse('2026-10-03T12:00:00.000Z')), integrationBindingId: bindingRef, query: 'node', area: null });
    step('pilot.run_occurrence.unrestricted', { outcome: unrestricted.outcome });
    assertEqual('an explicit null means an unrestricted search', unrestricted.outcome, 'result');
    assertEqual('the provider was read once', emulator.reads(), 1);
  });

  // 12. Живой smoke не выполняется: режим test account не зафиксирован (AC-30).
  await scenario('live_smoke_not_performed', 'ok', async (report) => {
    const { emulator, adapter } = await buildStack({ fault: 'ok' });

    const withoutBindings = adapter.requestLiveSmoke({ bindingNames: [] });
    step('adapter.request_live_smoke', { attempted: withoutBindings.attempted, performed: withoutBindings.performed, blockedBy: withoutBindings.blockedBy, missingBindings: withoutBindings.missingBindings });
    assertEqual('a live smoke is requested', withoutBindings.attempted, true);
    assertEqual('a live smoke is not performed', withoutBindings.performed, false);
    assertEqual('the missing test account blocks the live smoke', withoutBindings.blockedBy, 'NO_TEST_ACCOUNT_BINDING');

    const withBindings = adapter.requestLiveSmoke({ bindingNames: ['EXTERNAL_TEST_ACCOUNT_TOKEN', 'EXTERNAL_TEST_ACCOUNT_ID'] });
    step('adapter.request_live_smoke.bindings_present', { performed: withBindings.performed, blockedBy: withBindings.blockedBy });
    assertEqual('bindings alone are not enough without the owner decision', withBindings.performed, false);
    assertEqual('the owner decision is still required', withBindings.blockedBy, 'OWNER_DECISION_REQUIRED');
  });

  // 13. Что эмулировано и что требует живого провайдера.
  await scenario('fidelity', 'ok', async (report) => {
    const { emulator } = await buildStack({ fault: 'ok' });
    step('provider.fidelity', {
      mode: emulator.fidelity.mode,
      liveSandbox: emulator.fidelity.liveSandbox,
      readOnly: emulator.fidelity.readOnly,
      mutationsSupported: emulator.fidelity.mutationsSupported,
      containsPersonalData: emulator.fidelity.containsPersonalData,
      liveSmokePerformed: emulator.fidelity.liveSmoke.performed,
      emulatedProperties: emulator.fidelity.emulatedProperties,
    });
    assertEqual('the provider is emulated', emulator.fidelity.mode, 'emulator');
    assertEqual('no live sandbox is claimed', emulator.fidelity.liveSandbox, 'unsupported');
    assertEqual('the pilot is read-only', emulator.fidelity.readOnly, true);
    assertEqual('no mutation is supported', emulator.fidelity.mutationsSupported, false);
    assertEqual('the sample carries no personal data', emulator.fidelity.containsPersonalData, false);
    assertEqual('a green emulator run is not a live provider test', emulator.fidelity.liveSmoke.performed, false);
  });

  // 14. Границы: одна реализация адаптера и один источник cron-выражения.
  await scenario('single_adapter_and_single_cron_source', 'ok', async (report) => {
    const { gate, adapter, pilot } = await buildStack({ fault: 'ok' });
    step('gate.registry', { providers: gate.registry.list(), protocolVersion: adapter.protocolVersion });
    assertEqual('exactly one provider is registered', gate.registry.list().length, 1);

    let second;
    try {
      gate.registry.register({ provider: 'hh', protocolVersion: adapter.protocolVersion, capabilities: [] });
      second = 'registered';
    } catch (error) {
      second = error.code;
    }
    step('gate.registry.duplicate', { outcome: second });
    assertEqual('a second adapter implementation is refused', second, 'ADAPTER_ALREADY_REGISTERED');

    const declared = pilot.declareSchedule(DECLARATIONS.threeHourly);
    step('pilot.declare_schedule.three_hourly', { cron: declared.scheduleRequest.cron, intervalSource: declared.provenance.intervalSource });
    assertEqual('the cron expression is passed through unchanged', declared.scheduleRequest.cron, '39 0-23/3 * * *');
    assertEqual('the interval comes from the domain declaration', declared.provenance.intervalSource, 'domain_cron_declaration');

    const pilotFiles = [];
    (function collect(dir) {
      for (const entry of fs.readdirSync(dir, { withFileTypes: true })) {
        const full = path.join(dir, entry.name);
        if (entry.isDirectory()) collect(full);
        else if (entry.name.endsWith('.js')) pilotFiles.push(full);
      }
    })(path.join(ROOT, 'src', 'pilot'));
    const cronBuilders = pilotFiles.filter(file => {
      const source = fs.readFileSync(file, 'utf8');
      return source.includes('intervalToCron') || source.includes('* * * *');
    });
    step('pilot.cron_sources', { files: pilotFiles.length, cronBuilders: cronBuilders.length });
    assertEqual('the pilot holds no cron generator of its own', cronBuilders.length, 0);
  });

  await teardownAll();
  return results;
}

function renderTranscript(results) {
  const checks = results.flatMap(report => report.checks);
  const failed = checks.filter(entry => !entry.passed);
  return {
    schemaVersion: 1,
    card: 'P26',
    epic: 'E6 #22',
    stage: 'I08',
    contract: 'C10',
    occurredAt: new Date(START).toISOString(),
    clock: 'virtual',
    sandboxRoot: '.sandbox/p26-<pid>/',
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
      'AC-153': 'hourly cold search is mapped onto schedule occurrences but is not production enabled; the provider webhook and event stream are not promised; the integration data returns as the terminal result of the common task flow',
      'AC-154': 'provider operationId, http status, bounded retries, auth refresh, occurrence/schedule/task/run correlation and the reason of every transition are visible; raw provider payload and credential values stay out of the log',
      'AC-30': 'the HH/CRM test-account mode is not fixed: the live smoke is requested and not performed, blocked by the missing bindings and then by the missing owner decision',
    },
    fidelity: {
      provider: 'hh',
      mode: 'emulator',
      liveSandbox: 'unsupported',
      readOnly: true,
      mutationsSupported: false,
      liveSmokePerformed: false,
      note: 'a green emulator run is not a live provider test and not a production enablement',
    },
  };
}

function sanitizeTranscript(transcript) {
  const raw = JSON.stringify(transcript);
  const forbidden = [
    'synthetic-binding-value',
    'synthetic-refresh-token',
    'hh-access-',
    'Sanitized Candidate',
    'Sanitized Area',
    '127.0.0.1',
    process.pid,
    ROOT,
  ];
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
        const tempRoot = path.join(ROOT, '.sandbox', `p26-verify-${process.pid}`);
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
          const diffRoot = path.join(ROOT, '.sandbox', `p26-verify-diff-${process.pid}`);
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