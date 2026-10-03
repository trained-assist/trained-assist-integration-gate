'use strict';

// Приёмка P26 (карточка #65, эпик E6 #22, этап I08): реалистичный адаптер HH и
// существующий cron холодного поиска → срабатывания расписания.
//
// Покрытие: AC-153 (почасовая поддержка не выдаётся за production enabled;
// неподдержанный webhook не обещан; данные интеграции возвращаются через общий
// task flow), управляемые сбои провайдера (429, 401, невалидная полезная
// нагрузка, недоступность), логи этапа I08 (AC-154) и границы: одна реализация
// адаптера и один генератор cron-выражений.

const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('fs');
const path = require('path');

const { createIntegrationGate } = require('../src/gate/index.js');
const { createHhApiEmulator } = require('../src/provider-emulator/hh-api.js');
const { createHhAdapter } = require('../src/adapters/hh-adapter.js');
const { createColdSearchPilot } = require('../src/pilot/cold-search.js');
const { startHttpServer } = require('../src/http/server.js');

const ROOT = path.join(__dirname, '..');
const START = Date.parse('2026-10-03T09:00:00.000Z');
let tick = 0;
const now = () => new Date(START + tick);
const advance = ms => {
  tick += ms;
};

// Декларации существующего cron холодного поиска: те же значения, что выдаёт
// доменный `intervalToCron` (trained-assist-hh-skill, hh-cold-search-cron.js).
// Пилот обязан принимать их как данные, а не пересчитывать.
const CRON_DECLARATIONS = {
  hourly: { profileId: 'profile-sandbox-1', vacancyId: 'vacancy-8801', cron: '39 * * * *', timezone: 'Europe/Moscow', intervalHours: 1, goal: 'cold search for vacancy-8801' },
  halfHourly: { profileId: 'profile-sandbox-1', vacancyId: 'vacancy-8801', cron: '9,39 * * * *', timezone: 'Europe/Moscow', intervalHours: 0.5, goal: 'cold search for vacancy-8801' },
  threeHourly: { profileId: 'profile-sandbox-1', vacancyId: 'vacancy-8801', cron: '39 0-23/3 * * *', timezone: 'Europe/Moscow', intervalHours: 3, goal: 'cold search for vacancy-8801' },
  daily: { profileId: 'profile-sandbox-1', vacancyId: 'vacancy-9102', cron: '4 17 * * *', timezone: 'Europe/Moscow', intervalHours: 24, goal: 'cold search for vacancy-9102' },
};

function makeRoot(name) {
  const root = path.join(ROOT, '.sandbox', `p26-test-${process.pid}`, name);
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

/** Общий task flow песочницы: тот же контракт, что у задачи расписания P22. */
function createTaskFlowRecorder() {
  const submitted = [];
  const completed = [];
  let failSubmits = 0;
  return {
    submitted,
    completed,
    failNextSubmit() {
      failSubmits += 1;
    },
    async submit(input) {
      if (failSubmits > 0) {
        failSubmits -= 1;
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

async function buildStack({ fault = 'ok', bindingOptions = {}, providerOptions = {}, refreshToken = 'synthetic-refresh-token', withRefresh = false } = {}) {
  const root = makeRoot(`stack-${fault}-${teardowns.length}`);
  const gate = createIntegrationGate({ dataRoot: root, now });
  const facade = await startHttpServer(gate);
  teardowns.push(facade.stop);

  const emulator = createHhApiEmulator({
    root: path.join(root, 'provider'),
    now,
    fault,
    refreshToken,
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
            const result = await emulator.refreshAccess({ grant_type: 'refresh_token', refresh_token: refreshToken });
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

function occurrenceFor(declaration, { scheduledFor = Date.parse('2026-10-03T10:00:00.000Z') } = {}) {
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

test.afterEach(async () => {
  await teardownAll();
});

test('AC-153: почасовая поддержка не выдаётся за production enabled', async () => {
  const { pilot } = await buildStack();

  const hourly = pilot.declareSchedule(CRON_DECLARATIONS.hourly);
  assert.equal(hourly.outcome, 'result');
  assert.equal(hourly.scheduleRequest.cron, '39 * * * *', 'cron приходит из доменной декларации без пересчёта');
  assert.equal(hourly.scheduleRequest.timezone, 'Europe/Moscow');
  assert.equal(hourly.provenance.effectiveIntervalHours, 1);
  assert.equal(hourly.pilotStatus.hourlyMapped, true);
  assert.equal(hourly.pilotStatus.hourlyProductionEnabled, false);
  assert.equal(hourly.pilotStatus.prodTriggerConfigured, false);

  const production = pilot.declareSchedule({ ...CRON_DECLARATIONS.hourly, production: true });
  assert.equal(production.outcome, 'blocked');
  assert.equal(production.code, 'PILOT_PRODUCTION_NOT_ENABLED');
  assert.equal(production.scheduleRequest, null);
  assert.equal(production.pilotStatus.hourlyProductionEnabled, false);

  const status = pilot.status();
  assert.equal(status.mode, 'sandbox');
  assert.equal(status.liveTaskFlowWired, false);
  assert.equal(status.liveSmoke.performed, false);
});

test('AC-153: неподдержанный webhook провайдера не обещан', async () => {
  const { gate, adapter, bindingRef, profileId } = await buildStack();

  assert.equal(adapter.events.webhooks.supported, false);
  assert.equal(adapter.events.eventStream.supported, false);

  const subscription = await gate.subscribe({
    integrationBindingId: bindingRef,
    eventType: 'resume.search.completed',
    webhookUrl: 'http://127.0.0.1:9/v1/callbacks',
    profileId,
  });
  assert.equal(subscription.outcome, 'blocked');
  assert.equal(subscription.code, 'WEBHOOK_NOT_SUPPORTED');
  assert.equal(subscription.webhookSubscriptionId, null, 'фиктивная подписка не создаётся');
  assert.equal(subscription.lifecycleState, null);
  assert.equal(subscription.supportsWebhook, false);
  assert.deepEqual(subscription.transports, []);

  const poll = await gate.poll({ integrationBindingId: bindingRef, cursor: 0, limit: 10, profileId });
  assert.equal(poll.outcome, 'failed');
  assert.equal(poll.code, 'PROVIDER_EVENTS_NOT_SUPPORTED');
  assert.deepEqual(poll.events, [], 'пустой список событий не выдаётся за «событий нет»');

  const readiness = await gate.capabilities({ integrationBindingId: bindingRef, profileId });
  assert.equal(readiness.providerEvents.webhooks.supported, false);
  assert.equal(readiness.providerEvents.eventStream.supported, false);
  assert.deepEqual(readiness.transports, []);
  assert.deepEqual(readiness.capabilities.map(cap => cap.name), ['hh.search_resumes']);
});

test('AC-153: данные интеграции возвращаются через общий task flow', async () => {
  const { gate, emulator, bindingRef, profileId, taskFlow, pilot } = await buildStack();
  const declaration = CRON_DECLARATIONS.hourly;
  const occurrence = occurrenceFor(declaration);

  const result = await pilot.runOccurrence({
    occurrence,
    integrationBindingId: bindingRef,
    query: 'node developer',
    area: ['1'],
    replyContext: { channel: 'web', destinationRef: 'dest-sandbox-1' },
  });

  assert.equal(result.outcome, 'result');
  assert.equal(result.deduplicated, false);
  assert.equal(result.userTaskId, occurrence.userTaskId, 'задача подаётся с userTaskId расписания');
  assert.equal(typeof result.runId, 'string');
  assert.equal(result.result.itemCount, 4);
  assert.equal(result.result.itemIds.length, 4);
  assert.equal(result.result.privateDetailsRef, `hh-search/${result.operationId.slice(0, 16)}`, 'приватные данные остаются ссылкой на артефакт');

  assert.equal(taskFlow.submitted.length, 1);
  assert.equal(taskFlow.submitted[0].userTaskId, occurrence.userTaskId);
  assert.equal(taskFlow.submitted[0].scheduleId, occurrence.scheduleId);
  assert.equal(taskFlow.submitted[0].occurrenceKey, occurrence.occurrenceKey);
  assert.equal(taskFlow.submitted[0].source, 'schedule');
  assert.equal(taskFlow.submitted[0].autoRun, true);

  assert.equal(taskFlow.completed.length, 1);
  assert.equal(taskFlow.completed[0].userTaskId, occurrence.userTaskId);
  assert.equal(taskFlow.completed[0].runId, result.runId);
  assert.equal(taskFlow.completed[0].outcome, 'result');
  assert.equal(taskFlow.completed[0].result.itemCount, 4, 'результат поиска уходит в результат задачи');

  assert.equal(gate.taskPort.count(), 0, 'Gate не создаёт вторую задачу сам');
  assert.equal(gate.outbox.count(), 0, 'отдельный канал доставки не создаётся');
  assert.equal(gate.ledger.get(result.operationId).gtdId, null, 'GTD не создаётся');
  assert.equal(emulator.reads(), 1, 'ровно один запрос к провайдеру');
});

test('AC-153: повторная доставка того же occurrence не делает второй запрос и вторую задачу', async () => {
  const { emulator, bindingRef, pilot } = await buildStack();
  const occurrence = occurrenceFor(CRON_DECLARATIONS.hourly);

  const first = await pilot.runOccurrence({ occurrence, integrationBindingId: bindingRef, query: 'node', area: ['1'] });
  const second = await pilot.runOccurrence({ occurrence, integrationBindingId: bindingRef, query: 'node', area: ['1'] });

  assert.equal(first.outcome, 'result');
  assert.equal(second.deduplicated, true);
  assert.equal(second.outcome, 'result');
  assert.equal(second.userTaskId, first.userTaskId);
  assert.equal(second.operationId, first.operationId);
  assert.equal(emulator.reads(), 1, 'повторная доставка не читает провайдер второй раз');
  assert.equal(pilot.occurrences().length, 1, 'в журнале пилота одна запись');
});

test('управляемый сбой: 429 — ограниченные повторы, затем failed без эффекта', async () => {
  const { gate, emulator, bindingRef, profileId, taskFlow, pilot } = await buildStack({ fault: 'rate_limited' });
  const occurrence = occurrenceFor(CRON_DECLARATIONS.hourly);

  const result = await pilot.runOccurrence({ occurrence, integrationBindingId: bindingRef, query: 'node', area: ['1'] });

  assert.equal(result.outcome, 'failed');
  assert.equal(result.code, 'PROVIDER_ERROR');
  assert.equal(result.providerAttempts, 3, 'начальная попытка и два ограниченных повтора');
  assert.equal(result.attempts, 1, 'пилот сделал одну попытку occurrence');
  assert.equal(emulator.reads(), 3);
  assert.equal(emulator.refreshes(), 0, '429 не обновляет выдачу');

  assert.equal(taskFlow.completed.length, 1);
  assert.equal(taskFlow.completed[0].outcome, 'failed');
  assert.equal(taskFlow.completed[0].result.code, 'PROVIDER_ERROR');

  const entries = gate.log.entries();
  const retries = entries.filter(entry => entry.event === 'provider.read_retry');
  assert.equal(retries.length, 2, 'каждый повтор виден в логе с причиной');
  assert.ok(retries.every(entry => entry.reasonCode === 'PROVIDER_TRANSIENT' && entry.httpStatus === 429));
  assert.ok(entries.some(entry => entry.event === 'provider.read' && entry.outcome === 'failed' && entry.attempts === 3 && entry.httpStatus === 429));
  assert.ok(entries.some(entry => entry.event === 'pilot.occurrence_completed' && entry.userTaskId === occurrence.userTaskId));
});

test('управляемый сбой: 401 — ровно одна попытка обновить выдачу', async () => {
  const { emulator, bindingRef, pilot } = await buildStack({ fault: 'unauthorized', withRefresh: true });
  const occurrence = occurrenceFor(CRON_DECLARATIONS.hourly);

  const result = await pilot.runOccurrence({ occurrence, integrationBindingId: bindingRef, query: 'node', area: ['1'] });

  assert.equal(result.outcome, 'blocked');
  assert.equal(result.code, 'PROVIDER_AUTH_EXPIRED');
  assert.match(result.detail, /expired/);
  assert.equal(result.providerAttempts, 2, 'первая попытка и один повтор после обновления выдачи');
  assert.equal(result.attempts, 1);
  assert.equal(emulator.refreshes(), 1, 'выдача обновлялась ровно один раз');
  assert.equal(emulator.reads(), 2);
});

test('управляемый сбой: 401 без обновления выдачи — отказ без обращения к провайдеру повторно', async () => {
  const { emulator, bindingRef, pilot } = await buildStack({ fault: 'unauthorized' });
  const occurrence = occurrenceFor(CRON_DECLARATIONS.hourly);

  const result = await pilot.runOccurrence({ occurrence, integrationBindingId: bindingRef, query: 'node', area: ['1'] });

  assert.equal(result.outcome, 'blocked');
  assert.equal(result.code, 'PROVIDER_AUTH_EXPIRED');
  assert.equal(result.providerAttempts, 1);
  assert.equal(result.attempts, 1);
  assert.equal(emulator.reads(), 1);
  assert.equal(emulator.refreshes(), 0);
});

test('управляемый сбой: 200 без читаемой полезной нагрузки — это не успех', async () => {
  const { emulator, bindingRef, pilot } = await buildStack({ fault: 'invalid_items' });
  const occurrence = occurrenceFor(CRON_DECLARATIONS.hourly);

  const result = await pilot.runOccurrence({ occurrence, integrationBindingId: bindingRef, query: 'node', area: ['1'] });

  assert.equal(result.outcome, 'failed');
  assert.equal(result.code, 'PROVIDER_STATE_UNREADABLE');
  assert.equal(result.result.itemCount, null, 'частичный результат не публикуется');
  assert.equal(result.result.itemIds, undefined, 'идентификаторы не публикуются при отказе');
  assert.equal(emulator.reads(), 1);
});

test('управляемый сбой: недоступный провайдер — failed, а не неизвестность', async () => {
  const { emulator, bindingRef, pilot } = await buildStack({ fault: 'unreachable', providerOptions: { listen: false } });
  const occurrence = occurrenceFor(CRON_DECLARATIONS.hourly);

  const result = await pilot.runOccurrence({ occurrence, integrationBindingId: bindingRef, query: 'node', area: ['1'] });

  assert.equal(result.outcome, 'failed');
  assert.equal(result.code, 'PROVIDER_UNREACHABLE');
  assert.equal(result.attempts, 1, 'недоступность не порождает шторм повторов');
  assert.equal(emulator.reads(), 0);
});

test('управляемый сбой: отказ общего task flow ограничен попытками', async () => {
  const { bindingRef, taskFlow, pilot } = await buildStack();
  const occurrence = occurrenceFor(CRON_DECLARATIONS.hourly);
  taskFlow.failNextSubmit();
  taskFlow.failNextSubmit();

  const first = await pilot.runOccurrence({ occurrence, integrationBindingId: bindingRef, query: 'node', area: ['1'] });
  assert.equal(first.outcome, 'failed');
  assert.equal(first.code, 'TASK_SUBMIT_FAILED');
  assert.equal(first.attempts, 1);

  const second = await pilot.runOccurrence({ occurrence, integrationBindingId: bindingRef, query: 'node', area: ['1'] });
  assert.equal(second.outcome, 'failed');
  assert.equal(second.code, 'TASK_SUBMIT_FAILED');
  assert.equal(second.attempts, 2);

  const third = await pilot.runOccurrence({ occurrence, integrationBindingId: bindingRef, query: 'node', area: ['1'] });
  assert.equal(third.outcome, 'failed');
  assert.equal(third.code, 'ADMIT_ATTEMPTS_EXHAUSTED', 'после исчерпания попыток повтор отказывает');
  assert.equal(third.attempts, 3);
  assert.equal(taskFlow.submitted.length, 0);
  assert.equal(pilot.occurrences().filter(entry => entry.terminal).length, 0, 'task flow не принял задачу: occurrence не терминален');
});

test('география — часть контракта провайдера: молча расширить поиск нельзя', async () => {
  const { emulator, bindingRef, pilot } = await buildStack();
  // Каждый случай — отдельный occurrence: повторная доставка того же срабатывания
  // дедуплицируется и не дала бы второго ответа.
  const missing = await pilot.runOccurrence({ occurrence: occurrenceFor(CRON_DECLARATIONS.hourly), integrationBindingId: bindingRef, query: 'node' });
  assert.equal(missing.outcome, 'failed');
  assert.equal(missing.code, 'CAPABILITY_PAYLOAD_INVALID');
  assert.match(missing.detail, /география/i);
  assert.equal(emulator.reads(), 0, 'провайдер не вызывался');

  const notRegionId = await pilot.runOccurrence({ occurrence: occurrenceFor(CRON_DECLARATIONS.hourly, { scheduledFor: Date.parse('2026-10-03T11:00:00.000Z') }), integrationBindingId: bindingRef, query: 'node', area: ['Moscow'] });
  assert.equal(notRegionId.code, 'CAPABILITY_PAYLOAD_INVALID');
  assert.match(notRegionId.detail, /ID региона/);

  const unrestricted = await pilot.runOccurrence({ occurrence: occurrenceFor(CRON_DECLARATIONS.hourly, { scheduledFor: Date.parse('2026-10-03T12:00:00.000Z') }), integrationBindingId: bindingRef, query: 'node', area: null });
  assert.equal(unrestricted.outcome, 'result', 'явный null означает поиск без ограничения');
  assert.equal(emulator.reads(), 1);
});

test('запрос к провайдеру повторяет транспорт доменного кода', async () => {
  const { emulator, bindingRef, pilot } = await buildStack();
  const occurrence = occurrenceFor(CRON_DECLARATIONS.hourly);

  await pilot.runOccurrence({ occurrence, integrationBindingId: bindingRef, query: 'node developer', area: ['1', '113'] });

  const request = emulator.requestLog[0];
  assert.equal(request.method, 'GET');
  assert.equal(request.pathname, '/resumes');
  assert.equal(request.query.text, 'node developer');
  assert.equal(request.query.page, '0');
  assert.equal(request.query.per_page, '50');
  assert.equal(request.query.order_by, 'relevance');
  assert.equal(request.query.area, '1,113', 'области — ID регионов, дедуплицированные');
  assert.equal(request.hasAuthorization, true, 'выдача передаётся в заголовке, а не в логе');
  assert.ok(request.userAgent && request.hhUserAgent, 'HH требует идентификацию клиента');
  assert.ok(!JSON.stringify(request).includes('synthetic-binding-value'), 'значение выдачи не попадает в журнал запросов');
});

test('AC-154: логи этапа I08 содержат корреляцию и причины перехода без секретов', async () => {
  const { gate, emulator, bindingRef, profileId, pilot } = await buildStack({ fault: 'rate_limited' });
  const occurrence = occurrenceFor(CRON_DECLARATIONS.hourly);

  await pilot.runOccurrence({ occurrence, integrationBindingId: bindingRef, query: 'node', area: ['1'] });

  const entries = gate.log.entries();
  const serialized = JSON.stringify(entries);

  assert.ok(entries.some(entry => entry.event === 'operation.started' && entry.operationId === `op_${occurrence.userTaskId.slice(4, 20)}`), 'operationId пишется до вызова');
  assert.ok(entries.some(entry => entry.event === 'operation.outcome' && entry.outcome === 'failed'), 'исход записан');
  assert.ok(entries.some(entry => entry.event === 'pilot.occurrence_completed' && entry.scheduleId === occurrence.scheduleId && entry.occurrenceKey === occurrence.occurrenceKey), 'occurrence корреляция видна');
  assert.ok(entries.some(entry => entry.event === 'pilot.occurrence_completed' && entry.profileId === profileId && entry.userTaskId === occurrence.userTaskId && entry.runId), 'profile/userTask/run корреляция видна');
  assert.ok(entries.some(entry => entry.event === 'provider.read' && entry.httpStatus === 429 && entry.attempts === 3), 'статус провайдера и число попыток видны');

  assert.ok(!serialized.includes('synthetic-binding-value'), 'значение выдачи не попадает в лог');
  assert.ok(!serialized.includes('Sanitized Candidate'), 'сырые данные провайдера не попадают в лог');
  assert.ok(!serialized.includes('hh-access-'), 'токены не попадают в лог');
  assert.ok(serialized.includes(occurrence.userTaskId), 'идентификаторы задачи остаются в логе');
});

test('AC-154: приватный артефакт провайдера не публикуется', async () => {
  const { gate, bindingRef, pilot, root } = await buildStack();
  const occurrence = occurrenceFor(CRON_DECLARATIONS.hourly);

  const result = await pilot.runOccurrence({ occurrence, integrationBindingId: bindingRef, query: 'node', area: ['1'] });

  const published = gate.log.entries().filter(entry => entry.event === 'event.published');
  assert.equal(published.length, 0, 'read не публикует событие в outbox');
  assert.ok(result.result.privateDetailsRef.startsWith('hh-search/'), 'тяжёлые данные остаются ссылкой на артефакт');
  const serialized = JSON.stringify(gate.log.entries());
  assert.ok(!serialized.includes('Sanitized Candidate'), 'имена кандидатов не в логе');

  const artifact = path.join(root, 'provider', 'artifacts');
  const files = fs.existsSync(artifact) ? fs.readdirSync(artifact) : [];
  assert.equal(files.length, 1, 'сырые резюме пишутся приватным артефактом хоста');
  const stored = JSON.parse(fs.readFileSync(path.join(artifact, files[0]), 'utf8'));
  assert.equal(stored.ref, result.result.privateDetailsRef);
  assert.equal(stored.data.items.length, 4, 'в артефакте лежат сырые данные, а в логе — только ссылка');
});

test('AC-152: у провайдера одна реализация адаптера; cron не пересчитывается', async () => {
  const { gate, adapter, pilot } = await buildStack();

  assert.deepEqual(gate.registry.list(), ['hh']);
  assert.throws(
    () => gate.registry.register({ provider: 'hh', protocolVersion: adapter.protocolVersion, capabilities: [] }),
    error => error.code === 'ADAPTER_ALREADY_REGISTERED'
  );

  const declared = pilot.declareSchedule(CRON_DECLARATIONS.threeHourly);
  assert.equal(declared.scheduleRequest.cron, '39 0-23/3 * * *');
  assert.equal(declared.provenance.intervalSource, 'domain_cron_declaration');

  const pilotFiles = [];
  (function collect(dir) {
    for (const entry of fs.readdirSync(dir, { withFileTypes: true })) {
      const full = path.join(dir, entry.name);
      if (entry.isDirectory()) collect(full);
      else if (entry.name.endsWith('.js')) pilotFiles.push(full);
    }
  })(path.join(ROOT, 'src', 'pilot'));

  assert.ok(pilotFiles.length > 0);
  for (const file of pilotFiles) {
    const source = fs.readFileSync(file, 'utf8');
    assert.ok(!source.includes('intervalToCron'), `${path.relative(ROOT, file)} не заводит второй генератор cron`);
    assert.ok(!source.includes('* * * *'), `${path.relative(ROOT, file)} не вычисляет cron-выражение`);
  }
});

test('живой smoke не выполняется: режим test account не зафиксирован (AC-30)', async () => {
  const { emulator, adapter } = await buildStack();

  const withoutBindings = adapter.requestLiveSmoke({ bindingNames: [] });
  assert.equal(withoutBindings.attempted, true);
  assert.equal(withoutBindings.performed, false);
  assert.equal(withoutBindings.blockedBy, 'NO_TEST_ACCOUNT_BINDING');
  assert.deepEqual(withoutBindings.missingBindings, ['EXTERNAL_TEST_ACCOUNT_TOKEN', 'EXTERNAL_TEST_ACCOUNT_ID']);

  const withBindings = adapter.requestLiveSmoke({ bindingNames: ['EXTERNAL_TEST_ACCOUNT_TOKEN', 'EXTERNAL_TEST_ACCOUNT_ID'] });
  assert.equal(withBindings.performed, false);
  assert.equal(withBindings.blockedBy, 'OWNER_DECISION_REQUIRED');

  assert.equal(emulator.fidelity.mode, 'emulator');
  assert.equal(emulator.fidelity.liveSandbox, 'unsupported');
  assert.equal(emulator.fidelity.readOnly, true);
  assert.equal(emulator.fidelity.mutationsSupported, false);
  assert.equal(emulator.fidelity.liveSmoke.performed, false);
  assert.equal(emulator.fidelity.containsPersonalData, false);
});

test('фасад: статус пилота и отказ в проде видны по HTTP', async () => {
  const { gate, pilot } = await buildStack();
  const facadeWithPilot = await startHttpServer(gate, { pilot });
  teardowns.push(facadeWithPilot.stop);

  const status = await fetch(`${facadeWithPilot.baseUrl}/v1/pilot/cold-search/status`).then(response => response.json());
  assert.equal(status.outcome, 'result');
  assert.equal(status.hourlyMapped, true);
  assert.equal(status.hourlyProductionEnabled, false);
  assert.equal(status.prodTriggerConfigured, false);
  assert.equal(status.webhookSupported, false);
  assert.equal(status.gtdCreated, false);

  const production = await fetch(`${facadeWithPilot.baseUrl}/v1/pilot/cold-search/schedules`, {
    method: 'POST',
    headers: { 'content-type': 'application/json', 'x-gate-profile': 'profile-sandbox-1' },
    body: JSON.stringify({ ...CRON_DECLARATIONS.hourly, production: true }),
  }).then(response => response.json());
  assert.equal(production.outcome, 'blocked');
  assert.equal(production.code, 'PILOT_PRODUCTION_NOT_ENABLED');

  const mapped = await fetch(`${facadeWithPilot.baseUrl}/v1/pilot/cold-search/schedules`, {
    method: 'POST',
    headers: { 'content-type': 'application/json', 'x-gate-profile': 'profile-sandbox-1' },
    body: JSON.stringify(CRON_DECLARATIONS.daily),
  }).then(response => response.json());
  assert.equal(mapped.outcome, 'result');
  assert.equal(mapped.scheduleRequest.cron, '4 17 * * *');
  assert.equal(mapped.scheduleRequest.timezone, 'Europe/Moscow');
});

test('фасад: маршруты пилота недоступны, когда пилот не подключён', async () => {
  const root = makeRoot('no-pilot');
  const gate = createIntegrationGate({ dataRoot: root, now });
  const facade = await startHttpServer(gate);
  teardowns.push(facade.stop);

  const response = await fetch(`${facade.baseUrl}/v1/pilot/cold-search/status`);
  assert.equal(response.status, 404);
});

test('occurrence без идентификаторов расписания отказывается до провайдера', async () => {
  const { emulator, bindingRef, pilot } = await buildStack();

  const broken = await pilot.runOccurrence({ occurrence: { occurrenceId: 'occ-1' }, integrationBindingId: bindingRef, query: 'node', area: ['1'] });
  assert.equal(broken.outcome, 'blocked');
  assert.equal(broken.code, 'SCHEDULE_OCCURRENCE_INVALID');
  assert.equal(broken.userTaskId, null);
  assert.equal(emulator.reads(), 0, 'провайдер не вызывался');
});

test('декларация с некорректным cron или часовым поясом отказывается до расписания', async () => {  const { pilot } = await buildStack();

  const badCron = pilot.declareSchedule({ ...CRON_DECLARATIONS.hourly, cron: 'every hour' });
  assert.equal(badCron.outcome, 'blocked');
  assert.equal(badCron.code, 'CAPABILITY_PAYLOAD_INVALID');
  assert.match(badCron.detail, /cron/);

  const badTimezone = pilot.declareSchedule({ ...CRON_DECLARATIONS.hourly, timezone: 'Moscow/Now' });
  assert.equal(badTimezone.code, 'CAPABILITY_PAYLOAD_INVALID');
  assert.match(badTimezone.detail, /timezone/);

  const missingGoal = pilot.declareSchedule({ ...CRON_DECLARATIONS.hourly, goal: '' });
  assert.equal(missingGoal.code, 'CAPABILITY_PAYLOAD_INVALID');
  assert.match(missingGoal.detail, /goal/);
});