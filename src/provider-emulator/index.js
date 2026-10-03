'use strict';

// Эмулятор внешнего провайдера для External Integration Gate (P25, этап I08).
//
// Это ЭМУЛЯТОР: у внешнего сервиса нет provider sandbox, поэтому этап
// проверяется против эмулятора с очищенным контрактным сэмплом (SANDBOX.md:
// «passing emulator не называется успешным live provider test»). Зелёный прогон
// эмулятора не является живым тестом провайдера — это записано в `fidelity`.
//
// Что проверяется по-настоящему:
//   - внешний эффект на диске изолированного root, а не заглушка;
//   - happens-once по operationId: повтор возвращает ту же квитанцию;
//   - управляемые сбои: success / error / delay / auth_expiry /
//     duplicate_callback / unreachable;
//   - подписанные обратные вызовы (HMAC) с дубликатами: эмулятор отправляет их
//     настоящим HTTP POST в webhook-приёмник Gate;
//   - поздняя квитанция после delay и reconcile по operationId.
//
// Чего эмулятор не доказывает: свойства живого провайдера (см. `fidelity`).

const crypto = require('crypto');
const fs = require('fs');
const http = require('http');
const path = require('path');

const PROVIDER_NAME = 'hh-sandbox';
const STORE_DIR = 'operations';
const DEFAULT_DELAY_MS = 900;

const FAULT_MODES = ['success', 'error', 'delay', 'auth_expiry', 'duplicate_callback', 'unreachable'];

// Очищенный контрактный сэмпл: синтетика без персональных данных.
const SANITIZED_SAMPLE = {
  searchId: 'search-demo-1',
  vacancy: { title: 'Software Engineer (sandbox sample)', openedAt: '2026-09-01T09:00:00.000Z', closedAt: null, schedule: 'remote' },
  funnel: { total: 4, byStage: { applied: 1, screening: 2, interview: 1, offer: 0 } },
  applications: [
    { applicationId: 'app-demo-1', stage: 'screening' },
    { applicationId: 'app-demo-2', stage: 'applied' },
  ],
  updatedAt: '2026-10-03T09:00:00.000Z',
  source: 'sanitized-sample',
  containsPersonalData: false,
};

const REQUIRED_LIVE_BINDINGS = ['EXTERNAL_TEST_ACCOUNT_TOKEN', 'EXTERNAL_TEST_ACCOUNT_ID'];

function digest(...parts) {
  return crypto.createHash('sha256').update(parts.join('|')).digest('hex');
}

function short(hash, length = 12) {
  return hash.slice(0, length);
}

function eventIdFor(scope, key) {
  return `evt_${short(digest('event', scope, key))}`;
}

function receiptIdFor(operationId, payloadHash) {
  return `rcpt_${short(digest('receipt', operationId, payloadHash), 16)}`;
}

function externalRefFor(operationId) {
  return `op_${short(digest('external', operationId))}`;
}

function callbackIdFor(operationId, index) {
  return `cb_${short(digest('callback', operationId, String(index)))}`;
}

function subscriptionIdFor(bindingRef, eventType) {
  return `whsub_${short(digest('subscription', bindingRef, eventType))}`;
}

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

function signCallback({ secret, timestamp, rawBody }) {
  return `v1=${crypto.createHmac('sha256', secret).update(`${timestamp}.${rawBody}`).digest('hex')}`;
}

/**
 * Чтение записи внешнего сервиса. «Записи нет» и «запись нечитаема» — разные
 * состояния: второе нельзя трактовать как «эффекта не было», иначе повтор
 * создаст второй эффект (ловушка PR-04).
 */
function readRecord(file) {
  if (!fs.existsSync(file)) return { state: 'missing' };
  try {
    return { state: 'ok', record: JSON.parse(fs.readFileSync(file, 'utf8')) };
  } catch {
    return { state: 'corrupt' };
  }
}

function writeRecord(dir, record) {
  fs.mkdirSync(dir, { recursive: true, mode: 0o700 });
  const target = path.join(dir, `${short(digest('store', record.operationId), 32)}.json`);
  const temp = `${target}.tmp-${process.pid}`;
  fs.writeFileSync(temp, `${JSON.stringify(record)}\n`, { mode: 0o600 });
  fs.renameSync(temp, target);
  return record;
}

function sendJson(port, pathname, body, headers = {}) {
  const rawBody = JSON.stringify(body);
  return new Promise((resolve, reject) => {
    const request = http.request(
      { host: '127.0.0.1', port, path: pathname, method: 'POST', headers: { 'content-type': 'application/json', ...headers } },
      response => {
        const chunks = [];
        response.on('data', chunk => chunks.push(chunk));
        response.on('end', () => {
          const text = Buffer.concat(chunks).toString('utf8');
          try {
            resolve({ status: response.statusCode, body: JSON.parse(text) });
          } catch {
            resolve({ status: response.statusCode, body: { raw: text.slice(0, 200) } });
          }
        });
      }
    );
    request.on('error', reject);
    request.end(rawBody);
  });
}

/**
 * @param {object} options
 * @param {string} options.root изолированный корень эмулятора
 * @param {() => Date} [options.now]
 * @param {string} [options.fault] один из FAULT_MODES
 * @param {number} [options.delayMs]
 * @param {string} [options.webhookUrl] база webhook-приёмника Gate
 * @param {string} [options.webhookSecret] секрет подписи обратных вызовов (host-owned)
 * @param {boolean} [options.listen=false] не поднимать сервер: «сервис недоступен»
 */
function createProviderEmulator({ root, now = () => new Date(), fault = 'success', delayMs = DEFAULT_DELAY_MS, webhookUrl = '', webhookSecret = '', listen = true } = {}) {
  if (!root) throw new Error('provider emulator requires an isolated root (never a production data root)');
  if (!FAULT_MODES.includes(fault)) throw new Error(`unknown fault "${fault}"; expected one of ${FAULT_MODES.join('|')}`);

  const dir = path.join(root, STORE_DIR);
  const postedCallbacks = [];
  const deliveryLog = [];
  const subscriptions = new Map();
  let webhookSecretValue = webhookSecret;
  let subscriptionRef = '';
  let cursorSequence = 0;
  const cursorLog = [];

  // Обратные вызовы уходят на webhook-приёмник Gate, а не на собственный порт
  // эмулятора: иначе проверялась бы доставка сама в себя.
  const webhookPort = (() => {
    try {
      return Number(new URL(webhookUrl || 'http://127.0.0.1:0').port) || 0;
    } catch {
      return 0;
    }
  })();

  /** Секрет подписи выдаёт хост при подписке; эмулятор подписывает им вызовы. */
  function setWebhookSecret(value) {
    webhookSecretValue = value;
  }

  /** Провайдер возвращает идентификатор подписки в теле обратного вызова. */
  function setSubscriptionId(value) {
    subscriptionRef = String(value);
  }

  function storeFor(operationId) {
    return path.join(dir, `${short(digest('store', operationId), 32)}.json`);
  }

  function lookup(operationId) {
    const { state, record } = readRecord(storeFor(operationId));
    if (state !== 'ok') {
      return { found: false, applied: false, receiptId: null, externalRef: null, receiptPending: false, state };
    }
    return {
      found: true,
      applied: true,
      externalRef: record.externalRef,
      receiptId: record.receipt ? record.receipt.receiptId : null,
      receiptPending: Boolean(record.receiptPending),
      eventId: record.eventId,
      at: record.at,
    };
  }

  /**
   * Подписанный обратный вызов. Дубликаты — и есть проверка: тот же callbackId
   * повторно и один раз с новым callbackId, но тем же operationId.
   */
  async function deliverCallback(record, { duplicates = false } = {}) {
    const base = {
      callbackId: callbackIdFor(record.operationId, 0),
      provider: PROVIDER_NAME,
      kind: 'application.decision.recorded',
      eventId: record.eventId,
      providerEventId: record.eventId,
      subscriptionId: subscriptionRef || null,
      operationId: record.operationId,
      externalRef: record.externalRef,
      profileId: record.profileId,
      applicationId: record.applicationId,
      decision: record.decision,
      occurredAt: record.at,
      replyContext: record.replyContext || null,
    };
    const batch = duplicates
      ? [base, { ...base }, { ...base, callbackId: callbackIdFor(record.operationId, 1) }]
      : [base];

    const deliveries = [];
    for (const [index, envelope] of batch.entries()) {
      if (index > 0) await sleep(20);
      const entry = { ...envelope, applied: false, duplicate: false, httpStatus: 0, reason: 'NO_WEBHOOK_URL' };
      if (webhookUrl) {
        const rawBody = JSON.stringify(envelope);
        // Часы песочницы: иначе реальное время хоста выпадало бы из окна
        // допустимого смещения приёмника.
        const timestamp = String(now().getTime());
        try {
          const result = await sendJson(webhookPort, '/v1/callbacks', envelope, {
            'x-gate-signature': signCallback({ secret: webhookSecretValue, timestamp, rawBody }),
            'x-gate-timestamp': timestamp,
            'x-gate-delivery': envelope.providerEventId,
          });
          entry.httpStatus = result.status;
          entry.applied = Boolean(result.body && result.body.applied);
          entry.duplicate = Boolean(result.body && result.body.duplicate);
          entry.reason = result.body && result.body.reason ? result.body.reason : null;
        } catch (e) {
          entry.reason = 'CALLBACK_TRANSPORT_FAILED';
        }
      }
      postedCallbacks.push(entry);
      deliveryLog.push(entry);
      deliveries.push(entry);
    }
    return {
      posted: deliveries.length,
      applied: deliveries.filter(d => d.applied).length,
      duplicatesIgnored: deliveries.filter(d => d.duplicate).length,
      unacknowledged: deliveries.filter(d => !d.applied && !d.duplicate).length,
      callbackIds: deliveries.map(d => d.callbackId),
    };
  }

  const routes = {
    'POST /v1/searches/{searchId}/status': ({ searchId, bindingValue } = {}) => {
      if (!bindingValue) return { status: 403, body: { status: 'blocked', reason: 'credential binding value is empty; the provider cannot authenticate the read' } };
      if (fault === 'unreachable') return { status: 503, body: { status: 'unreachable' } };
      if (fault === 'auth_expiry') return { status: 403, body: { status: 'blocked', reason: 'provider rejected the credential binding: expired' } };
      if (fault === 'error') return { status: 500, body: { status: 'error', code: 'PROVIDER_ERROR', detail: 'external service returned a server error on read' } };
      if (fault === 'delay') return { status: 200, body: { status: 'late', eventId: eventIdFor('read', searchId), data: { ...SANITIZED_SAMPLE, searchId: String(searchId) } } };
      if (String(searchId) !== SANITIZED_SAMPLE.searchId) return { status: 404, body: { status: 'error', code: 'PROVIDER_NOT_FOUND', detail: `no sanitized sample for search "${searchId}"` } };
      return { status: 200, body: { status: 'ok', eventId: eventIdFor('read', String(searchId)), data: { ...SANITIZED_SAMPLE } } };
    },

    'POST /v1/applications/{applicationId}/decide': async ({ operationId, profileId, applicationId, decision, bindingRef, bindingScope, bindingValue, replyContext } = {}) => {
      if (!bindingValue) return { status: 403, body: { status: 'blocked', reason: 'credential binding value is empty; the provider cannot authenticate the write' } };
      if (fault === 'unreachable') return { status: 503, body: { status: 'unreachable' } };
      if (fault === 'auth_expiry') return { status: 403, body: { status: 'blocked', reason: 'provider rejected the credential binding: expired' } };

      const payload = { applicationId: String(applicationId), decision: String(decision) };
      const payloadHash = short(digest('payload', JSON.stringify(payload)), 32);
      const existing = readRecord(storeFor(operationId));
      if (existing.state === 'corrupt') {
        return { status: 500, body: { status: 'corrupt', code: 'PROVIDER_STATE_UNREADABLE', detail: 'the external service has an unreadable record for this operationId; no second effect was produced', lookup: lookup(operationId) } };
      }
      if (existing.state === 'ok') {
        const record = existing.record;
        if (record.payloadHash !== payloadHash) {
          return { status: 409, body: { status: 'conflict', existingPayloadHash: record.payloadHash, requestedPayloadHash: payloadHash, lookup: lookup(operationId) } };
        }
        if (record.receiptPending) return { status: 202, body: { status: 'pending', lookup: lookup(operationId) } };
        const delivery = await deliverCallback(record, { duplicates: fault === 'duplicate_callback' });
        return { status: 200, body: { status: 'replayed', receipt: record.receipt, lookup: lookup(operationId), eventId: record.eventId, delivery } };
      }

      if (fault === 'error') {
        return { status: 500, body: { status: 'error', code: 'PROVIDER_ERROR', detail: 'external service rejected the write with a server error', lookup: lookup(operationId) } };
      }

      const at = now().toISOString();
      const record = {
        operationId,
        payloadHash,
        profileId,
        bindingRef,
        bindingScope,
        applicationId: payload.applicationId,
        decision: payload.decision,
        replyContext: replyContext || null,
        eventId: eventIdFor('write', operationId),
        externalRef: externalRefFor(operationId),
        at,
        receiptPending: true,
        receipt: null,
        delivery: null,
      };
      writeRecord(dir, record);

      const issueReceipt = () => {
        const receipt = { receiptId: receiptIdFor(operationId, payloadHash), externalRef: record.externalRef, at };
        writeRecord(dir, { ...record, receiptPending: false, receipt });
        return receipt;
      };

      if (fault === 'delay') {
        // Ответ и квитанция приходят позже дедлайна клиента: вызывающий видит
        // неизвестный исход, эффект уже применён. Квитанция «догоняет» позже.
        await sleep(delayMs);
        const receipt = issueReceipt();
        const delivery = await deliverCallback(record, { duplicates: fault === 'duplicate_callback' });
        return { status: 200, body: { status: 'applied_late', receipt, lookup: lookup(operationId), eventId: record.eventId, delivery } };
      }

      const receipt = issueReceipt();
      const delivery = await deliverCallback(record, { duplicates: fault === 'duplicate_callback' });
      return { status: 200, body: { status: 'applied', receipt, lookup: lookup(operationId), eventId: record.eventId, delivery } };
    },

    'GET /v1/operations/{operationId}': ({ operationId } = {}) => {
      const result = lookup(operationId);
      if (!result.found) return { status: 404, body: { status: 'error', code: 'PROVIDER_NOT_FOUND', detail: 'no record for this operationId' } };
      return {
        status: 200,
        body: {
          status: result.receiptPending ? 'pending' : 'confirmed',
          outcome: result.receiptPending ? 'outcome_unknown' : 'result',
          receipt: result.receipt,
          externalRef: result.externalRef,
          eventId: result.eventId,
        },
      };
    },

    'POST /v1/subscriptions': ({ bindingRef, eventType, webhookUrl: target } = {}) => {
      const id = subscriptionIdFor(bindingRef, eventType);
      const record = { webhookSubscriptionId: id, provider: PROVIDER_NAME, eventType, bindingRef, webhookUrl: target, lifecycleState: 'active', createdAt: now().toISOString() };
      subscriptions.set(id, record);
      return { status: 201, body: record };
    },

    'DELETE /v1/subscriptions/{id}': ({ id } = {}) => {
      const record = subscriptions.get(id);
      if (!record) return { status: 404, body: { status: 'error', code: 'PROVIDER_NOT_FOUND', detail: 'no such subscription' } };
      record.lifecycleState = 'inactive';
      return { status: 200, body: record };
    },

    'GET /v1/events': ({ cursor, limit } = {}) => {
      const from = Number(cursor || 0);
      const bounded = Math.min(Number(limit || 20), 50);
      const events = cursorLog.slice(from, from + bounded).map(entry => entry.event);
      return { status: 200, body: { events, nextCursor: from + events.length, hasMore: from + events.length < cursorLog.length } };
    },
  };

  function recordEvent(event) {
    cursorSequence += 1;
    const entry = { sequence: cursorSequence, event, at: now().toISOString() };
    cursorLog.push(entry);
    return entry;
  }

  // Маршруты эмулятора задаются шаблонами: путь с идентификатором приводится к
  // виду `/v1/searches/{searchId}/status`, иначе реальный идентификатор не
  // совпал бы с ключом маршрута.
  const routeTable = [
    { method: 'POST', pattern: /^\/v1\/searches\/([^/]+)\/status$/, params: ['searchId'], handler: routes['POST /v1/searches/{searchId}/status'] },
    { method: 'POST', pattern: /^\/v1\/applications\/([^/]+)\/decide$/, params: ['applicationId'], handler: routes['POST /v1/applications/{applicationId}/decide'] },
    { method: 'GET', pattern: /^\/v1\/operations\/([^/]+)$/, params: ['operationId'], handler: routes['GET /v1/operations/{operationId}'] },
    { method: 'POST', pattern: /^\/v1\/subscriptions$/, params: [], handler: routes['POST /v1/subscriptions'] },
    { method: 'DELETE', pattern: /^\/v1\/subscriptions\/([^/]+)$/, params: ['id'], handler: routes['DELETE /v1/subscriptions/{id}'] },
    { method: 'GET', pattern: /^\/v1\/events$/, params: [], handler: routes['GET /v1/events'] },
  ];

  const server = http.createServer((request, response) => {
    const chunks = [];
    request.on('data', chunk => chunks.push(chunk));
    request.on('end', async () => {
      const url = new URL(request.url, 'http://127.0.0.1');
      const route = routeTable.find(entry => entry.method === request.method && entry.pattern.test(url.pathname));
      if (!route) {
        response.writeHead(404, { 'content-type': 'application/json' });
        response.end(JSON.stringify({ status: 'error', code: 'PROVIDER_NOT_FOUND', detail: `no route ${request.method} ${url.pathname}` }));
        return;
      }
      const params = { ...Object.fromEntries(url.searchParams.entries()) };
      const match = url.pathname.match(route.pattern);
      route.params.forEach((name, index) => {
        params[name] = match[index + 1];
      });
      // Тело запроса — часть контракта: без него провайдер не видел бы binding.
      const rawRequestBody = Buffer.concat(chunks).toString('utf8');
      if (rawRequestBody.length > 0) {
        try {
          Object.assign(params, JSON.parse(rawRequestBody));
        } catch {
          response.writeHead(400, { 'content-type': 'application/json' });
          response.end(JSON.stringify({ status: 'error', code: 'PROVIDER_ERROR', detail: 'request body is not valid JSON' }));
          return;
        }
      }
      try {
        const result = await route.handler(params);
        response.writeHead(result.status, { 'content-type': 'application/json' });
        response.end(JSON.stringify(result.body));
      } catch (e) {
        response.writeHead(500, { 'content-type': 'application/json' });
        response.end(JSON.stringify({ status: 'error', code: 'PROVIDER_ERROR', detail: String(e && e.message ? e.message : e) }));
      }
    });
  });

  let port = 0;

  async function start() {
    if (!listen) return { port: 0, listening: false };
    await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
    port = server.address().port;
    return { port, listening: true };
  }

  function stop() {
    return new Promise(resolve => server.close(() => resolve()));
  }

  const fidelity = {
    provider: PROVIDER_NAME,
    mode: 'emulator',
    liveSandbox: 'unsupported',
    reason: 'the external service ships no provider sandbox or webhook environment, so the stage is verified against an emulator with a sanitized contract sample',
    sanitizedSample: true,
    containsPersonalData: false,
    emulatedProperties: ['auth per operation', 'quota / rate limit', 'real webhook delivery', 'real provider data'],
    liveSmoke: {
      performed: false,
      required: [
        'read-only status of a test search owned by the sandbox test account',
        'real token lifetime/refresh behaviour (auth expiry fixture is synthetic here)',
        'real free-tier quota and rate-limit responses',
        'real signed webhook delivery in place of the emulated callback',
      ],
      bindingNames: [...REQUIRED_LIVE_BINDINGS],
      bindingSource: 'GCP Secret Manager or GitHub Actions secrets; never a production token',
      rule: 'a green emulator run is not a live provider test — performed stays false until a test-account read exists',
    },
  };

  function requestLiveSmoke({ bindingNames = [] } = {}) {
    const missing = REQUIRED_LIVE_BINDINGS.filter(name => !bindingNames.includes(name));
    if (missing.length > 0) {
      return { attempted: true, performed: false, blockedBy: 'NO_TEST_ACCOUNT_BINDING', missingBindings: missing, next: 'declare these names in Secret Manager / GitHub Actions secrets, then run the read-only live smoke with a sandbox-owned account' };
    }
    return { attempted: true, performed: false, blockedBy: 'LIVE_PROVIDER_CLIENT_NOT_BUILT', missingBindings: [], next: 'the emulator is the accepted evidence for this stage; a live client needs the owner decision on the first real domain (epic #22)' };
  }

  return {
    root,
    fault,
    dir,
    fidelity,
    postedCallbacks,
    deliveryLog,
    subscriptions,
    cursorLog,
    recordEvent,
    requestLiveSmoke,
    setWebhookSecret,
    setSubscriptionId,
    start,
    stop,
    get port() {
      return port;
    },
    isListening: () => listen,
    readSearchStatus: routes['POST /v1/searches/{searchId}/status'],
    decideApplication: routes['POST /v1/applications/{applicationId}/decide'],
    lookup,
    count: () => (fs.existsSync(dir) ? fs.readdirSync(dir).filter(file => file.endsWith('.json')).length : 0),
  };
}

module.exports = {
  createProviderEmulator,
  FAULT_MODES,
  STORE_DIR,
  PROVIDER_NAME,
  REQUIRED_LIVE_BINDINGS,
  SANITIZED_SAMPLE,
  DEFAULT_DELAY_MS,
};
