'use strict';

// Единственная реализация адаптера провайдера в этом репозитории (AC-152).
//
// Адаптер только переводит вызов в транспорт провайдера и нормализует ответ в
// исходы контракта C10. Бизнес-правила здесь не живут: ни промптов, ни порогов
// решений, ни стадий воронки — это остаётся в доменном репозитории.
//
// Транспорт один — HTTP к провайдеру. Второй транспортной реализации нет и не
// заводится: фасад Gate и адаптер не дублируют друг друга.

const { ADAPTER_PROTOCOL_VERSION } = require('../contract/version');

const DEFAULT_TIMEOUT_MS = 120;

const CAPABILITIES = [
  { name: 'hh.search_status', kind: 'read', scope: 'hh#read' },
  { name: 'hh.application_decide', kind: 'mutation', scope: 'hh#write' },
];

const REQUIRED_LIVE_BINDINGS = ['EXTERNAL_TEST_ACCOUNT_TOKEN', 'EXTERNAL_TEST_ACCOUNT_ID'];

function postJson(baseUrl, pathname, body, { timeoutMs = DEFAULT_TIMEOUT_MS, fetchImpl } = {}) {
  const rawBody = JSON.stringify(body);
  const doFetch = fetchImpl || fetch;
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  return doFetch(`${baseUrl}${pathname}`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: rawBody,
    signal: controller.signal,
  })
    .then(async response => {
      const text = await response.text();
      let parsed;
      try {
        parsed = JSON.parse(text);
      } catch {
        parsed = { status: 'error', code: 'PROVIDER_ERROR', detail: 'provider returned a non-JSON body' };
      }
      return { status: response.status, body: parsed };
    })
    .catch(error => {
      const aborted = Boolean(error && (error.name === 'AbortError' || controller.signal.aborted));
      return {
        status: 0,
        body: {
          status: aborted ? 'timeout' : 'unreachable',
          code: aborted ? 'EFFECT_STATE_UNKNOWN' : 'PROVIDER_UNREACHABLE',
          detail: aborted
            ? 'the provider did not answer before the caller deadline; the mutation may or may not have been applied'
            : 'the provider endpoint is unreachable',
        },
      };
    })
    .finally(() => clearTimeout(timer));
}

function getJson(baseUrl, pathname, { timeoutMs = DEFAULT_TIMEOUT_MS, fetchImpl } = {}) {
  const doFetch = fetchImpl || fetch;
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  return doFetch(`${baseUrl}${pathname}`, { method: 'GET', signal: controller.signal })
    .then(async response => {
      const text = await response.text();
      let parsed;
      try {
        parsed = JSON.parse(text);
      } catch {
        parsed = { status: 'error', code: 'PROVIDER_ERROR', detail: 'provider returned a non-JSON body' };
      }
      return { status: response.status, body: parsed };
    })
    .catch(() => ({ status: 0, body: { status: 'unreachable', code: 'PROVIDER_UNREACHABLE', detail: 'the provider endpoint is unreachable' } }))
    .finally(() => clearTimeout(timer));
}

/**
 * Нормализация ответа провайдера в исходы контракта C10.
 *
 * `outcome_unknown` — только таймаут после отправки: эффект мог произойти.
 * Недоступность и ошибка сервиса — это `failed`, а не неизвестность.
 */
function normalize({ native }) {
  const body = native && native.body ? native.body : {};
  const status = body.status || 'error';
  const code = body.code || 'PROVIDER_ERROR';
  const detail = body.detail || body.reason || 'provider rejected the operation';
  const receipt = body.receipt || null;
  const externalOperationRef = (receipt && receipt.externalRef) || body.externalRef || null;
  const eventId = body.eventId || null;

  if (status === 'timeout') {
    return { outcome: 'outcome_unknown', code: 'EFFECT_STATE_UNKNOWN', detail, receipt: null, externalOperationRef: null, eventId: null };
  }
  if (status === 'unreachable') {
    return { outcome: 'failed', code: 'PROVIDER_UNREACHABLE', detail, receipt: null, externalOperationRef: null, eventId: null };
  }
  if (status === 'error') {
    return { outcome: 'failed', code, detail, receipt: null, externalOperationRef: null, eventId: null };
  }
  if (status === 'corrupt') {
    return { outcome: 'failed', code: 'PROVIDER_STATE_UNREADABLE', detail, receipt: null, externalOperationRef: null, eventId: null };
  }
  if (status === 'conflict') {
    return { outcome: 'failed', code: 'PROVIDER_PAYLOAD_CONFLICT', detail, receipt: null, externalOperationRef: null, eventId: null };
  }
  if (status === 'blocked') {
    return { outcome: 'blocked', code: 'PROVIDER_AUTH_EXPIRED', detail, receipt: null, externalOperationRef: null, eventId: null };
  }
  if (status === 'pending') {
    return { outcome: 'outcome_unknown', code: 'EFFECT_STATE_UNKNOWN', detail: 'the provider accepted the mutation but issued no receipt yet', receipt: null, externalOperationRef: null, eventId: null };
  }
  if (status === 'ok' || status === 'applied' || status === 'applied_late' || status === 'replayed' || status === 'late') {
    return { outcome: 'result', code: null, detail: null, receipt, externalOperationRef, eventId };
  }
  return { outcome: 'failed', code: 'PROVIDER_ERROR', detail: `unrecognized provider status "${status}"`, receipt: null, externalOperationRef: null, eventId: null };
}

/**
 * @param {object} options
 * @param {string} options.baseUrl база API провайдера (эмулятора)
 * @param {object} [options.log] общий event log Gate
 * @param {number} [options.timeoutMs] дедлайн вызова
 * @param {Function} [options.fetchImpl] подмена HTTP-клиента (тесты)
 */
function createProviderAdapter({ baseUrl, log, timeoutMs = DEFAULT_TIMEOUT_MS, fetchImpl } = {}) {
  if (!baseUrl) throw new Error('provider adapter requires a provider baseUrl');

  async function invoke({ operationId, capability, payload, scope, binding, profileId = null, userTaskId = null, runId = null, replyContext = null, deadlineMs } = {}) {
    const isRead = capability === 'hh.search_status';
    const pathname = isRead
      ? `/v1/searches/${encodeURIComponent(String(payload.searchId))}/status`
      : `/v1/applications/${encodeURIComponent(String(payload.applicationId))}/decide`;
    const body = isRead
      ? { bindingValue: binding.value }
      : {
          operationId: String(operationId),
          profileId,
          applicationId: payload.applicationId,
          decision: payload.decision,
          bindingRef: binding.binding.ref,
          bindingScope: scope,
          bindingValue: binding.value,
          replyContext,
        };

    const native = await postJson(baseUrl, pathname, body, { timeoutMs: deadlineMs || timeoutMs, fetchImpl });
    const normalized = normalize({ native });

    log?.write(isRead ? 'provider.read' : 'provider.invoke', {
      operationId: String(operationId),
      capability,
      scope,
      profileId,
      userTaskId,
      runId,
      outcome: normalized.outcome,
      code: normalized.code,
      externalOperationRef: normalized.externalOperationRef,
      eventId: normalized.eventId,
      from: 'requested',
      to: normalized.outcome,
      reasonCode: normalized.code || 'PROVIDER_OUTCOME',
      detail: normalized.detail,
    });

    return { ...normalized, nativeStatus: native.body ? native.body.status : null };
  }

  async function subscribe({ bindingRef, eventType, webhookUrl } = {}) {
    const native = await postJson(baseUrl, '/v1/subscriptions', { bindingRef, eventType, webhookUrl }, { timeoutMs, fetchImpl });
    if (native.status !== 201) {
      return { outcome: 'failed', code: native.body.code || 'PROVIDER_ERROR', detail: native.body.detail, webhookSubscriptionId: null, lifecycleState: null };
    }
    return { outcome: 'result', code: null, detail: null, webhookSubscriptionId: native.body.webhookSubscriptionId, lifecycleState: native.body.lifecycleState };
  }

  async function unsubscribe({ webhookSubscriptionId } = {}) {
    const native = await getJson(baseUrl, `/v1/subscriptions/${encodeURIComponent(String(webhookSubscriptionId))}`, { timeoutMs, fetchImpl });
    if (native.status !== 200) {
      return { outcome: 'failed', code: native.body.code || 'PROVIDER_ERROR', detail: native.body.detail, lifecycleState: null };
    }
    return { outcome: 'result', code: null, detail: null, lifecycleState: native.body.lifecycleState };
  }

  async function poll({ cursor = 0, limit = 20 } = {}) {
    const native = await getJson(baseUrl, `/v1/events?cursor=${encodeURIComponent(cursor)}&limit=${encodeURIComponent(limit)}`, { timeoutMs, fetchImpl });
    if (native.status !== 200) {
      return { outcome: 'failed', code: native.body.code || 'PROVIDER_ERROR', detail: native.body.detail, events: [], nextCursor: Number(cursor), hasMore: false };
    }
    return { outcome: 'result', code: null, detail: null, events: native.body.events, nextCursor: native.body.nextCursor, hasMore: native.body.hasMore };
  }

  async function reconcile({ operationId } = {}) {
    const native = await getJson(baseUrl, `/v1/operations/${encodeURIComponent(String(operationId))}`, { timeoutMs, fetchImpl });
    if (native.status !== 200) {
      return { outcome: 'failed', code: native.body.code || 'PROVIDER_NOT_FOUND', detail: native.body.detail, receipt: null, externalOperationRef: null, eventId: null };
    }
    return {
      outcome: native.body.outcome,
      code: null,
      detail: null,
      receipt: native.body.receipt,
      externalOperationRef: native.body.externalRef,
      eventId: native.body.eventId,
    };
  }

  function requestLiveSmoke({ bindingNames = [] } = {}) {
    const missing = REQUIRED_LIVE_BINDINGS.filter(name => !bindingNames.includes(name));
    if (missing.length > 0) {
      return { attempted: true, performed: false, blockedBy: 'NO_TEST_ACCOUNT_BINDING', missingBindings: missing, next: 'declare these names in Secret Manager / GitHub Actions secrets, then run the read-only live smoke with a sandbox-owned account' };
    }
    return { attempted: true, performed: false, blockedBy: 'LIVE_PROVIDER_CLIENT_NOT_BUILT', missingBindings: [], next: 'the emulator is the accepted evidence for this stage; a live client needs the owner decision on the first real domain (epic #22)' };
  }

  return {
    provider: 'hh-sandbox',
    protocolVersion: ADAPTER_PROTOCOL_VERSION,
    capabilities: CAPABILITIES,
    invoke,
    subscribe,
    unsubscribe,
    poll,
    reconcile,
    requestLiveSmoke,
  };
}

module.exports = { createProviderAdapter, CAPABILITIES, DEFAULT_TIMEOUT_MS };
