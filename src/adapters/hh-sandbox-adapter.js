'use strict';

// Реализация адаптера провайдера hh-sandbox: ровно одна на провайдера (AC-152).
//
// Адаптер только переводит вызов в транспорт провайдера и нормализует ответ в
// исходы контракта C10. Бизнес-правила здесь не живут: ни промптов, ни порогов
// решений, ни стадий воронки — это остаётся в доменном репозитории.
//
// Транспорт один — HTTP к провайдеру (`http-client.js`, общий для всех
// адаптеров). Второй транспортной реализации нет и не заводится: фасад Gate и
// адаптер не дублируют друг друга.

const { ADAPTER_PROTOCOL_VERSION } = require('../contract/version');
const { requestLiveSmoke: liveSmokeRequest } = require('../contract/live-smoke');
const { requestJson, DEFAULT_TIMEOUT_MS } = require('./http-client');

const CAPABILITIES = [
  { name: 'hh.search_status', kind: 'read', scope: 'hh#read' },
  { name: 'hh.application_decide', kind: 'mutation', scope: 'hh#write' },
];

// Эмулятор провайдера действительно умеет и webhook, и поток событий, поэтому
// объявляет оба транспорта (см. `events` в манифесте адаптера).
const EVENTS = {
  webhooks: { supported: true, eventTypes: ['application.decision.recorded'] },
  eventStream: { supported: true },
};

const REQUIRED_LIVE_BINDINGS = ['EXTERNAL_TEST_ACCOUNT_TOKEN', 'EXTERNAL_TEST_ACCOUNT_ID'];

// Живая проверка этого провайдера невозможна: эмулятор — единственный источник
// свидетельства на этапе I08, пока владелец не зафиксирует режим test account.
const requestLiveSmoke = liveSmokeRequest({
  requiredBindings: REQUIRED_LIVE_BINDINGS,
  blockedByWhenPresent: 'LIVE_PROVIDER_CLIENT_NOT_BUILT',
  nextBlocked: 'declare these names in Secret Manager / GitHub Actions secrets, then run the read-only live smoke with a sandbox-owned account',
  nextDecision: 'the emulator is the accepted evidence for this stage; a live client needs the owner decision on the first real domain (epic #22)',
});

function postJson(baseUrl, pathname, body, { timeoutMs = DEFAULT_TIMEOUT_MS, fetchImpl } = {}) {
  return requestJson({ baseUrl, method: 'POST', pathname, body, timeoutMs, fetchImpl }).then(result =>
    result.body
      ? result
      : {
          status: 0,
          body: {
            status: result.transportError === 'timeout' ? 'timeout' : 'unreachable',
            code: result.transportError === 'timeout' ? 'EFFECT_STATE_UNKNOWN' : 'PROVIDER_UNREACHABLE',
            detail:
              result.transportError === 'timeout'
                ? 'the provider did not answer before the caller deadline; the mutation may or may not have been applied'
                : 'the provider endpoint is unreachable',
          },
        }
  );
}

function getJson(baseUrl, pathname, { timeoutMs = DEFAULT_TIMEOUT_MS, fetchImpl } = {}) {
  return requestJson({ baseUrl, method: 'GET', pathname, timeoutMs, fetchImpl }).then(result =>
    result.body
      ? result
      : {
          status: 0,
          body: {
            status: 'unreachable',
            code: 'PROVIDER_UNREACHABLE',
            detail: 'the provider endpoint is unreachable',
          },
        }
  );
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

  return {
    provider: 'hh-sandbox',
    protocolVersion: ADAPTER_PROTOCOL_VERSION,
    capabilities: CAPABILITIES,
    events: EVENTS,
    invoke,
    subscribe,
    unsubscribe,
    poll,
    reconcile,
    requestLiveSmoke,
  };
}

module.exports = { createProviderAdapter, CAPABILITIES, DEFAULT_TIMEOUT_MS };
