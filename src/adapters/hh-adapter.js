'use strict';

// Реалистичный адаптер HH — первый реальный домен пилота (карточка P26, эпик
// E6 #22, этап I08).
//
// Адаптер повторяет транспорт существующего доменного кода
// (trained-assist-hh-skill, `hh-cold-search-transport.js`): тот же запрос
// `GET /resumes` с `text`/`area`/`page`/`per_page`/`order_by`, те же заголовки
// `Authorization: Bearer`, `User-Agent` и `HH-User-Agent`, то же ограничение
// повторов и то же правило географии. Бизнес-правила здесь не живут: ни
// промптов, ни порогов, ни стадий воронки.
//
// Что адаптер объявляет честно:
//   - поддерживается только read (`hh.search_resumes`);
//   - webhook у HH нет → адаптер не реализует `subscribe` и не создаёт фиктивной
//     подписки (Gate отвечает `WEBHOOK_NOT_SUPPORTED`);
//   - потока событий у HH нет → `poll` не реализуется (`PROVIDER_EVENTS_NOT_SUPPORTED`);
//   - мутаций нет → `reconcile` не реализуется: сверять нечего, неизвестного
//     исхода у чтения не бывает.
//
// Таймаут чтения — это `failed`, а не `outcome_unknown`: GET безопасно
// повторить, эффекта не было. `outcome_unknown` остаётся только для мутаций.

const { ADAPTER_PROTOCOL_VERSION } = require('../contract/version');
const { requestLiveSmoke: liveSmokeRequest } = require('../contract/live-smoke');
const { requestJson, DEFAULT_TIMEOUT_MS } = require('./http-client');

const PROVIDER = 'hh';
const CAPABILITIES = [{ name: 'hh.search_resumes', kind: 'read', scope: 'hh#read' }];

// HH не имеет ни вебхуков, ни потока событий: объявляем это в манифесте, чтобы
// Gate не обещал транспорт, которого у провайдера нет.
const EVENTS = {
  webhooks: { supported: false, reason: 'HH ships no webhook API; a subscription cannot be created' },
  eventStream: { supported: false, reason: 'HH ships no event stream; polling is not a provider route' },
};

const REQUIRED_LIVE_BINDINGS = ['EXTERNAL_TEST_ACCOUNT_TOKEN', 'EXTERNAL_TEST_ACCOUNT_ID'];

const DEFAULT_MAX_RETRIES = 2;
const DEFAULT_RETRY_DELAY_MS = 0;
const DEFAULT_PER_PAGE = 50;
const MAX_PER_PAGE = 50;

const requestLiveSmoke = liveSmokeRequest({
  requiredBindings: REQUIRED_LIVE_BINDINGS,
  blockedByWhenPresent: 'OWNER_DECISION_REQUIRED',
  nextBlocked: 'declare these names in Secret Manager / GitHub Actions secrets and record the owner decision on the read-only test-account mode (AC-30), then run the read-only live smoke',
  nextDecision: 'bindings exist, but the read-only test-account mode is not fixed by the owner yet (AC-30); no live call is made until that decision is recorded',
});

/**
 * География — часть контракта провайдера, а не вкусовщина: молча расширить поиск
 * или молча сузить его до Москвы одинаково обманчиво. Текст ошибки тот же, что у
 * доменного транспорта, чтобы домен не получил второе сообщение.
 */
function resolveSearchAreas(area) {
  // `undefined` — география не задана вовсе: это ошибка. `null` — явный поиск
  // без ограничения, `[]` — то же самое в форме списка.
  if (area === undefined) {
    return { areas: [], error: 'Не задана география поиска. Укажи регион вакансии или area:null для поиска без ограничения.' };
  }
  if (area === null) {
    return { areas: [], error: null };
  }
  const areas = (Array.isArray(area) ? area : [area]).map(value => String(value && value.id !== undefined ? value.id : value));
  if (areas.some(value => !/^\d+$/.test(value))) {
    return { areas: [], error: 'География поиска должна содержать ID региона HH.' };
  }
  return { areas: [...new Set(areas)], error: null };
}

function normalizeRead({ httpStatus, body, transportError, providerCode, providerMessage }) {
  if (transportError === 'timeout') {
    return { outcome: 'failed', code: 'PROVIDER_UNREACHABLE', detail: 'the provider did not answer before the caller deadline; a read is safe to repeat', receipt: null, externalOperationRef: null, eventId: null };
  }
  if (transportError === 'unreachable') {
    return { outcome: 'failed', code: 'PROVIDER_UNREACHABLE', detail: 'the provider endpoint is unreachable', receipt: null, externalOperationRef: null, eventId: null };
  }
  if (httpStatus === 401 || httpStatus === 403) {
    return { outcome: 'blocked', code: 'PROVIDER_AUTH_EXPIRED', detail: providerMessage || 'the provider rejected the credential binding: expired', receipt: null, externalOperationRef: null, eventId: null };
  }
  if (httpStatus === 429) {
    return { outcome: 'failed', code: 'PROVIDER_ERROR', detail: 'the provider rate limited the caller; the bounded retries are exhausted', receipt: null, externalOperationRef: null, eventId: null };
  }
  if (httpStatus >= 500) {
    return { outcome: 'failed', code: 'PROVIDER_ERROR', detail: providerMessage || 'the provider returned a server error', receipt: null, externalOperationRef: null, eventId: null };
  }
  if (httpStatus !== 200) {
    return { outcome: 'failed', code: 'PROVIDER_ERROR', detail: providerMessage || `the provider answered ${httpStatus}`, receipt: null, externalOperationRef: null, eventId: null };
  }
  if (!body || !Array.isArray(body.items)) {
    return { outcome: 'failed', code: 'PROVIDER_STATE_UNREADABLE', detail: 'the provider answered 200 without a readable items payload', receipt: null, externalOperationRef: null, eventId: null };
  }
  return { outcome: 'result', code: null, detail: null, receipt: null, externalOperationRef: null, eventId: null, data: body };
}

/**
 * @param {object} options
 * @param {string} options.baseUrl база API провайдера (эмулятора или настоящего)
 * @param {object} [options.log] общий event log Gate
 * @param {number} [options.timeoutMs] дедлайн вызова
 * @param {number} [options.maxRetries] ограничение повторов на 429/5xx
 * @param {number} [options.retryDelayMs] пауза между повторами (в песочнице 0)
 * @param {Function} [options.sleep] подмена сна (виртуальные часы)
 * @param {Function} [options.refreshAccessToken] обновление выдачи у хоста
 * @param {Function} [options.recordArtifact] запись приватного артефакта провайдера
 * @param {Function} [options.fetchImpl] подмена HTTP-клиента (тесты)
 */
function createHhAdapter({
  baseUrl,
  log,
  timeoutMs = DEFAULT_TIMEOUT_MS,
  maxRetries = DEFAULT_MAX_RETRIES,
  retryDelayMs = DEFAULT_RETRY_DELAY_MS,
  sleep = ms => new Promise(resolve => setTimeout(resolve, ms)),
  refreshAccessToken = null,
  recordArtifact = null,
  fetchImpl,
} = {}) {
  if (!baseUrl) throw new Error('hh adapter requires a provider baseUrl');

  const agent = `trained-assist-integration-gate/0.1 (${process.env.HH_APP_CONTACT || 'sandbox@trained-assist.local'})`;

  function headersFor(token) {
    return {
      authorization: `Bearer ${token}`,
      'user-agent': agent,
      'hh-user-agent': agent,
    };
  }

  async function readOnce({ token, query, area, page, perPage }) {
    const result = await requestJson({
      baseUrl,
      method: 'GET',
      pathname: '/resumes',
      query: { text: query, page: String(page), per_page: String(perPage), order_by: 'relevance', ...(area.length > 0 ? { area: area.join(',') } : {}) },
      headers: headersFor(token),
      timeoutMs,
      fetchImpl,
    });
    return result;
  }

  /**
   * Один поиск резюме: ограниченные повторы на 429/5xx и ровно одна попытка
   * обновить выдачу при 401/403. Повторы логируются с причиной, чтобы шторм из
   * повторов не выглядел как один вызов.
   */
  async function searchResumes({ operationId, payload, binding, profileId = null, userTaskId = null, runId = null, replyContext = null, deadlineMs } = {}) {
    const query = String(payload.query ?? '').trim();
    const { areas, error } = resolveSearchAreas(payload.area);
    if (error) {
      log?.write('provider.read_rejected', {
        operationId: String(operationId),
        capability: 'hh.search_resumes',
        profileId,
        userTaskId,
        runId,
        from: 'requested',
        to: 'rejected',
        reasonCode: 'CAPABILITY_PAYLOAD_INVALID',
        detail: error,
      });
      return { outcome: 'failed', code: 'CAPABILITY_PAYLOAD_INVALID', detail: error, receipt: null, externalOperationRef: null, eventId: null };
    }

    const page = Number.isInteger(payload.page) && payload.page >= 0 ? payload.page : 0;
    const perPage = Number.isInteger(payload.perPage) && payload.perPage > 0 ? Math.min(payload.perPage, MAX_PER_PAGE) : DEFAULT_PER_PAGE;
    const deadline = deadlineMs || timeoutMs;
    let token = binding.value;
    let attempts = 0;
    let refreshed = false;
    let last = null;

    for (;;) {
      attempts += 1;
      const result = await readOnce({ token, query, area: areas, page, perPage });
      last = result;
      const retryable = result.status === 429 || result.status >= 500;
      if (retryable && attempts <= maxRetries) {
        log?.write('provider.read_retry', {
          operationId: String(operationId),
          capability: 'hh.search_resumes',
          profileId,
          userTaskId,
          runId,
          attempt: attempts,
          httpStatus: result.status,
          from: 'attempted',
          to: 'retried',
          reasonCode: 'PROVIDER_TRANSIENT',
          detail: 'the provider answered with a transient status; the bounded retry is allowed for a read',
        });
        await sleep(retryDelayMs);
        continue;
      }
      if ((result.status === 401 || result.status === 403) && !refreshed && typeof refreshAccessToken === 'function') {
        refreshed = true;
        log?.write('provider.auth_refresh', {
          operationId: String(operationId),
          capability: 'hh.search_resumes',
          profileId,
          userTaskId,
          runId,
          httpStatus: result.status,
          from: 'attempted',
          to: 'refresh_requested',
          reasonCode: 'AUTH_REFRESH_ATTEMPTED',
          detail: 'the provider rejected the credential binding; exactly one refresh attempt is made',
        });
        let fresh = null;
        try {
          fresh = await refreshAccessToken({ bindingRef: binding.binding.ref, profileId });
        } catch (error) {
          log?.write('provider.auth_refresh_failed', {
            operationId: String(operationId),
            capability: 'hh.search_resumes',
            profileId,
            userTaskId,
            runId,
            from: 'refresh_requested',
            to: 'refresh_failed',
            reasonCode: 'AUTH_REFRESH_FAILED',
            detail: 'the host could not refresh the credential binding',
          });
        }
        if (fresh) {
          token = fresh;
          continue;
        }
      }
      break;
    }

    const normalized = normalizeRead({
      httpStatus: last.status,
      body: last.body,
      transportError: last.transportError,
      providerCode: last.body && last.body.code,
      providerMessage: last.body && last.body.message,
    });

    if (normalized.outcome === 'result') {
      const items = normalized.data.items;
      const receipt = {
        receiptId: `rcpt_${String(operationId).slice(0, 16)}`,
        externalRef: null,
        itemCount: items.length,
        itemIds: items.slice(0, 20).map(item => String(item.id)),
        page: normalized.data.page,
        privateDetailsRef: `hh-search/${String(operationId).slice(0, 16)}`,
      };
      normalized.receipt = receipt;
      if (typeof recordArtifact === 'function') {
        recordArtifact({ ref: receipt.privateDetailsRef, items, query, area: areas, page, perPage });
      }
    }

    log?.write('provider.read', {
      operationId: String(operationId),
      capability: 'hh.search_resumes',
      scope: 'hh#read',
      profileId,
      userTaskId,
      runId,
      outcome: normalized.outcome,
      code: normalized.code,
      httpStatus: last.status,
      attempts,
      refreshed,
      itemCount: normalized.receipt ? normalized.receipt.itemCount : null,
      from: 'requested',
      to: normalized.outcome,
      reasonCode: normalized.code || 'PROVIDER_OUTCOME',
      detail: normalized.detail,
    });

    return { ...normalized, nativeStatus: last.status, attempts };
  }

  async function invoke({ operationId, capability, payload, scope, binding, profileId = null, userTaskId = null, runId = null, replyContext = null, deadlineMs } = {}) {
    if (capability !== 'hh.search_resumes') {
      return { outcome: 'failed', code: 'CAPABILITY_NOT_DECLARED', detail: `capability "${capability}" is not declared by adapter "${PROVIDER}"`, receipt: null, externalOperationRef: null, eventId: null };
    }
    return searchResumes({ operationId, payload, binding, profileId, userTaskId, runId, replyContext, deadlineMs });
  }

  return {
    provider: PROVIDER,
    protocolVersion: ADAPTER_PROTOCOL_VERSION,
    capabilities: CAPABILITIES,
    events: EVENTS,
    invoke,
    requestLiveSmoke,
  };
}

module.exports = { createHhAdapter, CAPABILITIES, EVENTS, resolveSearchAreas, DEFAULT_TIMEOUT_MS };