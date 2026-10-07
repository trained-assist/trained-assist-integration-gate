'use strict';

// External Integration Gate (карточка P25, эпик E6 #22, этап I08, контракт C10).
//
// Gate связывает платформу с API и событиями внешних сервисов от имени
// разрешённого пользователя/организации. Он не маршрутизирует Job types, не
// запускает Agent Runner и не контролирует достижение цели: бизнес-правила
// остаются в доменном репозитории (AC-152).
//
// Что здесь есть: versioned invoke / subscribe / reconcile / events,
// bindings с host-owned резолвером значений, леджер операций с записью
// operationId до вызова, входящий webhook inbox с проверкой подписи, дедупом и
// быстрым ACK, исходящий outbox нормализованных событий и порт передачи в Input.

const path = require('path');
const crypto = require('crypto');

const { GATE_CONTRACT_VERSION } = require('../contract/version');
const { blockedSummary } = require('../contract/outcomes');
const { createEventLog, reportError } = require('../contract/events');
const { resolveErrorPublisher } = require('../error-publisher');
const { createCredentialResolver, writeBindingValue, BINDING_STORE_DIR } = require('./bindings');
const { createOperationLedger } = require('./operation-ledger');
const { createAdapterRegistry } = require('./adapter-registry');
const { createWebhookInbox } = require('./webhook-inbox');
const { createEventOutbox } = require('./event-outbox');
const { createTaskPort } = require('./task-port');
const { createSubscriptionStore, STORE_DIR } = require('./subscription-store');

/**
 * @param {object} options
 * @param {string} options.dataRoot изолированный корень песочницы Gate
 * @param {() => Date} [options.now] детерминированные часы
 * @param {object} [options.log] свой event log (по умолчанию пишется в `${dataRoot}/gate/events.jsonl`)
 * @param {object} [options.credentialResolver] свой резолвер bindings
 * @param {object} [options.adapterRegistry] свой реестр адаптеров
 * @param {object} [options.bindingPolicy] явная политика создания задач из независимых событий
 * @param {object} [options.errorPublisher] publisher C12 ErrorEvent для собственного event log (иначе ERROR_WATCHER_URL/ERROR_WATCHER_KEY окружения)
 */
function createIntegrationGate({
  dataRoot,
  now = () => new Date(),
  log,
  credentialResolver,
  adapterRegistry,
  bindingPolicy = { createTaskForUnmatchedEvent: false },
  readProbePayload = null,
  errorPublisher = undefined,
} = {}) {
  if (!dataRoot) throw new Error('integration gate requires an isolated dataRoot');

  const resolvedLog = log || createEventLog({
    file: path.join(dataRoot, 'gate', 'events.jsonl'),
    now,
    errorPublisher: errorPublisher === undefined ? resolveErrorPublisher(process.env) : errorPublisher,
  });
  const bindingsDir = path.join(dataRoot, BINDING_STORE_DIR);
  const resolver = credentialResolver || createCredentialResolver({ storeDir: bindingsDir, now });
  const ledger = createOperationLedger({ root: path.join(dataRoot, 'gate'), now, log: resolvedLog });
  const registry = adapterRegistry || createAdapterRegistry({ log: resolvedLog });
  const outbox = createEventOutbox({ root: path.join(dataRoot, 'gate', 'outbox'), now, log: resolvedLog });
  const taskPort = createTaskPort({ log: resolvedLog, now });
  const subscriptionStore = createSubscriptionStore({ storeDir: path.join(dataRoot, STORE_DIR), now });

  const inbox = createWebhookInbox({
    root: path.join(dataRoot, 'gate', 'inbox'),
    log: resolvedLog,
    clock: now,
    secretResolver: ({ subscriptionId }) => (subscriptionId ? subscriptionStore.secretOf(subscriptionId) : undefined),
    dispatch: dispatchCallback,
  });

  function envelope({ profileId = null, userTaskId = null, runId = null, replyContext = null, gtdId = null, operationId = null }) {
    return {
      schemaVersion: GATE_CONTRACT_VERSION,
      profileId,
      userTaskId,
      runId,
      gtdId,
      replyContext,
      operationId,
    };
  }

  function resolveBinding({ integrationBindingId, scope, profileId = null }) {
    const resolved = resolver.resolve({ bindingRef: integrationBindingId, scope, profileId });
    return resolved;
  }

  function capabilityOf(adapter, capability) {
    const found = (adapter.capabilities || []).find(entry => entry.name === capability);
    if (!found) {
      const err = new Error(`capability "${capability}" is not declared by adapter "${adapter.provider}"`);
      err.code = 'CAPABILITY_NOT_DECLARED';
      throw err;
    }
    return found;
  }

  /**
   * Исходящая операция. operationId пишется в леджер ДО вызова провайдера;
   * повтор мутации с неизвестным исходом запрещён до reconcile (INV-07).
   */
  async function invoke({ integrationBindingId, capability, payload, operationId = null, userTaskId = null, runId = null, replyContext = null, gtdId = null, profileId = null, deadlineMs = null } = {}) {
    let binding;
    try {
      binding = resolveBinding({ integrationBindingId, scope: null, profileId });
    } catch (error) {
      return blockedResult({ code: error.code, detail: error.message, profileId, userTaskId, runId, replyContext, integrationBindingId });
    }

    const adapter = registry.get(binding.binding.provider);
    if (!adapter) {
      return blockedResult({ code: 'ADAPTER_NOT_REGISTERED', detail: `no adapter implementation for provider "${binding.binding.provider}"`, profileId, userTaskId, runId, replyContext, integrationBindingId });
    }

    let manifest;
    try {
      manifest = capabilityOf(adapter, capability);
    } catch (error) {
      return blockedResult({ code: error.code, detail: error.message, profileId, userTaskId, runId, replyContext, integrationBindingId });
    }

    try {
      resolver.resolve({ bindingRef: integrationBindingId, scope: manifest.scope, profileId });
    } catch (error) {
      return blockedResult({ code: error.code, detail: error.message, profileId, userTaskId, runId, replyContext, integrationBindingId });
    }

    const id = operationId || nextOperationId();
    const entry = ledger.begin({
      operationId: id,
      integrationBindingId,
      capability,
      scope: manifest.scope,
      profileId,
      userTaskId,
      runId,
      payloadHash: payloadHashOf(payload),
      replyContext,
      gtdId,
    });

    if (!ledger.canRetry(entry.operationId)) {
      resolvedLog.write('operation.retry_rejected', {
        operationId: entry.operationId,
        capability,
        scope: manifest.scope,
        profileId,
        userTaskId,
        runId,
        from: 'requested',
        to: 'rejected',
        reasonCode: 'RETRY_BEFORE_RECONCILE',
        detail: 'the outcome of this mutation is still unknown; reconcile must precede any retry',
      });
      return {
        ...envelope({ profileId, userTaskId, runId, replyContext, gtdId, operationId: entry.operationId }),
        outcome: 'outcome_unknown',
        code: 'RETRY_BEFORE_RECONCILE',
        detail: 'the outcome of this mutation is still unknown; reconcile must precede any retry',
        receipt: null,
        externalOperationRef: entry.externalOperationRef,
        retryAllowed: false,
      };
    }

    const result = await adapter.invoke({
      operationId: entry.operationId,
      capability,
      payload,
      scope: manifest.scope,
      binding,
      profileId,
      userTaskId,
      runId,
      replyContext,
      deadlineMs,
    });

    const outcome = result.outcome;
    ledger.recordOutcome(entry.operationId, outcome, {
      receipt: result.receipt,
      externalOperationRef: result.externalOperationRef,
      code: result.code,
      detail: result.detail,
    });

    const blockedDetail = outcome === 'blocked'
      ? blockedSummary(result.detail && result.detail.includes('expired') ? 'expired' : 'denied')
      : null;

    if (outcome === 'blocked') {
      resolvedLog.write('operation.blocked', {
        operationId: entry.operationId,
        capability,
        scope: manifest.scope,
        profileId,
        userTaskId,
        runId,
        from: 'pending',
        to: 'blocked',
        reasonCode: result.code,
        detail: blockedDetail,
      });
      reportError(resolvedLog, {
        code: 'OPERATION_BLOCKED',
        operation: 'invoke',
        detail: blockedDetail,
        profileId,
        userTaskId,
        runId,
        replyContext,
      });
    }

    return {
      ...envelope({ profileId, userTaskId, runId, replyContext, gtdId, operationId: entry.operationId }),
      outcome,
      code: result.code,
      detail: result.detail,
      safeSummary: blockedDetail,
      receipt: result.receipt,
      externalOperationRef: result.externalOperationRef,
      eventId: result.eventId,
      attempts: result.attempts ?? null,
      retryAllowed: outcome === 'outcome_unknown' ? ledger.canRetry(entry.operationId) : true,
    };
  }

  function blockedResult({ code, detail, profileId, userTaskId, runId, replyContext, integrationBindingId }) {
    const summary = blockedSummary(code === 'BINDING_EXPIRED' ? 'expired' : 'denied');
    resolvedLog.write('operation.blocked', {
      integrationBindingId,
      profileId,
      userTaskId,
      runId,
      from: 'requested',
      to: 'blocked',
      reasonCode: code,
      detail: summary,
    });
    reportError(resolvedLog, {
      code: 'OPERATION_BLOCKED',
      operation: 'invoke',
      detail: summary,
      profileId,
      userTaskId,
      runId,
      replyContext,
    });
    return {
      ...envelope({ profileId, userTaskId, runId, replyContext }),
      outcome: 'blocked',
      code,
      detail,
      safeSummary: summary,
      receipt: null,
      externalOperationRef: null,
      retryAllowed: false,
    };
  }

  function payloadHashOf(payload) {
    if (payload === undefined || payload === null) return null;
    return crypto.createHash('sha256').update(JSON.stringify(payload)).digest('hex').slice(0, 32);
  }

  let operationCounter = 0;
  function nextOperationId() {
    operationCounter += 1;
    return `op_${crypto.createHash('sha256').update(`operation|${operationCounter}`).digest('hex').slice(0, 16)}`;
  }

  /**
   * Подписка на события провайдера. Поддержка webhook не предполагается для
   * каждого провайдера: если провайдер не поддерживает webhook, остаётся
   * polling (capabilities объявляют только реально поддерживаемое).
   *
   * Адаптер, у которого нет `subscribe` или который объявляет
   * `events.webhooks.supported === false`, не получает фиктивной подписки: Gate
   * отказывает до обращения к провайдеру и не создаёт секрет подписи.
   */
  async function subscribe({ integrationBindingId, eventType, webhookUrl = null, profileId = null } = {}) {
    let binding;
    try {
      binding = resolveBinding({ integrationBindingId, scope: null, profileId });
    } catch (error) {
      return { outcome: 'blocked', code: error.code, detail: error.message, webhookSubscriptionId: null, lifecycleState: null };
    }
    const adapter = registry.get(binding.binding.provider);
    if (!adapter) {
      return { outcome: 'blocked', code: 'ADAPTER_NOT_REGISTERED', detail: `no adapter implementation for provider "${binding.binding.provider}"`, webhookSubscriptionId: null, lifecycleState: null };
    }
    if (adapter.events && adapter.events.webhooks && adapter.events.webhooks.supported === false) {
      resolvedLog.write('subscription.unsupported', {
        integrationBindingId,
        provider: adapter.provider,
        eventType,
        profileId,
        from: 'requested',
        to: 'blocked',
        reasonCode: 'WEBHOOK_NOT_SUPPORTED',
        detail: adapter.events.webhooks.reason || 'the provider ships no webhook API',
      });
      return {
        outcome: 'blocked',
        code: 'WEBHOOK_NOT_SUPPORTED',
        detail: adapter.events.webhooks.reason || 'the provider ships no webhook API',
        webhookSubscriptionId: null,
        lifecycleState: null,
        supportsWebhook: false,
        transports: [],
      };
    }
    if (typeof adapter.subscribe !== 'function') {
      return { outcome: 'blocked', code: 'WEBHOOK_NOT_SUPPORTED', detail: 'the adapter declares no subscription route', webhookSubscriptionId: null, lifecycleState: null, supportsWebhook: false, transports: [] };
    }

    const result = await adapter.subscribe({ bindingRef: integrationBindingId, eventType, webhookUrl });
    if (result.outcome !== 'result' || !result.webhookSubscriptionId) {
      return { ...result, lifecycleState: null };
    }

    const { record, secret } = subscriptionStore.create({
      webhookSubscriptionId: result.webhookSubscriptionId,
      bindingRef: integrationBindingId,
      profileId,
      eventType,
      webhookUrl,
    });
    void secret;

    resolvedLog.write('subscription.created', {
      webhookSubscriptionId: record.webhookSubscriptionId,
      eventType,
      profileId,
      hasWebhook: Boolean(webhookUrl),
      from: 'requested',
      to: 'active',
      reasonCode: 'SUBSCRIPTION_CREATED',
      detail: webhookUrl ? 'webhook subscription is active; polling stays available' : 'provider supports no webhook; polling is the supported route',
    });

    return {
      outcome: 'result',
      code: null,
      detail: null,
      webhookSubscriptionId: record.webhookSubscriptionId,
      lifecycleState: record.lifecycleState,
      transport: webhookUrl ? 'webhook' : 'poll',
    };
  }

  async function unsubscribe({ webhookSubscriptionId, profileId = null } = {}) {
    const record = subscriptionStore.get(webhookSubscriptionId);
    if (!record) return { outcome: 'failed', code: 'OPERATION_NOT_FOUND', detail: 'no such webhook subscription', lifecycleState: null };
    if (profileId && record.profileId && record.profileId !== profileId) {
      return { outcome: 'blocked', code: 'BINDING_SCOPE_MISMATCH', detail: 'the subscription belongs to another profile', lifecycleState: null };
    }
    const adapter = registry.get(resolver.read(record.bindingRef)?.provider || null);
    const result = adapter ? await adapter.unsubscribe({ webhookSubscriptionId }) : { outcome: 'result', lifecycleState: 'inactive' };
    subscriptionStore.setLifecycle(webhookSubscriptionId, 'inactive');
    resolvedLog.write('subscription.closed', {
      webhookSubscriptionId,
      profileId: record.profileId,
      from: 'active',
      to: 'inactive',
      reasonCode: 'SUBSCRIPTION_CLOSED',
      detail: 'the subscription is closed; no further callbacks are accepted',
    });
    return { outcome: result.outcome, code: result.code || null, detail: result.detail || null, lifecycleState: 'inactive' };
  }

  /**
   * Reconcile по operationId / externalOperationRef. Провайдер — источник
   * истины; локальный леджер только сохраняет подтверждение.
   */
  async function reconcile({ operationId = null, externalOperationRef = null, profileId = null } = {}) {
    const entry = (operationId && ledger.get(operationId)) || (externalOperationRef && ledger.findByExternalRef(externalOperationRef));
    if (!entry) {
      return { found: false, outcome: null, state: null, reconciled: false, retryAllowed: true };
    }
    if (profileId && entry.profileId && entry.profileId !== profileId) {
      return { found: true, outcome: null, state: null, reconciled: false, retryAllowed: false, code: 'BINDING_SCOPE_MISMATCH' };
    }

    const adapter = registry.get(resolver.read(entry.integrationBindingId)?.provider || null);
    // У провайдера без мутаций сверять нечего: адаптер не реализует reconcile,
    // и Gate не делает фиктивный вызов провайдера. Локальный леджер — единственный
    // источник состояния, и он возвращается как есть.
    const result = adapter && typeof adapter.reconcile === 'function'
      ? await adapter.reconcile({ operationId: entry.operationId })
      : { outcome: 'outcome_unknown', adapterReconcile: 'not_supported' };

    if (result.outcome === 'result') {
      ledger.recordOutcome(entry.operationId, 'result', {
        receipt: result.receipt,
        externalOperationRef: result.externalOperationRef,
        code: null,
        detail: 'reconciliation confirmed the outcome',
      });
    }
    const reconciled = ledger.reconcile(entry.operationId);
    ledger.markReconciled(entry.operationId);

    return {
      found: true,
      outcome: reconciled.outcome,
      state: reconciled.state,
      reconciled: reconciled.reconciled,
      retryAllowed: reconciled.reconciled,
      receipt: reconciled.entry.receipt,
      externalOperationRef: reconciled.entry.externalOperationRef,
      eventId: reconciled.entry.callback ? reconciled.entry.callback.eventId : null,
      adapterReconcile: result.adapterReconcile || 'provider',
    };
  }

  /**
   * Polling-маршрут: тот же scope, что у read и webhook — principal из binding,
   * чужой binding отклоняется до обращения к провайдеру.
   *
   * Поток событий — тоже объявляемый транспорт: если у провайдера его нет,
   * Gate не возвращает пустой список событий (иначе «нет событий» стало бы
   * недоказуемым), а отказывает с `PROVIDER_EVENTS_NOT_SUPPORTED`.
   */
  async function poll({ integrationBindingId, cursor = 0, limit = 20, profileId = null } = {}) {
    let binding;
    try {
      binding = resolveBinding({ integrationBindingId, scope: null, profileId });
    } catch (error) {
      return { outcome: 'blocked', code: error.code, detail: error.message, events: [], nextCursor: Number(cursor), hasMore: false };
    }
    const adapter = registry.get(binding.binding.provider);
    if (!adapter) {
      return { outcome: 'blocked', code: 'ADAPTER_NOT_REGISTERED', detail: `no adapter implementation for provider "${binding.binding.provider}"`, events: [], nextCursor: Number(cursor), hasMore: false };
    }
    if (adapter.events && adapter.events.eventStream && adapter.events.eventStream.supported === false) {
      resolvedLog.write('poll.unsupported', {
        integrationBindingId,
        provider: adapter.provider,
        profileId,
        from: 'requested',
        to: 'blocked',
        reasonCode: 'PROVIDER_EVENTS_NOT_SUPPORTED',
        detail: adapter.events.eventStream.reason || 'the provider ships no event stream',
      });
      return {
        outcome: 'failed',
        code: 'PROVIDER_EVENTS_NOT_SUPPORTED',
        detail: adapter.events.eventStream.reason || 'the provider ships no event stream',
        events: [],
        nextCursor: Number(cursor),
        hasMore: false,
      };
    }
    if (typeof adapter.poll !== 'function') {
      return { outcome: 'failed', code: 'PROVIDER_EVENTS_NOT_SUPPORTED', detail: 'the adapter declares no event stream', events: [], nextCursor: Number(cursor), hasMore: false };
    }
    const result = await adapter.poll({ cursor, limit });
    return {
      ...envelope({ profileId }),
      outcome: result.outcome,
      code: result.code,
      detail: result.detail,
      events: result.events,
      nextCursor: result.nextCursor,
      hasMore: result.hasMore,
    };
  }

  /**
   * Приём обратного вызова провайдера: подпись → durable receipt → дедуп →
   * быстрый ACK → асинхронный dispatch (AC-150).
   */
  function receiveCallback({ headers = {}, rawBody = '', provider = null } = {}) {
    return inbox.receive({ headers, rawBody, provider });
  }

  /**
   * Асинхронный dispatch принятого обратного вызова: нормализация, обновление
   * существующей операции (без второй задачи) и публикация события в outbox.
   */
  async function dispatchCallback(envelope = {}) {
    const operationId = envelope.operationId || null;
    const providerEventId = envelope.providerEventId || envelope.eventId || null;

    // Principal события берётся из подписки (binding), а не из поля profileId в
    // теле провайдера: тело — не доверенный источник принципала (AC-252).
    const boundSubscription = envelope.subscriptionId ? subscriptionStore.get(envelope.subscriptionId) : null;
    const profileId = boundSubscription && boundSubscription.profileId ? boundSubscription.profileId : envelope.profileId || null;

    if (operationId) {
      const entry = ledger.get(operationId);
      if (entry) {
        ledger.markCallback(operationId, { eventId: providerEventId, providerEventId, outcome: 'result' });
        ledger.markReconciled(operationId);
        resolvedLog.write('operation.reconciled_by_callback', {
          operationId,
          eventId: providerEventId,
          profileId: entry.profileId,
          userTaskId: entry.userTaskId,
          runId: entry.runId,
          from: 'unknown',
          to: 'reconciled',
          reasonCode: 'CALLBACK_CONFIRMED_OUTCOME',
          detail: 'the callback confirmed the outcome of the existing operation; no second task is created',
        });
      }
    }

    const published = outbox.publish({
      provider: envelope.provider,
      providerEventId,
      kind: envelope.kind,
      payload: null,
      operation: operationId ? { operationId, externalOperationRef: envelope.externalRef || null } : null,
      profileId,
      userTaskId: null,
      runId: null,
      replyContext: envelope.replyContext || null,
    });

    if (!published.published) {
      return { detail: 'duplicate provider event; nothing new was published', eventId: published.eventId, taskCreated: false };
    }

    let taskCreated = false;
    if (!operationId && bindingPolicy.createTaskForUnmatchedEvent) {
      taskPort.submit({
        userTaskId: `ut_${published.eventId.slice(4, 16)}`,
        profileId,
        replyContext: envelope.replyContext || null,
        causation: { eventId: published.eventId, providerEventId },
      });
      taskCreated = true;
    }

    return { detail: 'normalized event published after the ACK', eventId: published.eventId, taskCreated };
  }

  /**
   * Capabilities и readiness по авторизованному binding: поддерживаемые
   * операции, enabled/disabled и состояние выдачи.
   *
   * `probe: true` выполняет одну read-операцию, чтобы отличить «выдача есть» от
   * «провайдер отклоняет». Без probe этого не утверждаем: готовность binding'а
   * не доказывает здоровье выдачи у провайдера.
   */
  async function capabilities({ integrationBindingId, profileId = null, probe = false } = {}) {
    const record = resolver.read(integrationBindingId);
    if (!record) {
      return { outcome: 'blocked', code: 'BINDING_NOT_FOUND', detail: 'credential binding not found', capabilities: [], authHealth: 'unknown', transports: [] };
    }
    if (profileId && record.profileId && record.profileId !== profileId) {
      return { outcome: 'blocked', code: 'BINDING_SCOPE_MISMATCH', detail: 'credential binding belongs to another profile', capabilities: [], authHealth: 'unknown', transports: [] };
    }
    const adapter = registry.get(record.provider || null);
    if (!adapter) {
      return { outcome: 'blocked', code: 'ADAPTER_NOT_REGISTERED', detail: `no adapter implementation for provider "${record.provider}"`, capabilities: [], authHealth: 'unknown', transports: [] };
    }

    const expired = Boolean(record.expiresAt && new Date(record.expiresAt).getTime() <= now().getTime());
    let authHealth = expired ? 'expired' : 'ready';
    const events = adapter.events || null;
    // Транспорты событий объявляет адаптер, а не Gate: у HH нет ни вебхуков, ни
    // потока событий, и пустой список транспортов — это честный ответ, а не
    // «ничего не поддерживается по ошибке».
    const transports = events
      ? [
          events.webhooks && events.webhooks.supported ? 'webhook' : null,
          events.eventStream && events.eventStream.supported ? 'poll' : null,
        ].filter(Boolean)
      : ['poll'];

    if (probe && !expired) {
      if (!readProbePayload) {
        throw new Error('capabilities({ probe: true }) requires readProbePayload: без входных данных probe ничего не доказывает');
      }
      const readCapability = (adapter.capabilities || []).find(cap => cap.kind === 'read');
      if (readCapability) {
        try {
          const resolved = resolver.resolve({ bindingRef: integrationBindingId, scope: readCapability.scope, profileId });
          const result = await adapter.invoke({
            operationId: `probe_${readCapability.name}`,
            capability: readCapability.name,
            payload: readProbePayload,
            scope: readCapability.scope,
            binding: resolved,
            profileId,
            userTaskId: null,
            runId: null,
            replyContext: null,
          });
          if (result.code === 'PROVIDER_AUTH_EXPIRED') authHealth = 'expired';
          else if (result.code === 'PROVIDER_UNREACHABLE') authHealth = 'unreachable';
          else if (result.outcome === 'result') authHealth = 'ready';
          else authHealth = 'degraded';
        } catch (error) {
          authHealth = error.code === 'BINDING_EXPIRED' ? 'expired' : 'degraded';
        }
      }
    }

    resolvedLog.write('readiness.reported', {
      profileId,
      authHealth,
      hasWebhook: Boolean(subscriptionStore.findByBinding(integrationBindingId)),
      transports,
      from: 'readiness_request',
      to: authHealth,
      reasonCode: 'READINESS_REPORTED',
      detail: 'readiness describes the binding and, when probed, one read operation; it never claims provider health it did not observe',
    });

    return {
      ...envelope({ profileId }),
      outcome: 'result',
      code: null,
      detail: null,
      capabilities: (adapter.capabilities || []).map(cap => ({
        ...cap,
        enabled: !expired && authHealth === 'ready' && (!cap.scope || (record.scopes || []).includes(cap.scope)),
      })),
      authHealth,
      providerEvents: events,
      transports,
    };
  }

  return {
    contractVersion: GATE_CONTRACT_VERSION,
    dataRoot,
    log: resolvedLog,
    ledger,
    registry,
    inbox,
    outbox,
    taskPort,
    subscriptionStore,
    bindingsDir,
    invoke,
    subscribe,
    unsubscribe,
    reconcile,
    poll,
    receiveCallback,
    capabilities,
    drain: () => inbox.drain(),
    writeBindingValue: (ref, value, options) => writeBindingValue(bindingsDir, ref, value, options),
  };
}

module.exports = { createIntegrationGate };
