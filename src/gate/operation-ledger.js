'use strict';

// Леджер операций Gate: operationId записывается ДО вызова провайдера.
//
// Это единственное место, которое знает состояние исходящей операции. Таймаут
// после отправки переводит операцию в `outcome_unknown`; повтор той же
// мутации запрещён, пока операция не прошла reconcile (INV-07, AC-151).
// Обратный вызов существующей операции обновляет её состояние и не создаёт
// вторую задачу (AC-253).

const fs = require('fs');
const path = require('path');

const LEDGER_FILE = 'operations.jsonl';

function appendLine(file, line) {
  fs.mkdirSync(path.dirname(file), { recursive: true, mode: 0o700 });
  fs.appendFileSync(file, `${JSON.stringify(line)}\n`, { mode: 0o600 });
}

function readLines(file) {
  if (!fs.existsSync(file)) return [];
  return fs.readFileSync(file, 'utf8')
    .split('\n')
    .filter(line => line.trim().length > 0)
    .map(line => {
      try {
        return JSON.parse(line);
      } catch {
        return null;
      }
    })
    .filter(Boolean);
}

/**
 * @param {object} options
 * @param {string} options.root изолированный корень леджера
 * @param {() => Date} [options.now]
 * @param {object} [options.log] общий event log Gate
 */
function createOperationLedger({ root, now = () => new Date(), log } = {}) {
  if (!root) throw new Error('operation ledger requires an isolated root');
  const file = path.join(root, LEDGER_FILE);
  const index = new Map(readLines(file).map(entry => [entry.operationId, entry]));

  function persist(entry) {
    appendLine(file, entry);
    index.set(entry.operationId, entry);
  }

  function get(operationId) {
    return index.get(String(operationId)) || null;
  }

  function findByExternalRef(externalOperationRef) {
    for (const entry of index.values()) {
      if (entry.externalOperationRef === externalOperationRef) return entry;
    }
    return null;
  }

  /**
   * Запись operationId до вызова провайдера. Возвращает запись-черновик,
   * которую вызывающий обязан сохранить после ответа.
   */
  function begin({ operationId, integrationBindingId, capability, scope, profileId = null, userTaskId = null, runId = null, payloadHash = null, replyContext = null, gtdId = null }) {
    const id = String(operationId);
    if (index.has(id)) return index.get(id);
    const entry = {
      operationId: id,
      integrationBindingId: String(integrationBindingId),
      capability: String(capability),
      scope: String(scope),
      profileId,
      userTaskId,
      runId,
      gtdId,
      payloadHash,
      replyContext,
      state: 'pending',
      outcome: null,
      receipt: null,
      externalOperationRef: null,
      code: null,
      detail: null,
      attempts: 0,
      createdAt: now().toISOString(),
      updatedAt: now().toISOString(),
      reconciledAt: null,
      callback: null,
    };
    persist(entry);
    log?.write('operation.started', {
      operationId: id,
      integrationBindingId: entry.integrationBindingId,
      capability: entry.capability,
      scope: entry.scope,
      profileId,
      userTaskId,
      runId,
      from: 'requested',
      to: 'pending',
      reasonCode: 'OPERATION_RECORDED_BEFORE_CALL',
      detail: 'operationId is durable before the provider call; a later timeout means outcome_unknown, not a free retry',
    });
    return entry;
  }

  function recordOutcome(operationId, outcome, { receipt = null, externalOperationRef = null, code = null, detail = null } = {}) {
    const entry = get(operationId);
    if (!entry) return null;
    entry.outcome = outcome;
    entry.receipt = receipt;
    entry.externalOperationRef = externalOperationRef;
    entry.code = code;
    entry.detail = detail;
    entry.attempts += 1;
    entry.updatedAt = now().toISOString();
    if (outcome === 'outcome_unknown') entry.state = 'unknown';
    else if (outcome === 'blocked') entry.state = 'blocked';
    else if (outcome === 'failed') entry.state = 'failed';
    else entry.state = 'completed';
    persist(entry);
    log?.write('operation.outcome', {
      operationId: entry.operationId,
      outcome,
      externalOperationRef,
      profileId: entry.profileId,
      userTaskId: entry.userTaskId,
      runId: entry.runId,
      from: 'pending',
      to: entry.state,
      reasonCode: code || 'OUTCOME_RECORDED',
      detail,
    });
    return entry;
  }

  /**
   * Reconcile по operationId / externalOperationRef. Подтверждённый исход
   * возвращается как есть; неизвестный остаётся неизвестным, и повтор мутации
   * по-прежнему запрещён.
   */
  function reconcile(operationId) {
    const entry = get(operationId) || findByExternalRef(operationId);
    if (!entry) return { found: false, outcome: null, state: null, reconciled: false };
    const reconciled = entry.outcome !== 'outcome_unknown' && entry.outcome !== null;
    if (reconciled && !entry.reconciledAt) {
      entry.reconciledAt = now().toISOString();
      persist(entry);
    }
    log?.write('operation.reconciled', {
      operationId: entry.operationId,
      outcome: entry.outcome,
      externalOperationRef: entry.externalOperationRef,
      profileId: entry.profileId,
      userTaskId: entry.userTaskId,
      runId: entry.runId,
      from: entry.state,
      to: reconciled ? 'reconciled' : 'still_unknown',
      reasonCode: reconciled ? 'RECONCILE_CONFIRMED' : 'RECONCLE_STILL_UNKNOWN',
      detail: reconciled
        ? 'reconciliation confirmed the outcome; a retry of this mutation is now allowed'
        : 'the provider did not confirm the outcome; no blind retry is allowed',
    });
    return { found: true, outcome: entry.outcome, state: entry.state, reconciled, entry };
  }

  /**
   * Повтор мутации разрешён только после reconcile (INV-07).
   */
  function canRetry(operationId) {
    const entry = get(operationId);
    if (!entry) return true;
    if (entry.outcome !== 'outcome_unknown') return true;
    return Boolean(entry.reconciledAt);
  }

  /** Отметка о том, что операция прошла reconcile и исход подтверждён. */
  function markReconciled(operationId) {
    const entry = get(operationId);
    if (!entry) return null;
    if (entry.outcome === 'outcome_unknown') return entry;
    entry.reconciledAt = entry.reconciledAt || now().toISOString();
    entry.state = 'reconciled';
    persist(entry);
    return entry;
  }

  /**
   * Обратный вызов существующей операции: обновляет состояние операции, не
   * создаёт вторую задачу (AC-253).
   */
  function markCallback(operationId, { eventId = null, providerEventId = null, outcome = null } = {}) {
    const entry = get(operationId);
    if (!entry) return null;
    entry.callback = { eventId, providerEventId, at: now().toISOString() };
    if (outcome) {
      entry.outcome = outcome;
      entry.state = outcome === 'outcome_unknown' ? 'unknown' : 'completed';
    }
    entry.updatedAt = now().toISOString();
    persist(entry);
    log?.write('operation.callback_recorded', {
      operationId: entry.operationId,
      eventId,
      providerEventId,
      profileId: entry.profileId,
      userTaskId: entry.userTaskId,
      runId: entry.runId,
      from: 'callback_received',
      to: 'operation_updated',
      reasonCode: 'CALLBACK_UPDATES_EXISTING_OPERATION',
      detail: 'the callback updates the existing operation; no second task is created',
    });
    return entry;
  }

  return {
    file,
    begin,
    get,
    findByExternalRef,
    recordOutcome,
    reconcile,
    canRetry,
    markReconciled,
    markCallback,
    entries: () => readLines(file),
  };
}

module.exports = { createOperationLedger };
