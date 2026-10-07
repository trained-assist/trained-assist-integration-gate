'use strict';

// Входящий inbox обратных вызовов Gate.
//
// Порядок фиксирован контрактом (AC-150, AC-250): проверить подпись → durable
// receipt → дедупликация → быстрый ACK → асинхронный dispatch. Тяжёлые данные
// наружу не отдаются: сырой payload остаётся в приватном ref, а в ответе и в
// логе — только нормализованные идентификаторы.
//
// Дедупликация по двум ключам: providerEventId (тот же вызов пришёл повторно) и
// operationId (тот же эффект пришёл с новым идентификатором вызова). Повтор не
// создаёт второго действия и второго события (AC-253).

const crypto = require('crypto');
const fs = require('fs');
const path = require('path');

const { verify: verifySignature } = require('./webhook-signature');
const { reportError } = require('../contract/events');

const RECEIVED_FILE = 'received.jsonl';
const APPLIED_FILE = 'applied.jsonl';

function appendLine(file, line) {
  fs.mkdirSync(path.dirname(file), { recursive: true, mode: 0o700 });
  fs.appendFileSync(file, `${JSON.stringify(line)}\n`, { mode: 0o600 });
  const handle = fs.openSync(file, 'r+');
  try {
    fs.fsyncSync(handle);
  } finally {
    fs.closeSync(handle);
  }
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

function receiptIdFor(providerEventId) {
  return `inbox_${crypto.createHash('sha256').update(String(providerEventId)).digest('hex').slice(0, 24)}`;
}

/**
 * @param {object} options
 * @param {string} options.root изолированный корень inbox'а
 * @param {object} [options.log] общий event log Gate
 * @param {() => Date} [options.clock]
 * @param {(envelope: object) => Promise<object>} [options.dispatch] асинхронный обработчик
 * @param {(request: {provider: string, subscriptionId: string|null}) => string|undefined} [options.secretResolver]
 * @param {number} [options.maxAgeMs]
 */
function createWebhookInbox({ root, log, clock = () => new Date(), dispatch, secretResolver, maxAgeMs } = {}) {
  if (!root) throw new Error('webhook inbox requires an isolated root');
  const receivedFile = path.join(root, RECEIVED_FILE);
  const appliedFile = path.join(root, APPLIED_FILE);
  const seenProviderEvent = new Set(readLines(appliedFile).map(entry => entry.providerEventId));
  const seenOperation = new Set(readLines(appliedFile).map(entry => entry.operationId).filter(Boolean));
  const pending = new Set();

  function keyFor(envelope) {
    return `${envelope.providerEventId}::${envelope.operationId || '*'}`;
  }

  /**
   * Приём обратного вызова. Возвращает ACK сразу: dispatch выполняется после
   * подтверждения, а не до него.
   */
  function receive({ headers = {}, rawBody = '', provider = null, secret } = {}) {
    const signature = headers['x-gate-signature'] || headers['X-Gate-Signature'] || null;
    const timestamp = headers['x-gate-timestamp'] || headers['X-Gate-Timestamp'] || null;
    const delivery = headers['x-gate-delivery'] || headers['X-Gate-Delivery'] || null;
    const envelope = parseEnvelope(rawBody);

    const verification = verifySignature({
      secret: secret || (secretResolver ? secretResolver({ provider, subscriptionId: envelope.subscriptionId || null }) : undefined),
      signature,
      timestamp,
      rawBody,
      now: clock,
      ...(maxAgeMs ? { maxAgeMs } : {}),
    });

    const receipt = {
      receiptId: receiptIdFor(envelope.providerEventId || delivery || `unknown-${seenProviderEvent.size}`),
      providerEventId: envelope.providerEventId || delivery || null,
      subscriptionId: envelope.subscriptionId || null,
      operationId: envelope.operationId || null,
      signatureValid: verification.ok,
      receivedAt: clock().toISOString(),
    };

    // Durable receipt пишется до ACK: подтверждение означает, что вызов принят
    // и не потеряется, даже если dispatch ещё не выполнился.
    appendLine(receivedFile, receipt);

    if (!verification.ok) {
      log?.write('callback.rejected', {
        providerEventId: receipt.providerEventId,
        subscriptionId: receipt.subscriptionId,
        operationId: receipt.operationId,
        from: 'received',
        to: 'rejected',
        reasonCode: verification.reasonCode,
        detail: verification.detail,
      });
      return { status: 401, receipt, applied: false, duplicate: false, reasonCode: verification.reasonCode, detail: verification.detail, dispatched: false };
    }

    const duplicate = seenProviderEvent.has(receipt.providerEventId) || Boolean(envelope.operationId && seenOperation.has(envelope.operationId));
    if (duplicate) {
      log?.write('callback.duplicate_ignored', {
        providerEventId: receipt.providerEventId,
        subscriptionId: receipt.subscriptionId,
        operationId: receipt.operationId,
        from: 'received',
        to: 'ignored',
        reasonCode: 'DUPLICATE_CALLBACK_IGNORED',
        detail: 'the effect of this operation was already applied; no second action, no second event',
      });
      return { status: 202, receipt, applied: false, duplicate: true, reasonCode: 'DUPLICATE_CALLBACK_IGNORED', detail: null, dispatched: false };
    }

    seenProviderEvent.add(receipt.providerEventId);
    if (envelope.operationId) seenOperation.add(envelope.operationId);
    appendLine(appliedFile, { key: keyFor(envelope), providerEventId: receipt.providerEventId, operationId: envelope.operationId || null, at: clock().toISOString() });

    log?.write('callback.accepted', {
      providerEventId: receipt.providerEventId,
      subscriptionId: receipt.subscriptionId,
      operationId: receipt.operationId,
      from: 'received',
      to: 'accepted',
      reasonCode: 'CALLBACK_ACCEPTED',
      detail: 'durable receipt written; dispatch runs after the ACK',
    });

    let dispatched = false;
    if (dispatch) {
      const work = Promise.resolve()
        .then(() => dispatch(envelope))
        .then(result => {
          log?.write('callback.dispatched', {
            providerEventId: receipt.providerEventId,
            subscriptionId: receipt.subscriptionId,
            operationId: receipt.operationId,
            from: 'accepted',
            to: 'dispatched',
            reasonCode: 'CALLBACK_DISPATCHED',
            detail: result && result.detail ? result.detail : 'normalized event published after the ACK',
          });
          return result;
        })
        .catch(error => {
          const detail = String(error && error.message ? error.message : error);
          log?.write('callback.dispatch_failed', {
            providerEventId: receipt.providerEventId,
            subscriptionId: receipt.subscriptionId,
            operationId: receipt.operationId,
            from: 'accepted',
            to: 'dispatch_failed',
            reasonCode: 'CALLBACK_DISPATCH_FAILED',
            detail,
          });
          reportError(log, {
            code: 'CALLBACK_DISPATCH_FAILED',
            operation: 'dispatchCallback',
            detail,
            profileId: envelope.profileId || null,
            userTaskId: envelope.userTaskId || null,
            runId: envelope.runId || null,
            replyContext: envelope.replyContext || null,
          });
        })
        .finally(() => pending.delete(work));
      pending.add(work);
      dispatched = true;
    }

    return { status: 202, receipt, applied: true, duplicate: false, reasonCode: 'CALLBACK_ACCEPTED', detail: null, dispatched };
  }

  function parseEnvelope(rawBody) {
    if (!rawBody || typeof rawBody !== 'string') return {};
    try {
      return JSON.parse(rawBody);
    } catch {
      return {};
    }
  }

  return {
    root,
    receive,
    drain: () => Promise.all([...pending]),
    receivedCount: () => readLines(receivedFile).length,
    appliedCount: () => readLines(appliedFile).length,
    receivedEntries: () => readLines(receivedFile),
    appliedEntries: () => readLines(appliedFile),
  };
}

module.exports = { createWebhookInbox, RECEIVED_FILE, APPLIED_FILE };
