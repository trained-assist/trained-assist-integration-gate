'use strict';

// Общий event log Gate: второй формат лога этапа не заводим.
//
// Формат записи повторяет P15 (software-engineering-playbooks, PR #103):
// `log.write(event, { from, to, reasonCode, detail, ...ids })`. Здесь добавлено
// только то, что требует observability-контракт (C12): source/release/
// environment, scope, correlation, replyContext, safeSummary/privateDetailsRef и
// класс хранения (TTL). Значения секретов и сырой payload провайдера в запись
// не попадают: сырые данные уходят только в privateDetailsRef.

const crypto = require('crypto');
const fs = require('fs');
const path = require('path');

const SOURCE = {
  service: 'integration-gate',
  release: 'sandbox',
  environment: 'sandbox',
};

// Ключи, которые никогда не пишутся в общий лог (INV-12).
const SENSITIVE_KEYS = [
  'token',
  'secret',
  'password',
  'authorization',
  'credential',
  'bindingValue',
  'rawPayload',
  'raw',
  'payload',
  'privateDetails',
];

const MAX_SAFE_SUMMARY_LENGTH = 240;

function isSensitiveKey(key) {
  const normalized = String(key).toLowerCase();
  return SENSITIVE_KEYS.some(part => normalized.includes(part));
}

/** Вычищает чувствительные ключи из полей события. */
function sanitizeFields(fields) {
  if (!fields || typeof fields !== 'object') return {};
  const out = {};
  for (const [key, value] of Object.entries(fields)) {
    if (isSensitiveKey(key)) continue;
    if (value && typeof value === 'object' && !Array.isArray(value)) {
      out[key] = sanitizeFields(value);
      continue;
    }
    out[key] = value;
  }
  return out;
}

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
 * @param {string} options.file изолированный файл лога (production-логи неприкосновенны)
 * @param {() => Date} [options.now]
 * @param {(entry: object) => void} [options.sink] дополнительный durable sink
 * @param {{ publishError: (event: object) => Promise<object> }} [options.errorPublisher] публикация ErrorEvent в Error Watcher (C12)
 */
function createEventLog({ file, now = () => new Date(), sink, errorPublisher = null } = {}) {
  if (!file) throw new Error('event log requires an isolated file');

  function write(event, fields = {}) {
    const entry = {
      schemaVersion: 1,
      eventId: fields.eventId || null,
      event,
      occurredAt: now().toISOString(),
      source: SOURCE,
      ...sanitizeFields(fields),
    };
    delete entry.eventId;
    if (fields.eventId) entry.eventId = fields.eventId;
    // correlation заполнен у каждого события: известные идентификаторы
    // операции, отсутствующие — явный null (C12).
    entry.correlation = {
      userTaskId: fields.userTaskId ?? null,
      runId: fields.runId ?? null,
      traceId: fields.traceId ?? null,
      ...(fields.correlation || {}),
    };
    appendLine(file, entry);
    if (sink) sink(entry);
    return entry;
  }

  function error({ code, operation, severity = 'error', retryable = false, outcome, safeSummary, privateDetailsRef = null, scope = {}, correlation = null, replyContext = null, origin = { kind: 'application', incidentId: null, diagnosticDepth: 0 } }) {
    const entry = write('error', {
      eventId: `err_${crypto.randomBytes(12).toString('hex')}`,
      scope,
      correlation,
      replyContext,
      error: { code, operation, severity, retryable, outcome, safeSummary, privateDetailsRef },
      origin,
    });
    if (errorPublisher && typeof errorPublisher.publishError === 'function') {
      void errorPublisher.publishError(entry);
    }
    return entry;
  }

  return {
    file,
    write,
    error,
    entries: () => readLines(file),
    close: () => undefined,
  };
}

function replyContextOf(replyContext) {
  return {
    channel: replyContext && replyContext.channel ? replyContext.channel : null,
    destinationRef: replyContext && replyContext.destinationRef ? replyContext.destinationRef : null,
    status: 'not_applicable',
  };
}

/**
 * Вызов `log.error()` на сайте сбоев: собирает C12-поля (scope, correlation,
 * replyContext, origin) так, чтобы сайт не знал форму ErrorEvent.
 */
function reportError(log, { code, operation, detail = null, profileId = null, userTaskId = null, runId = null, traceId = null, replyContext = null } = {}) {
  if (!log || typeof log.error !== 'function') return null;
  const summary = detail === null || detail === undefined ? '' : String(detail).slice(0, MAX_SAFE_SUMMARY_LENGTH);
  return log.error({
    code,
    operation,
    severity: 'error',
    retryable: true,
    outcome: 'failed',
    safeSummary: summary || String(code),
    scope: {
      kind: profileId ? 'profile' : 'platform',
      tenantId: null,
      profileId: profileId || null,
    },
    correlation: {
      userTaskId: userTaskId || null,
      runId: runId || null,
      traceId: traceId || null,
    },
    replyContext: replyContextOf(replyContext),
    origin: { kind: 'application', incidentId: null, diagnosticDepth: 0 },
  });
}

module.exports = {
  createEventLog,
  reportError,
  sanitizeFields,
  SENSITIVE_KEYS,
  SOURCE,
};
