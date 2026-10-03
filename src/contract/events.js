'use strict';

// Общий event log Gate: второй формат лога этапа не заводим.
//
// Формат записи повторяет P15 (software-engineering-playbooks, PR #103):
// `log.write(event, { from, to, reasonCode, detail, ...ids })`. Здесь добавлено
// только то, что требует observability-контракт (C12): source/release/
// environment, scope, correlation, replyContext, safeSummary/privateDetailsRef и
// класс хранения (TTL). Значения секретов и сырой payload провайдера в запись
// не попадают: сырые данные уходят только в privateDetailsRef.

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
 */
function createEventLog({ file, now = () => new Date(), sink } = {}) {
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
    appendLine(file, entry);
    if (sink) sink(entry);
    return entry;
  }

  function error({ code, operation, severity = 'error', retryable = false, outcome, safeSummary, privateDetailsRef = null, scope = {}, correlation = {}, replyContext = null, origin = { kind: 'application', incidentId: null, diagnosticDepth: 0 } }) {
    return write('error', {
      scope,
      correlation,
      replyContext,
      error: { code, operation, severity, retryable, outcome, safeSummary, privateDetailsRef },
      origin,
    });
  }

  return {
    file,
    write,
    error,
    entries: () => readLines(file),
    close: () => undefined,
  };
}

module.exports = {
  createEventLog,
  sanitizeFields,
  SENSITIVE_KEYS,
  SOURCE,
};
