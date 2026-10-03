'use strict';

// Исходящий outbox нормализованных событий Gate.
//
// Событие публикуется в durable outbox, откуда его читают Input и Watcher
// (C12). Дедупликация по eventId: повторная публикация того же события не
// создаёт второго. Gate не создаёт GTD по факту внешнего события.

const crypto = require('crypto');
const fs = require('fs');
const path = require('path');

const OUTBOX_FILE = 'events.jsonl';

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

function eventIdFor(provider, providerEventId) {
  return `evt_${crypto.createHash('sha256').update(`${provider}|${providerEventId}`).digest('hex').slice(0, 24)}`;
}

/**
 * @param {object} options
 * @param {string} options.root изолированный корень outbox'а
 * @param {() => Date} [options.now]
 * @param {object} [options.log] общий event log Gate
 */
function createEventOutbox({ root, now = () => new Date(), log } = {}) {
  if (!root) throw new Error('event outbox requires an isolated root');
  const file = path.join(root, OUTBOX_FILE);
  const seen = new Set(readLines(file).map(entry => entry.eventId));

  /**
   * @param {object} event нормализованное событие
   * @param {string} event.providerEventId идентификатор события у провайдера
   * @param {string} event.kind тип события
   * @param {object} [event.operation] корреляция с исходящей операцией
   */
  function publish({ provider, providerEventId, kind, payload = null, operation = null, profileId = null, userTaskId = null, runId = null, replyContext = null, gtdId = null }) {
    const eventId = eventIdFor(provider, providerEventId);
    if (seen.has(eventId)) {
      log?.write('event.duplicate_ignored', {
        eventId,
        providerEventId,
        kind,
        from: 'received',
        to: 'ignored',
        reasonCode: 'DUPLICATE_EVENT_IGNORED',
        detail: 'this provider event was already published; no second event is created',
      });
      return { published: false, duplicate: true, eventId };
    }
    seen.add(eventId);
    const entry = {
      schemaVersion: 1,
      eventId,
      provider,
      providerEventId,
      kind,
      payload,
      operation,
      profileId,
      userTaskId,
      runId,
      gtdId,
      replyContext,
      occurredAt: now().toISOString(),
      createsGtd: false,
    };
    appendLine(file, entry);
    log?.write('event.published', {
      eventId,
      providerEventId,
      kind,
      profileId,
      userTaskId,
      runId,
      operationId: operation && operation.operationId ? operation.operationId : null,
      from: 'received',
      to: 'published',
      reasonCode: 'EVENT_PUBLISHED',
      detail: 'normalized event is durable in the outbox; readers dedup by eventId',
    });
    return { published: true, duplicate: false, eventId, entry };
  }

  return {
    file,
    publish,
    entries: () => readLines(file),
    count: () => readLines(file).length,
  };
}

module.exports = { createEventOutbox };
