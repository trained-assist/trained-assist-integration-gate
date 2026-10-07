'use strict';

// Публикация ErrorEvent (C12) в System Error Watcher: fire-and-forget POST на
// POST /errors с ключом и правом `error:write`.
//
// Публикация не бросает и не блокирует пользовательский путь: сбой доставки
// уходит в ограниченный in-memory spool, dropped count остаётся health
// сигналом (OBSERVABILITY-AND-ERROR-CONTRACT: «Сбой log delivery: bounded
// retry и локальный spool, overflow/dropped-count виден health signal»).

const SPOOL_LIMIT = 100;
const REQUEST_TIMEOUT_MS = 5000;

/**
 * @param {object} options
 * @param {string} options.watcherUrl базовый URL Error Watcher
 * @param {string} options.watcherKey ключ источника (заголовок x-watcher-key)
 * @param {string} [options.environment] окружение, которым publisher штампует source
 */
function createErrorPublisher({ watcherUrl, watcherKey, environment = 'sandbox' } = {}) {
  let dropped = 0;
  const spool = [];

  function keep(event) {
    spool.push(event);
    while (spool.length > SPOOL_LIMIT) spool.shift();
  }

  async function publishError(event) {
    const body = event && typeof event === 'object' ? { ...event } : {};
    delete body.event;
    const payload = {
      ...body,
      source: {
        service: 'integration-gate',
        ...(body.source || {}),
        environment,
      },
    };
    try {
      if (!watcherUrl) throw new Error('error watcher URL is not configured');
      const response = await fetch(`${String(watcherUrl).replace(/\/+$/, '')}/errors`, {
        method: 'POST',
        headers: {
          'content-type': 'application/json',
          'x-watcher-key': watcherKey,
          'x-watcher-scopes': 'error:write',
        },
        body: JSON.stringify(payload),
        signal: AbortSignal.timeout(REQUEST_TIMEOUT_MS),
      });
      if (!response.ok) throw new Error(`error watcher answered ${response.status}`);
      return { published: true };
    } catch {
      dropped += 1;
      keep(payload);
      return { published: false };
    }
  }

  return {
    publishError,
    getDroppedCount: () => dropped,
    getSpool: () => [...spool],
  };
}

function resolveErrorPublisher(env = process.env) {
  const watcherUrl = env && env.ERROR_WATCHER_URL;
  const watcherKey = env && env.ERROR_WATCHER_KEY;
  if (!watcherUrl || !watcherKey) return null;
  return createErrorPublisher({
    watcherUrl,
    watcherKey,
    environment: (env && env.NODE_ENV) || 'sandbox',
  });
}

module.exports = { createErrorPublisher, resolveErrorPublisher };
