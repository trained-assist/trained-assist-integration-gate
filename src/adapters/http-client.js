'use strict';

// HTTP-транспорт адаптеров Gate: единственная реализация для всех провайдеров.
//
// Инвариант AC-152 касается не только адаптеров, но и транспорта: у Gate один
// HTTP-клиент, а не копия на каждый провайдера. Адаптер отвечает только за
// адрес, заголовки и нормализацию ответа.
//
// Различие таймаута и недоступности различается здесь и возвращается полем
// `transportError`, потому что решение о нём принадлежит адаптеру: для мутации
// таймаут после отправки — это `outcome_unknown`, для чтения (GET) — обычный
// `failed`, который безопасно повторить.

const DEFAULT_TIMEOUT_MS = 120;

function buildUrl(baseUrl, pathname, query) {
  const url = new URL(pathname, baseUrl);
  for (const [key, value] of Object.entries(query || {})) {
    if (value === undefined || value === null) continue;
    url.searchParams.append(key, String(value));
  }
  return url;
}

function nonJsonBody() {
  return { status: 'error', code: 'PROVIDER_ERROR', detail: 'provider returned a non-JSON body' };
}

/**
 * Один JSON-запрос к провайдеру.
 *
 * @param {object} options
 * @param {string} options.baseUrl база API провайдера
 * @param {string} [options.method]
 * @param {string} options.pathname путь без query
 * @param {object} [options.query] параметры query
 * @param {object} [options.body] тело запроса (для POST/PUT)
 * @param {object} [options.headers] заголовки; вызывающий добавляет авторизацию
 * @param {number} [options.timeoutMs] дедлайн клиента
 * @param {Function} [options.fetchImpl] подмена HTTP-клиента (тесты)
 * @returns {Promise<{status: number, body: object|null, transportError: null|'timeout'|'unreachable'}>}
 */
async function requestJson({ baseUrl, method = 'GET', pathname, query = null, body = null, headers = {}, timeoutMs = DEFAULT_TIMEOUT_MS, fetchImpl } = {}) {
  if (!baseUrl) throw new Error('provider request requires a provider baseUrl');
  const doFetch = fetchImpl || fetch;
  const url = buildUrl(baseUrl, pathname, query);
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);

  const init = { method, headers, signal: controller.signal };
  if (body !== null && body !== undefined) {
    init.body = JSON.stringify(body);
    init.headers = { 'content-type': 'application/json', ...headers };
  }

  try {
    const response = await doFetch(url.toString(), init);
    const text = await response.text();
    let parsed;
    try {
      parsed = JSON.parse(text);
    } catch {
      parsed = nonJsonBody();
    }
    return { status: response.status, body: parsed, transportError: null };
  } catch (error) {
    const timedOut = Boolean(error && (error.name === 'AbortError' || controller.signal.aborted));
    return { status: 0, body: null, transportError: timedOut ? 'timeout' : 'unreachable', error };
  } finally {
    clearTimeout(timer);
  }
}

module.exports = { requestJson, DEFAULT_TIMEOUT_MS };