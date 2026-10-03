'use strict';

// Подпись входящих обратных вызовов провайдера.
//
// Схема: HMAC-SHA256 по строке `${timestamp}.${rawBody}`, заголовки
// `x-gate-signature: v1=<hex>`, `x-gate-timestamp`, `x-gate-delivery`. Секрет
// берётся из host-owned store по подписке — он не приходит от вызывающего и не
// пишется в лог. Сравнение в константном времени; окно допустимого смещения
// часов ограничено, иначе перехваченный вызов можно проиграть повторно.

const crypto = require('crypto');

const DEFAULT_MAX_AGE_MS = 5 * 60 * 1000;

function sign({ secret, timestamp, rawBody }) {
  return `v1=${crypto.createHmac('sha256', secret).update(`${timestamp}.${rawBody}`).digest('hex')}`;
}

function safeEqual(a, b) {
  const left = Buffer.from(String(a), 'utf8');
  const right = Buffer.from(String(b), 'utf8');
  if (left.length !== right.length) return false;
  return crypto.timingSafeEqual(left, right);
}

/**
 * @param {object} params
 * @param {string} params.secret секрет подписи подписки (host-owned)
 * @param {string} params.signature значение заголовка x-gate-signature
 * @param {string} params.timestamp значение заголовка x-gate-timestamp
 * @param {string} params.rawBody сырое тело запроса
 * @param {() => Date} [params.now]
 * @param {number} [params.maxAgeMs]
 * @returns {{ok: boolean, reasonCode: string|null, detail: string|null}}
 */
function verify({ secret, signature, timestamp, rawBody, now = () => new Date(), maxAgeMs = DEFAULT_MAX_AGE_MS }) {
  if (!secret) return { ok: false, reasonCode: 'CALLBACK_SIGNATURE_INVALID', detail: 'no signing secret is bound to this subscription' };
  if (!signature || !timestamp) return { ok: false, reasonCode: 'CALLBACK_SIGNATURE_INVALID', detail: 'callback carries no signature headers' };

  const expected = sign({ secret, timestamp, rawBody });
  if (!safeEqual(expected, signature)) {
    return { ok: false, reasonCode: 'CALLBACK_SIGNATURE_INVALID', detail: 'callback signature does not match the bound signing secret' };
  }

  const at = Number(timestamp);
  if (!Number.isFinite(at)) return { ok: false, reasonCode: 'CALLBACK_SIGNATURE_INVALID', detail: 'callback timestamp is not a number' };
  const age = now().getTime() - at;
  if (age > maxAgeMs || age < -maxAgeMs) {
    return { ok: false, reasonCode: 'CALLBACK_REPLAY_WINDOW', detail: 'callback timestamp is outside the accepted window' };
  }
  return { ok: true, reasonCode: null, detail: null };
}

module.exports = { sign, verify, DEFAULT_MAX_AGE_MS };
