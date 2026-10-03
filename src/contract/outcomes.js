'use strict';

// Нормализованные исходы и коды ошибок External Integration Gate.
//
// Словарь один и общий с P15: исходы внешнего сервиса и коды эффекта не
// переизобретаются. Добавлены только коды, которые принадлежат самому Gate
// (binding, подпись обратного вызова, повтор до reconcile, реестр адаптеров).
//
// Исходы (C10): accepted / result / failed / outcome_unknown / blocked.
// `outcome_unknown` — таймаут после отправки: эффект мог произойти, поэтому
// повтор запрещён до reconcile (INV-07, AC-151).

const OUTCOMES = ['accepted', 'result', 'failed', 'outcome_unknown', 'blocked'];

// Коды внешнего сервиса — из P15 (src/mcp-sandbox/errors.js), без изменений.
const PROVIDER_CODES = ['PROVIDER_ERROR', 'PROVIDER_NOT_FOUND', 'PROVIDER_STATE_UNREADABLE', 'PROVIDER_UNREACHABLE', 'PROVIDER_PAYLOAD_CONFLICT'];

// Коды эффекта и обратного вызова — из P15.
const EFFECT_CODES = ['EFFECT_STATE_UNKNOWN'];
const CALLBACK_CODES = [
  'CALLBACK_APPLIED',
  'CALLBACK_REJECTED',
  'CALLBACK_SIGNATURE_INVALID',
  'CALLBACK_REPLAY_WINDOW',
  'DUPLICATE_CALLBACK_IGNORED',
];

// Коды, принадлежащие Gate.
const GATE_CODES = [
  'BINDING_NOT_FOUND',
  'BINDING_SCOPE_MISMATCH',
  'BINDING_EXPIRED',
  'RETRY_BEFORE_RECONCILE',
  'OPERATION_NOT_FOUND',
  'POLL_CURSOR_INVALID',
  'ADAPTER_PROTOCOL_UNSUPPORTED',
  'ADAPTER_ALREADY_REGISTERED',
  'WEBHOOK_NOT_SUPPORTED',
];

// Истёкшая выдача намеренно НЕ код отказа для модели: это `blocked` с
// человеческим текстом (решение P15). Код остаётся только в логе и в
// readiness, чтобы оператор видел причину без машинного шума.
const AUTH_CODES = ['PROVIDER_AUTH_EXPIRED'];

const CODES = [...PROVIDER_CODES, ...EFFECT_CODES, ...CALLBACK_CODES, ...GATE_CODES, ...AUTH_CODES];

/**
 * Человекочитаемая причина для `blocked` (P15: истёкшая выдача — это
 * «подключите/обновите», а не машинный код для модели).
 */
function blockedSummary(reason) {
  if (reason === 'expired') return 'Подключение к внешнему сервису истекло: обновите доступ в настройках интеграции';
  return 'Операция не выполнена: доступ к внешнему сервису ограничен';
}

module.exports = {
  OUTCOMES,
  PROVIDER_CODES,
  EFFECT_CODES,
  CALLBACK_CODES,
  GATE_CODES,
  AUTH_CODES,
  CODES,
  blockedSummary,
};
