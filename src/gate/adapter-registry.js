'use strict';

// Реестр адаптеров провайдеров Gate.
//
// Инвариант AC-152: у провайдера есть ровно одна реализация адаптера. Вторая
// реализация того же провайдера не регистрируется — реестр отказывает, а не
// молча подменяет первую. Бизнес-правила здесь не живут: адаптер только
// переводит вызов в транспорт провайдера и нормализует ответ.

const { ADAPTER_PROTOCOL_VERSION, SUPPORTED_ADAPTER_PROTOCOLS } = require('../contract/version');

/**
 * @param {object} options
 * @param {object} [options.log] общий event log Gate
 */
function createAdapterRegistry({ log } = {}) {
  const byProvider = new Map();

  /**
   * @param {object} adapter манифест адаптера
   * @param {string} adapter.provider имя провайдера
   * @param {string} adapter.protocolVersion версия протокола адаптера
   * @param {Array<{name: string, kind: 'read'|'mutation', scope: string}>} adapter.capabilities
   */
  function register(adapter) {
    if (!adapter || typeof adapter.provider !== 'string' || adapter.provider.length === 0) {
      throw new Error('adapter manifest requires a provider name');
    }
    if (!SUPPORTED_ADAPTER_PROTOCOLS.includes(adapter.protocolVersion)) {
      const err = new Error(`adapter protocol "${adapter.protocolVersion}" is not supported; expected one of ${SUPPORTED_ADAPTER_PROTOCOLS.join('|')}`);
      err.code = 'ADAPTER_PROTOCOL_UNSUPPORTED';
      throw err;
    }
    if (byProvider.has(adapter.provider)) {
      const err = new Error(`provider "${adapter.provider}" already has an adapter implementation; a second implementation is not allowed`);
      err.code = 'ADAPTER_ALREADY_REGISTERED';
      throw err;
    }
    byProvider.set(adapter.provider, adapter);
    log?.write('adapter.registered', {
      provider: adapter.provider,
      protocolVersion: adapter.protocolVersion,
      capabilities: (adapter.capabilities || []).map(cap => cap.name),
      from: 'registration',
      to: 'registered',
      reasonCode: 'ADAPTER_REGISTERED',
      detail: 'exactly one adapter implementation per provider',
    });
    return adapter;
  }

  function get(provider) {
    return byProvider.get(String(provider)) || null;
  }

  function list() {
    return [...byProvider.keys()];
  }

  /** Структурная проверка AC-152: реализация одна. */
  return { register, get, list, protocolVersion: ADAPTER_PROTOCOL_VERSION };
}

module.exports = { createAdapterRegistry };
