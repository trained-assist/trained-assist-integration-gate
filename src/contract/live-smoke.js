'use strict';

// Заявка на живой smoke: одна реализация на репозиторий (карточка P26).
//
// Пилот не может объявить прод-настройку включённой, пока нет решения владельца о
// read-only test-account режиме (AC-30). Здесь фиксируется, что именно нужно для
// живой проверки и почему она не выполнена: имён переменных достаточно, значения
// секретов в репозитории не бывает.

/**
 * @param {object} options
 * @param {string[]} options.requiredBindings имена переменных, без значений
 * @param {string} options.blockedByWhenPresent причина, если binding'ы есть, а решения владельца нет
 * @param {string} options.nextBlocked что делать, когда binding'ов нет
 * @param {string} options.nextDecision что делать, когда binding'ы есть
 */
function requestLiveSmoke({ requiredBindings, blockedByWhenPresent, nextBlocked, nextDecision } = {}) {
  return function request({ bindingNames = [] } = {}) {
    const missing = (requiredBindings || []).filter(name => !bindingNames.includes(name));
    if (missing.length > 0) {
      return { attempted: true, performed: false, blockedBy: 'NO_TEST_ACCOUNT_BINDING', missingBindings: missing, next: nextBlocked };
    }
    return { attempted: true, performed: false, blockedBy: blockedByWhenPresent || 'OWNER_DECISION_REQUIRED', missingBindings: [], next: nextDecision };
  };
}

module.exports = { requestLiveSmoke };