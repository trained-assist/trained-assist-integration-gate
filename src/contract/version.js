'use strict';

// Версии контракта External Integration Gate (карточка P25, эпик E6 #22, этап I08).
//
// Второго словаря версий не заводим: имена и смысл исходов повторяют P15
// (software-engineering-playbooks, PR #103) и контракт C10
// (trained-agent-architecture, contracts/README.md). Здесь только то, что
// принадлежит выделяемому Gate: версия контракта операций и версия протокола
// адаптера провайдера.

const GATE_CONTRACT_VERSION = 'gate-contract/v1';
const ADAPTER_PROTOCOL_VERSION = 'provider-adapter/v1';
const WEBHOOK_SIGNATURE_VERSION = 'v1';

// Поддерживаемые версии протокола адаптера. Реестр отказывает адаптеру, чья
// версия не входит в список: две несовместимые реализации одного провайдера
// не могут сосуществовать (AC-152).
const SUPPORTED_ADAPTER_PROTOCOLS = [ADAPTER_PROTOCOL_VERSION];

module.exports = {
  GATE_CONTRACT_VERSION,
  ADAPTER_PROTOCOL_VERSION,
  WEBHOOK_SIGNATURE_VERSION,
  SUPPORTED_ADAPTER_PROTOCOLS,
};
