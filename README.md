# trained-assist-integration-gate

Trained Assist External Integration Gate: единый контракт транспорта и жизненного цикла внешних интеграций.

Статус: карточка P25 (этап I08, эпик E6) — versioned `invoke` / `subscribe` / `reconcile` / `events`, provider emulators, подписанные обратные вызовы, webhook inbox и `operationId` эффекта.

## Что здесь лежит

- **Контракт** (`src/contract/`): версии `gate-contract/v1` и `provider-adapter/v1`, нормализованные исходы и коды (словарь общий с P15), общий event log с вычисткой секретов.
- **Gate** (`src/gate/`): bindings с host-owned резолвером значений, леджер операций (`operationId` пишется до вызова, таймаут = `outcome_unknown` до reconcile), реестр адаптеров (одна реализация на провайдера), webhook inbox (подпись → durable receipt → дедуп → быстрый ACK → async dispatch), исходящий outbox и порт передачи в Input.
- **Адаптер** (`src/adapters/`): единственная реализация — транспорт плюс нормализация исходов. Бизнес-правила здесь не живут.
- **Эмулятор провайдера** (`src/provider-emulator/`): шесть управляемых сбоев (`success`, `error`, `delay`, `auth_expiry`, `duplicate_callback`, `unreachable`), эффект на диске, подписанные обратные вызовы с дубликатами.
- **Фасад** (`src/http/`): versioned HTTP API Gate.
- **Песочница этапа** (`scripts/sandbox/`, `tests/`, `docs/INTEGRATION-GATE.md`): одна команда setup → run → evidence → teardown, воспроизводимый sanitized transcript.

## Запуск

```bash
npm run check                 # синтаксис всех модулей
npm test                      # 19 проверок контракта и приёмки
npm run sandbox               # сценарий этапа I08 → docs/evidence/p25-integration-gate
npm run evidence:verify       # сверка transcript с закоммиченным
```

Песочница изолирована: `.sandbox/p25-<pid>/` на loopback, прод-данные и прод-токены не используются.

## Откуда берётся работа

- Границы и ownership: [ARCHITECTURE §9](https://github.com/trained-assist/trained-agent-architecture/blob/main/ARCHITECTURE.md#9-репозитории-и-ownership), контракт [C10](https://github.com/trained-assist/trained-agent-architecture/blob/main/contracts/README.md#c10--external-integration-gate--provider--task-system).
- Порядок и карточки с приёмкой: [план реализации и интеграции](https://github.com/trained-assist/trained-agent-architecture/blob/main/IMPLEMENTATION-AND-INTEGRATION-PLAN.md) — этап I08, карточка P25.
- Правила разработки и песочниц: [Engineering Approach](https://github.com/trained-assist/trained-agent-architecture/blob/main/ENGINEERING-APPROACH.md), [SANDBOX · I08](https://github.com/trained-assist/trained-agent-architecture/blob/main/SANDBOX.md#i08--external-integration-gate).

Живой сервис этим репозиторием не меняется: реализация идёт параллельно, в собственных sandbox-развёртываниях.

## Контекст репозитория

CI `Repository context` на каждый PR и main собирает карту [REPO-MAP.md](https://github.com/trained-assist/trained-assist-integration-gate/blob/repo-context/REPO-MAP.md) и сжатый пакет (I00/Z03).
