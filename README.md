# trained-assist-integration-gate

Trained Assist External Integration Gate: единый контракт транспорта и жизненного цикла внешних интеграций.

Статус: карточка P26 (этап I08, эпик E6) — реалистичный адаптер HH и существующий cron холодного поиска, смапленный на срабатывания расписания. Карточка P25 — versioned `invoke` / `subscribe` / `reconcile` / `events`, provider emulators, подписанные обратные вызовы, webhook inbox и `operationId` эффекта.

## Что здесь лежит

- **Контракт** (`src/contract/`): версии `gate-contract/v1` и `provider-adapter/v1`, нормализованные исходы и коды (словарь общий с P15), общий event log с вычисткой секретов, заявка на живой smoke.
- **Gate** (`src/gate/`): bindings с host-owned резолвером значений, леджер операций (`operationId` пишется до вызова, таймаут = `outcome_unknown` до reconcile), реестр адаптеров (одна реализация на провайдера), webhook inbox (подпись → durable receipt → дедуп → быстрый ACK → async dispatch), исходящий outbox и порт передачи в Input.
- **Адаптеры** (`src/adapters/`): транспорт один (`http-client.js`, общий для всех провайдеров) — `hh-sandbox-adapter.js` (эмулятор P25) и `hh-adapter.js` (реалистичный read-only HH: `GET /resumes`, выдача, ограниченные повторы, одна попытка обновить выдачу). Бизнес-правила здесь не живут.
- **Эмуляторы провайдеров** (`src/provider-emulator/`): эмулятор P25 с шестью управляемыми сбоями и эмулятор HH в форме настоящего API (`/resumes`, `/token`) с очищенным сэмплом.
- **Пилот P26** (`src/pilot/cold-search.js`): существующий cron холодного поиска → запрос расписания P22 и исполнение occurrence через общий task flow. Почасовая поддержка не выдаётся за production enabled; webhook и поток событий HH не обещаны.
- **Фасад** (`src/http/`): versioned HTTP API Gate, включая маршруты пилота.
- **Песочницы этапа** (`scripts/sandbox/`, `tests/`, `docs/INTEGRATION-GATE.md`): одна команда setup → run → evidence → teardown, воспроизводимый sanitized transcript.

## Запуск

```bash
npm run check                 # синтаксис всех модулей
npm test                      # 38 проверок контракта и приёмки (P25 + P26)
npm run sandbox               # сценарий P25 → docs/evidence/p25-integration-gate
npm run sandbox:p26           # сценарий P26 → docs/evidence/p26-hh-pilot
npm run evidence:verify       # сверка transcript P25 с закоммиченным
npm run evidence:verify:p26   # сверка transcript P26 с закоммиченным
```

Песочницы изолированы: `.sandbox/p25-<pid>/` и `.sandbox/p26-<pid>/` на loopback, прод-данные и прод-токены не используются.

## Откуда берётся работа

- Границы и ownership: [ARCHITECTURE §9](https://github.com/trained-assist/trained-agent-architecture/blob/main/ARCHITECTURE.md#9-репозитории-и-ownership), контракт [C10](https://github.com/trained-assist/trained-agent-architecture/blob/main/contracts/README.md#c10--external-integration-gate--provider--task-system).
- Порядок и карточки с приёмкой: [план реализации и интеграции](https://github.com/trained-assist/trained-agent-architecture/blob/main/IMPLEMENTATION-AND-INTEGRATION-PLAN.md) — этап I08, карточки P25 и P26.
- Правила разработки и песочниц: [Engineering Approach](https://github.com/trained-assist/trained-agent-architecture/blob/main/ENGINEERING-APPROACH.md), [SANDBOX · I08](https://github.com/trained-assist/trained-agent-architecture/blob/main/SANDBOX.md#i08--external-integration-gate).

Живой сервис этим репозиторием не меняется: реализация идёт параллельно, в собственных sandbox-развёртываниях.

## Контекст репозитория

CI `Repository context` на каждый PR и main собирает карту [REPO-MAP.md](https://github.com/trained-assist/trained-assist-integration-gate/blob/repo-context/REPO-MAP.md) и сжатый пакет (I00/Z03).
