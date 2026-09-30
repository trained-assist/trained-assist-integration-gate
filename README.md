# trained-assist-integration-gate

Trained Assist External Integration Gate: provider adapters, webhooks, bindings, inbox

Статус: создан 30.09.2026, кода пока нет.

## Что здесь будет

Адаптеры внешних провайдеров и webhook lifecycle, bindings, входящий inbox, квитанции операций; versioned invoke/subscribe/reconcile/events.

## Откуда берётся работа

- Границы и ownership: [ARCHITECTURE §9](https://github.com/trained-assist/trained-agent-architecture/blob/main/ARCHITECTURE.md#9-репозитории-и-ownership).
- Порядок и карточки с приёмкой: [план реализации и интеграции](https://github.com/trained-assist/trained-agent-architecture/blob/main/IMPLEMENTATION-AND-INTEGRATION-PLAN.md) — этап I08, карточки P25–P26.
- Правила разработки и песочниц: [Engineering Approach](https://github.com/trained-assist/trained-agent-architecture/blob/main/ENGINEERING-APPROACH.md), [Sandbox Plan](https://github.com/trained-assist/trained-agent-architecture/blob/main/SANDBOX-PLAN.md).

Живой сервис этим репозиторием не меняется: реализация идёт параллельно, в собственных sandbox-развёртываниях.

## Контекст репозитория

CI `Repository context` на каждый PR и main собирает карту [REPO-MAP.md](https://github.com/trained-assist/trained-assist-integration-gate/blob/repo-context/REPO-MAP.md) и сжатый пакет (I00/Z03).
