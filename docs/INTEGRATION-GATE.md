# External Integration Gate (P25, этап I08)

Карточка [#64](https://github.com/trained-assist/trained-agent-architecture/issues/64), эпик [#22](https://github.com/trained-assist/trained-agent-architecture/issues/22), приёмка **AC-150/151/152**, логи этапа **AC-154**.
Документ описывает долговременную архитектуру Gate; статус и чек-лист приёмки — в issue.

Контракт не новый: он продолжает C10 (`trained-agent-architecture`, `contracts/README.md`) и словарь исходов P15 ([software-engineering-playbooks, PR #103](https://github.com/trained-assist/software-engineering-playbooks/pull/103)). Второго словаря исходов, кодов и транспортов здесь не заводится.

## Топология

```
        версия контракта gate-contract/v1
                 │
 задача ─────────┤  POST /v1/invoke        (read / mutation)
                 │  POST /v1/subscribe     (webhookSubscriptionId + lifecycle)
                 │  POST /v1/reconcile     (operationId / externalOperationRef)
                 │  GET  /v1/events        (poll, cursor)
                 │  GET  /v1/capabilities  (readiness, probe)
                 │
                 ▼
        External Integration Gate  ← единственная реализация адаптера на провайдера
                 │  bindings (host-owned резолвер значений)
                 │  operation ledger (operationId пишется ДО вызова)
                 │  webhook inbox (подпись → receipt → dedup → ACK → dispatch)
                 │  event outbox (дедуп по eventId)
                 │  task port → Input (только по явной binding policy)
                 │
                 ▼  ровно один адаптер, один транспорт (HTTP)
        провайдер (в песочнице — эмулятор, в бою — настоящий API)
                 │
                 ▼  подписанные обратные вызовы (HMAC, с дубликатами)
```

Ключевое свойство, ради которого карточка и существует: **фасад Gate и адаптер не дублируют друг друга, а бизнес-правила не живут в Gate вовсе**. Ответ домена не содержит транспортных полей, поэтому «одинаковый исход по маршрутам» — это сравнение нормализованных исходов, а не похожих описаний.

## Модули

| Модуль | Роль |
|---|---|
| `src/contract/version.js` | версии контракта: `gate-contract/v1`, `provider-adapter/v1`, подпись `v1` |
| `src/contract/outcomes.js` | нормализованные исходы (`accepted`/`result`/`failed`/`outcome_unknown`/`blocked`) и коды; словарь общий с P15 |
| `src/contract/events.js` | общий event log: scope, correlation, `safeSummary`/`privateDetailsRef`, класс хранения; вычистка секретов |
| `src/gate/bindings.js` | credential bindings и host-owned резолвер значений (граница Credential Broker) |
| `src/gate/operation-ledger.js` | operationId до вызова; `outcome_unknown`; reconcile; запрет повтора до reconcile |
| `src/gate/adapter-registry.js` | реестр адаптеров: одна реализация на провайдера, версия протокола |
| `src/gate/webhook-signature.js` | HMAC-SHA256 по `${timestamp}.${rawBody}`, константное сравнение, окно повтора |
| `src/gate/webhook-inbox.js` | приём обратных вызовов: подпись → durable receipt → dedup → быстрый ACK → dispatch |
| `src/gate/event-outbox.js` | исходящий outbox нормализованных событий, дедуп по eventId |
| `src/gate/task-port.js` | передача события в Input; GTD не создаётся никогда |
| `src/gate/subscription-store.js` | `webhookSubscriptionId` → секрет подписи (host-owned) |
| `src/gate/index.js` | ядро: invoke / subscribe / reconcile / poll / capabilities / receiveCallback |
| `src/adapters/hh-sandbox-adapter.js` | единственная реализация адаптера: транспорт + нормализация исходов |
| `src/provider-emulator/index.js` | эмулятор провайдера: шесть управляемых сбоев, эффект на диске, подписанные вызовы |
| `src/http/server.js` | versioned HTTP-фасад Gate |
| `scripts/sandbox/integration-gate-sandbox.cjs` | сценарий этапа: одна команда, PASS/FAIL, воспроизводимый transcript |
| `tests/integration-gate.test.js` | 19 проверок контракта и приёмки |

Запуск:

```bash
npm run check                 # синтаксис всех модулей
npm test                      # 19 проверок, включая приёмку P25
npm run sandbox               # сценарий этапа → docs/evidence/p25-integration-gate
npm run evidence:verify       # сверка transcript с закоммиченным (детерминизм)
```

Песочница живёт в `.sandbox/p25-<pid>/` (в `.gitignore`), наружу не ходит: только loopback. Часы виртуальные, поэтому transcript воспроизводится побайтово.

## Шесть сценариев внешнего сервиса

| Сценарий | Что делает эмулятор | Ожидаемый исход | Почему это важно |
|---|---|---|---|
| `success` | применяет эффект, выдаёт квитанцию, один подписанный обратный вызов | `result` + `externalOperationRef`, доставка `applied=1` | базовая линия внешнего действия |
| `error` | отвечает ошибкой сервиса, эффекта нет | `failed PROVIDER_ERROR`, счётчик эффектов `0` | «ок» не рождается без эффекта |
| `delay` | применяет эффект, не выдаёт квитанцию вовремя (900 мс), квитанция приходит позже | клиент фиксирует таймаут → `outcome_unknown`; reconcile по `operationId` возвращает ту же квитанцию; повтор до reconcile отклоняется | AC-151 / INV-07: неизвестный исход не лечится слепым повтором |
| `auth_expiry` | отвергает выдачу: «expired» | `blocked PROVIDER_AUTH_EXPIRED` человеческим текстом; readiness `authHealth=expired` | истёкшая выдача — это «обновите доступ», а не машинный шум |
| `duplicate_callback` | доставляет обратный вызов трижды: два с тем же `callbackId`, один с новым | `applied=1`, `duplicatesIgnored=2`, одно событие в outbox | «один эффект на операцию» даже при ретраях вебхука |
| `unreachable` | сервис не отвечает | `failed PROVIDER_UNREACHABLE` | таймаут и недоступность различаются |

Второй слой истечения выдачи проверяется отдельно: у провайдера — `auth_expiry`, у хоста — истёкший binding (`BINDING_EXPIRED`, отказ до обращения к провайдеру).

## Что песочница доказывает и что не доказывает

Проверяется по-настоящему:

- два настоящих HTTP-сервера (фасад Gate и эмулятор провайдера) и нормализованный исход вместо транспортных полей;
- внешний эффект на диске изолированного root и его сверка с квитанцией;
- `profileId` / `userTaskId` / `runId` / `operationId` / `externalOperationRef` / event ids не теряются ни в ответе, ни в логе, ни в событии outbox;
- ровно один эффект и одно событие при повторе по `operationId` и при дубликатах обратных вызовов;
- границы доверия: значения binding'ов и секреты подписи не попадают в окружение, аргументы, ответ, лог и evidence; чужой profile отклоняется до провайдера на всех маршрутах;
- повтор мутации с неизвестным исходом отклоняется до reconcile, а после reconcile возвращает ту же квитанцию.

Не проверяется (нужен живой провайдер):

- настоящая авторизация внешнего сервиса, срок жизни токена и refresh;
- реальные квоты и rate limits;
- реальная доставка webhook вместо эмуляторного вызова;
- реальные данные воронки вместо очищенного сэмпла.

Это зафиксировано в `fidelity` каждого прогона (`mode: emulator`, `liveSandbox: unsupported`, `liveSmoke.performed: false`) и не даёт зелёному прогону эмулятора выдать себя за живой тест.

### Живой test-account read

У внешнего сервиса нет provider sandbox, поэтому read-операция эмулируется, а production-тестирование **не ожидается**. Заявка на живой smoke оформлена явно:

```
requestLiveSmoke({ bindingNames: [] })
  → { attempted: true, performed: false, blockedBy: 'NO_TEST_ACCOUNT_BINDING',
      missingBindings: ['EXTERNAL_TEST_ACCOUNT_TOKEN', 'EXTERNAL_TEST_ACCOUNT_ID'] }
```

Значения живых учётных данных в репозитории нет и не будет — только имена переменных.

## Выделение: порядок и что сделано в этой карточке

Порядок из EXTERNAL-INTEGRATION-GATE («Выделение») соблюдён по шагам:

1. **C10, normalized outcome и adapter versioning зафиксированы** — `src/contract/`: версии контракта и протокола адаптера, общий с P15 словарь исходов и кодов.
2. **Inventory transport parts** — один транспорт (HTTP) и один адаптер; второй транспортной реализации в репозитории нет.
3. **Адаптер с прежним facade** — фасад Gate (`src/http/server.js`) и адаптер (`src/adapters/`) разделены: фасад принимает вызов и берёт principal из binding, адаптер переводит его в транспорт провайдера. Одинаковые fixtures прогнаны по обоим путям (`read и poll возвращают одинаковый нормализованный результат`).
4. **Auth expiry, webhook duplicates, provider timeout и outcome unknown проверены** — шесть сценариев выше.
5. **Общие task/error events подключены** — исходящий outbox с дедупом по eventId; старая transport реализация удалена не была, потому что в этом репозитории её ещё не существовало: выделение идёт в чистый Gate, а не из существующего кода. Перенос существующего транспорта из доменных репозиториев — работа P26.

## Границы, которые песочница НЕ заявляет

- **OS-изоляция не доказана.** Фасад и эмулятор работают под одним хостом и одним UID: `isolation=same_host_not_os_isolated`. Границей приёмки остаются host-side scoped bindings.
- **Токены песочницы — синтетические фикстуры.** Это доказательство канала доставки, а не секреты.
- **Gate не маршрутизирует Job types и не запускает Runner.** `createsGtd=false` во всех событиях outbox; задача из независимого события создаётся только по явной binding policy.
- **Эмулятор — не первый реальный домен.** Выбор домена остаётся за владельцем (эпик #22, карточка P26).
- **Карточки P16–P21, #124/#125 не трогаются** — чужие треки.

## Операционный контур

| Класс | Имя | Где живёт значение | Владелец | Как получить | Ротация |
|---|---|---|---|---|---|
| токен хоста (внутренний API) | `SANDBOX_GATE_HOST_TOKEN` | память процесса-фасада | владелец песочницы | генерируется на каждый прогон | вместе с прогоном |
| секрет подписи обратных вызовов | `whsub_*` (на подписку) | `${dataRoot}/subscriptions/<hash>.secret`, 0600 | владелец песочницы | выдаётся при подписке | вместе с прогоном |
| credential binding'и | `sbx/hh#read`, `sbx/hh#write` | `${dataRoot}/bindings/<sha256(ref)>.value`, 0600 | владелец песочницы | синтетическая фикстура, пишет хост | вместе с прогоном |
| живой test account | `EXTERNAL_TEST_ACCOUNT_TOKEN`, `EXTERNAL_TEST_ACCOUNT_ID` | GCP Secret Manager или GitHub Actions secrets | владелец | **не выданы**, живой smoke заблокирован | не задана (binding не создан) |

Значения binding'ов не попадают в окружение процесса, аргументы, ответ, лог и evidence: их читает host-owned резолвер. Это проверяется тестом на потоке отказа и вычисткой в `src/contract/events.js`.

## Проверка

```bash
npm run check
npm test                 # 19 проверок
npm run sandbox          # 13 сценариев, 80 проверок
npm run evidence:verify  # детерминизм transcript
```

Evidence: `docs/evidence/p25-integration-gate/transcript.json` + `transcript.sha256` (sanitized: без значений binding'ов, секретов, портов и путей хоста).
