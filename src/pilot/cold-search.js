'use strict';

// Пилот карточки P26: существующий cron холодного поиска → срабатывания
// расписания (эпик E6 #22, этап I08).
//
// Граница модуля — важная часть приёмки:
//
//   1. Маппинг НЕ пересчитывает cron. Пять полей и часовой пояс приходят из
//      существующего доменного кода (`hh-cold-search-cron.js`: один cron-job на
//      вакансию, `Europe/Moscow`, смещение по вакансии). Gate только принимает
//      декларацию как данные и переводит её в запрос расписания P22. Второго
//      генератора cron-выражений здесь нет и не заводится.
//
//   2. Срабатывания принадлежат расписанию P22 (`scheduleId`, `occurrenceId`,
//      `occurrenceKey`, `userTaskId`, `scheduledFor`). Gate их не выводит: он
//      принимает готовое occurrence и исполняет его. Дедуп по
//      `(scheduleId, occurrenceKey)` живёт в расписании; здесь — только
//      идемпотентная запись результата, чтобы повторная доставка того же
//      occurrence не сделала второй запрос к провайдеру и не создала вторую
//      задачу.
//
//   3. Данные интеграции возвращаются через общий task flow: задача
//      подаётся тем же контрактом, что у задачи расписания P22
//      (`OccurrenceSubmitter`), а результат поиска уходит в терминальный
//      результат задачи. Отдельного канала доставки (уведомление, вебхук,
//      событие outbox) пилот не создаёт: холодные уведомления в домене уже
//      закрыты, результат живёт на странице/в задаче.
//
//   4. Почасовая поддержка НЕ выдаётся за production enabled: пилот объявляет
//      `hourlyProductionEnabled: false`, а запрос на прод отказывает до
//      настройки. Триггер продакшна (`tick`) — решение владельца (P22).

const fs = require('fs');
const path = require('path');

const OCCURRENCE_FILE = 'occurrences.jsonl';
const DEFAULT_MAX_ATTEMPTS = 2;

const CRON_FIELDS = 5;

function appendLine(file, line) {
  fs.mkdirSync(path.dirname(file), { recursive: true, mode: 0o700 });
  fs.appendFileSync(file, `${JSON.stringify(line)}\n`, { mode: 0o600 });
}

function readLines(file) {
  if (!fs.existsSync(file)) return [];
  return fs.readFileSync(file, 'utf8')
    .split('\n')
    .filter(line => line.trim().length > 0)
    .map(line => {
      try {
        return JSON.parse(line);
      } catch {
        return null;
      }
    })
    .filter(Boolean);
}

function isNonEmptyString(value) {
  return typeof value === 'string' && value.trim().length > 0;
}

function isValidTimezone(timezone) {
  if (!isNonEmptyString(timezone)) return false;
  try {
    new Intl.DateTimeFormat('en-US', { timeZone: timezone });
    return true;
  } catch {
    return false;
  }
}

function isValidCron(cron) {
  if (!isNonEmptyString(cron)) return false;
  const fields = cron.trim().split(/\s+/);
  return fields.length === CRON_FIELDS && fields.every(field => field.length > 0);
}

function occurrenceLedgerKey(scheduleId, occurrenceKey) {
  return `${scheduleId}|${occurrenceKey}`;
}

/**
 * @param {object} options
 * @param {object} options.gate ядро Gate (invoke)
 * @param {string} options.dataRoot изолированный корень пилота
 * @param {() => Date} [options.now]
 * @param {object} [options.log] общий event log Gate
 * @param {object} options.taskFlow общий task flow: { submit, complete }
 * @param {number} [options.maxAttempts] ограничение попыток на occurrence
 */
function createColdSearchPilot({ gate, dataRoot, now = () => new Date(), log, taskFlow, maxAttempts = DEFAULT_MAX_ATTEMPTS } = {}) {
  if (!gate) throw new Error('cold search pilot requires the gate core');
  if (!dataRoot) throw new Error('cold search pilot requires an isolated dataRoot');
  if (!taskFlow || typeof taskFlow.submit !== 'function' || typeof taskFlow.complete !== 'function') {
    throw new Error('cold search pilot requires the common task flow (submit + complete)');
  }

  const file = path.join(dataRoot, 'pilot', OCCURRENCE_FILE);
  const ledger = new Map(readLines(file).map(entry => [entry.ledgerKey, entry]));

  function persist(entry) {
    appendLine(file, entry);
    ledger.set(entry.ledgerKey, entry);
  }

  function status() {
    return {
      mode: 'sandbox',
      provider: 'hh',
      hourlyMapped: true,
      hourlyProductionEnabled: false,
      prodTriggerConfigured: false,
      liveTaskFlowWired: false,
      webhookSupported: false,
      eventStreamSupported: false,
      taskFlow: 'common_task_flow',
      deliveryChannel: 'task_result',
      gtdCreated: false,
      liveSmoke: { performed: false, blockedBy: 'NO_TEST_ACCOUNT_BINDING' },
    };
  }

  /**
   * Существующая декларация cron холодного поиска → запрос расписания P22.
   *
   * Поля ответа — ровно контракт `POST /schedules` расписания (P22): ничего
   * своего пилот не добавляет. `provenance` — откуда пришла декларация.
   */
  function declareSchedule({ profileId, vacancyId, cron, timezone, intervalHours, goal, destinationId = null, enabled = true, production = false } = {}) {
    if (production === true) {
      log?.write('schedule.production_refused', {
        profileId,
        vacancyId,
        from: 'requested',
        to: 'blocked',
        reasonCode: 'PILOT_PRODUCTION_NOT_ENABLED',
        detail: 'hourly cold search is not production enabled; the owner decision on the prod trigger is still pending',
      });
      return {
        outcome: 'blocked',
        code: 'PILOT_PRODUCTION_NOT_ENABLED',
        detail: 'hourly cold search is not production enabled; the owner decision on the prod trigger is still pending',
        scheduleRequest: null,
        provenance: null,
        pilotStatus: status(),
      };
    }

    const invalid = [];
    if (!isNonEmptyString(profileId)) invalid.push('profileId');
    if (!isNonEmptyString(vacancyId)) invalid.push('vacancyId');
    if (!isValidCron(cron)) invalid.push('cron (five fields, as declared by the domain cron)');
    if (!isValidTimezone(timezone)) invalid.push('timezone (IANA)');
    if (typeof intervalHours !== 'number' || !(intervalHours > 0)) invalid.push('intervalHours');
    if (!isNonEmptyString(goal)) invalid.push('goal');
    if (invalid.length > 0) {
      const detail = `the cold search cron declaration is invalid: ${invalid.join(', ')}`;
      log?.write('schedule.declaration_rejected', {
        profileId,
        vacancyId,
        from: 'requested',
        to: 'blocked',
        reasonCode: 'CAPABILITY_PAYLOAD_INVALID',
        detail,
      });
      return { outcome: 'blocked', code: 'CAPABILITY_PAYLOAD_INVALID', detail, scheduleRequest: null, provenance: null, pilotStatus: status() };
    }

    const scheduleRequest = {
      profileId,
      cron,
      timezone,
      goal,
      projectId: null,
      conversationId: null,
      audienceId: null,
      destinationId,
      overlapPolicy: 'skip',
      catchUpPolicy: 'coalesce',
      maxAdmitAttempts: 5,
      enabled: enabled !== false,
    };

    log?.write('schedule.mapped', {
      profileId,
      vacancyId,
      cron,
      timezone,
      intervalHours,
      from: 'cold_search_cron',
      to: 'schedule_occurrence_request',
      reasonCode: 'COLD_SEARCH_CRON_MAPPED',
      detail: 'the existing cold search cron declaration is mapped onto a schedule request; the cron expression itself is not recomputed here',
    });

    return {
      outcome: 'result',
      code: null,
      detail: null,
      scheduleRequest,
      provenance: {
        sourceAction: 'hh_proactive_search',
        sourceJobName: `cold-search:${vacancyId}`,
        requestedIntervalHours: intervalHours,
        effectiveIntervalHours: intervalHours,
        intervalSource: 'domain_cron_declaration',
      },
      pilotStatus: status(),
    };
  }

  /**
   * Исполнение одного occurrence: задача через общий task flow, поиск через
   * Gate, результат — в терминальный результат задачи.
   */
  async function runOccurrence({ occurrence, integrationBindingId, query, area, page = 0, perPage = 50, deadlineMs = null, replyContext = null } = {}) {
    const invalid = [];
    if (!occurrence || typeof occurrence !== 'object') invalid.push('occurrence');
    else {
      for (const field of ['occurrenceId', 'scheduleId', 'occurrenceKey', 'userTaskId', 'profileId']) {
        if (!isNonEmptyString(occurrence[field])) invalid.push(`occurrence.${field}`);
      }
      if (typeof occurrence.scheduledFor !== 'number') invalid.push('occurrence.scheduledFor');
    }
    if (!isNonEmptyString(integrationBindingId)) invalid.push('integrationBindingId');
    if (invalid.length > 0) {
      const detail = `the schedule occurrence is invalid: ${invalid.join(', ')}`;
      log?.write('pilot.occurrence_rejected', {
        from: 'requested',
        to: 'blocked',
        reasonCode: 'SCHEDULE_OCCURRENCE_INVALID',
        detail,
      });
      return { outcome: 'blocked', code: 'SCHEDULE_OCCURRENCE_INVALID', detail, deduplicated: false, userTaskId: null, runId: null, operationId: null };
    }

    const { occurrenceId, scheduleId, occurrenceKey, userTaskId, profileId, scheduledFor } = occurrence;
    const ledgerKey = occurrenceLedgerKey(scheduleId, occurrenceKey);
    const existing = ledger.get(ledgerKey) || null;

    if (existing && existing.terminal) {
      log?.write('pilot.occurrence_deduplicated', {
        scheduleId,
        occurrenceKey,
        occurrenceId,
        userTaskId: existing.userTaskId,
        runId: existing.runId,
        operationId: existing.operationId,
        from: 'requested',
        to: 'deduplicated',
        reasonCode: 'OCCURRENCE_ALREADY_DELIVERED',
        detail: 'the same occurrence was already delivered; no second task and no second provider read are made',
      });
      return {
        outcome: existing.outcome,
        code: existing.code || null,
        detail: existing.detail || null,
        deduplicated: true,
        userTaskId: existing.userTaskId,
        runId: existing.runId,
        operationId: existing.operationId,
        attempts: existing.attempts,
      };
    }

    const attempts = existing ? existing.attempts + 1 : 1;
    if (attempts > maxAttempts) {
      log?.write('pilot.occurrence_failed', {
        scheduleId,
        occurrenceKey,
        occurrenceId,
        userTaskId,
        from: 'requested',
        to: 'failed',
        reasonCode: 'ADMIT_ATTEMPTS_EXHAUSTED',
        detail: 'the occurrence reached the attempt limit; no further retry is scheduled by the pilot',
      });
      return { outcome: 'failed', code: 'ADMIT_ATTEMPTS_EXHAUSTED', detail: 'the occurrence reached the attempt limit', deduplicated: false, userTaskId, runId: null, operationId: null, attempts };
    }

    let submission;
    try {
      submission = await taskFlow.submit({
        occurrenceId,
        scheduleId,
        occurrenceKey,
        userTaskId,
        profileId,
        scheduledFor,
        source: 'schedule',
        autoRun: true,
      });
    } catch (error) {
      persist({
        ledgerKey,
        scheduleId,
        occurrenceKey,
        occurrenceId,
        userTaskId,
        runId: null,
        operationId: null,
        outcome: 'failed',
        code: 'TASK_SUBMIT_FAILED',
        detail: 'the common task flow refused the occurrence',
        attempts,
        terminal: false,
        at: now().toISOString(),
      });
      log?.write('pilot.occurrence_failed', {
        scheduleId,
        occurrenceKey,
        occurrenceId,
        userTaskId,
        from: 'requested',
        to: 'failed',
        reasonCode: 'TASK_SUBMIT_FAILED',
        detail: 'the common task flow refused the occurrence; the attempt is recorded and bounded',
      });
      return { outcome: 'failed', code: 'TASK_SUBMIT_FAILED', detail: 'the common task flow refused the occurrence', deduplicated: false, userTaskId, runId: null, operationId: null, attempts };
    }

    const runId = submission.runId || null;
    const operationId = `op_${String(userTaskId).slice(4, 20)}`;

    const read = await gate.invoke({
      integrationBindingId,
      capability: 'hh.search_resumes',
      payload: { query, area, page, perPage },
      operationId,
      profileId,
      userTaskId,
      runId,
      replyContext,
      deadlineMs,
    });

    const result = read.outcome === 'result'
      ? {
          kind: 'hh.search_resumes',
          itemCount: read.receipt ? read.receipt.itemCount : null,
          itemIds: read.receipt ? read.receipt.itemIds : [],
          page: read.receipt ? read.receipt.page : null,
          privateDetailsRef: read.receipt ? read.receipt.privateDetailsRef : null,
          operationId,
          externalOperationRef: read.externalOperationRef || null,
        }
      : { kind: 'hh.search_resumes', itemCount: null, code: read.code, safeSummary: read.safeSummary || null, operationId };

    await taskFlow.complete({ userTaskId, runId, outcome: read.outcome, result, code: read.code, detail: read.detail });

    // Задача принята общим task flow → occurrence терминален: исход провайдера
    // живёт в результате задачи, а не в новой попытке. Нетерминальным остаётся
    // только отказ самого task flow (`submit_failed`) — он ограничен
    // `maxAttempts`, как `max_admit_attempts` у расписания P22.
    const terminal = true;
    persist({
      ledgerKey,
      scheduleId,
      occurrenceKey,
      occurrenceId,
      userTaskId,
      runId,
      operationId,
      outcome: read.outcome,
      code: read.code || null,
      detail: read.detail || null,
      attempts,
      terminal,
      at: now().toISOString(),
    });

    log?.write(terminal ? 'pilot.occurrence_completed' : 'pilot.occurrence_failed', {
      scheduleId,
      occurrenceKey,
      occurrenceId,
      userTaskId,
      runId,
      operationId,
      profileId,
      outcome: read.outcome,
      code: read.code || null,
      itemCount: result.itemCount === undefined ? null : result.itemCount,
      providerAttempts: read.attempts || null,
      from: 'occurrence_due',
      to: terminal ? 'task_result' : 'failed',
      reasonCode: read.code || 'OCCURRENCE_DELIVERED',
      detail: terminal
        ? 'the integration data is returned as the terminal result of the task; no separate delivery channel is created'
        : 'the provider refused the read; the failure is returned as the task result and the attempt is bounded',
    });

    return {
      outcome: read.outcome,
      code: read.code || null,
      detail: read.detail || null,
      deduplicated: false,
      userTaskId,
      runId,
      operationId,
      attempts,
      providerAttempts: read.attempts || null,
      result,
    };
  }

  return {
    status,
    declareSchedule,
    runOccurrence,
    occurrences: () => readLines(file),
    maxAttempts,
  };
}

module.exports = { createColdSearchPilot, DEFAULT_MAX_ATTEMPTS };