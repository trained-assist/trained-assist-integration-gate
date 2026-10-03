'use strict';

// Порт передачи события в Input (C01).
//
// Gate не маршрутизирует Job types и не запускает Runner: независимое событие
// создаёт задачу только по явной binding policy, а callback существующей
// операции обновляет её состояние и вторую задачу не создаёт (AC-253). GTD по
// факту внешнего события не создаётся никогда.

/**
 * @param {object} options
 * @param {object} [options.log] общий event log Gate
 * @param {() => Date} [options.now]
 */
function createTaskPort({ log, now = () => new Date() } = {}) {
  const created = [];

  /**
   * @param {object} request
   * @param {string} request.userTaskId новый userTaskId задачи
   * @param {string|null} request.profileId
   * @param {object|null} request.replyContext
   * @param {object} request.causation eventId/operationId, из которых задача родилась
   * @param {string} [request.gtdId] только при зарегистрированном контроле
   */
  function submit({ userTaskId, profileId = null, replyContext = null, causation = null, gtdId = null }) {
    const task = {
      userTaskId: String(userTaskId),
      profileId,
      replyContext,
      causation,
      gtdId: gtdId || null,
      createsGtd: false,
      submittedAt: now().toISOString(),
    };
    created.push(task);
    log?.write('task.submitted', {
      userTaskId: task.userTaskId,
      profileId,
      causationEventId: causation && causation.eventId ? causation.eventId : null,
      causationOperationId: causation && causation.operationId ? causation.operationId : null,
      from: 'event',
      to: 'input',
      reasonCode: 'TASK_SUBMITTED_BY_BINDING_POLICY',
      detail: 'a new task is created only by an explicit binding policy; a callback of an existing operation never creates one',
    });
    return task;
  }

  return { submit, created, count: () => created.length };
}

module.exports = { createTaskPort };
