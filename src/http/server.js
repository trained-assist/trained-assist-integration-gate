'use strict';

// HTTP-фасад Gate: versioned API поверх ядра.
//
// Фасад не содержит бизнес-правил и не дублирует адаптер: он принимает
// запрос, берёт principal из доверенного host envelope (заголовок x-gate-profile
// ставит хост, а не модель) и вызывает ядро. Значения bindings и секреты
// подписи наружу не возвращаются.

const http = require('http');

const { reportError } = require('../contract/events');

const ROUTES = [
  { method: 'POST', pattern: /^\/v1\/invoke$/, handler: 'invoke' },
  { method: 'POST', pattern: /^\/v1\/subscribe$/, handler: 'subscribe' },
  { method: 'DELETE', pattern: /^\/v1\/subscribe\/([^/]+)$/, handler: 'unsubscribe' },
  { method: 'POST', pattern: /^\/v1\/reconcile$/, handler: 'reconcile' },
  { method: 'GET', pattern: /^\/v1\/events$/, handler: 'poll' },
  { method: 'GET', pattern: /^\/v1\/capabilities$/, handler: 'capabilities' },
  { method: 'POST', pattern: /^\/v1\/callbacks$/, handler: 'callbacks' },
];

// Маршруты пилота P26: существующий cron холодного поиска → срабатывания
// расписания. Пилот не является частью контракта Gate, поэтому он подключается
// фасадом и может отсутствовать.
const PILOT_ROUTES = [
  { method: 'GET', pattern: /^\/v1\/pilot\/cold-search\/status$/, handler: 'pilot_status' },
  { method: 'POST', pattern: /^\/v1\/pilot\/cold-search\/schedules$/, handler: 'pilot_schedule' },
  { method: 'POST', pattern: /^\/v1\/pilot\/cold-search\/occurrences$/, handler: 'pilot_occurrence' },
];

function sendJson(response, status, body) {
  response.writeHead(status, { 'content-type': 'application/json' });
  response.end(JSON.stringify(body));
}

/**
 * @param {object} gate ядро Gate (src/gate/index.js)
 * @param {object} [options]
 * @param {string} [options.hostToken] токен хоста для внутренних вызовов
 * @param {object} [options.pilot] пилот карточки P26 (src/pilot/cold-search.js)
 */
function createHttpServer(gate, { hostToken = null, pilot = null } = {}) {
  const routes = pilot ? [...ROUTES, ...PILOT_ROUTES] : ROUTES;
  const server = http.createServer((request, response) => {
    const chunks = [];
    request.on('data', chunk => chunks.push(chunk));
    request.on('end', async () => {
      const url = new URL(request.url, 'http://127.0.0.1');
      if (url.pathname === '/healthz') {
        sendJson(response, 200, { status: 'ok', service: 'integration-gate', contractVersion: gate.contractVersion });
        return;
      }

      const route = routes.find(entry => entry.method === request.method && entry.pattern.test(url.pathname));
      if (!route) {
        sendJson(response, 404, { status: 'error', code: 'ROUTE_NOT_FOUND', detail: `no route ${request.method} ${url.pathname}` });
        return;
      }

      if (hostToken && request.headers['x-gate-host-token'] !== hostToken) {
        sendJson(response, 401, { status: 'error', code: 'HOST_TOKEN_INVALID', detail: 'the internal API requires a host token' });
        return;
      }

      // Principal приходит из доверенного envelope хоста, а не из аргументов модели.
      const profileId = request.headers['x-gate-profile'] || null;
      const rawBody = Buffer.concat(chunks).toString('utf8');
      const query = Object.fromEntries(url.searchParams.entries());

      try {
        if (route.handler === 'callbacks') {
          const result = gate.receiveCallback({ headers: request.headers, rawBody, provider: request.headers['x-gate-provider'] || null });
          sendJson(response, result.status, {
            receiptId: result.receipt ? result.receipt.receiptId : null,
            providerEventId: result.receipt ? result.receipt.providerEventId : null,
            applied: result.applied,
            duplicate: result.duplicate,
            reasonCode: result.reasonCode,
            detail: result.detail,
          });
          return;
        }

        if (route.handler === 'invoke') {
          const body = rawBody ? JSON.parse(rawBody) : {};
          const result = await gate.invoke({
            integrationBindingId: body.integrationBindingId,
            capability: body.capability,
            payload: body.payload,
            operationId: body.operationId || null,
            userTaskId: body.userTaskId || null,
            runId: body.runId || null,
            replyContext: body.replyContext || null,
            gtdId: body.gtdId || null,
            profileId,
            deadlineMs: body.deadlineMs || null,
          });
          sendJson(response, 200, result);
          return;
        }

        if (route.handler === 'subscribe') {
          const body = rawBody ? JSON.parse(rawBody) : {};
          const result = await gate.subscribe({
            integrationBindingId: body.integrationBindingId,
            eventType: body.eventType,
            webhookUrl: body.webhookUrl || null,
            profileId,
          });
          sendJson(response, 200, result);
          return;
        }

        if (route.handler === 'unsubscribe') {
          const result = await gate.unsubscribe({ webhookSubscriptionId: url.pathname.match(route.pattern)[1], profileId });
          sendJson(response, 200, result);
          return;
        }

        if (route.handler === 'reconcile') {
          const body = rawBody ? JSON.parse(rawBody) : {};
          const result = await gate.reconcile({ operationId: body.operationId || null, externalOperationRef: body.externalOperationRef || null, profileId });
          sendJson(response, 200, result);
          return;
        }

        if (route.handler === 'poll') {
          const result = await gate.poll({
            integrationBindingId: query.integrationBindingId,
            cursor: Number(query.cursor || 0),
            limit: Number(query.limit || 20),
            profileId,
          });
          sendJson(response, 200, result);
          return;
        }

        if (route.handler === 'capabilities') {
          const result = await gate.capabilities({
            integrationBindingId: query.integrationBindingId,
            profileId,
            probe: query.probe === 'true',
          });
          sendJson(response, 200, result);
          return;
        }

        if (route.handler === 'pilot_status') {
          sendJson(response, 200, { outcome: 'result', ...pilot.status() });
          return;
        }

        if (route.handler === 'pilot_schedule') {
          const body = rawBody ? JSON.parse(rawBody) : {};
          const result = pilot.declareSchedule({ ...body, profileId });
          sendJson(response, 200, result);
          return;
        }

        if (route.handler === 'pilot_occurrence') {
          const body = rawBody ? JSON.parse(rawBody) : {};
          const occurrenceProfileId = body.occurrence && body.occurrence.profileId ? body.occurrence.profileId : profileId;
          const result = await pilot.runOccurrence({ ...body, profileId: occurrenceProfileId });
          sendJson(response, 200, result);
          return;
        }

        sendJson(response, 500, { status: 'error', code: 'ROUTE_NOT_WIRED', detail: `route ${route.handler} is not wired` });
      } catch (error) {
        const detail = String(error && error.message ? error.message : error);
        reportError(gate.log, {
          code: 'GATE_ERROR',
          operation: route.handler,
          detail,
          profileId,
        });
        sendJson(response, 500, { status: 'error', code: 'GATE_ERROR', detail });
      }
    });
  });

  return server;
}

/**
 * Поднять фасад на loopback и вернуть базовый URL.
 */
async function startHttpServer(gate, options = {}) {
  const server = createHttpServer(gate, options);
  await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
  const { port } = server.address();
  return { server, baseUrl: `http://127.0.0.1:${port}`, stop: () => new Promise(resolve => server.close(() => resolve())) };
}

module.exports = { createHttpServer, startHttpServer };
