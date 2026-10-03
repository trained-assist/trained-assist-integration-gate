'use strict';

// Эмулятор HH в форме настоящего API провайдера (карточка P26, эпик E6 #22).
//
// Первый реальный домен пилота. Отличие от эмулятора P25: здесь воспроизводится
// контракт НАСТОЯЩЕГО бесплатного HH API — `GET /resumes` с `text`/`area`/`page`/
// `per_page`/`order_by`, обязательными `Authorization: Bearer` и
// `User-Agent`/`HH-User-Agent`, телом ошибки `{message, code}` и обновлением
// выдачи через `POST /token`.
//
// Это ЭМУЛЯТОР, а не живой тест: режим HH test account не зафиксирован
// (AC-30, блокер этапа I08), поэтому read-only сценарий идёт против очищенного
// синтетического сэмпла. `fidelity.liveSmoke.performed` остаётся false, и
// зелёный прогон эмулятора не выдаётся за production enabled.
//
// Свойства, которые эмулируются: квота и rate limit (429), ответ 5xx,
// отказ авторизации (401) и обновление выдачи, а также невалидная полезная
// нагрузка (200 без `items`). Чего эмулировать НЕ нужно: HH не имеет вебхуков
// и потока событий, поэтому таких маршрутов здесь просто нет — адаптер не может
// получить от эмулятора фиктивную подписку.

const crypto = require('crypto');
const fs = require('fs');
const http = require('http');
const path = require('path');

const { requestLiveSmoke: liveSmokeRequest } = require('../contract/live-smoke');

const PROVIDER_NAME = 'hh';
const ARTIFACT_DIR = 'artifacts';
const TOKEN_DIR = 'tokens';

const FAULT_MODES = ['ok', 'rate_limited', 'server_error', 'unauthorized', 'invalid_items', 'unreachable'];

// Очищенный синтетический сэмпл: персональных данных нет — имена помечены как
// сэмпл, идентификаторы вымышленные.
const SANITIZED_ITEMS = [
  { id: 'hh-item-1', name: 'Sanitized Candidate 1', area: { id: '1', name: 'Sanitized Area' }, schedule: 'remote' },
  { id: 'hh-item-2', name: 'Sanitized Candidate 2', area: { id: '1', name: 'Sanitized Area' }, schedule: 'remote' },
  { id: 'hh-item-3', name: 'Sanitized Candidate 3', area: { id: '113', name: 'Sanitized Area' }, schedule: 'office' },
  { id: 'hh-item-4', name: 'Sanitized Candidate 4', area: { id: '113', name: 'Sanitized Area' }, schedule: 'office' },
];

const REQUIRED_LIVE_BINDINGS = ['EXTERNAL_TEST_ACCOUNT_TOKEN', 'EXTERNAL_TEST_ACCOUNT_ID'];

function short(hash, length = 12) {
  return hash.slice(0, length);
}

function digest(...parts) {
  return crypto.createHash('sha256').update(parts.join('|')).digest('hex');
}

function issueToken(seed) {
  return `hh-access-${short(digest('access', seed), 24)}`;
}

/**
 * @param {object} options
 * @param {string} options.root изолированный корень эмулятора
 * @param {() => Date} [options.now]
 * @param {string} [options.fault] один из FAULT_MODES
 * @param {string} [options.refreshToken] значение, которым хост обновляет выдачу
 * @param {boolean} [options.listen=true] не поднимать сервер: провайдер недоступен
 */
function createHhApiEmulator({ root, now = () => new Date(), fault = 'ok', refreshToken = 'synthetic-refresh-token', listen = true } = {}) {
  if (!root) throw new Error('hh emulator requires an isolated root (never a production data root)');
  if (!FAULT_MODES.includes(fault)) throw new Error(`unknown fault "${fault}"; expected one of ${FAULT_MODES.join('|')}`);

  const artifactDir = path.join(root, ARTIFACT_DIR);
  const requestLog = [];
  const tokenLog = [];
  let reads = 0;
  let tokens = 0;
  let accessTokenSeed = 0;

  /**
   * Значение выдачи, которое принимает провайдер. В песочнице оно синтетическое и
   * выдаётся хостом: реальная выдача HH живёт в Secret Manager, а не здесь.
   */
  function acceptedAccessToken(seed = 'default') {
    accessTokenSeed += 1;
    const token = issueToken(`${seed}|${accessTokenSeed}`);
    tokenLog.push({ kind: 'access_token', seed, at: now().toISOString() });
    return token;
  }

  /** Записать приватный артефакт провайдера: сырые данные не идут в лог. */
  function writeArtifact(ref, data) {
    fs.mkdirSync(artifactDir, { recursive: true, mode: 0o700 });
    const file = path.join(artifactDir, `${short(digest('artifact', ref), 32)}.json`);
    fs.writeFileSync(file, `${JSON.stringify({ ref, data })}\n`, { mode: 0o600 });
    return file;
  }

  function hhError(status, message, extra = {}) {
    return { status, body: { message, code: status, description: null, ...extra } };
  }

  function readResumes(query, headers = {}) {
    reads += 1;
    const request = {
      at: now().toISOString(),
      method: 'GET',
      pathname: '/resumes',
      query,
      hasAuthorization: Boolean(headers.authorization),
      userAgent: headers.userAgent || null,
      hhUserAgent: headers.hhUserAgent || null,
      fault,
    };
    requestLog.push(request);

    if (fault === 'unreachable') {
      return hhError(503, 'HH resumes: service unavailable in this run');
    }
    if (!headers.authorization) {
      return hhError(401, 'HH resumes: authorization header is required');
    }
    // Настоящий HH отвечает 400 без идентификации клиента: это и проверяется.
    if (!headers.userAgent || !headers.hhUserAgent) {
      return hhError(400, 'HH resumes: User-Agent and HH-User-Agent are required');
    }

    if (fault === 'unauthorized') {
      return hhError(401, 'HH resumes: authentication failed, token expired');
    }
    if (fault === 'rate_limited') {
      return hhError(429, 'HH resumes: too many requests');
    }
    if (fault === 'server_error') {
      return hhError(503, 'HH resumes: internal error, try later');
    }
    if (fault === 'invalid_items') {
      return { status: 200, body: { items: 'not-an-array', page: { page: 0, pages: 1, per_page: 50, found: 0 } } };
    }

    const items = SANITIZED_ITEMS;
    return {
      status: 200,
      body: {
        items,
        page: { page: Number((query && query.page) || 0), pages: 1, per_page: Number((query && query.per_page) || 50), found: items.length },
        source: 'sanitized-sample',
        containsPersonalData: false,
      },
    };
  }

  function refreshAccess({ grant_type, refresh_token: presented } = {}) {
    if (grant_type !== 'refresh_token' || String(presented || '') !== String(refreshToken)) {
      tokenLog.push({ kind: 'refresh_rejected', at: now().toISOString() });
      return hhError(401, 'HH token: the refresh token was rejected');
    }
    tokens += 1;
    const token = acceptedAccessToken('refreshed');
    tokenLog.push({ kind: 'refresh_granted', at: now().toISOString() });
    return { status: 200, body: { access_token: token, token_type: 'bearer', expires_in: 7200 } };
  }

  const routeTable = [
    { method: 'GET', pattern: /^\/resumes$/, handler: (params, headers) => readResumes(params, headers) },
    { method: 'POST', pattern: /^\/token$/, handler: params => refreshAccess(params) },
  ];

  const server = http.createServer((request, response) => {
    const chunks = [];
    request.on('data', chunk => chunks.push(chunk));
    request.on('end', () => {
      const url = new URL(request.url, 'http://127.0.0.1');
      const route = routeTable.find(entry => entry.method === request.method && entry.pattern.test(url.pathname));
      const rawBody = Buffer.concat(chunks).toString('utf8');
      if (!route) {
        response.writeHead(404, { 'content-type': 'application/json' });
        response.end(JSON.stringify({ message: `no route ${request.method} ${url.pathname}`, code: 404 }));
        return;
      }
      const params = Object.fromEntries(url.searchParams.entries());
      if (rawBody.length > 0) {
        try {
          Object.assign(params, JSON.parse(rawBody));
        } catch {
          response.writeHead(400, { 'content-type': 'application/json' });
          response.end(JSON.stringify({ message: 'HH: request body is not valid JSON', code: 400 }));
          return;
        }
      }
      const headers = {
        authorization: request.headers.authorization || null,
        userAgent: request.headers['user-agent'] || null,
        hhUserAgent: request.headers['hh-user-agent'] || null,
      };
      const result = route.handler(params, headers);
      response.writeHead(result.status, { 'content-type': 'application/json' });
      response.end(JSON.stringify(result.body));
    });
  });

  let port = 0;

  async function start() {
    if (!listen) return { port: 0, listening: false };
    await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
    port = server.address().port;
    return { port, listening: true };
  }

  function stop() {
    return new Promise(resolve => server.close(() => resolve()));
  }

  const fidelity = {
    provider: PROVIDER_NAME,
    mode: 'emulator',
    liveSandbox: 'unsupported',
    reason: 'the HH/CRM test-account mode is not fixed yet (AC-30), so the pilot read runs against a sanitized synthetic sample instead of a live account',
    readOnly: true,
    mutationsSupported: false,
    sanitizedSample: true,
    containsPersonalData: false,
    emulatedProperties: ['quota / rate limit (429)', 'real auth lifetime and refresh (POST /token)', 'real token lifetime', 'real resume data'],
    liveSmoke: {
      performed: false,
      required: [
        'read-only /resumes call of a sandbox-owned HH application token',
        'real token lifetime and refresh behaviour instead of the synthetic fixture',
        'real free-tier quota and rate-limit responses',
        'real resume data instead of the sanitized sample (with the owner-approved account)',
      ],
      bindingNames: [...REQUIRED_LIVE_BINDINGS],
      bindingSource: 'GCP Secret Manager or GitHub Actions secrets; never a production token',
      rule: 'a green emulator run is not a live provider test — performed stays false until the owner fixes the test-account mode (AC-30)',
    },
  };

  const requestLiveSmoke = liveSmokeRequest({
    requiredBindings: REQUIRED_LIVE_BINDINGS,
    blockedByWhenPresent: 'OWNER_DECISION_REQUIRED',
    nextBlocked: 'declare these names in Secret Manager / GitHub Actions secrets and record the owner decision on the read-only test-account mode (AC-30), then run the read-only live smoke',
    nextDecision: 'bindings exist, but the read-only test-account mode is not fixed by the owner yet (AC-30); no live call is made until that decision is recorded',
  });

  return {
    root,
    fault,
    artifactDir,
    fidelity,
    requestLog,
    tokenLog,
    acceptedAccessToken,
    writeArtifact,
    readResumes,
    refreshAccess,
    requestLiveSmoke,
    start,
    stop,
    reads: () => reads,
    refreshes: () => tokens,
    get port() {
      return port;
    },
    isListening: () => listen,
  };
}

module.exports = {
  createHhApiEmulator,
  FAULT_MODES,
  PROVIDER_NAME,
  REQUIRED_LIVE_BINDINGS,
  SANITIZED_ITEMS,
  ARTIFACT_DIR,
};