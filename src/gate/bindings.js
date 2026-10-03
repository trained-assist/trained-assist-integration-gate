'use strict';

// Credential bindings: principal берётся из binding, а не от модели (AC-252).
//
// Значение binding'а не приходит ни из аргументов вызывающего, ни из окружения
// процесса: его читает host-owned резолвер из изолированного store — ровно так,
// как Credential Broker (эпик #21, #30) читал бы его из Secret Manager. В
// песочнице там синтетическая фикстура: доказательство канала, а не секрет.
//
// Файл значения называется хешем ref, права 0600. В лог и в отказ значение не
// попадает никогда.

const crypto = require('crypto');
const fs = require('fs');
const path = require('path');

const BINDING_STORE_DIR = 'bindings';

function bindingStoreFile(storeDir, ref) {
  return path.join(storeDir, `${crypto.createHash('sha256').update(String(ref)).digest('hex').slice(0, 32)}.value`);
}

function writeBindingValue(storeDir, ref, value, { profileId = null, provider = null, scopes = [], expiresAt = null } = {}) {
  fs.mkdirSync(storeDir, { recursive: true, mode: 0o700 });
  const record = { ref: String(ref), value: String(value), profileId, provider, scopes, expiresAt, writtenAt: new Date().toISOString() };
  fs.writeFileSync(bindingStoreFile(storeDir, ref), `${JSON.stringify(record)}\n`, { mode: 0o600 });
  return bindingStoreFile(storeDir, ref);
}

function readBindingRecord(storeDir, ref) {
  const file = bindingStoreFile(storeDir, ref);
  if (!fs.existsSync(file)) return null;
  try {
    return JSON.parse(fs.readFileSync(file, 'utf8'));
  } catch {
    return null;
  }
}

/**
 * Host-owned резолвер credential binding'ов.
 *
 * @param {object} options
 * @param {string} options.storeDir изолированный каталог store
 * @param {() => Date} [options.now]
 */
function createCredentialResolver({ storeDir, now = () => new Date() } = {}) {
  if (!storeDir) throw new Error('credential resolver requires an isolated storeDir');

  /**
   * @param {object} request
   * @param {string} request.bindingRef
   * @param {string} request.scope запрашиваемый scope операции
   * @param {string|null} request.profileId профиль из доверенного envelope хоста
   * @returns {{value: string, binding: object}}
   */
  function resolve({ bindingRef, scope, profileId = null } = {}) {
    const record = readBindingRecord(storeDir, bindingRef);
    if (!record) {
      const err = new Error('credential binding not found');
      err.code = 'BINDING_NOT_FOUND';
      throw err;
    }
    if (profileId && record.profileId && record.profileId !== profileId) {
      const err = new Error('credential binding belongs to another profile; principal is taken from the binding, never from the caller');
      err.code = 'BINDING_SCOPE_MISMATCH';
      throw err;
    }
    if (scope && Array.isArray(record.scopes) && record.scopes.length > 0 && !record.scopes.includes(scope)) {
      const err = new Error('credential binding does not cover the requested scope');
      err.code = 'BINDING_SCOPE_MISMATCH';
      throw err;
    }
    if (record.expiresAt && new Date(record.expiresAt).getTime() <= now().getTime()) {
      const err = new Error('credential binding expired');
      err.code = 'BINDING_EXPIRED';
      err.reason = 'expired';
      throw err;
    }
    return { value: record.value, binding: record };
  }

  return { storeDir, resolve, read: ref => readBindingRecord(storeDir, ref) };
}

module.exports = {
  createCredentialResolver,
  writeBindingValue,
  readBindingRecord,
  bindingStoreFile,
  BINDING_STORE_DIR,
};
