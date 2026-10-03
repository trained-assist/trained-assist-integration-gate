'use strict';

// Хранилище подписок на webhook: webhookSubscriptionId → секрет подписи.
//
// Секрет выдаётся хостом при подписке и читается только хостом: обратный вызов
// подписывается им, а приёмник проверяет. Значение не попадает в лог, в ответ
// и в аргументы вызывающего.

const crypto = require('crypto');
const fs = require('fs');
const path = require('path');

const STORE_DIR = 'subscriptions';

function storeFile(storeDir, webhookSubscriptionId) {
  return path.join(storeDir, `${crypto.createHash('sha256').update(String(webhookSubscriptionId)).digest('hex').slice(0, 32)}.json`);
}

/**
 * @param {object} options
 * @param {string} options.storeDir изолированный каталог store
 * @param {() => Date} [options.now]
 */
function createSubscriptionStore({ storeDir, now = () => new Date() } = {}) {
  if (!storeDir) throw new Error('subscription store requires an isolated storeDir');

  function create({ webhookSubscriptionId, bindingRef, profileId = null, eventType, webhookUrl = null }) {
    fs.mkdirSync(storeDir, { recursive: true, mode: 0o700 });
    const secret = `whsec_${crypto.randomBytes(24).toString('hex')}`;
    const record = {
      webhookSubscriptionId: String(webhookSubscriptionId),
      bindingRef: String(bindingRef),
      profileId,
      eventType: String(eventType),
      webhookUrl,
      lifecycleState: 'active',
      createdAt: now().toISOString(),
    };
    fs.writeFileSync(storeFile(storeDir, webhookSubscriptionId), `${JSON.stringify(record)}\n`, { mode: 0o600 });
    // Секрет хранится отдельно от публичной записи: его читает только приёмник.
    fs.writeFileSync(`${storeFile(storeDir, webhookSubscriptionId)}.secret`, `${secret}\n`, { mode: 0o600 });
    return { record, secret };
  }

  function get(webhookSubscriptionId) {
    const file = storeFile(storeDir, webhookSubscriptionId);
    if (!fs.existsSync(file)) return null;
    try {
      return JSON.parse(fs.readFileSync(file, 'utf8'));
    } catch {
      return null;
    }
  }

  function secretOf(webhookSubscriptionId) {
    const file = `${storeFile(storeDir, webhookSubscriptionId)}.secret`;
    if (!fs.existsSync(file)) return undefined;
    const value = fs.readFileSync(file, 'utf8').trim();
    return value.length > 0 ? value : undefined;
  }

  function setLifecycle(webhookSubscriptionId, lifecycleState) {
    const record = get(webhookSubscriptionId);
    if (!record) return null;
    record.lifecycleState = lifecycleState;
    record.updatedAt = now().toISOString();
    fs.writeFileSync(storeFile(storeDir, webhookSubscriptionId), `${JSON.stringify(record)}\n`, { mode: 0o600 });
    return record;
  }

  function findByBinding(bindingRef) {
    if (!fs.existsSync(storeDir)) return null;
    const files = fs.readdirSync(storeDir).filter(file => file.endsWith('.json') && !file.endsWith('.secret'));
    for (const file of files) {
      try {
        const record = JSON.parse(fs.readFileSync(path.join(storeDir, file), 'utf8'));
        if (record && record.bindingRef === bindingRef && record.lifecycleState === 'active') return record;
      } catch {
        // повреждённая запись подписки не должна ронять чтение остальных
      }
    }
    return null;
  }

  return { storeDir, create, get, secretOf, setLifecycle, findByBinding };
}

module.exports = { createSubscriptionStore, STORE_DIR };
