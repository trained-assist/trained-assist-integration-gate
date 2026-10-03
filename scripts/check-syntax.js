'use strict';

// Синтаксис всех модулей репозитория: дешёвая проверка до тестов и сценария.

const fs = require('fs');
const path = require('path');
const { execFileSync } = require('child_process');

const ROOT = path.join(__dirname, '..');
const DIRS = ['src', 'scripts', 'tests'];

let checked = 0;
const failures = [];

for (const dir of DIRS) {
  const base = path.join(ROOT, dir);
  if (!fs.existsSync(base)) continue;
  const files = [];
  (function walk(current) {
    for (const entry of fs.readdirSync(current, { withFileTypes: true })) {
      const full = path.join(current, entry.name);
      if (entry.isDirectory()) walk(full);
      else if (entry.name.endsWith('.js')) files.push(full);
    }
  })(base);
  for (const file of files) {
    checked += 1;
    try {
      execFileSync(process.execPath, ['--check', file], { stdio: 'pipe' });
    } catch (error) {
      failures.push(`${path.relative(ROOT, file)}: ${String(error.stderr || error.message).trim()}`);
    }
  }
}

if (failures.length > 0) {
  process.stderr.write(`syntax check failed for ${failures.length} of ${checked} modules\n${failures.join('\n')}\n`);
  process.exit(1);
}
process.stdout.write(`syntax ok: ${checked} modules\n`);
