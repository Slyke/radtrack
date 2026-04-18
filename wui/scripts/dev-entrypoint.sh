#!/usr/bin/env bash
set -euo pipefail
umask 0002

cd /workspace

npm install --workspace wui
rm -rf wui/.svelte-kit node_modules/.vite node_modules/.vite-temp
npm exec --workspace wui svelte-kit sync
chmod -R g+rwX wui/.svelte-kit

exec npm run dev --workspace wui -- --force --host 0.0.0.0 --port "${PORT:-4096}"
