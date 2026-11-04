#!/usr/bin/env bash
# Exercise real independent central and remote hosts on a container network.
set -euo pipefail
runtime="${1:-docker}"
image="${2:-liveengine:local}"
run_id="liveengine-test-$$"
secrets_dir="$(mktemp -d)"
cleanup() {
  "$runtime" rm -f "$run_id-central" "$run_id-remote" >/dev/null 2>&1 || true
  "$runtime" network rm "$run_id" >/dev/null 2>&1 || true
  rm -rf "$secrets_dir"
}
trap cleanup EXIT
umask 077
openssl rand -hex 32 > "$secrets_dir/central-token"
openssl rand -hex 32 > "$secrets_dir/remote-token"
{ printf 'LIVEENGINE_TOKEN='; cat "$secrets_dir/central-token"; printf 'LIVEENGINE_ALLOWED_REMOTE_HOSTS=remote\n'; } > "$secrets_dir/central.env"
{ printf 'LIVEENGINE_TOKEN='; cat "$secrets_dir/remote-token"; } > "$secrets_dir/remote.env"
"$runtime" network create "$run_id" >/dev/null
"$runtime" run -d --name "$run_id-remote" --network "$run_id" --network-alias remote --env-file "$secrets_dir/remote.env" -p 127.0.0.1::8787 "$image" >/dev/null
"$runtime" run -d --name "$run_id-central" --network "$run_id" --env-file "$secrets_dir/central.env" -p 127.0.0.1::8787 "$image" >/dev/null
central_port="$("$runtime" port "$run_id-central" 8787/tcp | head -1 | awk -F: '{print $NF}')"
remote_port="$("$runtime" port "$run_id-remote" 8787/tcp | head -1 | awk -F: '{print $NF}')"
for port in "$central_port" "$remote_port"; do
  ready=false
  for attempt in $(seq 1 60); do
    if curl -fsS "http://127.0.0.1:$port/health" >/dev/null; then ready=true; break; fi
    sleep 0.5
  done
  if [ "$ready" != true ]; then "$runtime" logs "$run_id-central"; "$runtime" logs "$run_id-remote"; exit 1; fi
done
node scripts/test-container-hubs.js "$central_port" "$remote_port" "$secrets_dir"
