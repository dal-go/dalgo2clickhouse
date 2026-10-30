#!/usr/bin/env bash
set -euo pipefail
if [[ "$(uname -s)" != Linux ]]; then
  echo 'ClickHouse integration requires the approved Linux environment.' >&2
  exit 1
fi
root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$root"
fixture="dalgo-clickhouse-${GITHUB_RUN_ID:-local}-${GITHUB_RUN_ATTEMPT:-0}-$$"
umask 077
diagnostics=$(mktemp -d /tmp/dalgo-clickhouse-diagnostics.XXXXXX)

# Each command has a deadline and a 2 MiB per-file ceiling. Diagnostics never
# inspect environment/configuration or print process arguments. Excerpts are
# capped at 60 lines of 1024 bytes and omit likely credential/stack-dump lines.
capture() {
  local destination=$1
  shift
  (ulimit -f 2048; timeout --kill-after=1s 5s "$@" >"$destination" 2>&1)
}
excerpt() {
  [[ -f "$1" ]] || return 0
  tail -n 60 "$1" | LC_ALL=C awk '
    {
      if (tolower($0) ~ /password|passwd|authorization|access_token|secret|stack trace|stacktrace/) {
        print "[diagnostic line redacted]"
      } else {
        print substr($0, 1, 1024)
      }
    }' >&2
}
diagnostic() {
  local name=$1
  shift
  echo "--- ClickHouse fixture: $name ---" >&2
  if ! capture "$diagnostics/$name" "$@"; then
    echo 'Diagnostic command failed or reached its output/time limit.' >&2
  fi
  excerpt "$diagnostics/$name"
}
failure_diagnostics() {
  diagnostic state docker inspect --format 'status={{.State.Status}} running={{.State.Running}} exit={{.State.ExitCode}} oom={{.State.OOMKilled}} error={{.State.Error}}' "$fixture"
  diagnostic stats docker stats --no-stream --format 'memory={{.MemUsage}} memory_percent={{.MemPerc}} pids={{.PIDs}} cpu={{.CPUPerc}}' "$fixture"
  diagnostic threads docker top "$fixture" -eo pid,ppid,nlwp,comm
  echo '--- ClickHouse fixture: last readiness stderr ---' >&2
  excerpt "$diagnostics/readiness.err"
  echo '--- ClickHouse fixture: last readiness stdout ---' >&2
  excerpt "$diagnostics/readiness.out"
  diagnostic entrypoint docker logs --tail 60 "$fixture"
  echo '--- ClickHouse fixture: server error log ---' >&2
  if capture "$diagnostics/server-error-tail" docker exec "$fixture" tail -n 60 /var/log/clickhouse-server/clickhouse-server.err.log; then
    excerpt "$diagnostics/server-error-tail"
  else
    # docker exec may itself be refused at the PID ceiling. docker cp uses
    # the daemon and also works after the server has exited; copy only the
    # error log, never the giant trace log or configuration tree.
    excerpt "$diagnostics/server-error-tail"
    if ! capture "$diagnostics/server-error-copy-status" docker cp "$fixture:/var/log/clickhouse-server/clickhouse-server.err.log" "$diagnostics/server-error.log"; then
      echo 'Error-log copy failed or was incomplete at the 2 MiB/5s limit.' >&2
      excerpt "$diagnostics/server-error-copy-status"
    fi
    excerpt "$diagnostics/server-error.log"
  fi
}
cleanup() {
  local status=$?
  trap - EXIT INT TERM
  if [[ "$status" != 0 ]]; then failure_diagnostics || true; fi
  if ! timeout --kill-after=2s 10s docker rm -f "$fixture" >/dev/null 2>&1; then
    echo 'ClickHouse fixture container cleanup failed.' >&2
    status=1
  fi
  rm -rf -- "$diagnostics"
  exit "$status"
}
trap cleanup EXIT INT TERM
# Exact published ClickHouse image repository, version and verified index digest.
docker run --detach --name "$fixture" --memory=2g --memory-swap=2g --cpus=2 --pids-limit=128 \
  --publish 127.0.0.1::9000 --publish 127.0.0.1::8123 --env CLICKHOUSE_SKIP_USER_SETUP=1 \
  --mount "type=bind,source=$root/testdata/init.sql,target=/docker-entrypoint-initdb.d/01-fixture.sql,readonly" \
  --mount "type=bind,source=$root/testdata/access.xml,target=/etc/clickhouse-server/config.d/adapter-access.xml,readonly" \
  clickhouse/clickhouse-server:26.10.3.7@sha256:739e099abd1157390960a59e4532d21781d8fde57e6e4151cb920ea02c30b1ce >/dev/null
ready=false
for attempt in {1..60}; do
  if (ulimit -f 2048; timeout --kill-after=1s 2s docker exec "$fixture" clickhouse-client --query "SELECT count() FROM explore.events" >"$diagnostics/readiness.out" 2>"$diagnostics/readiness.err") && grep -qx 3 "$diagnostics/readiness.out"; then ready=true; break; fi
  sleep 1
done
[[ "$ready" == true ]] || { echo 'ClickHouse synthetic fixture did not initialize' >&2; exit 1; }
mapping=$(docker port "$fixture" 9000/tcp)
[[ "$mapping" =~ ^127\.0\.0\.1:[0-9]+$ ]] || exit 1
export CLICKHOUSE_TEST_ADDRESS="$mapping"
http_mapping=$(docker port "$fixture" 8123/tcp)
[[ "$http_mapping" =~ ^127\.0\.0\.1:[0-9]+$ ]] || exit 1
export CLICKHOUSE_TEST_HTTP_ADDRESS="$http_mapping"
go test -race -tags=integration -count=1 -timeout=3m ./...
